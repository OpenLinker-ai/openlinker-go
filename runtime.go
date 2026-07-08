package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type RuntimeAssignment struct {
	Type           string               `json:"type,omitempty"`
	RunID          string               `json:"run_id"`
	AgentID        string               `json:"agent_id,omitempty"`
	Input          any                  `json:"input,omitempty"`
	Metadata       any                  `json:"metadata,omitempty"`
	Source         string               `json:"source,omitempty"`
	ResultEndpoint string               `json:"result_endpoint,omitempty"`
	ResultMethod   string               `json:"result_method,omitempty"`
	ResultRequired bool                 `json:"result_required,omitempty"`
	A2A            *AgentA2AContext     `json:"a2a,omitempty"`
	Conversation   *ConversationContext `json:"conversation,omitempty"`
}

type RuntimeHandlers struct {
	OnReady    func(RuntimeWSServerMessage)
	OnAssigned func(RuntimeAssignment)
	OnMessage  func(RuntimeWSServerMessage)
	OnError    func(error)
}

type RuntimeConnector interface {
	Start(context.Context, RuntimeHandlers) error
	Stop(context.Context) error
	SupportsLiveEvents() bool
	SendRunEvent(context.Context, string, AgentEvent) error
	CompleteRun(context.Context, string, RuntimePullResultRequest) error
}

type RuntimePullConnector struct {
	Runtime     *Runtime
	Client      *Client
	Wait        time.Duration
	Heartbeat   time.Duration
	EmptyRetry  time.Duration
	MaxRuns     int
	StopOnEmpty bool

	ctx       context.Context
	cancel    context.CancelFunc
	handlers  RuntimeHandlers
	wg        sync.WaitGroup
	processed int
}

func NewRuntimePullConnector(runtime *Runtime) *RuntimePullConnector {
	return &RuntimePullConnector{Runtime: runtime}
}

func (c *RuntimePullConnector) SupportsLiveEvents() bool {
	return false
}

func (c *RuntimePullConnector) Start(ctx context.Context, handlers RuntimeHandlers) error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.Wait <= 0 {
		c.Wait = 25 * time.Second
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 60 * time.Second
	}
	if c.EmptyRetry <= 0 {
		c.EmptyRetry = 5 * time.Second
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.handlers = handlers
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.loop(); err != nil && c.ctx.Err() == nil && c.handlers.OnError != nil {
			c.handlers.OnError(err)
		}
	}()
	return nil
}

func (c *RuntimePullConnector) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (c *RuntimePullConnector) SendRunEvent(ctx context.Context, runID string, event AgentEvent) error {
	return nil
}

func (c *RuntimePullConnector) CompleteRun(ctx context.Context, runID string, result RuntimePullResultRequest) error {
	runtime := c.runtimeClient()
	if runtime == nil {
		return errors.New("openlinker: runtime pull connector requires runtime")
	}
	_, err := runtime.CompleteRuntimeRun(ctx, runID, result)
	return err
}

func (c *RuntimePullConnector) validate() error {
	runtime := c.runtimeClient()
	if runtime == nil {
		return errors.New("openlinker: runtime pull connector requires runtime")
	}
	if runtime.runtimeAuthToken() == "" {
		return errors.New("openlinker: agent token is required")
	}
	return nil
}

func (c *RuntimePullConnector) runtimeClient() *Runtime {
	if c == nil {
		return nil
	}
	if c.Runtime != nil {
		return c.Runtime
	}
	if c.Client != nil {
		return &Runtime{client: c.Client}
	}
	return nil
}

func (c *RuntimePullConnector) loop() error {
	lastHeartbeat := time.Time{}
	for c.ctx.Err() == nil && (c.MaxRuns == 0 || c.processed < c.MaxRuns) {
		if time.Since(lastHeartbeat) >= c.Heartbeat {
			_, _ = c.runtimeClient().HeartbeatAgent(c.ctx)
			lastHeartbeat = time.Now()
		}

		claimResult, err := c.runtimeClient().ClaimRuntimeRunDetailed(c.ctx, ClaimRuntimeRunParams{
			WaitSeconds: int32(c.Wait.Seconds()),
		})
		if err == nil && claimResult != nil && claimResult.Run != nil {
			if c.handlers.OnAssigned != nil {
				c.handlers.OnAssigned(RuntimeAssignmentFromPullRun(claimResult.Run))
			}
			c.processed++
			continue
		}
		if err == nil {
			if c.StopOnEmpty {
				return nil
			}
			if err := sleepContext(c.ctx, retryAfterFromClaimResult(claimResult, c.EmptyRetry)); err != nil {
				return err
			}
			continue
		}

		var sdkErr *Error
		if errors.As(err, &sdkErr) {
			if sdkErr.StatusCode == http.StatusTooManyRequests {
				if err := sleepContext(c.ctx, retryAfterFromError(sdkErr, c.EmptyRetry)); err != nil {
					return err
				}
				continue
			}
			if c.handlers.OnError != nil {
				c.handlers.OnError(fmt.Errorf("openlinker: runtime pull claim returned %d: %s", sdkErr.StatusCode, sdkErr.Message))
			}
		} else if c.handlers.OnError != nil {
			c.handlers.OnError(err)
		}
		if err := sleepContext(c.ctx, c.EmptyRetry); err != nil {
			return err
		}
	}
	return nil
}

type WebSocketDialer interface {
	DialContext(context.Context, string, http.Header) (*websocket.Conn, *http.Response, error)
}

type RuntimeWSConnector struct {
	Runtime      *Runtime
	Client       *Client
	Endpoint     string
	Reconnect    bool
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	Heartbeat    time.Duration
	Dialer       WebSocketDialer

	mu       sync.Mutex
	conn     *websocket.Conn
	handlers RuntimeHandlers
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewRuntimeWSConnector(runtime *Runtime) *RuntimeWSConnector {
	return &RuntimeWSConnector{
		Runtime:   runtime,
		Reconnect: true,
	}
}

func (c *RuntimeWSConnector) SupportsLiveEvents() bool {
	return true
}

func (c *RuntimeWSConnector) Start(ctx context.Context, handlers RuntimeHandlers) error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.ReconnectMin <= 0 {
		c.ReconnectMin = 500 * time.Millisecond
	}
	if c.ReconnectMax <= 0 {
		c.ReconnectMax = 10 * time.Second
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 60 * time.Second
	}
	if c.Dialer == nil {
		c.Dialer = websocket.DefaultDialer
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.handlers = handlers
	c.ctx, c.cancel = context.WithCancel(ctx)
	if err := c.connect(c.ctx); err != nil {
		return err
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.readLoop()
	}()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()
	return nil
}

func (c *RuntimeWSConnector) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(time.Second))
		_ = conn.Close()
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (c *RuntimeWSConnector) SendRunEvent(ctx context.Context, runID string, event AgentEvent) error {
	return c.send(ctx, RuntimeWSClientMessage{
		Type:      "run.event",
		ID:        fmt.Sprintf("event-%s-%d", runID, time.Now().UnixMilli()),
		RunID:     runID,
		EventType: event.EventType,
		Payload:   event.Payload,
	})
}

func (c *RuntimeWSConnector) CompleteRun(ctx context.Context, runID string, result RuntimePullResultRequest) error {
	return c.send(ctx, RuntimeWSClientMessage{
		Type:       "run.result",
		ID:         fmt.Sprintf("result-%s-%d", runID, time.Now().UnixMilli()),
		RunID:      runID,
		Status:     result.Status,
		Output:     result.Output,
		Events:     result.Events,
		Error:      result.Error,
		DurationMS: result.DurationMS,
	})
}

func (c *RuntimeWSConnector) validate() error {
	runtime := c.runtimeClient()
	if runtime == nil {
		return errors.New("openlinker: runtime websocket connector requires runtime")
	}
	if runtime.runtimeAuthToken() == "" {
		return errors.New("openlinker: agent token is required")
	}
	return nil
}

func (c *RuntimeWSConnector) runtimeClient() *Runtime {
	if c == nil {
		return nil
	}
	if c.Runtime != nil {
		return c.Runtime
	}
	if c.Client != nil {
		return &Runtime{client: c.Client}
	}
	return nil
}

func (c *RuntimeWSConnector) connect(ctx context.Context) error {
	endpoint := c.Endpoint
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "/agent-runtime/ws"
	}
	runtime := c.runtimeClient()
	if runtime == nil {
		return errors.New("openlinker: runtime websocket connector requires runtime")
	}
	wsURL, err := runtime.webSocketEndpoint(endpoint)
	if err != nil {
		return err
	}
	header := runtime.runtimeWebSocketHeaders()
	conn, _, err := c.Dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *RuntimeWSConnector) readLoop() {
	for {
		conn := c.currentConn()
		if conn == nil {
			return
		}
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if c.ctx.Err() == nil && c.handlers.OnError != nil {
					c.handlers.OnError(err)
				}
				break
			}
			c.handleMessage(data)
		}
		if c.ctx.Err() != nil || !c.Reconnect {
			return
		}
		delay := c.ReconnectMin
		for c.ctx.Err() == nil {
			timer := time.NewTimer(delay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := c.connect(c.ctx); err != nil {
				if c.handlers.OnError != nil {
					c.handlers.OnError(err)
				}
				delay *= 2
				if delay > c.ReconnectMax {
					delay = c.ReconnectMax
				}
				continue
			}
			break
		}
	}
}

func (c *RuntimeWSConnector) heartbeatLoop() {
	ticker := time.NewTicker(c.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			err := c.send(c.ctx, RuntimeWSClientMessage{
				Type: "heartbeat",
				ID:   fmt.Sprintf("heartbeat-%d", time.Now().UnixMilli()),
			})
			if err != nil && c.ctx.Err() == nil && c.handlers.OnError != nil {
				c.handlers.OnError(err)
			}
		}
	}
}

func (c *RuntimeWSConnector) handleMessage(data []byte) {
	var message RuntimeWSServerMessage
	if err := json.Unmarshal(data, &message); err != nil {
		if c.handlers.OnError != nil {
			c.handlers.OnError(err)
		}
		return
	}
	if c.handlers.OnMessage != nil {
		c.handlers.OnMessage(message)
	}
	switch message.Type {
	case "runtime.ready":
		if c.handlers.OnReady != nil {
			c.handlers.OnReady(message)
		}
	case "run.assigned":
		if err := c.send(c.ctx, RuntimeWSClientMessage{
			Type:  "run.assignment.accepted",
			ID:    fmt.Sprintf("assignment-ack-%s-%d", message.RunID, time.Now().UnixMilli()),
			RunID: message.RunID,
		}); err != nil {
			if c.handlers.OnError != nil {
				c.handlers.OnError(err)
			}
		}
		if c.handlers.OnAssigned != nil {
			c.handlers.OnAssigned(RuntimeAssignmentFromWSMessage(message))
		}
	case "error":
		if c.handlers.OnError != nil {
			c.handlers.OnError(runtimeMessageError(message))
		}
	default:
	}
}

func (c *RuntimeWSConnector) send(ctx context.Context, message RuntimeWSClientMessage) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("openlinker: runtime websocket is not open")
	}
	done := make(chan error, 1)
	go func(conn *websocket.Conn) {
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			done <- err
			return
		}
		done <- conn.WriteMessage(websocket.TextMessage, encoded)
	}(c.conn)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (c *RuntimeWSConnector) currentConn() *websocket.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func RuntimeAssignmentFromPullRun(run *RuntimePullRunResponse) RuntimeAssignment {
	if run == nil {
		return RuntimeAssignment{}
	}
	return RuntimeAssignment{
		Type:           "run.assigned",
		RunID:          run.RunID,
		AgentID:        run.AgentID,
		Input:          run.Input,
		Metadata:       run.Metadata,
		Source:         run.Source,
		ResultEndpoint: run.ResultEndpoint,
		ResultMethod:   run.ResultMethod,
		ResultRequired: run.ResultRequired,
		A2A:            run.A2A,
		Conversation:   run.Conversation,
	}
}

func RuntimeAssignmentFromWSMessage(message RuntimeWSServerMessage) RuntimeAssignment {
	return RuntimeAssignment{
		Type:           message.Type,
		RunID:          message.RunID,
		AgentID:        message.AgentID,
		Input:          message.Input,
		Metadata:       message.Metadata,
		Source:         message.Source,
		ResultEndpoint: message.ResultEndpoint,
		ResultMethod:   message.ResultMethod,
		ResultRequired: message.ResultRequired,
		A2A:            message.A2A,
		Conversation:   message.Conversation,
	}
}

func (c *Client) runtimeAuthToken() string {
	return c.agentToken
}

func (c *Client) runtimeWebSocketHeaders() http.Header {
	headers := make(http.Header)
	headers.Set("X-OpenLinker-SDK", c.sdkAgent)
	if token := c.runtimeAuthToken(); token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	for key, values := range c.headers {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	return headers
}

func (c *Client) webSocketEndpoint(path string) (string, error) {
	if strings.HasPrefix(path, "ws://") || strings.HasPrefix(path, "wss://") {
		return path, nil
	}
	raw := c.endpoint(path, nil)
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("openlinker: unsupported websocket scheme %q", u.Scheme)
	}
	return u.String(), nil
}

func retryAfterFromClaimResult(result *ClaimRuntimeRunResult, fallback time.Duration) time.Duration {
	if result != nil && result.RetryAfter > 0 {
		return result.RetryAfter
	}
	return fallback
}

func retryAfterFromError(err *Error, fallback time.Duration) time.Duration {
	if err != nil && err.RetryAfter > 0 {
		return err.RetryAfter
	}
	return fallback
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runtimeMessageError(message RuntimeWSServerMessage) error {
	if message.Error != nil {
		if message.Error.Code != "" {
			return fmt.Errorf("openlinker: runtime websocket error: %s: %s", message.Error.Code, message.Error.Message)
		}
		return fmt.Errorf("openlinker: runtime websocket error: %s", message.Error.Message)
	}
	return errors.New("openlinker: runtime websocket error")
}
