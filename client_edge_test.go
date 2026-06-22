package openlinker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientResourceMethodsUseHeadersAndPaths(t *testing.T) {
	type requestCall struct {
		Method string
		Path   string
		Auth   string
		SDK    string
		Custom string
		Query  url.Values
	}
	var calls []requestCall
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, requestCall{
			Method: r.Method,
			Path:   r.URL.Path,
			Auth:   r.Header.Get("Authorization"),
			SDK:    r.Header.Get("X-OpenLinker-SDK"),
			Custom: r.Header.Get("X-OpenLinker-Test"),
			Query:  r.URL.Query(),
		})
		switch r.URL.Path {
		case "/api/v1/agents/data-agent":
			writeJSON(t, w, AgentDetailResponse{MarketListItem: MarketListItem{ID: "agent-1", Slug: "data-agent"}})
		case "/api/v1/agents/data-agent/agent-card.json":
			writeJSON(t, w, AgentCardResponse{Name: "Data Agent", Version: "1.0"})
		case "/api/v1/agents/data-agent/agent-card.extended.json":
			writeJSON(t, w, AgentCardResponse{Name: "Data Agent Extended", Version: "1.0"})
		case "/api/v1/runs":
			writeJSON(t, w, RunResponse{RunID: "run-started", Status: "queued"})
		case "/api/v1/runs/run-1/events":
			writeJSON(t, w, ListRunEventsResponse{Events: []RunEventResponse{{EventID: "evt-1", Sequence: 4}}})
		case "/api/v1/runs/run-1/artifacts":
			writeJSON(t, w, ListItemsResponse[RunArtifactResponse]{Items: []RunArtifactResponse{{ID: "artifact-1", RunID: "run-1"}}})
		case "/api/v1/runs/run-1/messages":
			writeJSON(t, w, ListItemsResponse[RunMessageResponse]{Items: []RunMessageResponse{{ID: "message-1", RunID: "run-1", Role: "assistant"}}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(
		server.URL+"/api/v1/",
		WithAccessToken(" ol_live_user "),
		WithSDKAgent("openlinker-go-test"),
		WithHeader("X-OpenLinker-Test", "yes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if detail, err := client.GetAgent(ctx, "data-agent"); err != nil || detail.ID != "agent-1" {
		t.Fatalf("GetAgent = %+v err=%v", detail, err)
	}
	if card, err := client.GetAgentCard(ctx, "data-agent", false); err != nil || card.Name != "Data Agent" {
		t.Fatalf("GetAgentCard = %+v err=%v", card, err)
	}
	if card, err := client.GetAgentCard(ctx, "data-agent", true); err != nil || card.Name != "Data Agent Extended" {
		t.Fatalf("GetAgentCard extended = %+v err=%v", card, err)
	}
	if run, err := client.StartAgentRun(ctx, RunAgentRequest{AgentID: "agent-1", Input: "hello"}); err != nil || run.RunID != "run-started" {
		t.Fatalf("StartAgentRun = %+v err=%v", run, err)
	}
	if events, err := client.ListRunEvents(ctx, "run-1", ListRunEventsParams{AfterSequence: 3, Limit: 4}); err != nil || len(events.Events) != 1 {
		t.Fatalf("ListRunEvents = %+v err=%v", events, err)
	}
	if artifacts, err := client.ListRunArtifacts(ctx, "run-1"); err != nil || len(artifacts.Items) != 1 {
		t.Fatalf("ListRunArtifacts = %+v err=%v", artifacts, err)
	}
	if messages, err := client.ListRunMessages(ctx, "run-1"); err != nil || len(messages.Items) != 1 {
		t.Fatalf("ListRunMessages = %+v err=%v", messages, err)
	}

	if len(calls) != 7 {
		t.Fatalf("calls len = %d", len(calls))
	}
	for _, call := range calls {
		if call.Auth != "Bearer ol_live_user" || call.SDK != "openlinker-go-test" || call.Custom != "yes" {
			t.Fatalf("headers = %+v", call)
		}
	}
	if got := calls[4].Query.Get("after_sequence"); got != "3" {
		t.Fatalf("after_sequence = %q", got)
	}
	if got := calls[4].Query.Get("limit"); got != "4" {
		t.Fatalf("limit = %q", got)
	}
}

func TestClientEndpointAndConstructionValidation(t *testing.T) {
	for _, raw := range []string{"", "example.test", "http://[::1"} {
		if _, err := NewClient(raw); err == nil {
			t.Fatalf("NewClient(%q) succeeded", raw)
		}
	}
	client, err := NewClient("https://example.test/root/api/v1", WithHTTPClient(nil), WithAccessToken(" user "), WithRuntimeToken(" runtime "))
	if err != nil {
		t.Fatal(err)
	}
	query := url.Values{}
	query.Set("q", "hello world")
	if got := client.endpoint("/api/v1/agents", query); got != "https://example.test/root/api/v1/agents?q=hello+world" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := client.endpoint("https://other.test/custom", query); got != "https://other.test/custom?q=hello+world" {
		t.Fatalf("absolute endpoint = %q", got)
	}
	if got := client.runtimeAuthToken(); got != "runtime" {
		t.Fatalf("runtime token = %q", got)
	}
}

func TestErrorFallbackRetryAfterAndDecodeFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/runs/plain":
			w.Header().Set("X-Correlation-Id", "corr-1")
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("temporarily unavailable"))
		case "/api/v1/runs/bad-json":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{"))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetRun(context.Background(), "plain")
	var sdkErr *Error
	if !errors.As(err, &sdkErr) {
		t.Fatalf("err = %T %v", err, err)
	}
	if sdkErr.Code != "HTTP_503" || sdkErr.Message != "503 Service Unavailable" || sdkErr.RequestID != "corr-1" || sdkErr.RetryAfter != 2*time.Second {
		t.Fatalf("sdk err = %+v", sdkErr)
	}
	if got := sdkErr.Error(); got != "openlinker: HTTP_503: 503 Service Unavailable" {
		t.Fatalf("Error() = %q", got)
	}
	if got := (&Error{StatusCode: 500}).Error(); got != "openlinker: request failed with status 500" {
		t.Fatalf("empty code Error() = %q", got)
	}

	_, err = client.GetRun(context.Background(), "bad-json")
	if err == nil || !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("decode err = %v", err)
	}

	headers := http.Header{}
	headers.Set("Retry-After", time.Now().Add(time.Second).UTC().Format(http.TimeFormat))
	if d := retryAfter(headers); d <= 0 {
		t.Fatalf("retryAfter date = %v", d)
	}
	headers.Set("Retry-After", "not-a-date")
	if d := retryAfter(headers); d != 0 {
		t.Fatalf("retryAfter invalid = %v", d)
	}
	headers.Set("X-OpenLinker-Max-Claim-Wait-Seconds", "bad")
	if got := headerInt32(headers, "X-OpenLinker-Max-Claim-Wait-Seconds"); got != 0 {
		t.Fatalf("headerInt32 invalid = %d", got)
	}
}

func TestRuntimeHTTPFallbackTokenNoContentAndDecodeEdges(t *testing.T) {
	var auths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/api/v1/agent-runtime/heartbeat":
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/agent-runtime/call-agent":
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/agent-runtime/runs/claim":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{"))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithAccessToken("ol_live_access"))
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat, err := client.HeartbeatAgent(context.Background()); err != nil || heartbeat.AgentID != "" {
		t.Fatalf("HeartbeatAgent = %+v err=%v", heartbeat, err)
	}
	if child, err := client.CallAgent(context.Background(), CallAgentRequest{CurrentRunID: "run-1", TargetAgentID: "agent-2", Input: JSON{"q": "hello"}}); err != nil || child.RunID != "" {
		t.Fatalf("CallAgent no content = %+v err=%v", child, err)
	}
	if _, err := client.ClaimRuntimeRunDetailed(context.Background(), ClaimRuntimeRunParams{}); err == nil || !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("ClaimRuntimeRunDetailed decode err = %v", err)
	}
	if len(auths) != 3 {
		t.Fatalf("auths len = %d", len(auths))
	}
	for _, auth := range auths {
		if auth != "Bearer ol_live_access" {
			t.Fatalf("runtime fallback auth = %q", auth)
		}
	}
}

func TestReadSSEVariantsAndHandlerErrors(t *testing.T) {
	var events []StreamRunEvent
	err := readSSE(strings.NewReader(": keepalive\nid: 1\nevent:\ndata: one\ndata: two\n\nid: 2\ndata: final\n\n"), func(event StreamRunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v", events)
	}
	if events[0].ID != "1" || events[0].Event != "message" || string(events[0].Data) != "one\ntwo" {
		t.Fatalf("first event = %+v data=%q", events[0], events[0].Data)
	}
	if events[1].ID != "2" || string(events[1].Data) != "final" {
		t.Fatalf("second event = %+v data=%q", events[1], events[1].Data)
	}

	wantErr := errors.New("stop")
	err = readSSE(strings.NewReader("data: stop\n\n"), func(event StreamRunEvent) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("handler err = %v", err)
	}
}

func TestRuntimeHelpersValidationAndMessageBranches(t *testing.T) {
	if err := NewRuntimePullConnector(nil).Start(context.Background(), RuntimeHandlers{}); err == nil {
		t.Fatal("runtime pull without client succeeded")
	}
	noTokenClient, err := NewClient("https://example.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := NewRuntimePullConnector(noTokenClient).Start(context.Background(), RuntimeHandlers{}); err == nil {
		t.Fatal("runtime pull without token succeeded")
	}
	pull := NewRuntimePullConnector(noTokenClient)
	if pull.SupportsLiveEvents() {
		t.Fatal("runtime pull should not support live events")
	}
	if err := pull.SendRunEvent(context.Background(), "run-1", AgentEvent{}); err != nil {
		t.Fatalf("pull SendRunEvent = %v", err)
	}

	client, err := NewClient("http://example.test/base", WithAccessToken("access"), WithRuntimeToken("runtime"), WithHeader("X-Extra", "1"))
	if err != nil {
		t.Fatal(err)
	}
	headers := client.runtimeWebSocketHeaders()
	if headers.Get("Authorization") != "Bearer runtime" || headers.Get("X-Extra") != "1" {
		t.Fatalf("runtime ws headers = %#v", headers)
	}
	if got, err := client.webSocketEndpoint("/agent-runtime/ws"); err != nil || got != "ws://example.test/base/api/v1/agent-runtime/ws" {
		t.Fatalf("webSocketEndpoint http = %q err=%v", got, err)
	}
	if got, err := client.webSocketEndpoint("wss://agents.example/ws"); err != nil || got != "wss://agents.example/ws" {
		t.Fatalf("webSocketEndpoint absolute = %q err=%v", got, err)
	}
	ftpClient, err := NewClient("ftp://example.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ftpClient.webSocketEndpoint("/ws"); err == nil {
		t.Fatal("unsupported websocket scheme succeeded")
	}

	ws := NewRuntimeWSConnector(client)
	if !ws.SupportsLiveEvents() {
		t.Fatal("runtime ws should support live events")
	}
	if err := NewRuntimeWSConnector(nil).Start(context.Background(), RuntimeHandlers{}); err == nil {
		t.Fatal("runtime ws without client succeeded")
	}
	if err := NewRuntimeWSConnector(noTokenClient).Start(context.Background(), RuntimeHandlers{}); err == nil {
		t.Fatal("runtime ws without token succeeded")
	}
	if err := ws.SendRunEvent(context.Background(), "run-1", AgentEvent{}); err == nil {
		t.Fatal("send without websocket succeeded")
	}
	if err := ws.CompleteRun(context.Background(), "run-1", RuntimePullResultRequest{}); err == nil {
		t.Fatal("complete without websocket succeeded")
	}

	var readyCount, assignedCount, messageCount, errorCount int32
	ws.handlers = RuntimeHandlers{
		OnReady: func(message RuntimeWSServerMessage) {
			atomic.AddInt32(&readyCount, 1)
		},
		OnAssigned: func(assignment RuntimeAssignment) {
			if assignment.RunID != "run-1" {
				t.Fatalf("assignment = %+v", assignment)
			}
			atomic.AddInt32(&assignedCount, 1)
		},
		OnMessage: func(message RuntimeWSServerMessage) {
			atomic.AddInt32(&messageCount, 1)
		},
		OnError: func(err error) {
			atomic.AddInt32(&errorCount, 1)
		},
	}
	ws.handleMessage([]byte(`{"type":"runtime.ready","agent_id":"agent-1"}`))
	ws.handleMessage([]byte(`{"type":"run.assigned","run_id":"run-1","agent_id":"agent-1","a2a":{"current_run_id":"run-1"}}`))
	ws.handleMessage([]byte(`{"type":"error","error":{"code":"BAD","message":"bad message"}}`))
	ws.handleMessage([]byte(`{"type":"error","error":{"message":"message only"}}`))
	ws.handleMessage([]byte(`{"type":"error"}`))
	ws.handleMessage([]byte(`{"type":"noop"}`))
	ws.handleMessage([]byte(`not-json`))

	if readyCount != 1 || assignedCount != 1 || messageCount != 6 || errorCount != 4 {
		t.Fatalf("counts ready=%d assigned=%d message=%d error=%d", readyCount, assignedCount, messageCount, errorCount)
	}
	if assignment := RuntimeAssignmentFromPullRun(nil); assignment.RunID != "" {
		t.Fatalf("nil pull assignment = %+v", assignment)
	}
	if got := retryAfterFromClaimResult(&ClaimRuntimeRunResult{RetryAfter: 3 * time.Second}, time.Second); got != 3*time.Second {
		t.Fatalf("retryAfterFromClaimResult = %v", got)
	}
	if got := retryAfterFromError(&Error{RetryAfter: 4 * time.Second}, time.Second); got != 4*time.Second {
		t.Fatalf("retryAfterFromError = %v", got)
	}
}

func TestRuntimePullConnectorRetriesRateLimitBeforeAssignment(t *testing.T) {
	var claims int32
	assignedCh := make(chan RuntimeAssignment, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-1"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			if atomic.AddInt32(&claims, 1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"code":"RATE_LIMITED","message":"retry later"}}`))
				return
			}
			writeJSON(t, w, RuntimePullRunResponse{RunID: "run-after-retry", AgentID: "agent-1", Source: "api"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_live_retry"))
	if err != nil {
		t.Fatal(err)
	}
	connector := NewRuntimePullConnector(client)
	connector.Wait = time.Millisecond
	connector.Heartbeat = time.Millisecond
	connector.EmptyRetry = time.Millisecond
	connector.MaxRuns = 1
	if err := connector.Start(context.Background(), RuntimeHandlers{
		OnAssigned: func(assignment RuntimeAssignment) {
			assignedCh <- assignment
		},
		OnError: func(err error) {
			t.Errorf("unexpected runtime pull error: %v", err)
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer connector.Stop(context.Background())

	select {
	case assignment := <-assignedCh:
		if assignment.RunID != "run-after-retry" {
			t.Fatalf("assignment = %+v", assignment)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for assignment after retry")
	}
	if atomic.LoadInt32(&claims) < 2 {
		t.Fatalf("claims = %d", claims)
	}
}

func TestRuntimePullConnectorReportsClaimErrorsThenStopsOnEmpty(t *testing.T) {
	var claims int32
	errCh := make(chan error, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-1"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			if atomic.AddInt32(&claims, 1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":"BROKEN","message":"claim failed"}}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_live_claim_error"))
	if err != nil {
		t.Fatal(err)
	}
	connector := NewRuntimePullConnector(client)
	connector.Wait = time.Millisecond
	connector.Heartbeat = time.Millisecond
	connector.EmptyRetry = time.Millisecond
	connector.StopOnEmpty = true
	if err := connector.Start(context.Background(), RuntimeHandlers{
		OnAssigned: func(assignment RuntimeAssignment) {
			t.Errorf("unexpected assignment: %+v", assignment)
		},
		OnError: func(err error) {
			errCh <- err
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer connector.Stop(context.Background())

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "runtime pull claim returned 500") || !strings.Contains(err.Error(), "claim failed") {
			t.Fatalf("claim error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for claim error")
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&claims) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := connector.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&claims) < 2 {
		t.Fatalf("claims = %d", claims)
	}
}
