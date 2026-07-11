package openlinker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	latestEventSequence := int32(4)
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
			writeJSON(t, w, ListRunEventsResponse{
				Items: []RunEventResponse{{EventID: "evt-1", Sequence: 4}},
				Meta: RunEventPageMeta{
					RequestedAfterSequence:    3,
					EffectiveAfterSequence:    3,
					RetainedThroughSequence:   2,
					EarliestAvailableSequence: nil,
					LatestAvailableSequence:   &latestEventSequence,
					RetentionGap:              false,
					Terminal:                  true,
					StreamComplete:            true,
				},
			})
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
		WithUserToken(" ol_user_user "),
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
	if events, err := client.ListRunEvents(ctx, "run-1", ListRunEventsParams{AfterSequence: 3, Limit: 4}); err != nil ||
		len(events.Items) != 1 || events.Items[0].EventID != "evt-1" ||
		events.Meta.RequestedAfterSequence != 3 || events.Meta.EffectiveAfterSequence != 3 ||
		events.Meta.RetainedThroughSequence != 2 || events.Meta.EarliestAvailableSequence != nil ||
		events.Meta.LatestAvailableSequence == nil || *events.Meta.LatestAvailableSequence != 4 ||
		events.Meta.RetentionGap || !events.Meta.Terminal || !events.Meta.StreamComplete {
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
		if call.Auth != "Bearer ol_user_user" || call.SDK != "openlinker-go-test" || call.Custom != "yes" {
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
	client, err := NewClient("https://example.test/root/api/v1", WithHTTPClient(nil), WithUserToken(" user "))
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
	if _, err := NewClient("https://example.test", WithAgentToken(" runtime ")); err == nil || !strings.Contains(err.Error(), "NewRuntime") {
		t.Fatalf("NewClient with agent token err = %v", err)
	}
	if _, err := NewRuntime("https://example.test/root/api/v1", WithRuntimeToken(" runtime ")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuntime("https://example.test", WithUserToken(" user "), WithAgentToken(" runtime ")); err == nil || !strings.Contains(err.Error(), "user token") {
		t.Fatalf("NewRuntime with user token err = %v", err)
	}
	defaultAgentClient, err := NewClient("https://example.test", WithSDKAgent(" "))
	if err != nil {
		t.Fatal(err)
	}
	if defaultAgentClient.sdkAgent != defaultSDKAgent {
		t.Fatalf("default sdk agent = %q", defaultAgentClient.sdkAgent)
	}
}

func TestClientMethodsPropagateRequestErrors(t *testing.T) {
	client, err := NewClient(
		"https://example.test",
		WithUserToken("ol_user_access"),
		WithHTTPClient(sdkHTTPClient(http.StatusBadGateway, `{"error":{"code":"BROKEN","message":"bad gateway"}}`)),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{name: "get agent", call: func() error {
			_, err := client.GetAgent(ctx, "agent")
			return err
		}},
		{name: "get agent card", call: func() error {
			_, err := client.GetAgentCard(ctx, "agent", false)
			return err
		}},
		{name: "run agent", call: func() error {
			_, err := client.RunAgent(ctx, RunAgentRequest{AgentID: "agent", Input: JSON{"q": "hi"}})
			return err
		}},
		{name: "start run", call: func() error {
			_, err := client.StartAgentRun(ctx, RunAgentRequest{AgentID: "agent", Input: JSON{"q": "hi"}})
			return err
		}},
		{name: "get run", call: func() error {
			_, err := client.GetRun(ctx, "run")
			return err
		}},
		{name: "list events", call: func() error {
			_, err := client.ListRunEvents(ctx, "run", ListRunEventsParams{})
			return err
		}},
		{name: "list artifacts", call: func() error {
			_, err := client.ListRunArtifacts(ctx, "run")
			return err
		}},
		{name: "list messages", call: func() error {
			_, err := client.ListRunMessages(ctx, "run")
			return err
		}},
		{name: "stream events", call: func() error {
			return client.StreamRunEvents(ctx, "run", StreamRunEventsOptions{}, func(StreamRunEvent) error { return nil })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sdkErr *Error
			if err := tc.call(); !errors.As(err, &sdkErr) || sdkErr.Code != "BROKEN" {
				t.Fatalf("err = %T %v", err, err)
			}
		})
	}

	if err := client.do(ctx, http.MethodPost, "/run", nil, map[string]any{"bad": func() {}}, nil); err == nil || !strings.Contains(err.Error(), "encode request") {
		t.Fatalf("encode error = %v", err)
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
}

type sdkRoundTripper func(*http.Request) (*http.Response, error)

func (f sdkRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func sdkHTTPClient(status int, body string) *http.Client {
	return &http.Client{Transport: sdkRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})}
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
