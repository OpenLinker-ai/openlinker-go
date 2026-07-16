package openlinker

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRuntimeA2AProxyForwardsToCanonicalCorePathWithRuntimeIdentity(t *testing.T) {
	var receivedPath, receivedQuery, receivedAuthorization, receivedCookie, receivedBody string
	var receivedHeader http.Header
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedPath = request.URL.Path
		receivedQuery = request.URL.RawQuery
		receivedAuthorization = request.Header.Get("Authorization")
		receivedCookie = request.Header.Get("Cookie")
		receivedHeader = request.Header.Clone()
		body, _ := io.ReadAll(request.Body)
		receivedBody = string(body)
		w.Header().Set("A2A-Version", "0.3")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"task":{"id":"task-1"}}`))
	}))
	defer upstream.Close()

	proxy, err := newRuntimeA2AProxy(upstream.URL, "server-agent", "ol_agent_runtime", upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	request := httptest.NewRequest(http.MethodPost, "http://local/message:send?version=0.3", strings.NewReader(`{"message":{"messageId":"m1"}}`))
	request.Header.Set("Authorization", "Bearer public-adapter-token")
	request.Header.Set("Cookie", "private=browser")
	request.Header.Set("Proxy-Authorization", "Basic public-proxy-secret")
	request.Header.Set("X-OpenLinker-Internal-Token", "internal-secret")
	request.Header.Set("OpenLinker-Internal-Token", "legacy-internal-secret")
	request.Header.Set("X-OpenLinker-Run-Id", "attacker-run")
	request.Header.Set("X-OpenLinker-SDK", "attacker-sdk")
	request.Header["x-openlinker-noncanonical"] = []string{"attacker-noncanonical"}
	request.Header.Set(RuntimeAttachmentHeader, runtimeTestAttachmentID)
	request.Header.Set(RuntimeFallbackReasonHeader, string(runtimeFallbackRecovery))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if receivedPath != "/api/v1/agent-runtime/a2a-proxy/agents/server-agent/message:send" {
		t.Fatalf("path = %q", receivedPath)
	}
	if receivedQuery != "version=0.3" || receivedAuthorization != "Bearer ol_agent_runtime" {
		t.Fatalf("query/auth = %q / %q", receivedQuery, receivedAuthorization)
	}
	if receivedCookie != "" {
		t.Fatalf("browser cookie leaked upstream: %q", receivedCookie)
	}
	if receivedHeader.Get("Proxy-Authorization") != "" {
		t.Fatalf("public proxy credential leaked upstream: %q", receivedHeader.Get("Proxy-Authorization"))
	}
	for name, values := range receivedHeader {
		normalized := strings.ToLower(name)
		if (strings.HasPrefix(normalized, "x-openlinker-") || strings.HasPrefix(normalized, "openlinker-")) &&
			!strings.EqualFold(name, "X-OpenLinker-SDK") {
			t.Fatalf("untrusted OpenLinker header leaked upstream: %s=%q", name, values)
		}
	}
	if receivedHeader.Get(RuntimeAttachmentHeader) != "" || receivedHeader.Get(RuntimeFallbackReasonHeader) != "" {
		t.Fatalf("spoofed Runtime protocol headers leaked upstream: %v", receivedHeader)
	}
	if receivedHeader.Get("X-OpenLinker-SDK") != runtimeWorkerSDKAgent {
		t.Fatalf("SDK identity = %q", receivedHeader.Get("X-OpenLinker-SDK"))
	}
	if receivedBody != `{"message":{"messageId":"m1"}}` {
		t.Fatalf("body = %q", receivedBody)
	}
	if recorder.Header().Get("A2A-Version") != "0.3" {
		t.Fatalf("response header = %q", recorder.Header().Get("A2A-Version"))
	}
}

func TestRuntimeA2AProxyNormalizesOnlyExactCoreRuntimeErrors(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		contentType string
		normalized  bool
	}{
		{
			name:        "runtime unauthorized",
			status:      http.StatusUnauthorized,
			body:        `{"error":{"code":"UNAUTHORIZED","message":"Runtime authentication failed"}}`,
			contentType: "application/json; charset=UTF-8",
			normalized:  true,
		},
		{
			name:        "runtime forbidden",
			status:      http.StatusForbidden,
			body:        `{"error":{"code":"FORBIDDEN","message":"Runtime permission denied"}}`,
			contentType: "application/json",
			normalized:  true,
		},
		{
			name:        "runtime permission denied",
			status:      http.StatusForbidden,
			body:        `{"error":{"code":"PERMISSION_DENIED","message":"Runtime permission denied"}}`,
			contentType: "application/json",
			normalized:  true,
		},
		{
			name:        "runtime unavailable",
			status:      http.StatusServiceUnavailable,
			body:        `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Runtime service unavailable","retryable":true}}`,
			contentType: "application/json",
			normalized:  true,
		},
		{
			name:        "runtime A2A proxy unavailable",
			status:      http.StatusServiceUnavailable,
			body:        `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Agent Runtime A2A proxy is unavailable"}}`,
			contentType: "application/json",
			normalized:  true,
		},
		{
			name:        "A2A media type with exact Runtime body",
			status:      http.StatusServiceUnavailable,
			body:        `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Runtime service unavailable","retryable":true}}`,
			contentType: "application/a2a+json",
		},
		{
			name:   "legitimate A2A unauthorized",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"AuthRequiredError","message":"authorization required"}}`,
		},
		{
			name:   "legitimate A2A JSON-RPC forbidden",
			status: http.StatusForbidden,
			body:   `{"jsonrpc":"2.0","id":"request-1","error":{"code":-32001,"message":"forbidden"}}`,
		},
		{
			name:   "A2A envelope with runtime-like code and extra field",
			status: http.StatusServiceUnavailable,
			body:   `{"error":{"code":"SERVICE_UNAVAILABLE","message":"agent overloaded"},"a2aVersion":"1.0"}`,
		},
		{
			name:   "A2A exact envelope shape with runtime-like code",
			status: http.StatusServiceUnavailable,
			body:   `{"error":{"code":"SERVICE_UNAVAILABLE","message":"agent overloaded"}}`,
		},
		{
			name:   "runtime unavailable missing retryable marker",
			status: http.StatusServiceUnavailable,
			body:   `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Runtime service unavailable"}}`,
		},
		{
			name:   "runtime forbidden with unexpected retryable marker",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"FORBIDDEN","message":"Runtime permission denied","retryable":false}}`,
		},
		{
			name:   "duplicate Runtime envelope field",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"FORBIDDEN","code":"FORBIDDEN","message":"Runtime permission denied"}}`,
		},
		{
			name:   "wrong HTTP status",
			status: http.StatusInternalServerError,
			body:   `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Runtime service unavailable"}}`,
		},
		{
			name:   "unknown Runtime envelope field",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"FORBIDDEN","message":"Runtime permission denied","details":"public-a2a"}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				contentType := test.contentType
				if contentType == "" {
					contentType = "application/a2a+json"
				}
				w.Header().Set("Content-Type", contentType)
				w.Header().Set("X-Upstream-Error", "preserve-on-pass-through")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer upstream.Close()
			proxy, err := newRuntimeA2AProxy(upstream.URL, "server-agent", "ol_agent_runtime", upstream.Client())
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Close()

			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "http://local/message:send", nil))
			if test.normalized {
				if recorder.Code != http.StatusBadGateway || recorder.Header().Get("X-Upstream-Error") != "" || recorder.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("normalized response = %d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
				}
				if recorder.Body.String() != runtimeA2AProxyUnavailableBody {
					t.Fatalf("normalized body = %q", recorder.Body.String())
				}
				return
			}
			if recorder.Code != test.status || recorder.Body.String() != test.body || recorder.Header().Get("X-Upstream-Error") == "" {
				t.Fatalf("pass-through response = %d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestRuntimeA2AProxyPreservesOversizedCandidateErrorBody(t *testing.T) {
	body := bytes.Repeat([]byte("x"), int(runtimeA2AProxyErrorBodyMaxBytes)+1)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	proxy, err := newRuntimeA2AProxy(upstream.URL, "server-agent", "ol_agent_runtime", upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "http://local/message:send", nil))
	if recorder.Code != http.StatusServiceUnavailable || !bytes.Equal(recorder.Body.Bytes(), body) {
		t.Fatalf("oversized response = status %d bytes %d", recorder.Code, recorder.Body.Len())
	}
}

func TestRuntimeA2AProxyPreservesSSEStreamingFlush(t *testing.T) {
	releaseSecond := make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: task\ndata: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-releaseSecond:
		case <-time.After(2 * time.Second):
			return
		}
		_, _ = io.WriteString(w, "event: status\ndata: completed\n\n")
	}))
	defer upstream.Close()
	proxy, err := newRuntimeA2AProxy(upstream.URL, "server-agent", "ol_agent_runtime", upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	adapter := httptest.NewServer(proxy)
	defer adapter.Close()

	type responseResult struct {
		response *http.Response
		err      error
	}
	responseReady := make(chan responseResult, 1)
	go func() {
		response, requestErr := http.Get(adapter.URL + "/tasks/task-1/subscribe") // #nosec G107 -- local test server.
		responseReady <- responseResult{response: response, err: requestErr}
	}()
	var response *http.Response
	select {
	case result := <-responseReady:
		if result.err != nil {
			t.Fatal(result.err)
		}
		response = result.response
	case <-time.After(time.Second):
		t.Fatal("SSE response headers were not flushed")
	}
	defer response.Body.Close()
	firstEvent := make(chan string, 1)
	reader := bufio.NewReader(response.Body)
	go func() {
		var chunk strings.Builder
		for index := 0; index < 3; index++ {
			line, _ := reader.ReadString('\n')
			chunk.WriteString(line)
		}
		firstEvent <- chunk.String()
	}()
	select {
	case chunk := <-firstEvent:
		if chunk != "event: task\ndata: first\n\n" {
			t.Fatalf("first SSE event = %q", chunk)
		}
	case <-time.After(time.Second):
		t.Fatal("first SSE event was buffered until stream completion")
	}
	close(releaseSecond)
	remainder, err := io.ReadAll(reader)
	if err != nil || string(remainder) != "event: status\ndata: completed\n\n" {
		t.Fatalf("remaining SSE stream = %q, %v", remainder, err)
	}
}

func TestRuntimeA2AProxyMapsRootAndRejectsTraversal(t *testing.T) {
	var receivedPath string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedPath = request.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	proxy, err := newRuntimeA2AProxy(upstream.URL, "server-agent", "ol_agent_runtime", upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "http://local/", nil))
	if recorder.Code != http.StatusNoContent || receivedPath != "/api/v1/agent-runtime/a2a-proxy/agents/server-agent" {
		t.Fatalf("root status/path = %d / %q", recorder.Code, receivedPath)
	}

	request := httptest.NewRequest(http.MethodGet, "http://local/", nil)
	request.URL.Path = "/../admin"
	recorder = httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("traversal status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}
