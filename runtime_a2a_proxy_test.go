package openlinker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeA2AProxyForwardsToCanonicalCorePathWithRuntimeIdentity(t *testing.T) {
	var receivedPath, receivedQuery, receivedAuthorization, receivedCookie, receivedBody string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedPath = request.URL.Path
		receivedQuery = request.URL.RawQuery
		receivedAuthorization = request.Header.Get("Authorization")
		receivedCookie = request.Header.Get("Cookie")
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
	if receivedBody != `{"message":{"messageId":"m1"}}` {
		t.Fatalf("body = %q", receivedBody)
	}
	if recorder.Header().Get("A2A-Version") != "0.3" {
		t.Fatalf("response header = %q", recorder.Header().Get("A2A-Version"))
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
