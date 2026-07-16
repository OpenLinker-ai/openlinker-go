package openlinker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type runtimeDiscoveryPolicyFixture struct {
	Cases []struct {
		Name     string                         `json:"name"`
		Manifest openLinkerDiscoveryManifest    `json:"manifest"`
		Expected runtimeDiscoveryPolicyExpected `json:"expected"`
	} `json:"cases"`
	ConfiguredTransportCases []struct {
		Name         string                       `json:"name"`
		ManifestCase string                       `json:"manifest_case"`
		Manifest     *openLinkerDiscoveryManifest `json:"manifest"`
		Configured   RuntimeTransportMode         `json:"configured"`
		Effective    RuntimeTransportMode         `json:"effective"`
		Error        string                       `json:"error"`
	} `json:"configured_transport_cases"`
}

type runtimeDiscoveryPolicyExpected struct {
	Allowed                  []RuntimeTransportMode `json:"allowed"`
	Default                  RuntimeTransportMode   `json:"default"`
	HeartbeatIntervalMS      int64                  `json:"heartbeat_interval_ms"`
	SessionStaleAfterMS      int64                  `json:"session_stale_after_ms"`
	RetryMinimumMS           int64                  `json:"retry_minimum_ms"`
	RetryMaximumMS           int64                  `json:"retry_maximum_ms"`
	WebSocketProbeIntervalMS int64                  `json:"websocket_probe_interval_ms"`
	WebSocketProbeTimeoutMS  int64                  `json:"websocket_probe_timeout_ms"`
}

func loadRuntimeDiscoveryPolicyFixture(t *testing.T) runtimeDiscoveryPolicyFixture {
	t.Helper()
	body, err := os.ReadFile("contracts/runtime-discovery-policy-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture runtimeDiscoveryPolicyFixture
	if err = json.Unmarshal(body, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestRuntimeDiscoveryPolicyFixtures(t *testing.T) {
	fixture := loadRuntimeDiscoveryPolicyFixture(t)
	manifests := make(map[string]openLinkerDiscoveryManifest, len(fixture.Cases))
	for _, test := range fixture.Cases {
		manifests[test.Name] = test.Manifest
		t.Run(test.Name, func(t *testing.T) {
			policy, err := runtimeTransportPolicyFromManifest(
				test.Manifest.Runtime.Transports,
				test.Manifest.Runtime.DefaultTransport,
				test.Manifest.Runtime.TransportPolicy,
			)
			if err != nil {
				t.Fatal(err)
			}
			got := runtimeDiscoveryPolicyExpected{
				Allowed: policy.Allowed, Default: policy.Default,
				HeartbeatIntervalMS:      policy.HeartbeatInterval.Milliseconds(),
				SessionStaleAfterMS:      policy.SessionStaleAfter.Milliseconds(),
				RetryMinimumMS:           policy.RetryMinimum.Milliseconds(),
				RetryMaximumMS:           policy.RetryMaximum.Milliseconds(),
				WebSocketProbeIntervalMS: policy.WebSocketProbeInterval.Milliseconds(),
				WebSocketProbeTimeoutMS:  policy.WebSocketProbeTimeout.Milliseconds(),
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Fatalf("policy = %#v, want %#v", got, test.Expected)
			}
		})
	}
	for _, test := range fixture.ConfiguredTransportCases {
		t.Run(test.Name, func(t *testing.T) {
			manifest := test.Manifest
			if manifest == nil {
				value, ok := manifests[test.ManifestCase]
				if !ok {
					t.Fatalf("unknown manifest fixture %q", test.ManifestCase)
				}
				manifest = &value
			}
			policy, err := runtimeTransportPolicyFromManifest(
				manifest.Runtime.Transports,
				manifest.Runtime.DefaultTransport,
				manifest.Runtime.TransportPolicy,
			)
			if err != nil {
				t.Fatal(err)
			}
			worker := &RuntimeWorker{
				Transport:             test.Configured,
				HeartbeatInterval:     RuntimeWorkerDefaultHeartbeatInterval,
				RetryMinimum:          RuntimeWorkerDefaultRetryMinimum,
				RetryMaximum:          RuntimeWorkerDefaultRetryMaximum,
				webSocketProbeTimeout: 10 * time.Second,
			}
			err = worker.applyRuntimeTransportPolicy(policy)
			if test.Error != "" {
				if err == nil || !strings.Contains(err.Error(), test.Error) {
					t.Fatalf("error = %v, want substring %q", err, test.Error)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if worker.Transport != test.Effective {
				t.Fatalf("effective transport = %q, want %q", worker.Transport, test.Effective)
			}
		})
	}
}

func TestResolveRuntimeURLDiscoversWithoutRuntimeCredentials(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != "/.well-known/openlinker.json" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
			t.Fatalf("discovery leaked credentials: %#v", request.Header)
		}
		if request.TLS != nil && len(request.TLS.PeerCertificates) != 0 {
			t.Fatalf("discovery sent a client certificate: %#v", request.TLS.PeerCertificates)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"base_urls":{"runtime":"https://runtime.example.test:8443"},"runtime":{"enabled":true,"mtls_required":true,"transports":["websocket","long_poll"],"default_transport":"auto"}}`)
	}))
	defer server.Close()

	got, err := resolveRuntimeURL(context.Background(), server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://runtime.example.test:8443" || calls.Load() != 1 {
		t.Fatalf("runtime URL = %q, discovery calls = %d", got, calls.Load())
	}
}

func TestResolveRuntimeURLOverrideSkipsDiscovery(t *testing.T) {
	got, err := resolveRuntimeURL(context.Background(), "not-a-platform-url", " https://runtime.example.test:8443 ")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://runtime.example.test:8443" {
		t.Fatalf("runtime URL = %q", got)
	}
}

func TestResolveRuntimeURLFailsClosedForUnavailableManifest(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "disabled", body: `{"base_urls":{},"runtime":{"enabled":false,"mtls_required":true}}`, want: "does not provide"},
		{name: "missing origin", body: `{"base_urls":{},"runtime":{"enabled":true,"mtls_required":true}}`, want: "does not provide"},
		{name: "mTLS disabled", body: `{"base_urls":{"runtime":"https://runtime.example.test"},"runtime":{"enabled":true,"mtls_required":false}}`, want: "expected mTLS"},
		{name: "invalid JSON", body: `{`, want: "decode OpenLinker"},
		{name: "trailing JSON", body: `{"runtime":{"enabled":false}} {}`, want: "trailing JSON"},
		{name: "insecure runtime", body: `{"base_urls":{"runtime":"http://127.0.0.1:8443"},"runtime":{"enabled":true,"mtls_required":true}}`, want: "absolute HTTPS origin"},
		{name: "runtime path", body: `{"base_urls":{"runtime":"https://runtime.example.test/api"},"runtime":{"enabled":true,"mtls_required":true}}`, want: "must not include"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			_, err := resolveRuntimeURL(context.Background(), server.URL, "")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestResolveRuntimeURLRejectsOversizedManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", openLinkerDiscoveryMaxBytes+1))
		_, _ = fmt.Fprint(w, strings.Repeat(" ", openLinkerDiscoveryMaxBytes+1))
	}))
	defer server.Close()
	_, err := resolveRuntimeURL(context.Background(), server.URL, "")
	if err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Fatalf("oversized manifest error = %v", err)
	}
}

func TestResolveRuntimeURLRejectsRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	_, err := resolveRuntimeURL(context.Background(), server.URL, "")
	if err == nil || !strings.Contains(err.Error(), "redirects are not allowed") {
		t.Fatalf("redirect error = %v", err)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target calls = %d", targetCalls.Load())
	}
}

func TestDiscoveryClientIsCredentialFreeAndBounded(t *testing.T) {
	client := newOpenLinkerDiscoveryClient()
	if client.Timeout != 5*time.Second {
		t.Fatalf("timeout = %s", client.Timeout)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil {
		t.Fatalf("transport = %T", client.Transport)
	}
	if len(transport.TLSClientConfig.Certificates) != 0 || transport.TLSClientConfig.GetClientCertificate != nil {
		t.Fatalf("discovery TLS config includes a client identity: %#v", transport.TLSClientConfig)
	}
	if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %x", transport.TLSClientConfig.MinVersion)
	}
}

func TestValidateConnectionOrigins(t *testing.T) {
	for _, value := range []string{"https://example", "https://example:8443", "http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
		if _, err := validatePlatformOrigin(value); err != nil {
			t.Errorf("valid platform origin %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "example", "http://example", "https://user:pass@example", "https://example/", "https://example/api", "https://example?x=1", "https://example#", "https://example#part", "https://example:", "https://example:0", "https://example:65536", "https://example:https", "ftp://example"} {
		if _, err := validatePlatformOrigin(value); err == nil {
			t.Errorf("invalid platform origin accepted: %q", value)
		}
	}
	for _, value := range []string{"https://runtime.example", "https://runtime.example:8443"} {
		if _, err := validateRuntimeOrigin(value); err != nil {
			t.Errorf("valid runtime origin %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "http://localhost:8443", "http://127.0.0.1:8443", "https://runtime.example/", "https://runtime.example/path", "https://runtime.example?x=1", "https://runtime.example#part", "https://token@runtime.example"} {
		if _, err := validateRuntimeOrigin(value); err == nil {
			t.Errorf("invalid runtime origin accepted: %q", value)
		}
	}
}
