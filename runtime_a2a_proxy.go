package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
)

const runtimeA2AProxyPathPrefix = "/api/v1/agent-runtime/a2a-proxy/agents/"

var runtimeA2AProxySlugPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$`)

// RuntimeA2AProxyConfig configures the SDK-owned transport used by legacy
// AgentNode public A2A adapters. Core remains the sole Task/Run/push authority.
type RuntimeA2AProxyConfig struct {
	PlatformURL string
	RuntimeURL  string
	AgentToken  string
	AgentSlug   string
	MTLS        RuntimeMTLSConfig
}

// RuntimeA2AProxy forwards one local A2A HTTP surface to the authenticated
// Core Runtime listener. It is safe for concurrent use.
type RuntimeA2AProxy struct {
	proxy      *httputil.ReverseProxy
	httpClient *http.Client
}

// NewRuntimeA2AProxy discovers the canonical Runtime origin when needed and
// binds every upstream request to the configured Agent Token and device mTLS
// certificate. Incoming Authorization credentials are never forwarded.
func NewRuntimeA2AProxy(ctx context.Context, config RuntimeA2AProxyConfig) (*RuntimeA2AProxy, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	slug := strings.TrimSpace(config.AgentSlug)
	if !runtimeA2AProxySlugPattern.MatchString(slug) {
		return nil, errors.New("Agent slug must use lowercase letters, digits, and hyphens")
	}
	connection, err := resolveRuntimeConnection(ctx, config.PlatformURL, config.RuntimeURL)
	if err != nil {
		return nil, err
	}
	_, httpClient, err := newRuntimeClient(connection.RuntimeURL, config.AgentToken, config.MTLS)
	if err != nil {
		return nil, err
	}
	proxy, err := newRuntimeA2AProxy(connection.RuntimeURL, slug, config.AgentToken, httpClient)
	if err != nil {
		closeRuntimeA2AProxyHTTPClient(httpClient)
		return nil, err
	}
	return proxy, nil
}

func newRuntimeA2AProxy(runtimeAddress, slug, agentToken string, httpClient *http.Client) (*RuntimeA2AProxy, error) {
	target, err := url.Parse(runtimeAddress)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, errors.New("Runtime connection address must be an absolute origin")
	}
	if httpClient == nil || httpClient.Transport == nil {
		return nil, errors.New("Runtime A2A proxy HTTP transport is required")
	}
	if strings.TrimSpace(agentToken) == "" {
		return nil, errors.New("Agent Token is required")
	}
	if !runtimeA2AProxySlugPattern.MatchString(slug) {
		return nil, errors.New("Agent slug must use lowercase letters, digits, and hyphens")
	}
	basePath := runtimeA2AProxyPathPrefix + slug
	reverse := &httputil.ReverseProxy{
		Transport:     httpClient.Transport,
		FlushInterval: -1,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme = target.Scheme
			request.Out.URL.Host = target.Host
			request.Out.URL.Path = basePath + runtimeA2AProxySuffix(request.In.URL.Path)
			request.Out.URL.RawPath = ""
			request.Out.Host = target.Host
			request.Out.Header.Del("Cookie")
			request.Out.Header.Set("Authorization", "Bearer "+agentToken)
			request.Out.Header.Set("X-OpenLinker-SDK", runtimeWorkerSDKAgent)
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeRuntimeA2AProxyError(w, http.StatusBadGateway, "A2A_PROXY_UNAVAILABLE", "Core A2A service is unavailable")
		},
	}
	return &RuntimeA2AProxy{proxy: reverse, httpClient: httpClient}, nil
}

func (p *RuntimeA2AProxy) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if p == nil || p.proxy == nil || request == nil || !validRuntimeA2AProxyRequestPath(request.URL.Path) {
		writeRuntimeA2AProxyError(w, http.StatusBadRequest, "INVALID_A2A_PATH", "invalid A2A proxy path")
		return
	}
	p.proxy.ServeHTTP(w, request)
}

// Close releases idle mTLS connections owned by the proxy.
func (p *RuntimeA2AProxy) Close() {
	if p == nil {
		return
	}
	closeRuntimeA2AProxyHTTPClient(p.httpClient)
}

func closeRuntimeA2AProxyHTTPClient(client *http.Client) {
	if client == nil {
		return
	}
	if transport, ok := client.Transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

func validRuntimeA2AProxyRequestPath(path string) bool {
	if path == "" || path[0] != '/' || strings.Contains(path, "\\") || strings.ContainsRune(path, '\x00') {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func runtimeA2AProxySuffix(path string) string {
	if path == "/" {
		return ""
	}
	return path
}

func writeRuntimeA2AProxyError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
