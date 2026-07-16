package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	runtimeA2AProxyPathPrefix        = "/api/v1/agent-runtime/a2a-proxy/agents/"
	runtimeA2AProxyErrorBodyMaxBytes = int64(64 << 10)
	runtimeA2AProxyUnavailableBody   = "{\"error\":{\"code\":\"A2A_PROXY_UNAVAILABLE\",\"message\":\"Core A2A service is unavailable\"}}\n"
)

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
		Transport: httpClient.Transport,
		// ReverseProxy automatically flushes event streams immediately. Leaving
		// the general interval at zero avoids forcing a partial Runtime error
		// envelope through an outer compatibility response writer before it can
		// be normalized.
		FlushInterval: 0,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme = target.Scheme
			request.Out.URL.Host = target.Host
			request.Out.URL.Path = basePath + runtimeA2AProxySuffix(request.In.URL.Path)
			request.Out.URL.RawPath = ""
			request.Out.Host = target.Host
			sanitizeRuntimeA2AProxyRequestHeaders(request.Out.Header)
			request.Out.Header.Set("Authorization", "Bearer "+agentToken)
			request.Out.Header.Set("X-OpenLinker-SDK", runtimeWorkerSDKAgent)
		},
		ModifyResponse: normalizeRuntimeA2AProxyResponse,
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

func sanitizeRuntimeA2AProxyRequestHeaders(header http.Header) {
	if header == nil {
		return
	}
	for name := range header {
		normalized := strings.ToLower(name)
		if strings.HasPrefix(normalized, "x-openlinker-") || strings.HasPrefix(normalized, "openlinker-") ||
			normalized == "authorization" || normalized == "cookie" || normalized == "proxy-authorization" {
			delete(header, name)
		}
	}
	// The adapter-facing credential and browser state are never Core Runtime
	// credentials. ReverseProxy normally strips proxy authentication as a
	// hop-by-hop header, but deleting it here keeps the trust boundary explicit.
}

func normalizeRuntimeA2AProxyResponse(response *http.Response) error {
	if response == nil || response.Body == nil || !runtimeA2AProxyRuntimeStatus(response.StatusCode) ||
		!runtimeA2AProxyRuntimeContentType(response.Header.Get("Content-Type")) {
		return nil
	}
	originalBody := response.Body
	body, err := io.ReadAll(io.LimitReader(originalBody, runtimeA2AProxyErrorBodyMaxBytes+1))
	if err != nil {
		_ = originalBody.Close()
		return err
	}
	if int64(len(body)) > runtimeA2AProxyErrorBodyMaxBytes {
		response.Body = &runtimeA2AProxyReplayBody{
			Reader: io.MultiReader(bytes.NewReader(body), originalBody),
			Closer: originalBody,
		}
		return nil
	}
	_ = originalBody.Close()
	if !runtimeA2AProxyRuntimeErrorEnvelope(response.StatusCode, body) {
		response.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	unavailable := []byte(runtimeA2AProxyUnavailableBody)
	response.StatusCode = http.StatusBadGateway
	response.Status = strconv.Itoa(http.StatusBadGateway) + " " + http.StatusText(http.StatusBadGateway)
	response.Header = make(http.Header)
	response.Header.Set("Content-Type", "application/json")
	response.Header.Set("Content-Length", strconv.FormatInt(int64(len(unavailable)), 10))
	response.Body = io.NopCloser(bytes.NewReader(unavailable))
	response.ContentLength = int64(len(unavailable))
	response.TransferEncoding = nil
	response.Trailer = nil
	response.Uncompressed = false
	return nil
}

func runtimeA2AProxyRuntimeContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

type runtimeA2AProxyReplayBody struct {
	io.Reader
	io.Closer
}

func runtimeA2AProxyRuntimeStatus(status int) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable:
		return true
	default:
		return false
	}
}

func runtimeA2AProxyRuntimeErrorEnvelope(status int, raw []byte) bool {
	envelope, ok := decodeRuntimeA2AProxyUniqueObject(raw)
	if !ok || len(envelope) != 1 {
		return false
	}
	rawError, ok := envelope["error"]
	if !ok {
		return false
	}
	body, ok := decodeRuntimeA2AProxyUniqueObject(rawError)
	if !ok || len(body) < 2 || len(body) > 3 {
		return false
	}
	for name := range body {
		if name != "code" && name != "message" && name != "retryable" {
			return false
		}
	}
	var code, message string
	if err := json.Unmarshal(body["code"], &code); err != nil || json.Unmarshal(body["message"], &message) != nil || strings.TrimSpace(message) == "" {
		return false
	}
	retryable := false
	retryablePresent := false
	if rawRetryable, exists := body["retryable"]; exists {
		retryablePresent = true
		if err := json.Unmarshal(rawRetryable, &retryable); err != nil || string(bytes.TrimSpace(rawRetryable)) == "null" {
			return false
		}
	}
	switch status {
	case http.StatusUnauthorized:
		return code == "UNAUTHORIZED" && message == "Runtime authentication failed" && !retryablePresent
	case http.StatusForbidden:
		return (code == "FORBIDDEN" || code == "PERMISSION_DENIED") &&
			message == "Runtime permission denied" && !retryablePresent
	case http.StatusServiceUnavailable:
		if code != "SERVICE_UNAVAILABLE" {
			return false
		}
		switch message {
		case "Runtime service unavailable":
			return retryablePresent && retryable
		case "Agent Runtime A2A proxy is unavailable":
			return !retryablePresent
		default:
			return false
		}
	default:
		return false
	}
}

func decodeRuntimeA2AProxyUniqueObject(raw []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, tokenErr := decoder.Token()
		name, stringName := nameToken.(string)
		if tokenErr != nil || !stringName {
			return nil, false
		}
		if _, duplicate := object[name]; duplicate {
			return nil, false
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, false
		}
		object[name] = value
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') || rejectTrailingJSON(decoder) != nil {
		return nil, false
	}
	return object, true
}

func writeRuntimeA2AProxyError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
