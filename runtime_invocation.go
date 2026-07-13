package openlinker

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	runtimeCallAgentPath         = "/api/v1/agent-runtime/call-agent"
	runtimeInvocationProofDomain = "openlinker/runtime-v2/invocation-proof"
	runtimeInvocationHeader      = "OpenLinker-Invocation-Context"
	runtimeInvocationProofHeader = "OpenLinker-Invocation-Proof"
)

type RuntimeInvocationProofRequest struct {
	Method         string
	Path           string
	IdempotencyKey string
	Context        string
	Body           []byte
}

// BuildRuntimeInvocationProof implements Core's Runtime proof algorithm.
// Body is hashed exactly as supplied; callers must send the same bytes.
func BuildRuntimeInvocationProof(token string, request RuntimeInvocationProofRequest) (string, error) {
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	path := strings.TrimSpace(request.Path)
	contextValue := strings.TrimSpace(request.Context)
	if method == "" || path == "" || !strings.HasPrefix(path, "/") || contextValue == "" ||
		!strings.HasPrefix(token, "ol_inv_v2.") {
		return "", errors.New("openlinker: invalid runtime invocation proof input")
	}
	if err := validateIdempotencyKey(request.IdempotencyKey); err != nil {
		return "", err
	}
	bodyDigest := sha256.Sum256(request.Body)
	canonical, err := canonicalRuntimeInvocationProof(
		hex.EncodeToString(bodyDigest[:]), contextValue, request.IdempotencyKey, method, path,
	)
	if err != nil {
		return "", err
	}
	key := sha256.Sum256([]byte(runtimeInvocationProofDomain + "\x00" + token))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(canonical)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (r *Runtime) CallRuntimeAgent(
	ctx context.Context,
	authorization RuntimeCallAgentAuthorization,
	request RuntimeCallAgentRequest,
) (*RuntimeRunSummary, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	if err := r.client.requireRuntime(); err != nil {
		return nil, err
	}
	if err := validateRuntimeCallAgentAuthorization(authorization); err != nil {
		return nil, err
	}
	if err := validateRuntimeCallAgentRequest(request); err != nil {
		return nil, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("openlinker: encode runtime delegated call: %w", err)
	}
	if int64(len(body)) > RuntimeMaxMessageBytes {
		return nil, fmt.Errorf("openlinker: runtime delegated call exceeds %d bytes", RuntimeMaxMessageBytes)
	}
	proof, err := BuildRuntimeInvocationProof(authorization.AgentInvocationToken, RuntimeInvocationProofRequest{
		Method:         http.MethodPost,
		Path:           runtimeCallAgentPath,
		IdempotencyKey: authorization.IdempotencyKey,
		Context:        authorization.NodeEnvelope,
		Body:           body,
	})
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+authorization.AgentInvocationToken)
	headers.Set("Idempotency-Key", authorization.IdempotencyKey)
	headers.Set(runtimeInvocationHeader, authorization.NodeEnvelope)
	headers.Set(runtimeInvocationProofHeader, proof)
	response, err := r.client.newRequestWithTokenAndHeadersBytes(
		ctx, http.MethodPost, runtimeCallAgentPath, nil, body, "application/json",
		authorization.AgentInvocationToken, headers,
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, parseRuntimeError(response)
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, errors.New("openlinker: runtime delegated call returned no run summary")
	}
	var summary RuntimeRunSummary
	if err := decodeRuntimeResponse(response.Body, &summary); err != nil {
		return nil, fmt.Errorf("openlinker: decode runtime delegated call: %w", err)
	}
	if err := validateRuntimeRunSummary(summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func validateRuntimeCallAgentAuthorization(value RuntimeCallAgentAuthorization) error {
	if !runtimeInvocationCapability(value.NodeEnvelope, "ol_ctx_v2.") ||
		!runtimeInvocationCapability(value.AgentInvocationToken, "ol_inv_v2.") {
		return errors.New("openlinker: invalid runtime delegated call authority")
	}
	if err := validateIdempotencyKey(value.IdempotencyKey); err != nil {
		return err
	}
	return nil
}

func validateRuntimeCallAgentRequest(value RuntimeCallAgentRequest) error {
	if !runtimeUUID(value.TargetAgentID) || value.Input == nil ||
		!runtimeOptionalText(value.Reason, 500) {
		return errors.New("openlinker: invalid runtime delegated call")
	}
	return nil
}

func validateRuntimeRunSummary(value RuntimeRunSummary) error {
	if !runtimeUUID(value.RunID) || !runtimeRunStatus(value.Status) || !runtimeDispatchState(value.DispatchState) {
		return errors.New("openlinker: invalid runtime run summary")
	}
	switch value.Status {
	case RuntimeRunRunning:
		switch value.DispatchState {
		case RuntimeDispatchPending, RuntimeDispatchOffered, RuntimeDispatchExecuting, RuntimeDispatchRetryWait:
		default:
			return errors.New("openlinker: inconsistent runtime running summary")
		}
	case RuntimeRunSuccess, RuntimeRunTimeout, RuntimeRunCanceled:
		if value.DispatchState != RuntimeDispatchTerminal {
			return errors.New("openlinker: inconsistent runtime terminal summary")
		}
	case RuntimeRunFailed:
		if value.DispatchState != RuntimeDispatchTerminal && value.DispatchState != RuntimeDispatchDeadLetter {
			return errors.New("openlinker: inconsistent runtime failed summary")
		}
	}
	return nil
}

func runtimeInvocationCapability(value, prefix string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 8192 || !utf8.ValidString(value) ||
		!strings.HasPrefix(value, prefix) {
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
	}
	return true
}

func runtimeOptionalText(value string, maximum int) bool {
	return value == "" || (utf8.ValidString(value) && utf8.RuneCountInString(value) <= maximum)
}

func canonicalRuntimeInvocationProof(bodyDigest, contextValue, idempotencyKey, method, path string) ([]byte, error) {
	canonical := []byte(`{"body_sha256":`)
	var err error
	canonical, err = appendRuntimeCanonicalJSONString(canonical, bodyDigest)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"context":`...)
	canonical, err = appendRuntimeCanonicalJSONString(canonical, contextValue)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"idempotency_key":`...)
	canonical, err = appendRuntimeCanonicalJSONString(canonical, idempotencyKey)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"method":`...)
	canonical, err = appendRuntimeCanonicalJSONString(canonical, method)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"path":`...)
	canonical, err = appendRuntimeCanonicalJSONString(canonical, path)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"version":`...)
	canonical, err = appendRuntimeCanonicalJSONString(canonical, runtimeInvocationProofDomain)
	if err != nil {
		return nil, err
	}
	return append(canonical, '}'), nil
}

func appendRuntimeCanonicalJSONString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, errors.New("openlinker: runtime proof string is not valid Unicode")
	}
	const hexadecimal = "0123456789abcdef"
	dst = append(dst, '"')
	for _, char := range value {
		switch char {
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '"', '\\':
			dst = append(dst, '\\', byte(char))
		default:
			if char < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hexadecimal[byte(char)>>4], hexadecimal[byte(char)&0x0f])
				continue
			}
			dst = utf8.AppendRune(dst, char)
		}
	}
	return append(dst, '"'), nil
}
