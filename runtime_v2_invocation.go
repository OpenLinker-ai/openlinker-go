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
	runtimeV2CallAgentPath         = "/api/v1/agent-runtime/call-agent"
	runtimeV2InvocationProofDomain = "openlinker/runtime-v2/invocation-proof"
	runtimeV2InvocationHeader      = "OpenLinker-Invocation-Context"
	runtimeV2InvocationProofHeader = "OpenLinker-Invocation-Proof"
)

type RuntimeV2InvocationProofRequest struct {
	Method         string
	Path           string
	IdempotencyKey string
	Context        string
	Body           []byte
}

// BuildRuntimeV2InvocationProof implements Core's runtime-v2 proof algorithm.
// Body is hashed exactly as supplied; callers must send the same bytes.
func BuildRuntimeV2InvocationProof(token string, request RuntimeV2InvocationProofRequest) (string, error) {
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	path := strings.TrimSpace(request.Path)
	contextValue := strings.TrimSpace(request.Context)
	if method == "" || path == "" || !strings.HasPrefix(path, "/") || contextValue == "" ||
		!strings.HasPrefix(token, "ol_inv_v2.") {
		return "", errors.New("openlinker: invalid runtime v2 invocation proof input")
	}
	if err := validateIdempotencyKey(request.IdempotencyKey); err != nil {
		return "", err
	}
	bodyDigest := sha256.Sum256(request.Body)
	canonical, err := canonicalRuntimeV2InvocationProof(
		hex.EncodeToString(bodyDigest[:]), contextValue, request.IdempotencyKey, method, path,
	)
	if err != nil {
		return "", err
	}
	key := sha256.Sum256([]byte(runtimeV2InvocationProofDomain + "\x00" + token))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(canonical)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (r *Runtime) CallRuntimeV2Agent(
	ctx context.Context,
	authorization RuntimeV2CallAgentAuthorization,
	request RuntimeV2CallAgentRequest,
) (*RuntimeV2RunSummary, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	if err := r.client.requireRuntime(); err != nil {
		return nil, err
	}
	if err := validateRuntimeV2CallAgentAuthorization(authorization); err != nil {
		return nil, err
	}
	if err := validateRuntimeV2CallAgentRequest(request); err != nil {
		return nil, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("openlinker: encode runtime v2 delegated call: %w", err)
	}
	if int64(len(body)) > RuntimeV2MaxMessageBytes {
		return nil, fmt.Errorf("openlinker: runtime v2 delegated call exceeds %d bytes", RuntimeV2MaxMessageBytes)
	}
	proof, err := BuildRuntimeV2InvocationProof(authorization.AgentInvocationToken, RuntimeV2InvocationProofRequest{
		Method:         http.MethodPost,
		Path:           runtimeV2CallAgentPath,
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
	headers.Set(runtimeV2InvocationHeader, authorization.NodeEnvelope)
	headers.Set(runtimeV2InvocationProofHeader, proof)
	response, err := r.client.newRequestWithTokenAndHeadersBytes(
		ctx, http.MethodPost, runtimeV2CallAgentPath, nil, body, "application/json",
		authorization.AgentInvocationToken, headers,
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, parseRuntimeV2Error(response)
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, errors.New("openlinker: runtime v2 delegated call returned no run summary")
	}
	var summary RuntimeV2RunSummary
	if err := decodeRuntimeV2Response(response.Body, &summary); err != nil {
		return nil, fmt.Errorf("openlinker: decode runtime v2 delegated call: %w", err)
	}
	if err := validateRuntimeV2RunSummary(summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func validateRuntimeV2CallAgentAuthorization(value RuntimeV2CallAgentAuthorization) error {
	if !runtimeV2InvocationCapability(value.NodeEnvelope, "ol_ctx_v2.") ||
		!runtimeV2InvocationCapability(value.AgentInvocationToken, "ol_inv_v2.") {
		return errors.New("openlinker: invalid runtime v2 delegated call authority")
	}
	if err := validateIdempotencyKey(value.IdempotencyKey); err != nil {
		return err
	}
	return nil
}

func validateRuntimeV2CallAgentRequest(value RuntimeV2CallAgentRequest) error {
	if !runtimeV2UUID(value.TargetAgentID) || value.Input == nil ||
		!runtimeV2OptionalText(value.Reason, 500) {
		return errors.New("openlinker: invalid runtime v2 delegated call")
	}
	return nil
}

func validateRuntimeV2RunSummary(value RuntimeV2RunSummary) error {
	if !runtimeV2UUID(value.RunID) || !runtimeV2RunStatus(value.Status) || !runtimeV2DispatchState(value.DispatchState) {
		return errors.New("openlinker: invalid runtime v2 run summary")
	}
	switch value.Status {
	case RuntimeV2RunRunning:
		switch value.DispatchState {
		case RuntimeV2DispatchPending, RuntimeV2DispatchOffered, RuntimeV2DispatchExecuting, RuntimeV2DispatchRetryWait:
		default:
			return errors.New("openlinker: inconsistent runtime v2 running summary")
		}
	case RuntimeV2RunSuccess, RuntimeV2RunTimeout, RuntimeV2RunCanceled:
		if value.DispatchState != RuntimeV2DispatchTerminal {
			return errors.New("openlinker: inconsistent runtime v2 terminal summary")
		}
	case RuntimeV2RunFailed:
		if value.DispatchState != RuntimeV2DispatchTerminal && value.DispatchState != RuntimeV2DispatchDeadLetter {
			return errors.New("openlinker: inconsistent runtime v2 failed summary")
		}
	}
	return nil
}

func runtimeV2InvocationCapability(value, prefix string) bool {
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

func runtimeV2OptionalText(value string, maximum int) bool {
	return value == "" || (utf8.ValidString(value) && utf8.RuneCountInString(value) <= maximum)
}

func canonicalRuntimeV2InvocationProof(bodyDigest, contextValue, idempotencyKey, method, path string) ([]byte, error) {
	canonical := []byte(`{"body_sha256":`)
	var err error
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, bodyDigest)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"context":`...)
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, contextValue)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"idempotency_key":`...)
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, idempotencyKey)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"method":`...)
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, method)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"path":`...)
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, path)
	if err != nil {
		return nil, err
	}
	canonical = append(canonical, `,"version":`...)
	canonical, err = appendRuntimeV2CanonicalJSONString(canonical, runtimeV2InvocationProofDomain)
	if err != nil {
		return nil, err
	}
	return append(canonical, '}'), nil
}

func appendRuntimeV2CanonicalJSONString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, errors.New("openlinker: runtime v2 proof string is not valid Unicode")
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
