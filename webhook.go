package openlinker

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const defaultTaskCallbackSecretBytes = 32

// WebhookRunCallbackOptions configures an external task callback endpoint.
type WebhookRunCallbackOptions struct {
	Secret         string
	Token          string
	Authentication *TaskCallbackAuthentication
	Metadata       any
	EventTypes     []string
}

// NewWebhookRunCallback builds a caller-owned task callback config for an
// external webhook endpoint. When Secret is blank, the SDK generates one.
func NewWebhookRunCallback(url string, opts WebhookRunCallbackOptions) (*TaskCallbackConfig, error) {
	trimmedURL := strings.TrimSpace(url)
	if trimmedURL == "" {
		return nil, errors.New("openlinker: task callback URL is required")
	}
	secret := strings.TrimSpace(opts.Secret)
	if secret == "" {
		generated, err := GenerateTaskCallbackSecret()
		if err != nil {
			return nil, err
		}
		secret = generated
	}
	return &TaskCallbackConfig{
		URL:            trimmedURL,
		Token:          strings.TrimSpace(opts.Token),
		Secret:         secret,
		Authentication: opts.Authentication,
		Metadata:       opts.Metadata,
		EventTypes:     append([]string{}, opts.EventTypes...),
	}, nil
}

// GenerateTaskCallbackSecret returns a 32-byte random secret encoded as hex.
func GenerateTaskCallbackSecret() (string, error) {
	b := make([]byte, defaultTaskCallbackSecretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("openlinker: generate task callback secret: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// SignTaskCallbackPayload signs the raw webhook payload with HMAC-SHA256.
func SignTaskCallbackPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyTaskCallbackSignature verifies an X-OpenLinker-Signature value.
// The signature may be either raw hex or prefixed with "sha256=".
func VerifyTaskCallbackSignature(payload []byte, secret, signature string) bool {
	expected := normalizeTaskCallbackSignature(SignTaskCallbackPayload(payload, secret))
	actual := normalizeTaskCallbackSignature(signature)
	if expected == "" || actual == "" {
		return false
	}
	expectedBytes, err := hex.DecodeString(expected)
	if err != nil {
		return false
	}
	actualBytes, err := hex.DecodeString(actual)
	if err != nil {
		return false
	}
	if len(expectedBytes) != len(actualBytes) {
		return false
	}
	return subtle.ConstantTimeCompare(expectedBytes, actualBytes) == 1
}

// TaskCallbackSignatureFromHeader returns the OpenLinker callback signature.
func TaskCallbackSignatureFromHeader(header http.Header) string {
	return header.Get("X-OpenLinker-Signature")
}

// VerifyTaskCallbackRequest reads and verifies an incoming callback request.
// It restores r.Body before returning so handlers can decode the same body.
func VerifyTaskCallbackRequest(r *http.Request, secret string, maxBytes int64) ([]byte, bool, error) {
	if r == nil {
		return nil, false, errors.New("openlinker: task callback request is nil")
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	body, err := readLimitedBody(r.Body, maxBytes)
	if err != nil {
		return nil, false, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	signature := TaskCallbackSignatureFromHeader(r.Header)
	if signature == "" {
		return body, false, nil
	}
	return body, VerifyTaskCallbackSignature(body, secret, signature), nil
}

func readLimitedBody(body io.Reader, maxBytes int64) ([]byte, error) {
	if body == nil {
		return []byte{}, nil
	}
	limited := io.LimitReader(body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("openlinker: read task callback request body: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("openlinker: task callback request body exceeds %d bytes", maxBytes)
	}
	return data, nil
}

func normalizeTaskCallbackSignature(signature string) string {
	trimmed := strings.ToLower(strings.TrimSpace(signature))
	return strings.TrimPrefix(trimmed, "sha256=")
}
