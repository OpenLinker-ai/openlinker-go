package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestHandlerVerifiesRawBodyBeforeDecoding(t *testing.T) {
	const secret = "callback-secret"
	body := []byte(`{"event_type":"run.completed","run_id":"run-1"}`)
	var output bytes.Buffer
	handler := newHandler(secret, &output)
	request := httptest.NewRequest(http.MethodPost, "/callbacks/openlinker", bytes.NewReader(body))
	request.Header.Set("X-OpenLinker-Signature", "sha256="+openlinker.SignTaskCallbackPayload(body, secret))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !strings.Contains(output.String(), `"verified": true`) || !strings.Contains(output.String(), "run.completed") {
		t.Fatalf("status=%d output=%s body=%s", response.Code, output.String(), response.Body.String())
	}
}

func TestHandlerRejectsInvalidSignature(t *testing.T) {
	var output bytes.Buffer
	request := httptest.NewRequest(http.MethodPost, "/callbacks/openlinker", strings.NewReader(`{"event_type":"run.failed"}`))
	request.Header.Set("X-OpenLinker-Signature", "sha256=deadbeef")
	response := httptest.NewRecorder()
	newHandler("callback-secret", &output).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || output.Len() != 0 {
		t.Fatalf("status=%d output=%q", response.Code, output.String())
	}
}
