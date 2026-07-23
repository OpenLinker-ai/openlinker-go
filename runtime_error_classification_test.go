package openlinker

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
)

func TestRuntimePermanentErrorClassifierIsConservative(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
	}{
		{
			name: "canonical HTTP authentication failure",
			err: &Error{
				StatusCode: http.StatusUnauthorized,
				Code:       "UNAUTHORIZED",
				Message:    "Runtime authentication failed",
			},
			permanent: true,
		},
		{
			name: "canonical WebSocket authentication close",
			err: fmt.Errorf("wrapped: %w", &websocket.CloseError{
				Code: RuntimeWSCloseAuthenticationFailed,
				Text: "UNAUTHORIZED",
			}),
			permanent: true,
		},
		{
			name: "unknown auth-like error remains recoverable",
			err: &Error{
				StatusCode: http.StatusUnauthorized,
				Code:       "AUTH_BACKEND_BUSY",
				Message:    "authentication backend temporarily unavailable",
			},
			permanent: false,
		},
		{
			name: "unknown WebSocket close remains recoverable",
			err: fmt.Errorf("wrapped: %w", &websocket.CloseError{
				Code: 4499,
				Text: "AUTH_BACKEND_BUSY",
			}),
			permanent: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := runtimeErrorIsPermanent(test.err); got != test.permanent {
				t.Fatalf("runtimeErrorIsPermanent() = %t, want %t", got, test.permanent)
			}
		})
	}
}
