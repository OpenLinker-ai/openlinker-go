package openlinker

import (
	"fmt"
	"time"
)

type Error struct {
	StatusCode   int
	Code         string
	Message      string
	Details      any
	RequestID    string
	RetryAfter   time.Duration
	ResponseBody []byte
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("openlinker: request failed with status %d", e.StatusCode)
	}
	return fmt.Sprintf("openlinker: %s: %s", e.Code, e.Message)
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details,omitempty"`
	} `json:"error"`
}
