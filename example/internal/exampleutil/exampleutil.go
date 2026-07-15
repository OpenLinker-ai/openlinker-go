// Package exampleutil contains boilerplate shared by examples. It deliberately
// does not wrap the OpenLinker SDK calls that each example is intended to teach.
package exampleutil

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

// SignalContext returns a context canceled by SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// FirstNonEmpty returns the first non-empty trimmed value.
func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// EnvBool reports whether key contains a value accepted by strconv.ParseBool.
func EnvBool(key string) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}
