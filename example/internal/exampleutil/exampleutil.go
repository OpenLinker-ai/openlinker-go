// Package exampleutil contains boilerplate shared by examples. It deliberately
// does not wrap the OpenLinker SDK calls that each example is intended to teach.
package exampleutil

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
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

// RequiredEnv returns a trimmed environment value or a configuration error.
func RequiredEnv(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return "", fmt.Errorf("缺少必填环境变量 %s", key)
	}
	return value, nil
}

// EnvDuration reads a duration such as 30s or 2m and uses fallback when empty.
func EnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("环境变量 %s 必须是正数 duration: %q", key, value)
	}
	return duration, nil
}

// PrintJSON writes indented JSON to output.
func PrintJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

// IsTerminalRunStatus reports whether a Client Run no longer needs polling.
func IsTerminalRunStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success", "completed", "failed", "canceled", "cancelled":
		return true
	default:
		return false
	}
}

// SplitCSV returns trimmed non-empty comma-separated values.
func SplitCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

// NewUUID creates a random RFC 4122 version 4 UUID for low-level examples.
func NewUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
