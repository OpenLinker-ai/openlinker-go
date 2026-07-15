package exampleutil

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestFirstNonEmpty(t *testing.T) {
	if got := FirstNonEmpty("", "  ", " value ", "later"); got != "value" {
		t.Fatalf("FirstNonEmpty() = %q, want value", got)
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("OPENLINKER_EXAMPLE_BOOL", "true")
	if !EnvBool("OPENLINKER_EXAMPLE_BOOL") {
		t.Fatal("EnvBool() = false, want true")
	}

	t.Setenv("OPENLINKER_EXAMPLE_BOOL", "invalid")
	if EnvBool("OPENLINKER_EXAMPLE_BOOL") {
		t.Fatal("EnvBool() = true for invalid value")
	}
}

func TestRequiredEnvAndDuration(t *testing.T) {
	t.Setenv("OPENLINKER_EXAMPLE_REQUIRED", " value ")
	if got, err := RequiredEnv("OPENLINKER_EXAMPLE_REQUIRED"); err != nil || got != "value" {
		t.Fatalf("RequiredEnv() = %q, %v", got, err)
	}
	t.Setenv("OPENLINKER_EXAMPLE_MISSING", "")
	if _, err := RequiredEnv("OPENLINKER_EXAMPLE_MISSING"); err == nil {
		t.Fatal("RequiredEnv() accepted missing value")
	}

	t.Setenv("OPENLINKER_EXAMPLE_DURATION", "250ms")
	if got, err := EnvDuration("OPENLINKER_EXAMPLE_DURATION", time.Second); err != nil || got != 250*time.Millisecond {
		t.Fatalf("EnvDuration() = %v, %v", got, err)
	}
	t.Setenv("OPENLINKER_EXAMPLE_DURATION", "invalid")
	if _, err := EnvDuration("OPENLINKER_EXAMPLE_DURATION", time.Second); err == nil {
		t.Fatal("EnvDuration() accepted invalid value")
	}
}

func TestPrintJSONAndTerminalStatus(t *testing.T) {
	var output bytes.Buffer
	if err := PrintJSON(&output, map[string]string{"text": "你好"}); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "你好") || strings.Contains(got, `\u4f60`) {
		t.Fatalf("PrintJSON() = %q", got)
	}
	for _, status := range []string{"success", "completed", "failed", "canceled"} {
		if !IsTerminalRunStatus(status) {
			t.Fatalf("IsTerminalRunStatus(%q) = false", status)
		}
	}
	if IsTerminalRunStatus("running") {
		t.Fatal("running is terminal")
	}
}
