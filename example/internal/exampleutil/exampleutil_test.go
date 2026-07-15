package exampleutil

import "testing"

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
