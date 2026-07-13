package main

import (
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestInputPhaseAcceptsRuntimeJSONMap(t *testing.T) {
	t.Parallel()

	if got := inputPhase(openlinker.RuntimeJSONMap{"phase": "core-a-to-core-b-inflight"}); got != "core-a-to-core-b-inflight" {
		t.Fatalf("inputPhase() = %q", got)
	}
}
