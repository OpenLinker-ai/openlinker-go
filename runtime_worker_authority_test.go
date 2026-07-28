package openlinker

import (
	"errors"
	"strings"
	"testing"
)

const (
	testAuthorityWorkerID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testAuthoritySession  = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	testPrincipalScopeID  = "ps1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

func TestRuntimeAuthorityUsesCoreOwnedMetadataAndWorkerIdentity(t *testing.T) {
	metadata := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id": testPrincipalScopeID,
			"source":             "core",
		},
		"tenant": "seller-research",
	}
	authority, err := runtimeAuthorityFromMetadata(metadata, RuntimeIdentity{
		WorkerID:         testAuthorityWorkerID,
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     7,
	}, &RuntimeReadyPayload{AttachmentID: testAttachmentID})

	if err != nil {
		t.Fatalf("Core-owned authority error = %v", err)
	}
	if authority == nil {
		t.Fatal("Core-owned authority was rejected")
	}
	if authority.PrincipalScopeID != testPrincipalScopeID ||
		authority.RuntimeSessionID != testAuthoritySession ||
		authority.RuntimeSessionEpoch != 7 ||
		authority.RuntimeAttachmentID != testAttachmentID {
		t.Fatalf("authority = %#v", authority)
	}
	if _, exists := metadata[runtimeAuthorityMetadataKey]; exists {
		t.Fatalf("private authority leaked into handler metadata: %#v", metadata)
	}
	if metadata["tenant"] != "seller-research" {
		t.Fatalf("ordinary metadata was changed: %#v", metadata)
	}
}

func TestRuntimeAuthorityRejectsCallerSourceAndStillRedacts(t *testing.T) {
	metadata := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id": testAgentID,
			"source":             "caller",
		},
	}
	authority, err := runtimeAuthorityFromMetadata(metadata, RuntimeIdentity{
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     1,
	}, &RuntimeReadyPayload{AttachmentID: testAttachmentID})

	if !errors.Is(err, errRuntimeAuthorityInvalid) {
		t.Fatalf("caller-owned authority error = %v, want invalid authority", err)
	}
	if authority != nil {
		t.Fatalf("caller-owned authority was accepted: %#v", authority)
	}
	if _, exists := metadata[runtimeAuthorityMetadataKey]; exists {
		t.Fatalf("untrusted authority leaked into handler metadata: %#v", metadata)
	}
}

func TestRuntimeAuthorityAcceptsOpaqueScopeAndRejectsUnknownField(t *testing.T) {
	identity := RuntimeIdentity{
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     1,
	}
	ready := &RuntimeReadyPayload{AttachmentID: testAttachmentID}
	legacy := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id": testPrincipalScopeID,
			"source":             "core",
		},
	}
	authority, err := runtimeAuthorityFromMetadata(legacy, identity, ready)
	if err != nil {
		t.Fatalf("legacy authority error = %v", err)
	}
	if authority == nil || authority.PrincipalScopeID != testPrincipalScopeID {
		t.Fatalf("legacy authority = %#v", authority)
	}
	unknown := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id": testPrincipalScopeID,
			"source":             "core",
			"execution_profile":  "privileged",
		},
	}
	authority, err = runtimeAuthorityFromMetadata(unknown, identity, ready)
	if !errors.Is(err, errRuntimeAuthorityInvalid) {
		t.Fatalf("unknown execution profile error = %v, want invalid authority", err)
	}
	if authority != nil {
		t.Fatalf("unknown execution profile was accepted: %#v", authority)
	}
	if _, exists := unknown[runtimeAuthorityMetadataKey]; exists {
		t.Fatalf("unknown execution profile leaked into handler metadata: %#v", unknown)
	}
}

func TestRuntimePrincipalScopeIDUsesTheOpaqueRuntimeContract(t *testing.T) {
	for _, value := range []string{
		testPrincipalScopeID,
		"tenant.scope:agent-1",
		strings.Repeat("a", 256),
	} {
		if !validRuntimePrincipalScopeID(value) {
			t.Fatalf("valid principal scope %q was rejected", value)
		}
	}
	for _, value := range []string{
		"",
		" leading",
		"trailing ",
		"contains/slash",
		strings.Repeat("a", 257),
	} {
		if validRuntimePrincipalScopeID(value) {
			t.Fatalf("invalid principal scope %q was accepted", value)
		}
	}
}
