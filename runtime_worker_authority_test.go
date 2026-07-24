package openlinker

import "testing"

const (
	testAuthorityWorkerID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testAuthoritySession  = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func TestRuntimeAuthorityUsesCoreOwnedMetadataAndWorkerIdentity(t *testing.T) {
	metadata := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id": testAgentID,
			"source":             "core",
		},
		"tenant": "seller-research",
	}
	authority := runtimeAuthorityFromMetadata(metadata, RuntimeIdentity{
		WorkerID:         testAuthorityWorkerID,
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     7,
	}, &RuntimeReadyPayload{AttachmentID: testAttachmentID})

	if authority == nil {
		t.Fatal("Core-owned authority was rejected")
	}
	if authority.PrincipalScopeID != testAgentID ||
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
	authority := runtimeAuthorityFromMetadata(metadata, RuntimeIdentity{
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     1,
	}, &RuntimeReadyPayload{AttachmentID: testAttachmentID})

	if authority != nil {
		t.Fatalf("caller-owned authority was accepted: %#v", authority)
	}
	if _, exists := metadata[runtimeAuthorityMetadataKey]; exists {
		t.Fatalf("untrusted authority leaked into handler metadata: %#v", metadata)
	}
}
