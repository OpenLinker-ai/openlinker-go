package openlinker

import (
	"errors"
	"reflect"
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

func TestRuntimeAuthorityAcceptsCompleteBrowserPolicy(t *testing.T) {
	origins := []string{"https://example.com", "https://xn--bcher-kva.example:8443"}
	_, digest, err := canonicalRuntimeBrowserMutationOrigins(origins)
	if err != nil {
		t.Fatalf("canonical origins: %v", err)
	}
	metadata := RuntimeJSONMap{
		runtimeAuthorityMetadataKey: map[string]any{
			"principal_scope_id":                    testPrincipalScopeID,
			"source":                                "core",
			"execution_profile":                     "browser",
			"browser_interaction_policy":            "full",
			"browser_interaction_policy_generation": int64(7),
			"browser_mutation_origins":              origins,
			"browser_mutation_origins_sha256":       digest,
		},
	}
	authority, err := runtimeAuthorityFromMetadata(metadata, RuntimeIdentity{
		RuntimeSessionID: testAuthoritySession,
		SessionEpoch:     3,
	}, &RuntimeReadyPayload{AttachmentID: testAttachmentID})
	if err != nil {
		t.Fatalf("Browser authority error = %v", err)
	}
	if authority == nil || authority.ExecutionProfile != "browser" ||
		authority.BrowserInteractionPolicy != "full" ||
		authority.BrowserInteractionPolicyGeneration != 7 ||
		authority.BrowserMutationOriginsSHA256 != digest ||
		!reflect.DeepEqual(authority.BrowserMutationOrigins, origins) {
		t.Fatalf("Browser authority = %#v", authority)
	}
	if _, exists := metadata[runtimeAuthorityMetadataKey]; exists {
		t.Fatalf("Browser authority leaked into handler metadata: %#v", metadata)
	}
}

func TestRuntimeAuthorityRejectsIncompleteOrBroadenedBrowserPolicy(t *testing.T) {
	canonical := []string{"https://example.com"}
	_, digest, err := canonicalRuntimeBrowserMutationOrigins(canonical)
	if err != nil {
		t.Fatalf("canonical origins: %v", err)
	}
	valid := map[string]any{
		"principal_scope_id":                    testPrincipalScopeID,
		"source":                                "core",
		"execution_profile":                     "browser",
		"browser_interaction_policy":            "full",
		"browser_interaction_policy_generation": int64(1),
		"browser_mutation_origins":              canonical,
		"browser_mutation_origins_sha256":       digest,
	}
	tests := map[string]func(map[string]any){
		"missing policy": func(value map[string]any) {
			delete(value, "browser_interaction_policy")
		},
		"zero generation": func(value map[string]any) {
			value["browser_interaction_policy_generation"] = int64(0)
		},
		"digest mismatch": func(value map[string]any) {
			value["browser_mutation_origins_sha256"] = strings.Repeat("0", 64)
		},
		"noncanonical origin": func(value map[string]any) {
			value["browser_mutation_origins"] = []string{"https://EXAMPLE.com:443"}
		},
		"origin path": func(value map[string]any) {
			value["browser_mutation_origins"] = []string{"https://example.com/path"}
		},
		"restricted with origin": func(value map[string]any) {
			value["browser_interaction_policy"] = "restricted"
		},
		"full without origin": func(value map[string]any) {
			value["browser_mutation_origins"] = []string{}
			_, value["browser_mutation_origins_sha256"], _ = canonicalRuntimeBrowserMutationOrigins([]string{})
		},
	}
	identity := RuntimeIdentity{RuntimeSessionID: testAuthoritySession, SessionEpoch: 1}
	ready := &RuntimeReadyPayload{AttachmentID: testAttachmentID}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := make(map[string]any, len(valid))
			for key, item := range valid {
				value[key] = item
			}
			mutate(value)
			metadata := RuntimeJSONMap{runtimeAuthorityMetadataKey: value}
			authority, err := runtimeAuthorityFromMetadata(metadata, identity, ready)
			if !errors.Is(err, errRuntimeAuthorityInvalid) || authority != nil {
				t.Fatalf("authority = %#v, error = %v", authority, err)
			}
			if _, exists := metadata[runtimeAuthorityMetadataKey]; exists {
				t.Fatalf("invalid authority leaked: %#v", metadata)
			}
		})
	}
}

func TestCanonicalRuntimeBrowserMutationOriginsIsDeterministicAndBounded(t *testing.T) {
	canonical, digest, err := canonicalRuntimeBrowserMutationOrigins([]string{
		"https://xn--bcher-kva.example:8443",
		"https://example.com",
		"https://example.com",
	})
	if err != nil {
		t.Fatalf("canonical origins: %v", err)
	}
	want := []string{"https://example.com", "https://xn--bcher-kva.example:8443"}
	if !reflect.DeepEqual(canonical, want) || len(digest) != 64 {
		t.Fatalf("canonical = %#v, digest = %q", canonical, digest)
	}
	invalid := []string{
		"http://example.com",
		"https://user@example.com",
		"https://example.com/",
		"https://example.com?query=1",
		"https://example.com#fragment",
		"https://*.example.com",
		"https://example.com.",
		"https://example.com%2fcollector",
		"https://127.1",
		"https://2130706433",
		"https://0x7f.1",
		"https://0177.1",
		"https://09.1",
		"https://1.2.3.999",
		"https://１２７。１",
		"https://[::ffff:127.0.0.1]",
		"https://[::ffff:7f00:1]",
	}
	for _, origin := range invalid {
		if _, _, err := canonicalRuntimeBrowserMutationOrigins([]string{origin}); err == nil {
			t.Fatalf("invalid origin %q was accepted", origin)
		}
	}
	canonicalPorts, _, err := canonicalRuntimeBrowserMutationOrigins([]string{
		"HTTPS://BÜCHER.example:0443",
		"https://example.com:08443",
	})
	if err != nil || !reflect.DeepEqual(canonicalPorts, []string{
		"https://example.com:8443",
		"https://xn--bcher-kva.example",
	}) {
		t.Fatalf("IDNA/default-port origins = %#v, %v", canonicalPorts, err)
	}
	tooMany := make([]string, 33)
	for index := range tooMany {
		tooMany[index] = "https://" + strings.Repeat("a", index/26+1) + string(rune('a'+index%26)) + ".example"
	}
	if _, _, err := canonicalRuntimeBrowserMutationOrigins(tooMany); err == nil {
		t.Fatal("more than 32 origins were accepted")
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
