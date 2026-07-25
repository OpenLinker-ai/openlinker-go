package openlinker

import "encoding/json"

const runtimeAuthorityMetadataKey = "_openlinker_runtime_authority"

type runtimeAuthorityWire struct {
	PrincipalScopeID string `json:"principal_scope_id"`
	Source           string `json:"source"`
}

func runtimeAuthorityFromMetadata(
	metadata RuntimeJSONMap,
	identity RuntimeIdentity,
	ready *RuntimeReadyPayload,
) *RuntimeAuthorityContext {
	if metadata == nil {
		return nil
	}
	value, found := metadata[runtimeAuthorityMetadataKey]
	delete(metadata, runtimeAuthorityMetadataKey)
	if !found || ready == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var wire runtimeAuthorityWire
	if decodeStrictJSON(raw, &wire) != nil ||
		wire.Source != "core" ||
		!validRuntimeID(wire.PrincipalScopeID) ||
		!validRuntimeID(identity.RuntimeSessionID) ||
		identity.SessionEpoch < 1 ||
		!validRuntimeID(ready.AttachmentID) {
		return nil
	}
	return &RuntimeAuthorityContext{
		PrincipalScopeID:    wire.PrincipalScopeID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		RuntimeSessionEpoch: identity.SessionEpoch,
		RuntimeAttachmentID: ready.AttachmentID,
	}
}
