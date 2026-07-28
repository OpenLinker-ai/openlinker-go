package openlinker

import (
	"encoding/json"
	"errors"
	"strings"
)

const runtimeAuthorityMetadataKey = "_openlinker_runtime_authority"

var errRuntimeAuthorityInvalid = errors.New("runtime authority metadata is invalid")

type runtimeAuthorityWire struct {
	PrincipalScopeID string `json:"principal_scope_id"`
	Source           string `json:"source"`
}

func runtimeAuthorityFromMetadata(
	metadata RuntimeJSONMap,
	identity RuntimeIdentity,
	ready *RuntimeReadyPayload,
) (*RuntimeAuthorityContext, error) {
	if metadata == nil {
		return nil, nil
	}
	value, found := metadata[runtimeAuthorityMetadataKey]
	delete(metadata, runtimeAuthorityMetadataKey)
	if !found {
		return nil, nil
	}
	if ready == nil {
		return nil, errRuntimeAuthorityInvalid
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, errRuntimeAuthorityInvalid
	}
	var wire runtimeAuthorityWire
	if decodeStrictJSON(raw, &wire) != nil {
		return nil, errRuntimeAuthorityInvalid
	}
	if wire.Source != "core" ||
		!validRuntimePrincipalScopeID(wire.PrincipalScopeID) ||
		!validRuntimeID(identity.RuntimeSessionID) ||
		identity.SessionEpoch < 1 ||
		!validRuntimeID(ready.AttachmentID) {
		return nil, errRuntimeAuthorityInvalid
	}
	return &RuntimeAuthorityContext{
		PrincipalScopeID:    wire.PrincipalScopeID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		RuntimeSessionEpoch: identity.SessionEpoch,
		RuntimeAttachmentID: ready.AttachmentID,
	}, nil
}

func validRuntimePrincipalScopeID(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}
