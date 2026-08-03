package openlinker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

const runtimeAuthorityMetadataKey = "_openlinker_runtime_authority"

var errRuntimeAuthorityInvalid = errors.New("runtime authority metadata is invalid")

type runtimeAuthorityWire struct {
	PrincipalScopeID                   string   `json:"principal_scope_id"`
	Source                             string   `json:"source"`
	ExecutionProfile                   string   `json:"execution_profile,omitempty"`
	BrowserInteractionPolicy           string   `json:"browser_interaction_policy,omitempty"`
	BrowserInteractionPolicyGeneration int64    `json:"browser_interaction_policy_generation,omitempty"`
	BrowserMutationOrigins             []string `json:"browser_mutation_origins,omitempty"`
	BrowserMutationOriginsSHA256       string   `json:"browser_mutation_origins_sha256,omitempty"`
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
	if err := validateRuntimeAuthorityWire(&wire); err != nil {
		return nil, errRuntimeAuthorityInvalid
	}
	return &RuntimeAuthorityContext{
		PrincipalScopeID:                   wire.PrincipalScopeID,
		RuntimeSessionID:                   identity.RuntimeSessionID,
		RuntimeSessionEpoch:                identity.SessionEpoch,
		RuntimeAttachmentID:                ready.AttachmentID,
		ExecutionProfile:                   wire.ExecutionProfile,
		BrowserInteractionPolicy:           wire.BrowserInteractionPolicy,
		BrowserInteractionPolicyGeneration: wire.BrowserInteractionPolicyGeneration,
		BrowserMutationOrigins:             append([]string{}, wire.BrowserMutationOrigins...),
		BrowserMutationOriginsSHA256:       wire.BrowserMutationOriginsSHA256,
	}, nil
}

func validateRuntimeAuthorityWire(wire *runtimeAuthorityWire) error {
	if wire == nil {
		return errRuntimeAuthorityInvalid
	}
	switch wire.ExecutionProfile {
	case "":
		// Compatibility for non-Browser assignments produced before execution
		// profile authority was introduced. Browser-specific fields must never
		// be accepted on that legacy shape.
		if wire.BrowserInteractionPolicy != "" ||
			wire.BrowserInteractionPolicyGeneration != 0 ||
			wire.BrowserMutationOrigins != nil ||
			wire.BrowserMutationOriginsSHA256 != "" {
			return errRuntimeAuthorityInvalid
		}
		return nil
	case "standard":
		if wire.BrowserInteractionPolicy != "" ||
			wire.BrowserInteractionPolicyGeneration != 0 ||
			wire.BrowserMutationOrigins != nil ||
			wire.BrowserMutationOriginsSHA256 != "" {
			return errRuntimeAuthorityInvalid
		}
		return nil
	case "browser":
	default:
		return errRuntimeAuthorityInvalid
	}

	if wire.BrowserInteractionPolicyGeneration < 1 ||
		wire.BrowserMutationOrigins == nil ||
		len(wire.BrowserMutationOrigins) > 32 ||
		len(wire.BrowserMutationOriginsSHA256) != sha256.Size*2 {
		return errRuntimeAuthorityInvalid
	}
	canonical, digest, err := canonicalRuntimeBrowserMutationOrigins(
		wire.BrowserMutationOrigins,
	)
	if err != nil || digest != wire.BrowserMutationOriginsSHA256 ||
		len(canonical) != len(wire.BrowserMutationOrigins) {
		return errRuntimeAuthorityInvalid
	}
	for index := range canonical {
		if canonical[index] != wire.BrowserMutationOrigins[index] {
			return errRuntimeAuthorityInvalid
		}
	}
	switch wire.BrowserInteractionPolicy {
	case "restricted":
		if len(canonical) != 0 {
			return errRuntimeAuthorityInvalid
		}
	case "full":
		if len(canonical) == 0 {
			return errRuntimeAuthorityInvalid
		}
	default:
		return errRuntimeAuthorityInvalid
	}
	return nil
}

func canonicalRuntimeBrowserMutationOrigins(origins []string) ([]string, string, error) {
	if origins == nil || len(origins) > 32 {
		return nil, "", errRuntimeAuthorityInvalid
	}
	canonical := make([]string, 0, len(origins))
	seen := make(map[string]struct{}, len(origins))
	for _, raw := range origins {
		origin, err := canonicalRuntimeBrowserMutationOrigin(raw)
		if err != nil {
			return nil, "", err
		}
		if _, exists := seen[origin]; exists {
			continue
		}
		seen[origin] = struct{}{}
		canonical = append(canonical, origin)
	}
	sort.Strings(canonical)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", errRuntimeAuthorityInvalid
	}
	digest := sha256.Sum256(encoded)
	return canonical, hex.EncodeToString(digest[:]), nil
}

func canonicalRuntimeBrowserMutationOrigin(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.Contains(raw, "%") {
		return "", errRuntimeAuthorityInvalid
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.Host == "" || parsed.Path != "" ||
		parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.ForceQuery {
		return "", errRuntimeAuthorityInvalid
	}
	host := parsed.Hostname()
	if host == "" || strings.HasSuffix(host, ".") {
		return "", errRuntimeAuthorityInvalid
	}
	port := parsed.Port()
	if port != "" {
		portNumber, parseErr := strconv.ParseUint(port, 10, 16)
		if parseErr != nil || portNumber == 0 {
			return "", errRuntimeAuthorityInvalid
		}
		if portNumber == 443 {
			port = ""
		} else {
			port = strconv.FormatUint(portNumber, 10)
		}
	}

	canonicalHost := ""
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		if address.Is4In6() {
			return "", errRuntimeAuthorityInvalid
		}
		if address.Is6() {
			canonicalHost = "[" + address.String() + "]"
		} else {
			canonicalHost = address.String()
		}
	} else {
		ascii, idnaErr := idna.Lookup.ToASCII(strings.ToLower(host))
		if idnaErr != nil || !validRuntimeAuthorityDomain(ascii) ||
			ambiguousRuntimeAuthorityIPv4Domain(ascii) {
			return "", errRuntimeAuthorityInvalid
		}
		canonicalHost = strings.ToLower(ascii)
	}
	if port != "" {
		canonicalHost += ":" + port
	}
	return "https://" + canonicalHost, nil
}

// ambiguousRuntimeAuthorityIPv4Domain rejects legacy WHATWG IPv4 spellings
// before Chromium can reinterpret a value that the authority layer treated as
// DNS. Keep this in lockstep with Core and Browser protocol canonicalization.
func ambiguousRuntimeAuthorityIPv4Domain(host string) bool {
	parts := strings.Split(strings.ToLower(host), ".")
	if len(parts) == 0 || len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		digits := part
		base := byte('0')
		if strings.HasPrefix(part, "0x") {
			digits = part[2:]
			base = 'a'
		}
		if digits == "" {
			return false
		}
		for index := 0; index < len(digits); index++ {
			character := digits[index]
			if character >= '0' && character <= '9' {
				continue
			}
			if base == 'a' && character >= 'a' && character <= 'f' {
				continue
			}
			return false
		}
	}
	return true
}

func validRuntimeAuthorityDomain(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character >= 'a' && character <= 'z') ||
				(character >= '0' && character <= '9') || character == '-' {
				continue
			}
			return false
		}
	}
	return true
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
