package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const RuntimeMaxExtensionRoutes = 16

// RuntimeExtensionRoute registers one optional WebSocket extension without
// assigning its product semantics to the SDK. CommandType is a server push;
// RequestType and ReplyType form one correlated client request.
type RuntimeExtensionRoute struct {
	CommandType RuntimeMessageType
	RequestType RuntimeMessageType
	ReplyType   RuntimeMessageType
}

// RuntimeExtensionCommand is an Attempt-scoped, opaque server push. The SDK
// validates only the standard Attempt identity needed for safe routing. The
// extension owner must strictly decode and validate Payload.
type RuntimeExtensionCommand struct {
	Type            RuntimeMessageType
	AttemptIdentity RuntimeAttemptIdentity
	Payload         json.RawMessage
}

// RuntimeExtensionRequest is an opaque, registered WebSocket request. Payload
// must be a JSON object containing the current attempt_identity.
type RuntimeExtensionRequest struct {
	Type    RuntimeMessageType
	Payload json.RawMessage
}

// RuntimeExtensionReply is the exact registered response to a request.
// Extension-specific acknowledgement validation remains with the owner.
type RuntimeExtensionReply struct {
	Type    RuntimeMessageType
	Payload json.RawMessage
}

// RuntimeExtensions is an Attempt-scoped transient extension channel. Its
// messages are never added to the durable Runtime event spool.
type RuntimeExtensions struct {
	commands <-chan RuntimeExtensionCommand
	publish  func(context.Context, RuntimeExtensionRequest) (*RuntimeExtensionReply, error)
}

func (extensions *RuntimeExtensions) Commands() <-chan RuntimeExtensionCommand {
	if extensions == nil {
		return nil
	}
	return extensions.commands
}

func (extensions *RuntimeExtensions) Publish(
	ctx context.Context,
	request RuntimeExtensionRequest,
) (*RuntimeExtensionReply, error) {
	if extensions == nil || extensions.publish == nil {
		return nil, errors.New("openlinker: Runtime extension transport is unavailable")
	}
	return extensions.publish(ctx, request)
}

type runtimeExtensionRegistry struct {
	routesByCommand map[RuntimeMessageType]RuntimeExtensionRoute
	routesByRequest map[RuntimeMessageType]RuntimeExtensionRoute
	routesByReply   map[RuntimeMessageType]RuntimeExtensionRoute
}

func normalizeRuntimeExtensionRoutes(
	routes []RuntimeExtensionRoute,
) ([]RuntimeExtensionRoute, *runtimeExtensionRegistry, error) {
	if len(routes) > RuntimeMaxExtensionRoutes {
		return nil, nil, fmt.Errorf(
			"runtime extension routes must not exceed %d",
			RuntimeMaxExtensionRoutes,
		)
	}
	normalized := append([]RuntimeExtensionRoute(nil), routes...)
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].CommandType < normalized[right].CommandType
	})
	registry := &runtimeExtensionRegistry{
		routesByCommand: make(map[RuntimeMessageType]RuntimeExtensionRoute, len(routes)),
		routesByRequest: make(map[RuntimeMessageType]RuntimeExtensionRoute, len(routes)),
		routesByReply:   make(map[RuntimeMessageType]RuntimeExtensionRoute, len(routes)),
	}
	seen := make(map[RuntimeMessageType]struct{}, len(routes)*3)
	for _, route := range normalized {
		for _, messageType := range []RuntimeMessageType{
			route.CommandType,
			route.RequestType,
			route.ReplyType,
		} {
			if !runtimeExtensionMessageType(messageType) ||
				runtimeWSMessageType(messageType) {
				return nil, nil, errors.New("runtime extension message type is invalid or reserved")
			}
			if _, duplicate := seen[messageType]; duplicate {
				return nil, nil, errors.New("runtime extension message types must be unique")
			}
			seen[messageType] = struct{}{}
		}
		registry.routesByCommand[route.CommandType] = route
		registry.routesByRequest[route.RequestType] = route
		registry.routesByReply[route.ReplyType] = route
	}
	return normalized, registry, nil
}

func runtimeExtensionMessageType(value RuntimeMessageType) bool {
	text := string(value)
	if len(text) < 3 || len(text) > 100 || text[0] == '.' ||
		text[len(text)-1] == '.' ||
		strings.HasPrefix(text, "runtime.") ||
		strings.HasPrefix(text, "run.") {
		return false
	}
	hasDot := false
	for _, character := range text {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case character == '_', character == '-':
		case character == '.':
			hasDot = true
		default:
			return false
		}
	}
	return hasDot
}

func (registry *runtimeExtensionRegistry) command(
	messageType RuntimeMessageType,
) (RuntimeExtensionRoute, bool) {
	if registry == nil {
		return RuntimeExtensionRoute{}, false
	}
	route, ok := registry.routesByCommand[messageType]
	return route, ok
}

func (registry *runtimeExtensionRegistry) request(
	messageType RuntimeMessageType,
) (RuntimeExtensionRoute, bool) {
	if registry == nil {
		return RuntimeExtensionRoute{}, false
	}
	route, ok := registry.routesByRequest[messageType]
	return route, ok
}

func (registry *runtimeExtensionRegistry) reply(
	messageType RuntimeMessageType,
) (RuntimeExtensionRoute, bool) {
	if registry == nil {
		return RuntimeExtensionRoute{}, false
	}
	route, ok := registry.routesByReply[messageType]
	return route, ok
}

func decodeRuntimeExtensionCommand(
	command RuntimePendingCommand,
	registry *runtimeExtensionRegistry,
) (RuntimeDecodedPendingCommand, error) {
	if _, registered := registry.command(command.Type); !registered {
		return RuntimeDecodedPendingCommand{}, errors.New("openlinker: unknown runtime command type")
	}
	identity, err := runtimeExtensionAttemptIdentity(command.Payload)
	if err != nil {
		return RuntimeDecodedPendingCommand{}, err
	}
	payload := append(json.RawMessage(nil), command.Payload...)
	return RuntimeDecodedPendingCommand{
		Type: command.Type,
		Extension: &RuntimeExtensionCommand{
			Type:            command.Type,
			AttemptIdentity: identity,
			Payload:         payload,
		},
	}, nil
}

func runtimeExtensionAttemptIdentity(
	payload json.RawMessage,
) (RuntimeAttemptIdentity, error) {
	var object map[string]json.RawMessage
	if len(payload) == 0 ||
		json.Unmarshal(payload, &object) != nil ||
		object == nil {
		return RuntimeAttemptIdentity{}, errors.New(
			"openlinker: Runtime extension payload must be a JSON object",
		)
	}
	rawIdentity, ok := object["attempt_identity"]
	if !ok {
		return RuntimeAttemptIdentity{}, errors.New(
			"openlinker: Runtime extension payload requires attempt_identity",
		)
	}
	var identity RuntimeAttemptIdentity
	// attempt_identity always carries the complete standard Runtime identity,
	// but an extension may bind additional product-owned evidence to that same
	// identity object. The SDK validates every standard field below and leaves
	// those additional fields to the extension owner's strict payload decoder.
	if err := json.Unmarshal(rawIdentity, &identity); err != nil {
		return RuntimeAttemptIdentity{}, errors.New(
			"openlinker: Runtime extension attempt_identity is invalid",
		)
	}
	if err := validateRuntimeAttemptIdentity(identity); err != nil {
		return RuntimeAttemptIdentity{}, err
	}
	return identity, nil
}
