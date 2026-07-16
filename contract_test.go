package openlinker

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestCoreClientV1ContractMapsToImplementedMethods(t *testing.T) {
	raw := readContractFile(t, "contracts/core-client.v1.json")
	var contract struct {
		Scope string `json:"scope"`
		Rules struct {
			ForbiddenPathPrefixes []string `json:"forbidden_path_prefixes"`
		} `json:"rules"`
		Endpoints []struct {
			ClientMethod    string   `json:"client_method"`
			Path            string   `json:"path"`
			RequiredHeaders []string `json:"required_headers"`
			SuccessStatuses []int    `json:"success_statuses"`
			ResponseHeaders []string `json:"response_headers"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Scope != "core" {
		t.Fatalf("scope = %q, want core", contract.Scope)
	}
	if len(contract.Endpoints) == 0 {
		t.Fatal("core client contract has no endpoints")
	}

	clientType := reflect.TypeOf(&Client{})
	for _, endpoint := range contract.Endpoints {
		if !strings.HasPrefix(endpoint.Path, "/api/v1/") {
			t.Fatalf("endpoint path must be core API path: %s", endpoint.Path)
		}
		for _, prefix := range contract.Rules.ForbiddenPathPrefixes {
			if strings.HasPrefix(endpoint.Path, prefix) {
				t.Fatalf("forbidden endpoint in core contract: %s", endpoint.Path)
			}
		}
		methodName := exportedMethodName(endpoint.ClientMethod)
		if _, ok := clientType.MethodByName(methodName); !ok {
			t.Fatalf("Client missing contract method %s", endpoint.ClientMethod)
		}
		if endpoint.ClientMethod == "runAgent" || endpoint.ClientMethod == "startAgentRun" {
			if !slices.Contains(endpoint.RequiredHeaders, "Idempotency-Key") {
				t.Fatalf("%s contract missing Idempotency-Key", endpoint.ClientMethod)
			}
			if !slices.Equal(endpoint.SuccessStatuses, []int{200, 201, 202}) {
				t.Fatalf("%s success statuses = %#v", endpoint.ClientMethod, endpoint.SuccessStatuses)
			}
			for _, header := range []string{"Location", "Idempotency-Replayed"} {
				if !slices.Contains(endpoint.ResponseHeaders, header) {
					t.Fatalf("%s contract missing response header %s", endpoint.ClientMethod, header)
				}
			}
		}
	}
}

func TestRuntimeContractMatchesExportedConstants(t *testing.T) {
	raw := readContractFile(t, "contracts/core-runtime.json")
	type runtimeEndpointContract struct {
		ClientMethod      string   `json:"client_method"`
		Method            string   `json:"http_method"`
		Path              string   `json:"path"`
		RequiredHeaders   []string `json:"required_headers"`
		RequestBodySchema struct {
			Ref string `json:"$ref"`
		} `json:"request_body_schema"`
		SuccessResponseSchema struct {
			Ref string `json:"$ref"`
		} `json:"success_response_schema"`
		EmptyResponseStatus int `json:"empty_response_status"`
		ErrorResponseSchema struct {
			Ref string `json:"$ref"`
		} `json:"error_response_schema"`
	}
	var contract struct {
		Name              string   `json:"name"`
		Scope             string   `json:"scope"`
		Version           string   `json:"version"`
		RuntimeContractID string   `json:"runtime_contract_id"`
		ProtocolVersion   int      `json:"protocol_version"`
		RequiredFeatures  []string `json:"required_features"`
		WebSocket         struct {
			Path           string `json:"path"`
			EnvelopeSchema struct {
				Ref string `json:"$ref"`
			} `json:"envelope_schema"`
			Messages []struct {
				Type   string `json:"type"`
				Schema struct {
					Ref string `json:"$ref"`
				} `json:"schema"`
			} `json:"messages"`
		} `json:"websocket"`
		Endpoints    []runtimeEndpointContract `json:"endpoints"`
		LegacyRoutes []struct {
			ResponseStatus int    `json:"response_status"`
			ErrorCode      string `json:"error_code"`
		} `json:"legacy_routes"`
		StableErrorCodes []string                   `json:"stable_error_codes"`
		Definitions      map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}

	if contract.Name != "openlinker-runtime" {
		t.Fatalf("name = %q", contract.Name)
	}
	if contract.Scope != "core-runtime" {
		t.Fatalf("scope = %q", contract.Scope)
	}
	if contract.Version != "v2" {
		t.Fatalf("version = %q", contract.Version)
	}
	if contract.RuntimeContractID != RuntimeContractID {
		t.Fatalf("runtime_contract_id = %q, want %q", contract.RuntimeContractID, RuntimeContractID)
	}
	if contract.ProtocolVersion != RuntimeProtocolVersion {
		t.Fatalf("protocol_version = %d, want %d", contract.ProtocolVersion, RuntimeProtocolVersion)
	}
	if !slices.Equal(contract.RequiredFeatures, RuntimeRequiredFeatures()) {
		t.Fatalf("required_features = %#v, want %#v", contract.RequiredFeatures, RuntimeRequiredFeatures())
	}
	digest := sha256.Sum256(raw)
	if got := fmt.Sprintf("%x", digest); got != RuntimeContractDigest {
		t.Fatalf("contract digest = %q, want %q", got, RuntimeContractDigest)
	}

	if contract.WebSocket.Path != "/api/v1/agent-runtime/ws" {
		t.Fatalf("websocket path = %q", contract.WebSocket.Path)
	}
	if strings.Contains(contract.WebSocket.Path, "/v2/") {
		t.Fatalf("websocket path exposes protocol version: %q", contract.WebSocket.Path)
	}
	if contract.WebSocket.EnvelopeSchema.Ref != "#/$defs/RuntimeMessage" {
		t.Fatalf("websocket envelope schema = %q", contract.WebSocket.EnvelopeSchema.Ref)
	}
	messageTypes := make([]string, 0, len(contract.WebSocket.Messages))
	for _, message := range contract.WebSocket.Messages {
		if !strings.HasPrefix(message.Schema.Ref, "#/$defs/") {
			t.Fatalf("message %q has invalid schema ref %q", message.Type, message.Schema.Ref)
		}
		if _, ok := contract.Definitions[strings.TrimPrefix(message.Schema.Ref, "#/$defs/")]; !ok {
			t.Fatalf("message %q references missing schema %q", message.Type, message.Schema.Ref)
		}
		messageTypes = append(messageTypes, message.Type)
	}
	for _, messageType := range []string{"runtime.hello", "run.assigned", "run.result", "runtime.resume", "run.cancel", "runtime.drain", "runtime.error"} {
		if !slices.Contains(messageTypes, messageType) {
			t.Fatalf("runtime contract missing message %q", messageType)
		}
	}

	endpoints := make(map[string]runtimeEndpointContract, len(contract.Endpoints))
	for _, endpoint := range contract.Endpoints {
		if endpoint.ClientMethod == "" || endpoint.Method == "" || endpoint.Path == "" {
			t.Fatalf("runtime endpoint is incomplete: %#v", endpoint)
		}
		if !strings.HasPrefix(endpoint.Path, "/api/v1/agent-runtime/") {
			t.Fatalf("runtime endpoint is outside canonical prefix: %q", endpoint.Path)
		}
		if strings.Contains(endpoint.Path, "/agent-runtime/"+contract.Version+"/") {
			t.Fatalf("runtime endpoint exposes protocol version: %q", endpoint.Path)
		}
		key := endpoint.Method + " " + endpoint.Path
		if _, exists := endpoints[key]; exists {
			t.Fatalf("runtime contract has duplicate endpoint %q", key)
		}
		endpoints[key] = endpoint
	}
	requiredEndpoints := []string{
		"POST /api/v1/agent-runtime/sessions",
		"POST /api/v1/agent-runtime/sessions/{id}/heartbeat",
		"POST /api/v1/agent-runtime/sessions/{id}/drain",
		"POST /api/v1/agent-runtime/sessions/{id}/close",
		"POST /api/v1/agent-runtime/runs/claim",
		"POST /api/v1/agent-runtime/runs/{id}/assignment-ack",
		"POST /api/v1/agent-runtime/runs/{id}/assignment-reject",
		"POST /api/v1/agent-runtime/runs/{id}/lease-renew",
		"POST /api/v1/agent-runtime/runs/{id}/events",
		"POST /api/v1/agent-runtime/runs/{id}/result",
		"POST /api/v1/agent-runtime/runs/resume",
		"POST /api/v1/agent-runtime/runs/{id}/cancel-ack",
		"GET /api/v1/agent-runtime/commands",
		"POST /api/v1/agent-runtime/call-agent",
	}
	if len(endpoints) != len(requiredEndpoints) {
		t.Fatalf("runtime endpoint count = %d, want %d", len(endpoints), len(requiredEndpoints))
	}
	for _, requiredEndpoint := range requiredEndpoints {
		if _, ok := endpoints[requiredEndpoint]; !ok {
			t.Fatalf("runtime contract missing endpoint %q", requiredEndpoint)
		}
	}
	for key, endpoint := range endpoints {
		if key == "POST /api/v1/agent-runtime/sessions" {
			if len(endpoint.RequiredHeaders) != 0 {
				t.Fatalf("%s must not require an attachment header: %#v", key, endpoint.RequiredHeaders)
			}
			continue
		}
		if key == "POST /api/v1/agent-runtime/call-agent" {
			if slices.Contains(endpoint.RequiredHeaders, RuntimeAttachmentHeader) {
				t.Fatalf("%s must not require an attachment header: %#v", key, endpoint.RequiredHeaders)
			}
			continue
		}
		if !slices.Equal(endpoint.RequiredHeaders, []string{RuntimeAttachmentHeader}) {
			t.Fatalf("%s attachment headers = %#v", key, endpoint.RequiredHeaders)
		}
	}

	heartbeat := endpoints["POST /api/v1/agent-runtime/sessions/{id}/heartbeat"]
	if heartbeat.ClientMethod != "heartbeatRuntimeSession" ||
		heartbeat.RequestBodySchema.Ref != "#/$defs/RuntimeHelloPayload" ||
		heartbeat.SuccessResponseSchema.Ref != "#/$defs/RuntimeReadyPayload" ||
		heartbeat.EmptyResponseStatus != 0 ||
		heartbeat.ErrorResponseSchema.Ref != "#/$defs/RuntimeError" {
		t.Fatalf("runtime heartbeat contract = %#v", heartbeat)
	}
	closeEndpoint := endpoints["POST /api/v1/agent-runtime/sessions/{id}/close"]
	if closeEndpoint.ClientMethod != "closeRuntimeSession" ||
		closeEndpoint.RequestBodySchema.Ref != "#/$defs/RuntimeSessionCloseRequest" ||
		closeEndpoint.SuccessResponseSchema.Ref != "" ||
		closeEndpoint.EmptyResponseStatus != 204 ||
		closeEndpoint.ErrorResponseSchema.Ref != "#/$defs/RuntimeError" {
		t.Fatalf("runtime close contract = %#v", closeEndpoint)
	}
	drainEndpoint := endpoints["POST /api/v1/agent-runtime/sessions/{id}/drain"]
	if drainEndpoint.ClientMethod != "drainRuntimeSession" ||
		drainEndpoint.RequestBodySchema.Ref != "#/$defs/RuntimeDrainPayload" ||
		drainEndpoint.SuccessResponseSchema.Ref != "#/$defs/RuntimeDrainPayload" ||
		drainEndpoint.EmptyResponseStatus != 0 ||
		drainEndpoint.ErrorResponseSchema.Ref != "#/$defs/RuntimeError" {
		t.Fatalf("runtime drain contract = %#v", drainEndpoint)
	}

	for _, definition := range []string{
		"AttemptIdentity",
		"RunResultPayload",
		"ResumeAttempt",
		"PendingCommand",
		"RuntimeCommandsResponse",
		"RuntimeSessionCloseRequest",
		"RuntimeDrainPayload",
	} {
		if _, ok := contract.Definitions[definition]; !ok {
			t.Fatalf("runtime contract missing definition %q", definition)
		}
	}

	if len(contract.LegacyRoutes) != 0 {
		t.Fatalf("runtime v1 routes must be absent, got %d", len(contract.LegacyRoutes))
	}
	if len(contract.StableErrorCodes) == 0 {
		t.Fatal("runtime contract has no stable error codes")
	}
}

func TestRuntimeRequiredFeaturesReturnsCopy(t *testing.T) {
	features := RuntimeRequiredFeatures()
	features[0] = "mutated"
	if got := RuntimeRequiredFeatures()[0]; got != "lease_fence" {
		t.Fatalf("first required feature = %q", got)
	}
}

func TestRegistrationContractMapsToImplementedMethods(t *testing.T) {
	raw := readContractFile(t, "contracts/core-registration.v1.json")
	var contract struct {
		Scope     string `json:"scope"`
		Endpoints []struct {
			ClientMethod string `json:"client_method"`
			Method       string `json:"http_method"`
			Path         string `json:"path"`
			Auth         string `json:"auth"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Scope != "core-registration" || len(contract.Endpoints) == 0 {
		t.Fatalf("registration contract = %#v", contract)
	}
	clientType := reflect.TypeOf(&Client{})
	for _, endpoint := range contract.Endpoints {
		if endpoint.Method == "" || !strings.HasPrefix(endpoint.Path, "/api/v1/") {
			t.Fatalf("invalid registration endpoint: %#v", endpoint)
		}
		if endpoint.Auth != "user_token" && endpoint.Auth != "agent_token" {
			t.Fatalf("invalid registration auth: %#v", endpoint)
		}
		if endpoint.ClientMethod == "registerAgentViaToken" {
			continue
		}
		if _, ok := clientType.MethodByName(exportedMethodName(endpoint.ClientMethod)); !ok {
			t.Fatalf("Client missing registration method %s", endpoint.ClientMethod)
		}
	}
}

func TestRuntimeProtocolSourcesDoNotDependOnManagedWorker(t *testing.T) {
	for _, path := range []string{
		"runtime_client.go", "runtime_http.go", "runtime_websocket.go",
		"runtime_websocket_client.go", "runtime_invocation.go",
	} {
		raw := readContractFile(t, path)
		for _, forbidden := range []string{"RuntimeWorker", "NativeRun", "NativeResult"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("low-level protocol source %s depends on managed symbol %s", path, forbidden)
			}
		}
	}
}

func readContractFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func exportedMethodName(name string) string {
	if name == "" {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}
