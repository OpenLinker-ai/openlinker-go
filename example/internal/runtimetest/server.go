// Package runtimetest provides a small offline Runtime v2/Core server for examples.
package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

const (
	NodeID        = "11111111-1111-4111-8111-111111111111"
	AgentID       = "22222222-2222-4222-8222-222222222222"
	TargetAgentID = "33333333-3333-4333-8333-333333333333"
	CoreID        = "44444444-4444-4444-8444-444444444444"
	AttachmentID  = "55555555-5555-4555-8555-555555555555"
	RunID         = "66666666-6666-4666-8666-666666666666"
	AttemptID     = "77777777-7777-4777-8777-777777777777"
	LeaseID       = "88888888-8888-4888-8888-888888888888"
	ChildRunID    = "99999999-9999-4999-8999-999999999999"
	AgentToken    = "ol_agent_example_runtime"
	UserToken     = "ol_user_example_creator"
)

type Server struct {
	HTTP *httptest.Server

	mu                sync.Mutex
	hello             openlinker.RuntimeHelloPayload
	claimed           bool
	events            []openlinker.RuntimeRunEventPayload
	delegated         []openlinker.RuntimeCallAgentRequest
	registrationCalls int
	firstErr          error
	result            chan openlinker.RuntimeRunResultPayload
}

func New() *Server {
	server := &Server{result: make(chan openlinker.RuntimeRunResultPayload, 4)}
	server.HTTP = httptest.NewServer(server)
	return server
}

func (server *Server) Close()               { server.HTTP.Close() }
func (server *Server) URL() string          { return server.HTTP.URL }
func (server *Server) Client() *http.Client { return server.HTTP.Client() }

func (server *Server) WaitResult(ctx context.Context) (openlinker.RuntimeRunResultPayload, error) {
	select {
	case result := <-server.result:
		return result, nil
	case <-ctx.Done():
		return openlinker.RuntimeRunResultPayload{}, ctx.Err()
	}
}

func (server *Server) Events() []openlinker.RuntimeRunEventPayload {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]openlinker.RuntimeRunEventPayload(nil), server.events...)
}

func (server *Server) DelegatedCalls() []openlinker.RuntimeCallAgentRequest {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]openlinker.RuntimeCallAgentRequest(nil), server.delegated...)
}

func (server *Server) RegistrationCalls() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.registrationCalls
}

func (server *Server) Err() error {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.firstErr
}

func (server *Server) fail(w http.ResponseWriter, err error) {
	server.mu.Lock()
	if server.firstErr == nil {
		server.firstErr = err
	}
	server.mu.Unlock()
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (server *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, "/api/v1/creator/") || request.URL.Path == "/api/v1/agent-registration/agents" {
		server.serveRegistration(w, request)
		return
	}
	if request.URL.Path == "/api/v1/agent-runtime/call-agent" {
		var body openlinker.RuntimeCallAgentRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		server.mu.Lock()
		server.delegated = append(server.delegated, body)
		server.mu.Unlock()
		writeJSON(w, openlinker.RuntimeRunSummary{RunID: ChildRunID, Status: openlinker.RuntimeRunRunning, DispatchState: openlinker.RuntimeDispatchPending})
		return
	}

	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/api/v1/agent-runtime/sessions":
		var hello openlinker.RuntimeHelloPayload
		if err := json.NewDecoder(request.Body).Decode(&hello); err != nil {
			server.fail(w, err)
			return
		}
		server.mu.Lock()
		server.hello = hello
		server.mu.Unlock()
		writeJSON(w, ready())
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/heartbeat"):
		writeJSON(w, ready())
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/close"):
		w.WriteHeader(http.StatusNoContent)
	case request.Method == http.MethodPost && request.URL.Path == "/api/v1/agent-runtime/runs/resume":
		var body openlinker.RuntimeResumePayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		decisions := make([]openlinker.RuntimeResumeAcceptedPayload, len(body.Attempts))
		for index, attempt := range body.Attempts {
			expires := time.Now().Add(time.Minute).UTC()
			decisions[index] = openlinker.RuntimeResumeAcceptedPayload{
				AttemptIdentity: attempt.AttemptIdentity, Decision: openlinker.RuntimeResumeContinue, LeaseExpiresAt: &expires,
				AllowedActions: []openlinker.RuntimeResumeAction{openlinker.RuntimeActionContinueExecution, openlinker.RuntimeActionUploadEvents, openlinker.RuntimeActionUploadResult},
			}
		}
		writeJSON(w, openlinker.RuntimeResumeResponse{Decisions: decisions})
	case request.Method == http.MethodPost && request.URL.Path == "/api/v1/agent-runtime/runs/claim":
		server.serveClaim(w, request)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/assignment-ack"):
		var body openlinker.RuntimeAssignmentAckPayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		writeJSON(w, openlinker.RuntimeAssignmentConfirmedPayload{AttemptIdentity: body.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/assignment-reject"):
		var body openlinker.RuntimeAssignmentRejectPayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		writeJSON(w, openlinker.RuntimeAssignmentRejectedPayload{
			AttemptIdentity: body.AttemptIdentity, Outcome: openlinker.RuntimeOfferRejected, DispatchState: openlinker.RuntimeDispatchPending,
		})
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/lease-renew"):
		var body openlinker.RuntimeLeaseRenewPayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		writeJSON(w, openlinker.RuntimeLeaseRenewedPayload{AttemptIdentity: body.AttemptIdentity, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/events"):
		var body openlinker.RuntimeRunEventPayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		server.mu.Lock()
		server.events = append(server.events, body)
		server.mu.Unlock()
		writeJSON(w, openlinker.RuntimeRunEventAckPayload{ClientEventID: body.ClientEventID, ClientEventSeq: body.ClientEventSeq, Sequence: body.ClientEventSeq})
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/result"):
		var body openlinker.RuntimeRunResultPayload
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			server.fail(w, err)
			return
		}
		select {
		case server.result <- body:
		default:
		}
		writeJSON(w, openlinker.RuntimeRunResultAckPayload{ResultID: body.ResultID, Classification: openlinker.RuntimeResultSuccess, RunStatus: openlinker.RuntimeRunSuccess, DispatchState: openlinker.RuntimeDispatchTerminal})
	case request.Method == http.MethodGet && request.URL.Path == "/api/v1/agent-runtime/commands":
		writeJSON(w, openlinker.RuntimeCommandsResponse{Commands: []openlinker.RuntimePendingCommand{}, DatabaseTime: time.Now().UTC()})
	default:
		server.fail(w, fmt.Errorf("unexpected request %s %s", request.Method, request.URL.Path))
	}
}

func (server *Server) serveClaim(w http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	if server.claimed {
		server.mu.Unlock()
		select {
		case <-request.Context().Done():
		case <-time.After(25 * time.Millisecond):
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	server.claimed = true
	hello := server.hello
	server.mu.Unlock()
	identity := openlinker.RuntimeAttemptIdentity{
		RunID: RunID, AttemptID: AttemptID, LeaseID: LeaseID, FencingToken: 1,
		NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID, RuntimeSessionID: hello.RuntimeSessionID,
	}
	writeJSON(w, openlinker.RuntimeRunAssignedPayload{
		AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: time.Now().Add(time.Minute).UTC(),
		AttemptDeadlineAt: time.Now().Add(time.Minute).UTC(), RunDeadlineAt: time.Now().Add(2 * time.Minute).UTC(),
		Input: map[string]any{"text": "hello runtime"}, Metadata: map[string]any{"source": "example-test"},
		NodeEnvelope: "ol_ctx_v2.header.payload.signature", AgentInvocationToken: "ol_inv_v2.header.payload.signature",
	})
}

func (server *Server) serveRegistration(w http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	server.registrationCalls++
	server.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch request.Method + " " + request.URL.Path {
	case "POST /api/v1/creator/agent-tokens":
		if request.Header.Get("Authorization") != "Bearer "+UserToken {
			server.fail(w, fmt.Errorf("unexpected User Token authorization %q", request.Header.Get("Authorization")))
			return
		}
		writeJSON(w, openlinker.AgentTokenResponse{ID: TargetAgentID, Prefix: "ol_agent_example", Status: "pending_registration", PlaintextToken: AgentToken})
	case "POST /api/v1/agent-registration/agents":
		if request.Header.Get("Authorization") != "Bearer "+AgentToken {
			server.fail(w, fmt.Errorf("unexpected Agent Token authorization %q", request.Header.Get("Authorization")))
			return
		}
		writeJSON(w, openlinker.RegisterAgentViaTokenResponse{
			Agent:      openlinker.AgentResponse{ID: AgentID, Slug: "example-register-agent", Name: "Example Register Agent"},
			AgentToken: openlinker.AgentTokenResponse{ID: TargetAgentID, Prefix: "ol_agent_example", Status: "active_runtime"},
		})
	default:
		server.fail(w, fmt.Errorf("unexpected registration request %s %s", request.Method, request.URL.Path))
	}
}

func ready() openlinker.RuntimeReadyPayload {
	return openlinker.RuntimeReadyPayload{
		CoreInstanceID: CoreID, AttachmentID: AttachmentID, Features: openlinker.RuntimeRequiredFeatures(),
		OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: time.Now().UTC(),
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil && !errors.Is(err, context.Canceled) {
		return
	}
}
