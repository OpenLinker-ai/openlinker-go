package openlinker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func delegatedTestToken() string {
	return "ol_inv_v2.current." + base64.RawURLEncoding.EncodeToString([]byte(`{"audience":"openlinker.runtime.v2/delegation"}`)) + ".signature"
}

func TestReadDelegatedRunSignsOnlyAttemptAuthority(t *testing.T) {
	authorization := RuntimeCallAgentAuthorization{NodeEnvelope: "ol_ctx_v2.current.payload.signature", AgentInvocationToken: delegatedTestToken(), IdempotencyKey: "read-child"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if req.Method != "POST" || req.URL.Path != runtimeDelegatedRunReadPath || req.Header.Get("Authorization") != "Bearer "+authorization.AgentInvocationToken {
			t.Error("incorrect delegated read identity")
		}
		proof, _ := BuildRuntimeInvocationProof(authorization.AgentInvocationToken, RuntimeInvocationProofRequest{Method: req.Method, Path: req.URL.Path, Body: body, Context: req.Header.Get(runtimeInvocationHeader), IdempotencyKey: req.Header.Get("Idempotency-Key")})
		if req.Header.Get(runtimeInvocationProofHeader) != proof {
			t.Error("proof does not bind exact request")
		}
		var request map[string]any
		json.Unmarshal(body, &request)
		if len(request) != 1 || request["run_id"] != runtimeTestRunID {
			t.Error("unexpected read payload")
		}
		json.NewEncoder(w).Encode(RuntimeDelegatedRun{RuntimeRunSummary: RuntimeRunSummary{RunID: runtimeTestRunID, Status: RuntimeRunSuccess, DispatchState: RuntimeDispatchTerminal}, Output: map[string]any{"summary": "done"}})
	}))
	defer server.Close()
	client, err := NewRuntime(server.URL, WithAgentToken("long-lived-must-not-be-used"), WithHeader("Authorization", "Bearer must-not-win"))
	if err != nil {
		t.Fatal(err)
	}
	websocket := &RuntimeWebSocket{runtime: client}
	gate := newSwitchingRuntimeClient(websocket)
	for name, reader := range map[string]runtimeDelegatedRunClient{
		"http": client, "websocket": websocket, "switching": gate,
		"policy": &policyRecoveringRuntimeClient{node: &RuntimeWorker{}, transport: gate},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := reader.ReadRuntimeDelegatedRun(context.Background(), authorization, runtimeTestRunID)
			if err != nil || result == nil || result.Output["summary"] != "done" {
				t.Fatalf("result=%v err=%v", result, err)
			}
			old := authorization
			old.AgentInvocationToken = "ol_inv_v2.current.payload.signature"
			if _, err := reader.ReadRuntimeDelegatedRun(context.Background(), old, runtimeTestRunID); !errors.Is(err, ErrRuntimeDelegationUnsupported) {
				t.Fatal(err)
			}
		})
	}
}

func TestReadDelegatedRunRejectsAnotherRunResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(RuntimeDelegatedRun{RuntimeRunSummary: RuntimeRunSummary{RunID: runtimeTestTargetAgentID, Status: RuntimeRunSuccess, DispatchState: RuntimeDispatchTerminal}})
	}))
	defer server.Close()
	client, _ := NewRuntime(server.URL, WithAgentToken("agent-token"))
	_, err := client.ReadRuntimeDelegatedRun(context.Background(), RuntimeCallAgentAuthorization{NodeEnvelope: "ol_ctx_v2.current.payload.signature", AgentInvocationToken: delegatedTestToken(), IdempotencyKey: "read-child"}, runtimeTestRunID)
	if !errors.Is(err, ErrRuntimeProtocolMismatch) {
		t.Fatal(err)
	}
}

type delegatedWorkerFake struct {
	*fakeRuntimeClient
	started chan struct{}
}

func (fake *delegatedWorkerFake) ReadRuntimeDelegatedRun(ctx context.Context, auth RuntimeCallAgentAuthorization, runID string) (*RuntimeDelegatedRun, error) {
	close(fake.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestDelegatedReadStopsWithAttemptAndRejectsFinishedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &delegatedWorkerFake{newFakeRuntimeClient(), make(chan struct{})}
	worker := &RuntimeWorker{}
	worker.runtimeClient = &policyRecoveringRuntimeClient{node: worker, transport: newSwitchingRuntimeClient(fake)}
	attempt := &activeRuntimeAttempt{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		_, err := worker.readDelegatedRunForAttempt(context.Background(), attempt, runtimeTestRunID)
		done <- err
	}()
	<-fake.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read ignored cancellation")
	}
	attempt.finished.Store(true)
	if _, err := worker.readDelegatedRunForAttempt(context.Background(), attempt, runtimeTestRunID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if (RuntimeContext{}).CanReadDelegatedRuns() {
		t.Fatal("empty context advertises delegation")
	}
}

type delegatedTransportTestClient struct {
	*fakeRuntimeClient
	read func(context.Context, RuntimeCallAgentAuthorization, string) (*RuntimeDelegatedRun, error)
}

func (client *delegatedTransportTestClient) ReadRuntimeDelegatedRun(ctx context.Context, auth RuntimeCallAgentAuthorization, runID string) (*RuntimeDelegatedRun, error) {
	return client.read(ctx, auth, runID)
}

func TestRuntimeWorkerDelegatedReadSurvivesTransportWrapping(t *testing.T) {
	for _, mode := range []RuntimeTransportMode{RuntimeTransportPull, RuntimeTransportWebSocket, RuntimeTransportAuto} {
		for _, negotiated := range []bool{false, true} {
			name := string(mode) + "/legacy"
			if negotiated {
				name = string(mode) + "/delegation"
			}
			t.Run(name, func(t *testing.T) {
				base := newFakeRuntimeClient()
				var once sync.Once
				base.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
					var assigned *RuntimeRunAssignedPayload
					once.Do(func() {
						assigned = assignedRunForHello(base.helloSnapshot())
						if negotiated {
							assigned.AgentInvocationToken = delegatedTestToken()
						}
					})
					if assigned != nil {
						return assigned, nil
					}
					<-ctx.Done()
					return nil, ctx.Err()
				}
				client := &delegatedTransportTestClient{fakeRuntimeClient: base, read: func(_ context.Context, auth RuntimeCallAgentAuthorization, id string) (*RuntimeDelegatedRun, error) {
					if !negotiated || auth.AgentInvocationToken != delegatedTestToken() || auth.NodeEnvelope != "ol_ctx_v2.header.payload.signature" || id != runtimeTestRunID {
						t.Error("delegated read lost Attempt authority")
					}
					return &RuntimeDelegatedRun{Output: map[string]any{"summary": "child result"}}, nil
				}}
				completed := make(chan error, 1)
				handler := testRuntimeHandlerFunc(func(ctx context.Context, _ any, run RuntimeContext) (any, error) {
					if run.CanReadDelegatedRuns() != negotiated {
						completed <- errors.New("Worker lost the negotiated delegated-read capability")
						return nil, nil
					}
					result, err := run.ReadDelegatedRun(ctx, runtimeTestRunID)
					if !negotiated {
						if !errors.Is(err, ErrRuntimeDelegationUnsupported) {
							err = errors.New("legacy assignment did not reject delegated read")
						} else {
							err = nil
						}
					} else if err == nil && (result == nil || result.Output["summary"] != "child result") {
						err = errors.New("child result was not returned to the handler")
					}
					completed <- err
					return map[string]any{"done": true}, nil
				})
				worker := newRuntimeWorkerForTest(t.TempDir(), client, handler)
				worker.Transport = mode
				worker.runtimeDialer = &fakeRuntimeTransportDialer{connections: []RuntimeDuplexClient{newFakeRuntimeDuplex(base)}}
				done := make(chan error, 1)
				go func() { done <- worker.Start(context.Background()) }()
				t.Cleanup(func() { stopRuntimeWorkerTest(t, worker, done) })
				select {
				case err := <-completed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Worker did not execute the delegation handler")
				}
			})
		}
	}
}

func TestDelegatedReadTransportRejectsUnavailableClients(t *testing.T) {
	for _, test := range []struct {
		client RuntimeClient
		want   error
	}{{nil, ErrRuntimeTransportSwitching}, {newFakeRuntimeClient(), ErrRuntimeDelegationUnsupported}} {
		_, err := newSwitchingRuntimeClient(test.client).ReadRuntimeDelegatedRun(context.Background(), RuntimeCallAgentAuthorization{}, runtimeTestRunID)
		if !errors.Is(err, test.want) {
			t.Fatalf("read error = %v, want %v", err, test.want)
		}
	}
}
