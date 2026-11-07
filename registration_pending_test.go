package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const testPendingAgentID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

// EnvRegistrationStore must stay comparable, as it was before the pending
// journal; callers may compare it or use it with a comparable constraint.
func assertComparableRegistrationType[T comparable]() {}

var _ = assertComparableRegistrationType[EnvRegistrationStore]

// replayRegistrationCore models Core's registration endpoint, including the
// idempotent replay of an already redeemed token for the same slug.
type replayRegistrationCore struct {
	t      *testing.T
	server *httptest.Server

	mu               sync.Mutex
	pending          map[string]bool
	redeemed         map[string]RegisterAgentViaTokenResponse
	slugs            map[string]bool
	createTokenCalls int
	registerTokens   []string
	dropAfterCommit  int
	afterCommit      func()
	beforeRegister   func(token string)
}

func newReplayRegistrationCore(t *testing.T) *replayRegistrationCore {
	t.Helper()
	core := &replayRegistrationCore{
		t: t, pending: map[string]bool{}, redeemed: map[string]RegisterAgentViaTokenResponse{}, slugs: map[string]bool{},
	}
	core.server = httptest.NewServer(http.HandlerFunc(core.serve))
	t.Cleanup(core.server.Close)
	return core
}

func (core *replayRegistrationCore) serve(w http.ResponseWriter, request *http.Request) {
	core.mu.Lock()
	defer core.mu.Unlock()
	switch request.Method + " " + request.URL.Path {
	case "POST /api/v1/creator/agent-tokens":
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ol_user_") {
			core.t.Errorf("create token auth = %q", request.Header.Get("Authorization"))
		}
		core.createTokenCalls++
		token := fmt.Sprintf("ol_agent_minted_%d", core.createTokenCalls)
		core.pending[token] = true
		w.WriteHeader(http.StatusCreated)
		writeRegistrationTestJSON(core.t, w, AgentTokenResponse{
			ID: testRegistrationTokenID, Prefix: "ol_agent_mint", Status: "pending_registration", PlaintextToken: token,
		})
	case "POST /api/v1/agent-registration/agents":
		token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		core.registerTokens = append(core.registerTokens, token)
		if core.beforeRegister != nil {
			core.beforeRegister(token)
		}
		var body RegisterAgentViaTokenRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			core.t.Errorf("decode register body: %v", err)
		}
		switch {
		case core.pending[token] && core.slugs[body.Slug]:
			writeRegistrationTestError(w, http.StatusConflict, "CONFLICT", "slug 已被占用")
		case core.pending[token]:
			response := RegisterAgentViaTokenResponse{
				Agent:      AgentResponse{ID: testPendingAgentID, Slug: body.Slug, Name: body.Name},
				AgentToken: AgentTokenResponse{ID: testRegistrationTokenID, Prefix: "ol_agent_reg", Status: "active_runtime"},
			}
			delete(core.pending, token)
			core.redeemed[token] = response
			core.slugs[body.Slug] = true
			if core.afterCommit != nil {
				core.afterCommit()
			}
			if core.dropAfterCommit > 0 {
				core.dropAfterCommit--
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					core.t.Errorf("hijack: %v", err)
					return
				}
				_ = connection.Close()
				return
			}
			w.WriteHeader(http.StatusCreated)
			writeRegistrationTestJSON(core.t, w, response)
		case core.redeemed[token].Agent.Slug != "" && core.redeemed[token].Agent.Slug == body.Slug:
			w.WriteHeader(http.StatusCreated)
			writeRegistrationTestJSON(core.t, w, core.redeemed[token])
		default:
			writeRegistrationTestError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Agent Token 已过期 / 已撤销 / 已被使用")
		}
	default:
		core.t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (core *replayRegistrationCore) calls() (int, []string) {
	core.mu.Lock()
	defer core.mu.Unlock()
	return core.createTokenCalls, append([]string(nil), core.registerTokens...)
}

func writeRegistrationTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func writeRegistrationTestError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func readPendingTestEnv(t *testing.T, path string) (string, os.FileMode) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), info.Mode().Perm()
}

// pendingRequestFor is the request EnsureAgent sends for req.
func pendingRequestFor(req EnsureAgentRequest) RegisterAgentViaTokenRequest {
	return registerAgentViaTokenRequestFrom(normalizeEnsureAgentRequest(req))
}

// pendingMintFor is the mint record EnsureAgent writes for req.
func pendingMintFor(req EnsureAgentRequest) *pendingMintParameters {
	normalized := normalizeEnsureAgentRequest(req)
	return &pendingMintParameters{
		TokenName: normalized.TokenName, TokenScopes: normalized.TokenScopes,
		TokenExpiresInMinutes: normalized.TokenExpiresInMinutes,
		CreatorFingerprint:    creatorCredentialFingerprint(req.UserToken),
	}
}

func TestEnsureAgentResumesMintedRegistrationAfterLostResponse(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	core.dropAfterCommit = 1
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("UNRELATED=value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewEnvRegistrationStore(path)
	spec := AgentSpec{Slug: "demo-agent", Name: "Demo Agent", EndpointAuthHeader: "Bearer endpoint-secret"}
	options := []RegistrationOption{
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store),
	}

	if _, err := EnsureAgent(context.Background(), spec, options...); err == nil {
		t.Fatal("lost registration response succeeded")
	}
	raw, mode := readPendingTestEnv(t, path)
	if !strings.Contains(raw, pendingRegistrationEnvKey+"=") || !strings.Contains(raw, "ol_agent_minted_1") ||
		strings.Contains(raw, "endpoint-secret") || mode != 0o600 {
		t.Fatalf("pending env mode=%o content=%s", mode, raw)
	}
	if loaded, err := store.LoadAgentRegistration(); err != nil || loaded != nil {
		t.Fatalf("pending record changed completed load: %#v err=%v", loaded, err)
	}

	registration, err := EnsureAgent(context.Background(), spec, options...)
	if err != nil {
		t.Fatal(err)
	}
	created, tokens := core.calls()
	if created != 1 || strings.Join(tokens, ",") != "ol_agent_minted_1,ol_agent_minted_1" {
		t.Fatalf("create-token calls=%d register tokens=%v", created, tokens)
	}
	if registration.AgentID != testPendingAgentID || registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("registration = %#v", registration)
	}
	raw, mode = readPendingTestEnv(t, path)
	if strings.Contains(raw, pendingRegistrationEnvKey) || !strings.HasPrefix(raw, "UNRELATED=value\n") || mode != 0o600 {
		t.Fatalf("completed env mode=%o content=%s", mode, raw)
	}

	again, err := EnsureAgent(context.Background(), spec, options...)
	if err != nil || again.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("reuse = %#v err=%v", again, err)
	}
	if created, tokens := core.calls(); created != 1 || len(tokens) != 2 {
		t.Fatalf("reuse sent requests: create-token=%d register=%v", created, tokens)
	}
}

func TestEnsureAgentResumesMintedRegistrationAfterSaveFailure(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the current user cannot write")
	}
	t.Parallel()
	core := newReplayRegistrationCore(t)
	dir := filepath.Join(t.TempDir(), "state")
	store := NewEnvRegistrationStore(filepath.Join(dir, ".env"))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	core.afterCommit = func() {
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Errorf("chmod: %v", err)
		}
	}
	spec := AgentSpec{Slug: "demo-agent", Name: "Demo Agent"}
	options := []RegistrationOption{
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store),
	}

	if _, err := EnsureAgent(context.Background(), spec, options...); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("first EnsureAgent err = %v", err)
	}
	core.afterCommit = nil
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	registration, err := EnsureAgent(context.Background(), spec, options...)
	if err != nil {
		t.Fatal(err)
	}
	if created, tokens := core.calls(); created != 1 || strings.Join(tokens, ",") != "ol_agent_minted_1,ol_agent_minted_1" ||
		registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("create-token=%d register=%v registration=%#v", created, tokens, registration)
	}
}

func TestEnsureAgentProvidedTokenRetriesSameTokenAfterLostResponse(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	core.pending["ol_agent_provided"] = true
	core.dropAfterCommit = 1
	store := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
	spec := AgentSpec{Slug: "provided-agent", Name: "Provided Agent"}
	options := []RegistrationOption{
		WithRegistrationAPIBase(core.server.URL), WithRegistrationAgentToken("ol_agent_provided"), WithRegistrationStore(store),
	}

	if _, err := EnsureAgent(context.Background(), spec, options...); err == nil {
		t.Fatal("lost registration response succeeded")
	}
	if pending, err := store.loadPendingAgentRegistration(); err != nil || pending == nil || pending.AgentToken != "ol_agent_provided" {
		t.Fatalf("pending after lost response = %#v err=%v", pending, err)
	}
	registration, err := EnsureAgent(context.Background(), spec, options...)
	if err != nil || registration.AgentToken != "ol_agent_provided" {
		t.Fatalf("registration = %#v err=%v", registration, err)
	}
	if _, tokens := core.calls(); strings.Join(tokens, ",") != "ol_agent_provided,ol_agent_provided" {
		t.Fatalf("register tokens = %v", tokens)
	}
	if pending, err := store.loadPendingAgentRegistration(); err != nil || pending != nil {
		t.Fatalf("pending after completion = %#v err=%v", pending, err)
	}
}

func TestEnsureAgentNeverReplaysMismatchedPendingRecord(t *testing.T) {
	t.Parallel()
	base := EnsureAgentRequest{
		Slug: "demo-agent", Name: "Demo Agent", EndpointURL: "https://agent.example.test/old", UserToken: "ol_user_creator",
	}
	with := func(edit func(*EnsureAgentRequest)) EnsureAgentRequest {
		request := base
		edit(&request)
		return request
	}
	// exact is the record an interrupted start with request r leaves behind.
	exact := func(r EnsureAgentRequest) pendingAgentRegistration {
		return pendingAgentRegistration{AgentToken: "ol_agent_stale_a", Request: pendingRequestFor(r), Mint: pendingMintFor(r)}
	}
	withoutMint := exact(base)
	withoutMint.Mint = nil
	withEndpointAuth := with(func(r *EnsureAgentRequest) { r.EndpointAuthHeader = "Bearer endpoint-a" })
	otherPlatform := exact(base)
	otherPlatform.APIBase = "https://other.example.test"
	for _, tc := range []struct {
		name    string
		record  pendingAgentRegistration
		current EnsureAgentRequest
		want    string
		// legacy writes the record without an endpoint auth fingerprint.
		legacy bool
	}{
		{"changed slug", exact(with(func(r *EnsureAgentRequest) { r.Slug = "old-agent" })), base, "ol_agent_minted_1", false},
		{"changed endpoint url", exact(base),
			with(func(r *EnsureAgentRequest) { r.EndpointURL = "https://agent.example.test/new" }), "ol_agent_minted_1", false},
		{"different supplied token", exact(base),
			with(func(r *EnsureAgentRequest) { r.UserToken, r.AgentToken = "", "ol_agent_supplied_b" }), "ol_agent_supplied_b", false},
		{"force_new with a different slug", exact(with(func(r *EnsureAgentRequest) { r.Slug = "first-agent" })),
			with(func(r *EnsureAgentRequest) { r.Slug, r.Policy = "second-agent", RegisterPolicyForceNew }), "ol_agent_minted_1", false},
		{"other platform", otherPlatform, base, "ol_agent_minted_1", false},
		{"changed token scopes", exact(base),
			with(func(r *EnsureAgentRequest) { r.TokenScopes = []string{"agent:pull"} }), "ol_agent_minted_1", false},
		{"changed token expiry", exact(base),
			with(func(r *EnsureAgentRequest) { r.TokenExpiresInMinutes = 60 }), "ol_agent_minted_1", false},
		{"changed token name", exact(base),
			with(func(r *EnsureAgentRequest) { r.TokenName = "Renamed token" }), "ol_agent_minted_1", false},
		{"different user token", exact(base),
			with(func(r *EnsureAgentRequest) { r.UserToken = "ol_user_other" }), "ol_agent_minted_1", false},
		{"record without mint parameters", withoutMint, base, "ol_agent_minted_1", false},
		{"changed endpoint auth header", exact(withEndpointAuth),
			with(func(r *EnsureAgentRequest) { r.EndpointAuthHeader = "Bearer endpoint-b" }), "ol_agent_minted_1", false},
		{"removed endpoint auth header", exact(withEndpointAuth), base, "ol_agent_minted_1", false},
		{"record without endpoint auth fingerprint", exact(base), base, "ol_agent_minted_1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			// The stale token is still redeemable, so replaying it would succeed visibly.
			core.pending["ol_agent_stale_a"] = true
			core.pending["ol_agent_supplied_b"] = true
			store := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
			record := tc.record
			if record.APIBase == "" {
				record.APIBase = core.server.URL
			}
			if tc.legacy {
				record.Version = pendingRegistrationVersion
				raw := mustMarshalRegistrationTest(t, record)
				if err := writeManagedRegistrationEnv(store.Path, map[string]string{pendingRegistrationEnvKey: string(raw)},
					pendingRegistrationEnvKeys); err != nil {
					t.Fatal(err)
				}
			} else if err := store.savePendingAgentRegistration(&record); err != nil {
				t.Fatal(err)
			}
			current := tc.current
			current.APIBase, current.Store = core.server.URL, store
			registration, err := EnsureAgent(context.Background(), current)
			if err != nil {
				t.Fatal(err)
			}
			if _, tokens := core.calls(); strings.Join(tokens, ",") != tc.want || registration.AgentToken != tc.want {
				t.Fatalf("register tokens=%v registration=%#v", tokens, registration)
			}
			if pending, err := store.loadPendingAgentRegistration(); err != nil || pending != nil {
				t.Fatalf("pending after completion = %#v err=%v", pending, err)
			}
		})
	}
}

func TestEnsureAgentDropsPendingRecordOnDefinitiveRejection(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	core.slugs["taken-agent"] = true
	path := filepath.Join(t.TempDir(), ".env")
	store := NewEnvRegistrationStore(path)

	_, err := EnsureAgent(context.Background(), AgentSpec{Slug: "taken-agent", Name: "Taken Agent"},
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || err.Error() != "openlinker: CONFLICT: slug 已被占用" {
		t.Fatalf("err = %v", err)
	}
	// As before the change, a refused first registration leaves no env file.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		raw, _ := os.ReadFile(path)
		t.Fatalf("env after conflict: err=%v content=%s", statErr, raw)
	}
}

func TestEnsureAgentRefusedAttemptKeepsConcurrentRecord(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	core.slugs["taken-agent"] = true
	store := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
	concurrent := &pendingAgentRegistration{
		APIBase: core.server.URL, AgentToken: "ol_agent_concurrent_a",
		Request: RegisterAgentViaTokenRequest{Slug: "other-agent", Name: "Other Agent"},
	}
	// Attempt A records its token while attempt B's redemption is in flight.
	core.beforeRegister = func(string) {
		if err := store.savePendingAgentRegistration(concurrent); err != nil {
			t.Errorf("save concurrent record: %v", err)
		}
	}

	_, err := EnsureAgent(context.Background(), AgentSpec{Slug: "taken-agent", Name: "Taken Agent"},
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	if !registrationStatus(err, http.StatusConflict) {
		t.Fatalf("err = %v", err)
	}
	if pending, err := store.loadPendingAgentRegistration(); err != nil || pending == nil || pending.AgentToken != "ol_agent_concurrent_a" {
		t.Fatalf("attempt B removed attempt A's record: %#v err=%v", pending, err)
	}
}

func TestEnsureAgentContinuesAfterMatchingRecordIsRefused(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	store := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
	matching := EnsureAgentRequest{Slug: "demo-agent", Name: "Demo Agent", UserToken: "ol_user_creator"}
	if err := store.savePendingAgentRegistration(&pendingAgentRegistration{
		APIBase: core.server.URL, AgentToken: "ol_agent_expired_unused",
		Request: pendingRequestFor(matching), Mint: pendingMintFor(matching),
	}); err != nil {
		t.Fatal(err)
	}

	registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	if err != nil {
		t.Fatal(err)
	}
	created, tokens := core.calls()
	if created != 1 || strings.Join(tokens, ",") != "ol_agent_expired_unused,ol_agent_minted_1" ||
		registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("create-token=%d register=%v registration=%#v", created, tokens, registration)
	}
	if pending, err := store.loadPendingAgentRegistration(); err != nil || pending != nil {
		t.Fatalf("pending after completion = %#v err=%v", pending, err)
	}
}

func TestEnsureAgentLeavesCompletedRegistrationUntouched(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	path := filepath.Join(t.TempDir(), ".env")
	store := NewEnvRegistrationStore(path)
	if err := store.SaveAgentRegistration(&AgentRegistration{
		AgentID: testPendingAgentID, AgentSlug: "demo-agent", AgentName: "Demo Agent", AgentToken: "ol_agent_active",
		TokenID: testRegistrationTokenID, APIBase: core.server.URL, RegisteredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.savePendingAgentRegistration(&pendingAgentRegistration{
		APIBase: core.server.URL, AgentToken: "ol_agent_stale",
		Request: pendingRequestFor(EnsureAgentRequest{Slug: "demo-agent", Name: "Demo Agent"}),
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := readPendingTestEnv(t, path)
	registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
		WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	if err != nil || registration.AgentToken != "ol_agent_active" {
		t.Fatalf("registration = %#v err=%v", registration, err)
	}
	after, _ := readPendingTestEnv(t, path)
	if created, tokens := core.calls(); created != 0 || len(tokens) != 0 || after != before {
		t.Fatalf("completed registration was not reused untouched: create-token=%d register=%v", created, tokens)
	}
}

const completedElsewhereEnv = "OPENLINKER_AGENT_ID=\"agent-elsewhere\"\nOPENLINKER_AGENT_TOKEN=\"ol_agent_completed_elsewhere\"\n"

func completedElsewhere(apiBase string) *AgentRegistration {
	return &AgentRegistration{
		AgentID: "agent-elsewhere", AgentSlug: "other-agent", AgentName: "Other Agent",
		AgentToken: "ol_agent_completed_elsewhere", APIBase: apiBase, UpdatedAt: time.Now().UTC(),
	}
}

// writeAsOtherProcess appends a completed registration directly, as another
// process sharing the env file would, without this process's path lock.
func writeAsOtherProcess(t *testing.T, path string) {
	t.Helper()
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Errorf("read: %v", err)
	}
	if err := os.WriteFile(path, append(existing, completedElsewhereEnv...), 0o600); err != nil {
		t.Errorf("write: %v", err)
	}
}

// saveConcurrently starts another store instance's completed save for path
// and gives it 200ms to land. While the caller holds the path lock it cannot.
func saveConcurrently(path string, registration *AgentRegistration) <-chan error {
	done := make(chan error, 1)
	go func() { done <- NewEnvRegistrationStore(path).SaveAgentRegistration(registration) }()
	select {
	case err := <-done:
		done <- err
	case <-time.After(200 * time.Millisecond):
	}
	return done
}

func assertCompletedElsewhereSurvived(t *testing.T, path, existing string, saved <-chan error) {
	t.Helper()
	if saved != nil {
		if err := <-saved; err != nil {
			t.Fatalf("concurrent save: %v", err)
		}
	}
	loaded, err := NewEnvRegistrationStore(path).LoadAgentRegistration()
	if err != nil || loaded == nil || loaded.AgentToken != "ol_agent_completed_elsewhere" {
		t.Fatalf("completed registration lost: %#v err=%v", loaded, err)
	}
	raw, _ := readPendingTestEnv(t, path)
	if !strings.HasPrefix(raw, existing) {
		t.Fatalf("other lines lost:\n%s", raw)
	}
}

func TestPendingCleanupKeepsCompletedRegistrationWrittenMeanwhile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		existing    string
		sameProcess bool
	}{
		{"other process, file created by the record", "", false},
		{"other process, file with other lines", "UNRELATED=value\n", false},
		{"another store instance, file created by the record", "", true},
		{"another store instance, file with other lines", "UNRELATED=value\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			core.slugs["taken-agent"] = true
			path := filepath.Join(t.TempDir(), ".env")
			if tc.existing != "" {
				if err := os.WriteFile(path, []byte(tc.existing), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			store := NewEnvRegistrationStore(path)
			var saved <-chan error
			// Another writer completes a registration after this attempt
			// compared its record: another process before the guarding
			// re-read, or another store instance just before the remove or
			// rename.
			pendingTestHooks.Store(store, func(stage string) {
				switch {
				case !tc.sameProcess && stage == "cleanup.reread":
					writeAsOtherProcess(t, path)
				case tc.sameProcess && stage == "cleanup.mutate" && saved == nil:
					saved = saveConcurrently(path, completedElsewhere(core.server.URL))
				}
			})
			t.Cleanup(func() { pendingTestHooks.Delete(store) })

			_, err := EnsureAgent(context.Background(), AgentSpec{Slug: "taken-agent", Name: "Taken Agent"},
				WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
			if !registrationStatus(err, http.StatusConflict) {
				t.Fatalf("err = %v", err)
			}
			if tc.sameProcess && saved == nil {
				t.Fatal("the cleanup never reached its mutation")
			}
			assertCompletedElsewhereSurvived(t, path, tc.existing, saved)
		})
	}
}

func TestPendingWriteKeepsCompletedRegistrationWrittenMeanwhile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		sameProcess bool
	}{{"other process", false}, {"another store instance", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			core.slugs["taken-agent"] = true
			path := filepath.Join(t.TempDir(), ".env")
			storeB := NewEnvRegistrationStore(path)
			var saved <-chan error
			wrote := false
			// Start A completes while start B's record write is in flight:
			// another process before B's guarding re-read, or another store
			// instance just before B's rename.
			pendingTestHooks.Store(storeB, func(stage string) {
				switch {
				case !tc.sameProcess && stage == "write.reread" && !wrote:
					wrote = true
					writeAsOtherProcess(t, path)
				case tc.sameProcess && stage == "write.rename" && saved == nil:
					saved = saveConcurrently(path, completedElsewhere(core.server.URL))
				}
			})
			t.Cleanup(func() { pendingTestHooks.Delete(storeB) })

			_, err := EnsureAgent(context.Background(), AgentSpec{Slug: "taken-agent", Name: "Taken Agent"},
				WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(storeB))
			if !registrationStatus(err, http.StatusConflict) {
				t.Fatalf("err = %v", err)
			}
			if tc.sameProcess && saved == nil {
				t.Fatal("the record write never reached its rename")
			}
			assertCompletedElsewhereSurvived(t, path, "", saved)
			if pending, err := storeB.loadPendingAgentRegistration(); err != nil || pending != nil {
				t.Fatalf("B's record after its 409 = %#v err=%v", pending, err)
			}
		})
	}
}

func TestPendingWriteGivesUpUnderContentionAndContinues(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	path := filepath.Join(t.TempDir(), ".env")
	store := NewEnvRegistrationStore(path)
	writes := 0
	// Another process changes the file before every guarding re-read.
	pendingTestHooks.Store(store, func(stage string) {
		if stage != "write.reread" {
			return
		}
		writes++
		if err := os.WriteFile(path, []byte(fmt.Sprintf("OTHER_WRITER=%d\n", writes)), 0o600); err != nil {
			t.Errorf("write: %v", err)
		}
	})
	t.Cleanup(func() { pendingTestHooks.Delete(store) })

	registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	if err != nil || registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("registration = %#v err=%v", registration, err)
	}
	raw, _ := readPendingTestEnv(t, path)
	if writes != pendingRegistrationWriteAttempts || strings.Contains(raw, pendingRegistrationEnvKey) ||
		!strings.Contains(raw, fmt.Sprintf("OTHER_WRITER=%d", writes)) {
		t.Fatalf("writes=%d env:\n%s", writes, raw)
	}
}

// embeddedEnvStore embeds the env store but keeps registrations elsewhere,
// as an encrypting or remote store might.
type embeddedEnvStore struct {
	*EnvRegistrationStore
	registration *AgentRegistration
}

func (s *embeddedEnvStore) LoadAgentRegistration() (*AgentRegistration, error) {
	return s.registration, nil
}

func (s *embeddedEnvStore) SaveAgentRegistration(registration *AgentRegistration) error {
	copyRegistration := *registration
	s.registration = &copyRegistration
	return nil
}

func TestEnsureAgentEmbeddedEnvStoreKeepsPreviousBehaviour(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	core.dropAfterCommit = 1
	path := filepath.Join(t.TempDir(), ".env")
	store := &embeddedEnvStore{EnvRegistrationStore: NewEnvRegistrationStore(path)}
	options := []RegistrationOption{
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store),
	}
	spec := AgentSpec{Slug: "demo-agent", Name: "Demo Agent"}

	if _, err := EnsureAgent(context.Background(), spec, options...); err == nil {
		t.Fatal("lost registration response succeeded")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("embedding store got a plaintext journal at %s: %v", path, err)
	}
	// Exactly as before: a new token is minted and the slug is already taken.
	if _, err := EnsureAgent(context.Background(), spec, options...); !registrationStatus(err, http.StatusConflict) {
		t.Fatalf("second EnsureAgent err = %v", err)
	}
	if created, tokens := core.calls(); created != 2 || strings.Join(tokens, ",") != "ol_agent_minted_1,ol_agent_minted_2" {
		t.Fatalf("create-token=%d register=%v", created, tokens)
	}

	var typedNil *EnvRegistrationStore
	if pendingRegistrationJournalFor(store) != nil || pendingRegistrationJournalFor(&memoryRegistrationStore{}) != nil ||
		pendingRegistrationJournalFor(nil) != nil || pendingRegistrationJournalFor(typedNil) != nil {
		t.Fatal("only *EnvRegistrationStore itself may journal")
	}
}

func TestEnvRegistrationStorePendingRecordIsSeparateFromCompletedKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".env")
	original := "# keep\nUNRELATED=value\nOPENLINKER_AGENT_ID=\"" + testPendingAgentID + "\"\nOPENLINKER_AGENT_TOKEN=\"ol_agent_saved\"\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewEnvRegistrationStore(path)
	record := &pendingAgentRegistration{
		APIBase: "https://api.example.test", AgentToken: "ol_agent_pending", TokenID: testRegistrationTokenID,
		TokenPrefix: "ol_agent_pend", Request: RegisterAgentViaTokenRequest{
			Slug: "demo-agent", Name: "Demo \"Agent\" = ü", Description: "line\nbreak", EndpointAuthHeader: "Bearer endpoint-secret",
			Tags: []string{"agent", "runtime"}, AbilityTags: []string{"agent", "runtime"}, Visibility: "private", ConnectionMode: "runtime",
		},
	}
	if err := store.savePendingAgentRegistration(record); err != nil {
		t.Fatal(err)
	}
	raw, mode := readPendingTestEnv(t, path)
	if mode != 0o600 || strings.Contains(raw, "endpoint-secret") || !strings.HasPrefix(raw, original) {
		t.Fatalf("pending env mode=%o content=%s", mode, raw)
	}
	loaded, err := store.LoadAgentRegistration()
	if err != nil || loaded.AgentID != testPendingAgentID || loaded.AgentToken != "ol_agent_saved" {
		t.Fatalf("completed registration = %#v err=%v", loaded, err)
	}
	pending, err := store.loadPendingAgentRegistration()
	if err != nil || pending == nil {
		t.Fatalf("pending = %#v err=%v", pending, err)
	}
	want := *record
	want.Version = pendingRegistrationVersion
	want.EndpointAuthFingerprint = endpointAuthFingerprint("Bearer endpoint-secret")
	want.Request.EndpointAuthHeader = ""
	if got, _ := json.Marshal(pending); string(got) != string(mustMarshalRegistrationTest(t, want)) {
		t.Fatalf("pending = %s", got)
	}
	mint := pendingMintParameters{TokenName: "Demo Agent", TokenScopes: []string{"agent:pull", "agent:call"},
		CreatorFingerprint: creatorCredentialFingerprint("ol_user_creator")}
	if pendingRegistrationMatches(pending, "https://api.example.test", record.Request, mint) {
		t.Fatal("record without mint parameters matched a minting start")
	}
	pending.Mint = &mint
	if !pendingRegistrationMatches(pending, "https://api.example.test", record.Request, mint) {
		t.Fatal("recorded attempt does not match its own request")
	}
	if got := endpointAuthFingerprint("Bearer endpoint-secret"); len(got) != 32 || got != strings.ToLower(got) ||
		strings.Contains(got, "endpoint-secret") || endpointAuthFingerprint("") != "none" ||
		endpointAuthFingerprint(" Bearer endpoint-secret") == got {
		t.Fatalf("endpoint auth fingerprint = %q", got)
	}
	if got := creatorCredentialFingerprint(" ol_user_creator "); len(got) != 32 || got != strings.ToLower(got) ||
		got != creatorCredentialFingerprint("ol_user_creator") || strings.Contains(got, "ol_user") {
		t.Fatalf("fingerprint = %q", got)
	}
	if err := store.clearPendingAgentRegistration("ol_agent_someone_else"); err != nil {
		t.Fatal(err)
	}
	if raw, _ = readPendingTestEnv(t, path); !strings.Contains(raw, "ol_agent_pending") {
		t.Fatalf("clear removed another attempt's record:\n%s", raw)
	}
	if err := store.clearPendingAgentRegistration("ol_agent_pending"); err != nil {
		t.Fatal(err)
	}
	if raw, _ = readPendingTestEnv(t, path); raw != original {
		t.Fatalf("clear changed completed env:\n%s", raw)
	}

	created := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
	if err := created.savePendingAgentRegistration(record); err != nil {
		t.Fatal(err)
	}
	if pending, err := created.loadPendingAgentRegistration(); err != nil || pending == nil || !pending.CreatedFile {
		t.Fatalf("pending in new file = %#v err=%v", pending, err)
	}
	if err := created.clearPendingAgentRegistration("ol_agent_pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(created.Path); !os.IsNotExist(err) {
		t.Fatalf("file created only for the pending record remains: %v", err)
	}

	if err := os.WriteFile(path, []byte(pendingRegistrationEnvKey+"=\"{not json\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.loadPendingAgentRegistration(); err != nil || pending != nil {
		t.Fatalf("damaged pending = %#v err=%v", pending, err)
	}
	oversized := &pendingAgentRegistration{APIBase: "https://api.example.test", AgentToken: "ol_agent_big",
		Request: RegisterAgentViaTokenRequest{Name: "Big", Description: strings.Repeat("x", maxPendingRegistrationBytes)}}
	if err := store.savePendingAgentRegistration(oversized); err != nil {
		t.Fatal(err)
	}
	if raw, _ = readPendingTestEnv(t, path); strings.Contains(raw, "ol_agent_big") {
		t.Fatalf("oversized pending record was written: %d bytes", len(raw))
	}
}

func mustMarshalRegistrationTest(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
