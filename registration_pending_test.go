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
	"sync/atomic"
	"syscall"
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
	beforeMint       func()
	// arrivals counts requests as they reach the server, before its lock.
	arrivals atomic.Int32
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
	core.arrivals.Add(1)
	core.mu.Lock()
	defer core.mu.Unlock()
	switch request.Method + " " + request.URL.Path {
	case "POST /api/v1/creator/agent-tokens":
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ol_user_") {
			core.t.Errorf("create token auth = %q", request.Header.Get("Authorization"))
		}
		if core.beforeMint != nil {
			core.beforeMint()
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

// aliasedEnvPath returns path reached through a symlinked directory, so a
// store using it shares the env file under another name.
func aliasedEnvPath(t *testing.T, path string) string {
	t.Helper()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Dir(path), alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	return filepath.Join(alias, filepath.Base(path))
}

// dotdotAliasEnvPath returns path as "<link>/../<name>", where link points to a
// directory inside path's directory: the file system resolves link before
// "..", so this names the same file although a lexical clean would not.
func dotdotAliasEnvPath(t *testing.T, path string) string {
	t.Helper()
	inner := filepath.Join(filepath.Dir(path), "inner")
	if err := os.Mkdir(inner, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(inner, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	alias := link + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(path)
	if got, want := canonicalRegistrationEnvPath(alias), canonicalRegistrationEnvPath(path); got != want {
		t.Fatalf("canonical(%q) = %q, want %q", alias, got, want)
	}
	return alias
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
			alias := aliasedEnvPath(t, path)
			var saved <-chan error
			// Another writer completes a registration after this attempt
			// compared its record: another process before the guarding
			// re-read, or another store instance (reaching the file through
			// a symlinked directory) just before the remove or rename.
			pendingTestHooks.Store(store, func(stage string) {
				switch {
				case !tc.sameProcess && stage == "cleanup.reread":
					writeAsOtherProcess(t, path)
				case tc.sameProcess && stage == "cleanup.mutate" && saved == nil:
					saved = saveConcurrently(alias, completedElsewhere(core.server.URL))
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
			alias := aliasedEnvPath(t, path)
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
					saved = saveConcurrently(alias, completedElsewhere(core.server.URL))
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

func TestConcurrentFirstRegistrationsShareOneAttempt(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"same path", "symlinked directory", "dotdot after a symlinked directory"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			core.dropAfterCommit = 1
			pathA := filepath.Join(t.TempDir(), ".env")
			pathB := pathA
			switch mode {
			case "symlinked directory":
				pathB = aliasedEnvPath(t, pathA)
			case "dotdot after a symlinked directory":
				pathB = dotdotAliasEnvPath(t, pathA)
			}
			ensure := func(path string) (*AgentRegistration, error) {
				return EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
					WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"),
					WithRegistrationStore(NewEnvRegistrationStore(path)))
			}
			type result struct {
				registration *AgentRegistration
				err          error
			}
			resultB := make(chan result, 1)
			started := false
			// Start B while A is minting. Without the registration lock B
			// would read the empty record, mint a second token and reach Core
			// within 200ms; with it, B waits for A's whole attempt.
			core.beforeMint = func() {
				if started {
					return
				}
				started = true
				go func() {
					registration, err := ensure(pathB)
					resultB <- result{registration, err}
				}()
				for deadline := time.Now().Add(200 * time.Millisecond); time.Now().Before(deadline) && core.arrivals.Load() < 2; {
					time.Sleep(5 * time.Millisecond)
				}
			}

			// A's redemption commits but its response is lost.
			if _, err := ensure(pathA); err == nil {
				t.Fatal("lost registration response succeeded")
			}
			b := <-resultB
			if b.err != nil {
				t.Fatalf("B err = %v", b.err)
			}
			created, tokens := core.calls()
			// B replays A's recorded token, so nothing is orphaned.
			if b.registration.AgentToken != "ol_agent_minted_1" || created != 1 ||
				strings.Join(tokens, ",") != "ol_agent_minted_1,ol_agent_minted_1" {
				t.Fatalf("B=%#v create-token=%d register=%v", b.registration, created, tokens)
			}
			loaded, err := NewEnvRegistrationStore(pathA).LoadAgentRegistration()
			if err != nil || loaded == nil || loaded.AgentToken != "ol_agent_minted_1" {
				t.Fatalf("stored = %#v err=%v", loaded, err)
			}
		})
	}
}

func TestPendingRecordDirectorySyncErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		syncErr error
		fails   bool
	}{
		{"genuine I/O error fails before redemption", syscall.EIO, true},
		{"unsupported directory sync is skipped", syscall.EINVAL, false},
		{"unsupported operation is skipped", syscall.ENOTSUP, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			store := NewEnvRegistrationStore(filepath.Join(t.TempDir(), ".env"))
			pendingDirSyncTestHooks.Store(store, func(string) error { return tc.syncErr })
			t.Cleanup(func() { pendingDirSyncTestHooks.Delete(store) })

			registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
				WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
			created, tokens := core.calls()
			if tc.fails {
				if !errors.Is(err, syscall.EIO) || created != 1 || len(tokens) != 0 {
					t.Fatalf("err=%v create-token=%d register=%v", err, created, tokens)
				}
				return
			}
			if err != nil || registration.AgentToken != "ol_agent_minted_1" || len(tokens) != 1 {
				t.Fatalf("registration=%#v err=%v register=%v", registration, err, tokens)
			}
		})
	}
}

func TestPersistentDirectorySyncErrorNeverRedeems(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	path := filepath.Join(t.TempDir(), ".env")
	ensure := func(syncErr error) (*AgentRegistration, error) {
		store := NewEnvRegistrationStore(path)
		pendingDirSyncTestHooks.Store(store, func(string) error { return syncErr })
		t.Cleanup(func() { pendingDirSyncTestHooks.Delete(store) })
		return EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
			WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
	}
	// The record's directory never syncs: the first start fails after
	// writing it, the second before replaying it, and neither redeems.
	for start := 1; start <= 2; start++ {
		if _, err := ensure(syscall.EIO); !errors.Is(err, syscall.EIO) {
			t.Fatalf("start %d err = %v", start, err)
		}
	}
	if created, tokens := core.calls(); created != 1 || len(tokens) != 0 {
		t.Fatalf("create-token=%d register=%v", created, tokens)
	}
	// Once the directory syncs, the recorded token is redeemed.
	registration, err := ensure(nil)
	if err != nil || registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("registration = %#v err=%v", registration, err)
	}
	if created, tokens := core.calls(); created != 1 || strings.Join(tokens, ",") != "ol_agent_minted_1" {
		t.Fatalf("create-token=%d register=%v", created, tokens)
	}
}

func TestPendingRecordSyncsParentsOfCreatedDirectories(t *testing.T) {
	t.Parallel()
	for _, failTop := range []bool{false, true} {
		t.Run(fmt.Sprintf("failTop=%t", failTop), func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			root := t.TempDir()
			path := filepath.Join(root, "a", "b", ".env")
			store := NewEnvRegistrationStore(path)
			var synced []string
			pendingDirSyncTestHooks.Store(store, func(dir string) error {
				synced = append(synced, dir)
				if failTop && dir == root {
					return syscall.EIO
				}
				return nil
			})
			t.Cleanup(func() { pendingDirSyncTestHooks.Delete(store) })

			registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
				WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
			_, tokens := core.calls()
			if failTop {
				// The parent sync runs before the record is written, so a
				// failure leaves nothing to replay and nothing is redeemed.
				if _, statErr := os.Stat(path); !errors.Is(err, syscall.EIO) || len(tokens) != 0 || !os.IsNotExist(statErr) {
					t.Fatalf("err=%v register=%v stat=%v synced=%v", err, tokens, statErr, synced)
				}
				return
			}
			want := []string{root, filepath.Join(root, "a"), filepath.Join(root, "a", "b")}
			if err != nil || registration.AgentToken != "ol_agent_minted_1" ||
				len(synced) < len(want) || strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
				t.Fatalf("registration=%#v err=%v synced=%v", registration, err, synced)
			}
		})
	}
}

// syncEventLog records directory syncs and register requests in the order
// they happen; the server and the client append from different goroutines.
type syncEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *syncEventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *syncEventLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	events := l.events
	l.events = nil
	return events
}

// syncedBeforeRegister returns the directories synced before the first
// register request and whether one was sent.
func syncedBeforeRegister(events []string) ([]string, bool) {
	var synced []string
	for _, event := range events {
		if event == "register" {
			return synced, true
		}
		synced = append(synced, strings.TrimPrefix(event, "sync "))
	}
	return synced, false
}

// ensureWithSyncLog runs one start against path with a fresh store whose
// directory syncs are logged and fail with EIO for dir == fail.
func ensureWithSyncLog(t *testing.T, core *replayRegistrationCore, log *syncEventLog, path, fail string) (*AgentRegistration, error) {
	t.Helper()
	store := NewEnvRegistrationStore(path)
	pendingDirSyncTestHooks.Store(store, func(dir string) error {
		log.add("sync " + dir)
		if dir == fail {
			return syscall.EIO
		}
		return nil
	})
	defer pendingDirSyncTestHooks.Delete(store)
	return EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
		WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
}

func TestPendingRecordResyncsDirectoriesLeftBySyncFailure(t *testing.T) {
	t.Parallel()
	core := newReplayRegistrationCore(t)
	var log syncEventLog
	core.beforeRegister = func(string) { log.add("register") }
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(a, "b")
	path := filepath.Join(b, ".env")

	// The first start creates a and b, syncs root for a and then fails to
	// sync a for b: nothing is written or redeemed, but both directories stay.
	if _, err := ensureWithSyncLog(t, core, &log, path, a); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first start err = %v", err)
	}
	if events := log.take(); strings.Join(events, "|") != "sync "+root+"|sync "+a {
		t.Fatalf("first start events = %v", events)
	}
	if _, err := os.Stat(b); err != nil {
		t.Fatalf("created directory is gone: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("record written after a failed parent sync: %v", err)
	}

	// The next start creates nothing, yet syncs the record's directory, a and
	// root again before the redeem request is sent.
	registration, err := ensureWithSyncLog(t, core, &log, path, "")
	if err != nil || registration.AgentToken != "ol_agent_minted_2" {
		t.Fatalf("second start registration = %#v err=%v", registration, err)
	}
	synced, sent := syncedBeforeRegister(log.take())
	want := []string{b, a, root}
	if !sent || len(synced) < len(want) || strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
		t.Fatalf("synced before register = %v (sent %t), want prefix %v", synced, sent, want)
	}
}

func TestPendingRecordReplayResyncsAncestors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// The first start either fails to sync the directory above the
		// record's or loses Core's response after Core committed.
		failSync     bool
		dropResponse bool
	}{
		{name: "ancestor sync failed after the record was written", failSync: true},
		{name: "response lost after Core committed", dropResponse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			core := newReplayRegistrationCore(t)
			var log syncEventLog
			core.beforeRegister = func(string) { log.add("register") }
			if tc.dropResponse {
				core.dropAfterCommit = 1
			}
			root := t.TempDir()
			a := filepath.Join(root, "a")
			b := filepath.Join(a, "b")
			path := filepath.Join(b, ".env")
			// a and b already exist, as an earlier start that created them and
			// failed to sync them leaves them: no start creates them again.
			if err := os.MkdirAll(b, 0o700); err != nil {
				t.Fatal(err)
			}

			fail := ""
			if tc.failSync {
				fail = a
			}
			_, err := ensureWithSyncLog(t, core, &log, path, fail)
			synced, sent := syncedBeforeRegister(log.take())
			switch {
			case tc.failSync && (!errors.Is(err, syscall.EIO) || sent || strings.Join(synced, "|") != b+"|"+a):
				// The record is written and its directory synced, but a failed
				// ancestor sync stops the start before the redeem request.
				t.Fatalf("first start err=%v sent=%t synced=%v", err, sent, synced)
			case tc.dropResponse && (err == nil || !sent):
				t.Fatalf("first start err=%v sent=%t", err, sent)
			}
			if record, loadErr := NewEnvRegistrationStore(path).loadPendingAgentRegistration(); loadErr != nil || record == nil {
				t.Fatalf("record after first start = %#v err=%v", record, loadErr)
			}

			// The replay syncs the record's directory, a and root before it
			// sends the recorded token again.
			registration, err := ensureWithSyncLog(t, core, &log, path, "")
			if err != nil || registration.AgentToken != "ol_agent_minted_1" {
				t.Fatalf("replay registration = %#v err=%v", registration, err)
			}
			synced, sent = syncedBeforeRegister(log.take())
			want := []string{b, a, root}
			if !sent || len(synced) < len(want) || strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
				t.Fatalf("synced before replay = %v (sent %t), want prefix %v", synced, sent, want)
			}
			if created, tokens := core.calls(); created != 1 || tokens[len(tokens)-1] != "ol_agent_minted_1" {
				t.Fatalf("create-token=%d register=%v", created, tokens)
			}
		})
	}
}

func TestPendingRecordDotSpellingsResyncAncestors(t *testing.T) {
	t.Parallel()
	separator := string(filepath.Separator)
	for _, spelling := range []struct {
		name string
		// below spells the record's directory, base/new, under base.
		below string
	}{
		{name: "dot", below: "new" + separator + "."},
		{name: "dot-dot", below: "new" + separator + "sub" + separator + ".."},
	} {
		for _, scenario := range []string{"retry", "replay"} {
			t.Run(spelling.name+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				core := newReplayRegistrationCore(t)
				var log syncEventLog
				core.beforeRegister = func(string) { log.add("register") }
				base := filepath.Join(t.TempDir(), "base")
				newDir := filepath.Join(base, "new")
				if err := os.Mkdir(base, 0o700); err != nil {
					t.Fatal(err)
				}
				recordDir := base + separator + spelling.below
				path := recordDir + separator + ".env"
				// retry: the first start creates new itself. replay: new
				// already exists, as an earlier start that created it and
				// failed to sync base leaves it, so the record is written.
				wantFirst, wantToken, wantMinted := base, "ol_agent_minted_2", 2
				if scenario == "replay" {
					if err := os.MkdirAll(filepath.Join(newDir, "sub"), 0o700); err != nil {
						t.Fatal(err)
					}
					wantFirst, wantToken, wantMinted = recordDir+"|"+base, "ol_agent_minted_1", 1
				}

				// The first start fails to sync base, which holds new's entry.
				_, err := ensureWithSyncLog(t, core, &log, path, base)
				synced, sent := syncedBeforeRegister(log.take())
				if !errors.Is(err, syscall.EIO) || sent || strings.Join(synced, "|") != wantFirst {
					t.Fatalf("first start err=%v sent=%t synced=%v, want synced %s", err, sent, synced, wantFirst)
				}
				_, statErr := os.Stat(filepath.Join(newDir, ".env"))
				if (scenario == "retry") != os.IsNotExist(statErr) {
					t.Fatalf("%s: record after the first start: %v", scenario, statErr)
				}

				// The next start syncs base again before the redeem request,
				// however the record's directory is spelled.
				registration, err := ensureWithSyncLog(t, core, &log, path, "")
				if err != nil || registration.AgentToken != wantToken {
					t.Fatalf("second start registration=%#v err=%v", registration, err)
				}
				synced, sent = syncedBeforeRegister(log.take())
				if want := []string{recordDir, base}; !sent || len(synced) < len(want) ||
					strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
					t.Fatalf("synced before register = %v (sent %t), want prefix %v", synced, sent, want)
				}
				if created, tokens := core.calls(); created != wantMinted || strings.Join(tokens, ",") != wantToken {
					t.Fatalf("create-token=%d register=%v", created, tokens)
				}
			})
		}
	}
}

func TestPendingRecordPathSyncStopsAtDirectoriesThatAlwaysExisted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, dir := range []string{
		filepath.Join(root, "a", "b"), filepath.Join(root, "c"), filepath.Join(root, "real", "sub"),
		filepath.Join(root, "real", "inner"), filepath.Join(root, "real", "x"),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	syncedFor := func(path string) []string {
		t.Helper()
		store := NewEnvRegistrationStore(path)
		var synced []string
		pendingDirSyncTestHooks.Store(store, func(dir string) error {
			synced = append(synced, dir)
			return nil
		})
		defer pendingDirSyncTestHooks.Delete(store)
		if err := store.syncRecordPath(""); err != nil {
			t.Fatalf("syncRecordPath(%s) = %v", path, err)
		}
		return synced
	}
	separator := string(filepath.Separator)

	// A plain chain is synced bottom up through root and, on the same file
	// system, beyond it, each directory the parent of the one before.
	synced := syncedFor(filepath.Join(root, "a", "b", ".env"))
	want := []string{filepath.Join(root, "a", "b"), filepath.Join(root, "a"), root}
	if len(synced) < len(want) || strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
		t.Fatalf("plain chain synced %v, want prefix %v", synced, want)
	}
	for i := 1; i < len(synced); i++ {
		if synced[i] != registrationEnvDir(synced[i-1]) {
			t.Fatalf("plain chain synced %v: %s is not the parent of %s", synced, synced[i], synced[i-1])
		}
	}

	// "." and "name/.." are spelled as the file system resolves them, so the
	// walk continues to the directories the record really sits under.
	dot := root + separator + "a" + separator + "." + separator + "b"
	synced = syncedFor(dot + separator + ".env")
	if want := []string{dot, filepath.Join(root, "a"), root}; len(synced) < len(want) ||
		strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
		t.Fatalf("dot chain synced %v, want prefix %v", synced, want)
	}
	dotdot := root + separator + "a" + separator + ".." + separator + "c"
	synced = syncedFor(dotdot + separator + ".env")
	if want := []string{dotdot, root}; len(synced) < len(want) || strings.Join(synced[:len(want)], "|") != strings.Join(want, "|") {
		t.Fatalf("dot-dot chain synced %v, want prefix %v", synced, want)
	}

	// A symlink always existed: the directory holding it is synced through
	// it, nothing above it.
	if runtime.GOOS != "windows" {
		link := filepath.Join(root, "link")
		if err := os.Symlink(filepath.Join(root, "real"), link); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if synced := syncedFor(filepath.Join(link, "sub", ".env")); strings.Join(synced, "|") != filepath.Join(link, "sub")+"|"+link {
			t.Fatalf("symlink chain synced %v", synced)
		}
		// ".." after a symlink is the parent of its target, another tree: its
		// directory is synced and nothing above it.
		innerLink := filepath.Join(root, "inner-link")
		if err := os.Symlink(filepath.Join(root, "real", "inner"), innerLink); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		afterLink := innerLink + separator + ".." + separator + "x"
		if synced := syncedFor(afterLink + separator + ".env"); strings.Join(synced, "|") != afterLink+"|"+innerLink+separator+".." {
			t.Fatalf("dot-dot after a symlink synced %v", synced)
		}
		for spelled, want := range map[string]string{
			root + "/a/./b/":            filepath.Join(root, "a", "b"),
			root + "//a/b/..":           filepath.Join(root, "a"),
			root + "/a/b/../../c":       filepath.Join(root, "c"),
			root + "/link/sub/..":       filepath.Join(root, "link"),
			root + "/inner-link/../x":   root + "/inner-link/../x",
			root + "/inner-link/./../x": root + "/inner-link/../x",
			"/..":                       "/",
			"./a":                       "a",
			".":                         ".",
			"../x/./y":                  "../x/y",
		} {
			if got, err := physicalRegistrationDir(spelled); err != nil || got != want {
				t.Fatalf("physicalRegistrationDir(%q) = %q, %v; want %q", spelled, got, err, want)
			}
		}
		// A directory that cannot be inspected is an error, never a stop.
		if _, ok, err := creatableRegistrationDirParent(filepath.Join(root, "missing")); ok || !os.IsNotExist(err) {
			t.Fatalf("missing directory ok=%t err=%v", ok, err)
		}
		if _, err := physicalRegistrationDir(root + "/missing/../c"); !os.IsNotExist(err) {
			t.Fatalf("missing directory before .. err=%v", err)
		}
	}

	for _, dir := range []string{"", ".", "..", "x" + separator + "..", separator, filepath.VolumeName(root) + separator} {
		if parent, ok, err := creatableRegistrationDirParent(dir); ok || err != nil {
			t.Fatalf("creatableRegistrationDirParent(%q) = %q, %t, %v", dir, parent, ok, err)
		}
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
