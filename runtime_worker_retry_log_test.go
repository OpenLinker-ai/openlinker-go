package openlinker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// lockedLogBuffer lets the race detector check concurrent Worker logging.
type lockedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (output *lockedLogBuffer) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.buffer.Write(data)
}

func (output *lockedLogBuffer) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.buffer.String()
}

func TestRuntimeRetryLogLimiterLogsFirstOccurrenceThenOncePerInterval(t *testing.T) {
	var limiter runtimeRetryLogLimiter
	start := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	interval := 30 * time.Second
	if allowed, suppressed := limiter.allow("claim|a", start, interval); !allowed || suppressed != 0 {
		t.Fatalf("first occurrence = %v/%d", allowed, suppressed)
	}
	for offset := time.Second; offset < interval; offset += time.Second {
		if allowed, _ := limiter.allow("claim|a", start.Add(offset), interval); allowed {
			t.Fatalf("repeat after %s was logged inside the interval", offset)
		}
	}
	if allowed, _ := limiter.allow("claim|b", start.Add(time.Second), interval); !allowed {
		t.Fatal("a different cause must not be suppressed by another cause")
	}
	allowed, suppressed := limiter.allow("claim|a", start.Add(interval), interval)
	if !allowed || suppressed != int(interval/time.Second)-1 {
		t.Fatalf("after interval = %v/%d", allowed, suppressed)
	}
	if allowed, _ := limiter.allow("claim|a", start.Add(interval+time.Second), interval); allowed {
		t.Fatal("interval did not restart after a logged line")
	}

	// Distinct causes are bounded so varying errors cannot grow memory forever.
	for index := 0; index < 3*runtimeRetryLogMaxCauses; index++ {
		limiter.allow(fmt.Sprintf("cause-%d", index), start, interval)
	}
	limiter.mu.Lock()
	size := len(limiter.entries)
	limiter.mu.Unlock()
	if size > runtimeRetryLogMaxCauses {
		t.Fatalf("limiter tracks %d causes, maximum %d", size, runtimeRetryLogMaxCauses)
	}
}

func TestRuntimeRetryLogLimiterOverflowKeepsTrackedCauses(t *testing.T) {
	var limiter runtimeRetryLogLimiter
	start := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	interval := 30 * time.Second
	for index := 0; index < runtimeRetryLogMaxCauses; index++ {
		if allowed, _ := limiter.allow(fmt.Sprintf("cause-%d", index), start.Add(time.Duration(index)*time.Millisecond), interval); !allowed {
			t.Fatalf("tracked cause %d was not logged first", index)
		}
	}
	// Every tracked cause is unexpired: new causes share one bounded bucket.
	if allowed, _ := limiter.allow("new-a", start.Add(time.Second), interval); !allowed {
		t.Fatal("first overflow cause was not logged")
	}
	if allowed, _ := limiter.allow("new-b", start.Add(2*time.Second), interval); allowed {
		t.Fatal("overflow bucket was not rate-limited")
	}
	// Revisiting a tracked cause after the overflow is still suppressed: the
	// map was neither cleared nor did it forget the cause.
	if allowed, _ := limiter.allow("cause-0", start.Add(3*time.Second), interval); allowed {
		t.Fatal("overflow cleared a tracked cause, which was logged again early")
	}
	limiter.mu.Lock()
	size := len(limiter.entries)
	_, newTracked := limiter.entries["new-a"]
	limiter.mu.Unlock()
	if size != runtimeRetryLogMaxCauses || newTracked {
		t.Fatalf("limiter size = %d, overflow cause tracked = %v", size, newTracked)
	}
	// After its interval the tracked cause logs again with its suppressed count.
	allowed, suppressed := limiter.allow("cause-0", start.Add(interval), interval)
	if !allowed || suppressed != 1 {
		t.Fatalf("revisited cause = %v/%d, want logged with 1 suppressed", allowed, suppressed)
	}
	// cause-1 is now the oldest expired entry; a new cause evicts only it.
	if allowed, _ := limiter.allow("new-c", start.Add(interval+time.Second), interval); !allowed {
		t.Fatal("new cause was not tracked after an entry expired")
	}
	limiter.mu.Lock()
	_, oldestKept := limiter.entries["cause-1"]
	_, newestKept := limiter.entries["cause-63"]
	_, revisitedKept := limiter.entries["cause-0"]
	_, newCTracked := limiter.entries["new-c"]
	size = len(limiter.entries)
	limiter.mu.Unlock()
	if oldestKept || !newestKept || !revisitedKept || !newCTracked || size != runtimeRetryLogMaxCauses {
		t.Fatalf("eviction kept cause-1=%v cause-63=%v cause-0=%v new-c=%v size=%d", oldestKept, newestKept, revisitedKept, newCTracked, size)
	}
}

func TestRuntimeRetryLogTextUsesOnlySafeParts(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"platform error", fmt.Errorf("claim: %w", &Error{StatusCode: 503, Code: "RUNTIME_UNAVAILABLE", Message: "upstream detail"}), "RUNTIME_UNAVAILABLE (HTTP 503)"},
		{"unsafe platform code", &Error{StatusCode: 502, Code: "bad code\nforged", Message: "x"}, "UNRECOGNIZED_CODE (HTTP 502)"},
		{"credential in code", &Error{StatusCode: 401, Code: "ol_agent_errorcodesecret", Message: "x"}, "UNRECOGNIZED_CODE (HTTP 401)"},
		{"WebSocket close", fmt.Errorf("read: %w", &websocket.CloseError{Code: 1008, Text: "reason text"}), "WebSocket closed with code 1008"},
		{"capacity mismatch", &runtimeWebSocketCapacityChangedError{Attached: 0, Current: 2}, "WebSocket attached with capacity 0; current capacity 2 applies on the next attach"},
		{"URL error", fmt.Errorf("attach: %w", &url.Error{Op: "Post", URL: "https://core.example.test/x",
			Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}), "Post request: connection refused"},
		{"deadline", fmt.Errorf("heartbeat: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", context.Canceled, "canceled"},
		{"protocol mismatch", fmt.Errorf("%w: resume response count", ErrRuntimeProtocolMismatch), "Runtime protocol mismatch"},
		{"unclassified", fmt.Errorf("attach: %w", errors.New("any message text")), "unclassified error (*fmt.wrapError > *errors.errorString)"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := runtimeRetryLogText(testCase.err); got != testCase.want {
				t.Fatalf("log text = %q, want %q", got, testCase.want)
			}
		})
	}
	var closeErr *websocket.CloseError
	wrapped := fmt.Errorf("read: %w", &websocket.CloseError{Code: 1011, Text: "kept"})
	original := wrapped.Error()
	_ = runtimeRetryLogText(wrapped)
	if wrapped.Error() != original || !errors.As(wrapped, &closeErr) || closeErr.Text != "kept" {
		t.Fatal("building log text changed the error chain")
	}
}

// Every credential shape raised in review, carried by errors of every kind the
// retry paths see. No logged line may contain any secret substring.
func TestRuntimeRetryLogNeverContainsSecretsFromErrorText(t *testing.T) {
	secrets := []string{
		"bearer-secret", "basic-secret", "quoted-bearer-secret", "quoted-basic-secret",
		"folded-secret", "first-quoted-secret", "json-multiline-secret", "cookie-secret",
		"digest-secret", "url-password", "p@ssword", "ol_agent_platformsecret", "ol_user_platformsecret",
		"olat_platformsecret", "eyJhbGciOiJIUzI1NiJ9", "sk-providersecret0123456789",
	}
	texts := []string{
		"Authorization: Bearer bearer-secret",
		"Authorization: Basic basic-secret",
		`Authorization: Bearer "quoted-bearer-secret"`,
		`Authorization: Basic "quoted-basic-secret"`,
		"Authorization: Bearer\r\n\tfolded-secret",
		"X-Auth-Token: \"first-quoted-secret\"\r\n folded-secret",
		"{\"Cookie\":\n \"session=json-multiline-secret\"}",
		"Cookie: theme=dark; session=cookie-secret",
		"Set-Cookie: sid=cookie-secret; Path=/; HttpOnly",
		`Authorization: Digest username="u", nonce="digest-secret", response="digest-secret"`,
		"Post https://user:url-password@core.example.test/x: refused",
		"Get https://user:p@ssword@core.example.test/: refused",
		"token ol_agent_platformsecret and ol_user_platformsecret and olat_platformsecret",
		"jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl",
		"provider key sk-providersecret0123456789",
	}
	var errs []error
	for _, text := range texts {
		errs = append(errs,
			errors.New(text),
			fmt.Errorf("wrapped: %w", errors.New(text)),
			&url.Error{Op: "Post", URL: "https://user:p@ssword@core.example.test/?token=" + text, Err: errors.New(text)},
			&net.OpError{Op: "read", Net: "tcp", Err: errors.New(text)},
			&Error{StatusCode: 401, Code: "AUTHENTICATION_FAILED", Message: text, ResponseBody: []byte(text)},
			&Error{StatusCode: 400, Code: text, Message: text},
			&websocket.CloseError{Code: 1008, Text: text},
			errors.Join(errors.New(text), context.DeadlineExceeded),
		)
	}
	for index, err := range errs {
		var output lockedLogBuffer
		node := &RuntimeWorker{Logger: log.New(&output, "", 0)}
		node.logRuntimeRetry("claim", err)
		logged := output.String() + runtimeRetryLogText(err) + runtimeRetryCause(err, runtimeRetryLogText(err))
		if !strings.Contains(output.String(), "runtime claim retrying: ") {
			t.Fatalf("error %d was not logged: %q", index, output.String())
		}
		for _, secret := range secrets {
			if strings.Contains(logged, secret) {
				t.Fatalf("error %d (%T) leaked %q into the retry log: %q", index, err, secret, logged)
			}
		}
	}
}

func TestRuntimeWorkerRetryLogNeverWritesCloseReasonOrToken(t *testing.T) {
	var output lockedLogBuffer
	node := &RuntimeWorker{Logger: log.New(&output, "", 0)}
	token := "ol_agent_closereason_secret"
	node.logRuntimeRetry("WebSocket activation", fmt.Errorf("dial: %w", &websocket.CloseError{Code: 1008, Text: "token " + token}))
	node.logRuntimeRetry("claim", errors.New("proxy rejected Authorization: Bearer "+token+"\nforged log line"))
	logged := output.String()
	if strings.Contains(logged, token) || strings.Contains(logged, "Bearer "+token) {
		t.Fatalf("retry log leaked a token:\n%s", logged)
	}
	if strings.Count(logged, "\n") != 2 || strings.Contains(logged, "\nforged") {
		t.Fatalf("retry log allowed line injection:\n%q", logged)
	}
	if !strings.Contains(logged, "runtime WebSocket activation retrying: WebSocket closed with code 1008") {
		t.Fatalf("close code missing:\n%s", logged)
	}
}

func TestRuntimeRetryLogLimiterPromotionKeepsOverflowCooldown(t *testing.T) {
	var limiter runtimeRetryLogLimiter
	start := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	interval := 30 * time.Second
	for index := 0; index < runtimeRetryLogMaxCauses; index++ {
		limiter.allow(fmt.Sprintf("cause-%d", index), start, interval)
	}
	// While every tracked cause is unexpired, cause X logs through overflow.
	overflowLogged := start.Add(29 * time.Second)
	if allowed, _ := limiter.allow("cause-x", overflowLogged, interval); !allowed {
		t.Fatal("cause X did not log through the overflow bucket")
	}
	// At t=30 tracked causes expire and X is promoted into its own entry; it
	// must keep the overflow cooldown instead of logging again after 1s.
	if allowed, _ := limiter.allow("cause-x", start.Add(interval), interval); allowed {
		t.Fatal("promotion out of the overflow bucket bypassed the 30s limit")
	}
	if allowed, _ := limiter.allow("cause-x", overflowLogged.Add(interval-time.Millisecond), interval); allowed {
		t.Fatal("promoted cause logged before the overflow cooldown ended")
	}
	allowed, suppressed := limiter.allow("cause-x", overflowLogged.Add(interval), interval)
	if !allowed || suppressed != 2 {
		t.Fatalf("promoted cause after cooldown = %v/%d, want logged with 2 suppressed", allowed, suppressed)
	}
	// Without a recent overflow line, a new cause still logs immediately.
	if allowed, _ := limiter.allow("cause-y", overflowLogged.Add(2*interval), interval); !allowed {
		t.Fatal("a new cause outside any overflow cooldown was suppressed")
	}
}

func TestRuntimeRetryLogNamesOnlyNamedErrorTypes(t *testing.T) {
	// An anonymous struct that embeds error prints its fields and tags with %T.
	anonymous := struct {
		error
		Secret string `json:"ol_agent_tagsecret"`
	}{error: errors.New("inner")}
	// A reflection-built type is unnamed as well. Its embedded Error method
	// would panic if called, which also proves log text never calls Error().
	built := reflect.StructOf([]reflect.StructField{
		{Name: "Error", Type: reflect.TypeOf((*error)(nil)).Elem(), Anonymous: true},
		{Name: "Secret", Type: reflect.TypeOf(""), Tag: reflect.StructTag(`json:"ol_agent_reflectsecret"`)},
	})
	reflected, ok := reflect.New(built).Elem().Interface().(error)
	if !ok {
		t.Fatal("reflection-built type does not implement error")
	}
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"anonymous", anonymous, "unclassified error (unnamed)"},
		{"wrapped anonymous", fmt.Errorf("wrapped: %w", anonymous), "unclassified error (*fmt.wrapError > unnamed)"},
		{"reflection built", reflected, "unclassified error (unnamed)"},
		{"named", errors.New("x"), "unclassified error (*errors.errorString)"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := runtimeRetryLogText(testCase.err)
			if got != testCase.want {
				t.Fatalf("log text = %q, want %q", got, testCase.want)
			}
			for _, forbidden := range []string{"struct", "{", "`", `"`, "tagsecret", "reflectsecret", "Secret"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("log text %q disclosed type structure %q", got, forbidden)
				}
			}
		})
	}
}

func TestRuntimeRetryCauseIsStableAcrossEphemeralDetails(t *testing.T) {
	cause := func(err error) string { return runtimeRetryCause(err, runtimeRetryLogText(err)) }
	reset := func(port int) error {
		return &net.OpError{Op: "read", Net: "tcp", Source: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}, Err: syscall.ECONNRESET}
	}
	if cause(reset(61234)) != cause(reset(50001)) {
		t.Fatal("ephemeral ports split one retry cause")
	}
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	if cause(reset(61234)) == cause(refused) {
		t.Fatal("different network failures share one cause")
	}
	if cause(errors.New("message one")) != cause(errors.New("message two")) {
		t.Fatal("unclassified message text split one retry cause")
	}
	conflict := &Error{StatusCode: 409, Code: "RUNTIME_SESSION_CONFLICT", Message: "attempt 1"}
	conflictAgain := &Error{StatusCode: 409, Code: "RUNTIME_SESSION_CONFLICT", Message: "attempt 2"}
	unavailable := &Error{StatusCode: 503, Code: "SERVICE_UNAVAILABLE", Message: "attempt 1"}
	if runtimeRetryCause(conflict, "") != runtimeRetryCause(fmt.Errorf("wrapped: %w", conflictAgain), "") {
		t.Fatal("one platform code produced different causes")
	}
	if runtimeRetryCause(conflict, "") == runtimeRetryCause(unavailable, "") {
		t.Fatal("different platform codes share one cause")
	}
	closeErr := &websocket.CloseError{Code: websocket.CloseGoingAway, Text: "restart 1"}
	if runtimeRetryCause(closeErr, "") != runtimeRetryCause(&websocket.CloseError{Code: websocket.CloseGoingAway, Text: "restart 2"}, "") {
		t.Fatal("WebSocket close code is not the cause")
	}
}

func TestRuntimeWorkerRetryLogIsScrubbedAndRateLimited(t *testing.T) {
	var output lockedLogBuffer
	node := &RuntimeWorker{Logger: log.New(&output, "", 0)}
	secret := &Error{StatusCode: 503, Code: "SERVICE_UNAVAILABLE", Message: "private upstream detail ol_agent_secret"}
	for index := 0; index < 5; index++ {
		node.logRuntimeRetry("claim", secret)
	}
	node.logRuntimeRetry("claim", context.Canceled)
	node.logRuntimeRetry("claim", fmt.Errorf("wrapped: %w", ErrRuntimeTransportSwitching))
	node.logRuntimeRetry("command poll", secret)
	logged := output.String()
	if strings.Count(logged, "runtime claim retrying: SERVICE_UNAVAILABLE (HTTP 503)") != 1 {
		t.Fatalf("claim retries were not logged exactly once:\n%s", logged)
	}
	if strings.Count(logged, "runtime command poll retrying: SERVICE_UNAVAILABLE (HTTP 503)") != 1 {
		t.Fatalf("a separate operation was suppressed:\n%s", logged)
	}
	if strings.Contains(logged, "private upstream detail") || strings.Contains(logged, "ol_agent_secret") {
		t.Fatalf("retry log leaked the platform message:\n%s", logged)
	}
	if strings.Contains(logged, "canceled") || strings.Contains(logged, "switching") {
		t.Fatalf("shutdown and transition noise was logged:\n%s", logged)
	}
	if lines := strings.Count(logged, "\n"); lines != 2 {
		t.Fatalf("logged %d lines, want 2:\n%s", lines, logged)
	}
}

func TestRuntimeSessionAttachRetryLogsCauseOnceAndStillRetries(t *testing.T) {
	var output lockedLogBuffer
	client := newFakeRuntimeClient()
	var calls int
	client.createFn = func(_ context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		calls++
		if calls <= 4 {
			return nil, &Error{StatusCode: 503, Code: "RUNTIME_UNAVAILABLE", Message: "Core is restarting"}
		}
		return client.readyPayload(), nil
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return nil, nil
	}))
	node.Logger = log.New(&output, "", 0)
	if err := node.applyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	store := openRuntimeStoreForTest(t, node.DataDir)
	node.store = store
	node.runtimeCtx = context.Background()
	ready, err := node.createSessionWithRetry(context.Background())
	if err != nil || ready == nil {
		t.Fatalf("session attach = %v, %v", ready, err)
	}
	if calls != 5 {
		t.Fatalf("attach calls = %d, retry behaviour changed", calls)
	}
	if count := strings.Count(output.String(), "runtime session attach retrying: RUNTIME_UNAVAILABLE (HTTP 503)"); count != 1 {
		t.Fatalf("attach retry cause logged %d times:\n%s", count, output.String())
	}
}

func TestRuntimeDrainCommandLogsTransitionOnce(t *testing.T) {
	var output lockedLogBuffer
	node := &RuntimeWorker{Logger: log.New(&output, "", 0), Capacity: 2}
	command := RuntimeDecodedPendingCommand{Type: RuntimeDrain, Drain: &RuntimeDrainPayload{ReasonCode: "ADMIN_REQUESTED"}}
	node.handleDecodedCommand(command)
	node.handleDecodedCommand(command)
	if capacity, _ := node.capacitySnapshot(); capacity != 0 || !node.isDraining() {
		t.Fatalf("drain command did not fence admission: capacity=%d", capacity)
	}
	if count := strings.Count(output.String(), "runtime drain requested by OpenLinker (reason ADMIN_REQUESTED)"); count != 1 {
		t.Fatalf("drain transition logged %d times:\n%s", count, output.String())
	}
	// Any reason outside the platform code shape, including a bare credential,
	// is replaced by a fixed label.
	for _, reason := range []string{"bad reason\nforged line", "ol_agent_drainreasonsecret", "olat_drainsecret", "operator_request"} {
		output.mu.Lock()
		output.buffer.Reset()
		output.mu.Unlock()
		unsafe := &RuntimeWorker{Logger: log.New(&output, "", 0)}
		unsafe.handleDecodedCommand(RuntimeDecodedPendingCommand{Type: RuntimeDrain, Drain: &RuntimeDrainPayload{ReasonCode: reason}})
		logged := output.String()
		if strings.Contains(logged, reason) || strings.Contains(logged, "forged") || !strings.Contains(logged, "reason UNRECOGNIZED_CODE") {
			t.Fatalf("drain reason %q was logged: %q", reason, logged)
		}
	}
}

func TestRuntimePlatformErrorCodeAcceptsOnlyPlatformCodeShape(t *testing.T) {
	for _, testCase := range []struct {
		body string
		want string
	}{
		{`{"error":{"code":"FORBIDDEN","message":"private detail"}}`, "FORBIDDEN"},
		{`{"error":{"code":"RUNTIME_SESSION_CONFLICT"}}`, "RUNTIME_SESSION_CONFLICT"},
		{`{"error":{"code":"HTTP_503"}}`, "HTTP_503"},
		{`{"error":{"code":"ol_agent_codesecret"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"olat_codesecret"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"eyJhbGciOiJIUzI1NiJ9"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"sk-providersecret0123"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"operator_request"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"bad code"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"FORBIDDEN\nforged"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"A"}}`, runtimeUnrecognizedCode},
		{`{"error":{"code":"` + strings.Repeat("A", runtimePlatformCodeMaxLength+1) + `"}}`, runtimeUnrecognizedCode},
		{`{"error":{"message":"no code"}}`, ""},
		{`not json`, ""},
		{``, ""},
	} {
		if got := runtimePlatformErrorCode([]byte(testCase.body)); got != testCase.want {
			t.Errorf("code(%q) = %q, want %q", testCase.body, got, testCase.want)
		}
	}
	oversized := []byte(`{"error":{"code":"FORBIDDEN"},"padding":"` + strings.Repeat("x", openLinkerDiscoveryMaxBytes) + `"}`)
	if got := runtimePlatformErrorCode(oversized); got != "" {
		t.Fatalf("oversized body code = %q", got)
	}
	// Every code the SDK acts on has the platform shape.
	for _, code := range []string{
		"STALE_LEASE", "LEASE_EXPIRED", "RUN_ALREADY_TERMINAL", "RUN_CANCEL_REQUESTED", "AUTHENTICATION_FAILED",
		"UNAUTHORIZED", "FORBIDDEN", "PERMISSION_DENIED", "RUNTIME_CLIENT_UPGRADE_REQUIRED",
		"RUNTIME_REQUIRED_FEATURE_MISSING", "RUNTIME_SESSION_CONFLICT", "SDK_GRACEFUL_SHUTDOWN", "ADMIN_REQUESTED",
		"NODE_AT_CAPACITY", "NODE_DRAINING", "HTTP_503", runtimeUnrecognizedCode,
	} {
		if !runtimePlatformCodeShape(code) {
			t.Errorf("expected platform code %q does not match the code shape", code)
		}
	}
}

func TestRuntimeSecurityPolicyErrorIncludesPlatformCodeOnly(t *testing.T) {
	token := "ol_agent_policy_code_synthetic"
	status := http.StatusForbidden
	body := `{"error":{"code":"RUNTIME_POLICY_DENIED","message":"private policy detail"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	node := &RuntimeWorker{PlatformURL: server.URL, AgentToken: token, AgentID: testAgentID}
	connection := runtimeConnectionInformation{SecurityPolicyEndpoint: server.URL + "/api/v1/runtime-security-policy"}
	_, err := node.resolveRuntimeIdentityPolicy(context.Background(), connection)
	if err == nil || err.Error() != "Runtime security policy unavailable: HTTP 403 (RUNTIME_POLICY_DENIED)" {
		t.Fatalf("policy error = %v", err)
	}
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		t.Fatal("policy error type changed")
	}
	status, body = http.StatusForbidden, `{"error":{"code":"ol_agent_policycodesecret","message":"x"}}`
	_, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection)
	if err == nil || err.Error() != "Runtime security policy unavailable: HTTP 403 (UNRECOGNIZED_CODE)" {
		t.Fatalf("policy error with a credential code = %v", err)
	}
	status, body = http.StatusBadGateway, "<html>proxy private detail</html>"
	_, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection)
	if err == nil || err.Error() != "Runtime security policy unavailable: HTTP 502" {
		t.Fatalf("policy error without code = %v", err)
	}
}

func TestRuntimeCertificateRequestErrorIncludesPlatformCodeOnly(t *testing.T) {
	status := http.StatusConflict
	body := `{"error":{"code":"RUNTIME_NODE_VERSION_NOT_APPROVED","message":"private approval detail"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	manager, err := newRuntimeCredentialManager(
		t.TempDir(), server.URL, "ol_agent_certificate_code_synthetic", testNodeID, testAgentID,
		"test-node/v1", 1, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = manager.issue(context.Background())
	if err == nil || err.Error() != "Runtime certificate request failed with HTTP 409 (RUNTIME_NODE_VERSION_NOT_APPROVED)" {
		t.Fatalf("certificate error = %v", err)
	}
	status, body = http.StatusConflict, `{"error":{"code":"ol_agent_certcodesecret","message":"x"}}`
	err = manager.issue(context.Background())
	if err == nil || err.Error() != "Runtime certificate request failed with HTTP 409 (UNRECOGNIZED_CODE)" {
		t.Fatalf("certificate error with a credential code = %v", err)
	}
	status, body = http.StatusServiceUnavailable, "upstream private detail"
	err = manager.issue(context.Background())
	if err == nil || err.Error() != "Runtime certificate request failed with HTTP 503" {
		t.Fatalf("certificate error without code = %v", err)
	}
}
