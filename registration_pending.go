package openlinker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// pendingRegistrationEnvKey holds a first registration whose pending Agent
// Token may already have been redeemed by Core but whose result has not been
// saved yet. It sits beside the completed registration keys in the same
// private env file, so it shares their location, 0600 mode and ignore rules;
// loaders that only read the completed keys skip it.
const pendingRegistrationEnvKey = "OPENLINKER_PENDING_REGISTRATION"

const pendingRegistrationVersion = 1

// maxPendingRegistrationBytes keeps the record far below the env reader's
// 64 KiB line limit. A request within Core's field limits needs a few KiB; a
// larger one is sent without a record, as before.
const maxPendingRegistrationBytes = 16 << 10

var pendingRegistrationEnvKeys = []string{pendingRegistrationEnvKey}

// pendingAgentRegistration is the redemption in progress: the pending token,
// the platform it belongs to and the exact request. EndpointAuthHeader is
// never written, because that credential was not stored before; only its
// fingerprint is, and a retry takes the header from the current request.
type pendingAgentRegistration struct {
	Version     int                          `json:"version"`
	APIBase     string                       `json:"api_base"`
	AgentToken  string                       `json:"agent_token"`
	TokenID     string                       `json:"token_id,omitempty"`
	TokenPrefix string                       `json:"token_prefix,omitempty"`
	Request     RegisterAgentViaTokenRequest `json:"request"`
	// EndpointAuthFingerprint stands in for the endpoint auth header, which is
	// never written: see endpointAuthFingerprint.
	EndpointAuthFingerprint string `json:"endpoint_auth_fingerprint,omitempty"`
	// Mint is set when this SDK minted the token; a minting start replays only
	// a record whose mint parameters and creator match its own.
	Mint *pendingMintParameters `json:"mint,omitempty"`
	// CreatedFile marks a file that the record itself created, so clearing the
	// record after a refused registration does not leave an empty file behind.
	CreatedFile bool `json:"created_file,omitempty"`
}

// pendingMintParameters are the normalized token request and a non-secret
// fingerprint of the creator credential that minted the pending token.
type pendingMintParameters struct {
	TokenName             string   `json:"token_name"`
	TokenScopes           []string `json:"token_scopes"`
	TokenExpiresInMinutes int32    `json:"token_expires_in_minutes"`
	CreatorFingerprint    string   `json:"creator_fingerprint"`
}

// creatorCredentialFingerprint is the lowercase hex SHA-256 of the User Token,
// truncated to 32 characters; it identifies the creator without storing the
// credential.
func creatorCredentialFingerprint(userToken string) string {
	userToken = strings.TrimSpace(userToken)
	if userToken == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(userToken))
	return hex.EncodeToString(sum[:])[:32]
}

// noEndpointAuthHeader is the endpoint_auth_fingerprint of a request without
// an endpoint auth header.
const noEndpointAuthHeader = "none"

// endpointAuthFingerprint identifies the exact endpoint auth header bytes
// without storing the credential: lowercase hex SHA-256 truncated to 32
// characters, or noEndpointAuthHeader when there is none.
func endpointAuthFingerprint(header string) string {
	if header == "" {
		return noEndpointAuthHeader
	}
	sum := sha256.Sum256([]byte(header))
	return hex.EncodeToString(sum[:])[:32]
}

// pendingTestHooks maps an *EnvRegistrationStore to a test hook called with a
// stage name inside its pending-record write and removal: "write.reread" and
// "cleanup.reread" run between the read the change is based on and the
// guarding re-read; "write.rename" and "cleanup.mutate" run after that guard,
// just before the rename or remove. It lives outside the store so the exported
// struct stays comparable.
var pendingTestHooks sync.Map

// pendingDirSyncTestHooks maps an *EnvRegistrationStore to a test stand-in for
// the directory sync that makes its pending-record changes durable.
var pendingDirSyncTestHooks sync.Map

// registrationLocks holds one lock per canonical env file path. It serializes
// a whole first registration in this process: loading the stored registration
// and the record, choosing, minting, writing the record, redeeming, and saving
// or cleaning up. A second start for the same file waits, then reuses the
// completed registration or replays the matching record. It is taken before
// any store or path mutation lock and is the only lock held across network
// calls. Processes are not coordinated, as before.
var registrationLocks sync.Map

// lockFirstRegistration takes the registration lock of req's store, which it
// defaults as EnsureAgent always has, and returns an idempotent unlock. Stores
// other than *EnvRegistrationStore itself are not locked, as before. Waiting
// ends early when ctx is done.
func lockFirstRegistration(ctx context.Context, req *EnsureAgentRequest) (func(), error) {
	if req.Store == nil {
		req.Store = NewEnvRegistrationStore(DefaultRegistrationEnvPath)
	}
	store := pendingRegistrationJournalFor(req.Store)
	if store == nil {
		return func() {}, nil
	}
	value, _ := registrationLocks.LoadOrStore(canonicalRegistrationEnvPath(store.Path), make(chan struct{}, 1))
	lock := value.(chan struct{})
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	select {
	case lock <- struct{}{}:
	case <-done:
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { <-lock }) }, nil
}

func runPendingTestHook(s *EnvRegistrationStore, stage string) {
	if hook, ok := pendingTestHooks.Load(s); ok {
		hook.(func(string))(stage)
	}
}

// pendingRegistrationJournalFor journals only when the store's dynamic type is
// exactly *EnvRegistrationStore. Custom stores, including types that embed it
// to encrypt or relocate registrations, keep their previous behaviour, so no
// token is written anywhere except through their own Save.
func pendingRegistrationJournalFor(store RegistrationStore) *EnvRegistrationStore {
	journal, _ := store.(*EnvRegistrationStore)
	return journal
}

// pendingRegistrationMatches reports whether record is exactly the attempt a
// minting start would make: same platform, every recorded request field equal,
// the same endpoint auth header (by fingerprint), and the same mint parameters
// and creator. A record without mint parameters or an endpoint auth
// fingerprint never matches.
func pendingRegistrationMatches(
	record *pendingAgentRegistration,
	apiBase string,
	request RegisterAgentViaTokenRequest,
	mint pendingMintParameters,
) bool {
	if record == nil || record.APIBase != apiBase || record.Mint == nil || mint.CreatorFingerprint == "" ||
		record.EndpointAuthFingerprint == "" || record.EndpointAuthFingerprint != endpointAuthFingerprint(request.EndpointAuthHeader) {
		return false
	}
	recorded, current := record.Request, request
	recorded.EndpointAuthHeader, current.EndpointAuthHeader = "", ""
	return sameJSON(recorded, current) && sameJSON(*record.Mint, mint)
}

func sameJSON(left, right any) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func (s *EnvRegistrationStore) loadPendingAgentRegistration() (*pendingAgentRegistration, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	values, err := readRegistrationEnv(s.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodePendingAgentRegistration(values[pendingRegistrationEnvKey]), nil
}

// decodePendingAgentRegistration ignores a damaged or unknown-version record
// instead of blocking start-up; the next attempt replaces it.
func decodePendingAgentRegistration(raw string) *pendingAgentRegistration {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var record pendingAgentRegistration
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil
	}
	record.AgentToken = strings.TrimSpace(record.AgentToken)
	if record.Version != pendingRegistrationVersion || record.AgentToken == "" || record.APIBase == "" {
		return nil
	}
	return &record
}

// errPendingRegistrationContended reports that other writers kept changing the
// env file while the record was being written; the attempt then continues
// without a record, exactly as before the journal existed.
var errPendingRegistrationContended = errors.New("openlinker: pending registration record contended")

const pendingRegistrationWriteAttempts = 3

// savePendingAgentRegistration writes the record without losing data another
// writer stores meanwhile. In this process it holds the env file's path lock
// from its read through the rename, like every other mutation of that file.
// Against other processes it renders from one read, re-reads right before the
// rename and replaces the file only if it is unchanged, otherwise it renders
// again from the fresh content, a bounded number of times. There is no
// cross-process lock, so another process's write between the final re-read
// and the rename can still be lost; that is the same pre-existing limitation
// as concurrent registrations against one env file. The record's directory and
// its ancestors are synced after the rename (see syncRecordPath), so the record
// is durable before the token is redeemed.
func (s *EnvRegistrationStore) savePendingAgentRegistration(record *pendingAgentRegistration) error {
	if s == nil || record == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer lockRegistrationEnvPath(s.Path)()
	if err := s.makePendingRegistrationDir(registrationEnvDir(s.Path)); err != nil {
		return err
	}
	for attempt := 0; attempt < pendingRegistrationWriteAttempts; attempt++ {
		before, err := os.ReadFile(s.Path)
		missing := os.IsNotExist(err)
		if err != nil && !missing {
			return err
		}
		values, err := parseRegistrationEnv(before)
		if err != nil {
			return err
		}
		persisted := *record
		persisted.Version = pendingRegistrationVersion
		persisted.EndpointAuthFingerprint = endpointAuthFingerprint(record.Request.EndpointAuthHeader)
		persisted.Request.EndpointAuthHeader = ""
		previous := decodePendingAgentRegistration(values[pendingRegistrationEnvKey])
		persisted.CreatedFile = missing || (previous != nil && previous.CreatedFile)
		raw, err := json.Marshal(persisted)
		if err != nil {
			return err
		}
		if len(raw) > maxPendingRegistrationBytes {
			return nil
		}
		values[pendingRegistrationEnvKey] = string(raw)
		content, err := renderManagedRegistrationEnv(before, values, pendingRegistrationEnvKeys)
		if err != nil {
			return err
		}
		runPendingTestHook(s, "write.reread")
		replaced, err := replaceRegistrationEnvFile(s.Path, content, func() bool {
			if !registrationEnvUnchanged(s.Path, before, missing) {
				return false
			}
			runPendingTestHook(s, "write.rename")
			return true
		}, s.syncRecordPath)
		if err != nil || replaced {
			return err
		}
	}
	return errPendingRegistrationContended
}

// registrationEnvUnchanged reports whether path still holds before, or is
// still absent when it was missing.
func registrationEnvUnchanged(path string, before []byte, missing bool) bool {
	current, err := os.ReadFile(path)
	if missing {
		return os.IsNotExist(err)
	}
	return err == nil && bytes.Equal(current, before)
}

// clearPendingAgentRegistration removes this attempt's record and never
// another writer's data. It decides from one read of the file, then re-reads it
// immediately before mutating and leaves everything untouched if anything
// changed: a newer record, a completed registration or any other line written
// meanwhile survives, and only this attempt's leftover record may remain.
//
//   - The whole file is deleted only if this record created it and the file
//     holds nothing but this attempt's own pending key.
//   - Otherwise only the pending key is removed, and only while it still holds
//     agentToken; completed keys and other lines are kept as they are.
//
// In this process the env file's path lock is held from the first read through
// the rename or remove, like every other mutation of that file. There is no
// cross-process lock: another process can still write between the final
// re-read and the rename or remove. That is the same pre-existing limitation
// as concurrent registrations against one env file.
func (s *EnvRegistrationStore) clearPendingAgentRegistration(agentToken string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer lockRegistrationEnvPath(s.Path)()
	before, err := os.ReadFile(s.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	values, err := parseRegistrationEnv(before)
	if err != nil {
		return err
	}
	record := decodePendingAgentRegistration(values[pendingRegistrationEnvKey])
	if record == nil || record.AgentToken != strings.TrimSpace(agentToken) {
		return nil
	}
	deleteFile := record.CreatedFile && registrationEnvHasOnlyKey(string(before), pendingRegistrationEnvKey)
	content := ""
	if !deleteFile {
		if content, err = renderManagedRegistrationEnv(before, nil, pendingRegistrationEnvKeys); err != nil {
			return err
		}
	}
	runPendingTestHook(s, "cleanup.reread")
	unchanged := func() bool {
		if !registrationEnvUnchanged(s.Path, before, false) {
			return false
		}
		runPendingTestHook(s, "cleanup.mutate")
		return true
	}
	if deleteFile {
		if !unchanged() {
			return nil
		}
		if err := os.Remove(s.Path); err != nil {
			return err
		}
		return s.syncRecordDir("")
	}
	_, err = replaceRegistrationEnvFile(s.Path, content, unchanged, s.syncRecordDir)
	return err
}

// makePendingRegistrationDir creates dir as MkdirAll does and then syncs the
// parent of every directory it created, top down, so the record's path is
// durable before the token is redeemed. It runs before the record is written,
// so a failed sync leaves no record behind. A later start finds those
// directories existing and creates nothing, so the record write does not rely
// on this: syncRecordPath syncs the whole chain again on every write and replay.
func (s *EnvRegistrationStore) makePendingRegistrationDir(dir string) error {
	if dir == "." || dir == "" {
		return nil
	}
	var missing []string
	for current := dir; ; {
		if _, err := os.Stat(current); err == nil || !os.IsNotExist(err) {
			break
		}
		missing = append(missing, current)
		parent := registrationEnvDir(current)
		if parent == current || parent == "." {
			break
		}
		current = parent
	}
	if len(missing) == 0 {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := s.syncPendingRegistrationDir(registrationEnvDir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

// registrationEnvDir is the directory that holds path as the file system
// reads it: path without its last element and without lexical cleaning, so a
// "link/.." stays the parent of link's target.
func registrationEnvDir(path string) string {
	dir, _ := filepath.Split(path)
	if dir == "" {
		return "."
	}
	return trimRegistrationPathSeparators(dir)
}

// syncRecordDir syncs the directory holding the store's env file, whatever
// directory a caller passes, so the record's own directory entry is the one
// made durable.
func (s *EnvRegistrationStore) syncRecordDir(string) error {
	return s.syncPendingRegistrationDir(registrationEnvDir(s.Path))
}

// syncRecordPath makes the record durable before Core is asked to redeem it,
// whatever directory a caller passes. The guarantee is that every directory
// MkdirAll can create along the configured path, as written, is durable
// before the token is redeemed or replayed. An earlier start may have created
// some of them and then failed or stopped before syncing them (MkdirAll itself
// can fail after creating directories, for example at an existing symlink); a
// later start finds them existing and cannot tell, so every record write and
// every replay syncs them all again. Nothing extra is persisted.
//
// It syncs the directory holding the store's env file, then, for every
// component of that directory's path as written, from the deepest up, the
// directory that holds the component's entry: the path through the components
// before it. "." is skipped; "name/.." traverses name, so name is synced too.
// Each distinct directory (device and inode) is synced once. For
// "base/a/b/../../link" that is link's target, base, base/a and base/a/b. No
// component cuts the walk short, but symlink targets are not followed beyond
// the written path: MkdirAll never creates a symlink's target or the
// directories above it. A relative path ends at ".", which MkdirAll never
// creates.
//
// The record's own directory is synced strictly, and so is the parent of every
// directory this start created: makePendingRegistrationDir already synced
// those before the record was written. Any other directory reached only by
// the walk is skipped when it cannot be synced for one of two reasons, because
// starts in such deployments succeeded before the walk existed and must keep
// doing so:
//
//   - this process may not open it (a permission error, for example a 0311
//     directory), so its durability cannot be confirmed;
//   - it is on a read-only file system (EROFS from the open or the sync), as
//     the root of a container with a writable volume is. Nothing can have
//     been created there, so durability is unaffected.
//
// Every other open, sync or stat failure is reported, as the file system
// returned it, so nothing is redeemed on an unconfirmed path.
func (s *EnvRegistrationStore) syncRecordPath(string) error {
	dir := registrationEnvDir(s.Path)
	if err := s.syncPendingRegistrationDir(dir); err != nil {
		return err
	}
	recordDir, err := registrationDirIdentity(dir)
	if err != nil {
		return err
	}
	synced := map[any]bool{recordDir: true}
	prefixes := registrationPathPrefixes(dir)
	for j := len(prefixes) - 2; j >= 0; j-- {
		container := prefixes[j]
		identity, err := registrationDirIdentity(container)
		if err != nil {
			return err
		}
		if synced[identity] {
			continue
		}
		synced[identity] = true
		if err := s.syncMarkedRegistrationDir(container); err != nil && !skippableRegistrationAncestorError(err) {
			return unmarkRegistrationDirOpenError(err)
		}
	}
	return nil
}

// registrationPathPrefixes returns the paths through each component of dir
// as written, without empty and "." components, which name the directory they
// are in. prefixes[0] is where dir starts (".", a root or a volume), and
// prefixes[j-1] holds the entry of component j, the last element of
// prefixes[j].
func registrationPathPrefixes(dir string) []string {
	volume := filepath.VolumeName(dir)
	rest := dir[len(volume):]
	rooted := rest != "" && os.IsPathSeparator(rest[0])
	start := volume
	if rooted {
		start += string(filepath.Separator)
	}
	if start == "" {
		start = "."
	}
	prefixes := []string{start}
	current := start
	for _, name := range strings.FieldsFunc(rest, func(r rune) bool { return r < 0x80 && os.IsPathSeparator(uint8(r)) }) {
		if name == "." {
			continue
		}
		switch {
		case current == ".":
			current = name
		case current == volume && !rooted, os.IsPathSeparator(current[len(current)-1]):
			current += name
		default:
			current += string(filepath.Separator) + name
		}
		prefixes = append(prefixes, current)
	}
	return prefixes
}

// registrationDirOpenError marks a failure to open a directory for a sync, as
// opposed to a failure of the sync itself. It is only ever the outermost error
// of a sync result and never reaches a caller: syncPendingRegistrationDir and
// syncRecordPath return the file system's own error in its place.
type registrationDirOpenError struct{ err error }

func (e *registrationDirOpenError) Error() string { return e.err.Error() }

func (e *registrationDirOpenError) Unwrap() error { return e.err }

// skippableRegistrationAncestorError reports a sync failure the walk skips
// for a directory it reached only by walking: a permission error opening it,
// or a read-only file system (EROFS) from the open or the sync.
func skippableRegistrationAncestorError(err error) bool {
	return unopenableRegistrationDir(err) || errors.Is(err, syscall.EROFS)
}

// unopenableRegistrationDir reports a permission error (EACCES or EPERM)
// opening a directory for a sync. A permission error from the sync itself is
// not one.
func unopenableRegistrationDir(err error) bool {
	openErr, ok := err.(*registrationDirOpenError)
	return ok && errors.Is(openErr.err, fs.ErrPermission)
}

// unmarkRegistrationDirOpenError returns the file system's own error for a
// marked open failure, and any other error as it is.
func unmarkRegistrationDirOpenError(err error) error {
	if openErr, ok := err.(*registrationDirOpenError); ok {
		return openErr.err
	}
	return err
}

// syncPendingRegistrationDir makes a pending-record change durable before the
// token is redeemed. Unlike completed saves it reports a genuine I/O error, so
// the start fails as it would for a failed record write; a directory that
// cannot be synced (EINVAL, ENOTSUP, EOPNOTSUPP, or Windows) is skipped. The
// error is the file system's own, unmarked.
func (s *EnvRegistrationStore) syncPendingRegistrationDir(dir string) error {
	return unmarkRegistrationDirOpenError(s.syncMarkedRegistrationDir(dir))
}

// syncMarkedRegistrationDir is syncPendingRegistrationDir with a failure to
// open dir still marked as a *registrationDirOpenError.
func (s *EnvRegistrationStore) syncMarkedRegistrationDir(dir string) error {
	if hook, ok := pendingDirSyncTestHooks.Load(s); ok {
		return ignoreUnsupportedDirSync(hook.(func(string) error)(dir))
	}
	return syncRegistrationDirStrict(dir)
}

func ignoreUnsupportedDirSync(err error) error {
	if err == nil || errors.Is(err, errors.ErrUnsupported) || errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) {
		return nil
	}
	return err
}

func registrationEnvHasOnlyKey(content, key string) bool {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
		if line == "" {
			continue
		}
		name, _, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) != key {
			return false
		}
	}
	return true
}

// redeemNewAgentRegistration redeems a pending Agent Token for a new Agent and
// saves the result. The journal is set only when no completed registration exists.
//
// With a journal, the token and request are recorded before Core is asked to
// redeem them. If the process stops, the response is lost or the save fails
// after Core committed the registration, the next start sends the same token
// and request again instead of minting a new token; Core answers that exact
// replay with the original registration. suppliedToken is the caller's pending
// token; without one, mint issues a token for a fresh attempt.
func (c *Client) redeemNewAgentRegistration(
	ctx context.Context,
	req EnsureAgentRequest,
	journal *EnvRegistrationStore,
	suppliedToken string,
	mint func(context.Context) (pendingAgentRegistration, error),
) (*AgentRegistration, error) {
	request := registerAgentViaTokenRequestFrom(req)
	apiBase := c.baseURL.String()
	mintParameters := pendingMintParameters{
		TokenName: req.TokenName, TokenScopes: req.TokenScopes, TokenExpiresInMinutes: req.TokenExpiresInMinutes,
		CreatorFingerprint: creatorCredentialFingerprint(c.userToken),
	}
	// Only a minting start replays a record. With a supplied token, an exactly
	// matching record holds that same token and request, which is what the
	// flow below sends anyway, so replaying it first would only repeat it.
	if journal != nil && suppliedToken == "" {
		previous, err := journal.loadPendingAgentRegistration()
		if err != nil {
			return nil, err
		}
		// A record that differs in any way (platform, request, mint parameters
		// or creator) is never replayed; it stays until the attempt below
		// replaces it.
		if pendingRegistrationMatches(previous, apiBase, request, mintParameters) {
			// An earlier start may have written this record and then failed
			// to sync its directory or one of the directories above it, so
			// confirm the whole path is durable before Core is asked to
			// redeem it.
			if err := journal.syncRecordPath(""); err != nil {
				return nil, err
			}
			replay := *previous
			replay.Request.EndpointAuthHeader = request.EndpointAuthHeader
			registration, err := c.redeemPendingAgentRegistration(ctx, req, journal, replay)
			if err == nil || !definitiveRegistrationRejection(err) {
				return registration, err
			}
			// Core refused the recorded attempt outright (for example its token
			// expired unused) and its record is gone. Continue with the normal
			// flow instead of returning this error: without the record this
			// start would have minted a fresh token, and a stale record must not
			// turn a start that succeeds today into a failure.
		}
	}
	attempt := pendingAgentRegistration{AgentToken: suppliedToken}
	if suppliedToken == "" {
		minted, err := mint(ctx)
		if err != nil {
			return nil, err
		}
		attempt = minted
		attempt.Mint = &mintParameters
	}
	attempt.APIBase = apiBase
	attempt.Request = request
	if journal != nil {
		err := journal.savePendingAgentRegistration(&attempt)
		switch {
		case errors.Is(err, errPendingRegistrationContended):
			journal = nil
		case err != nil:
			return nil, err
		}
	}
	return c.redeemPendingAgentRegistration(ctx, req, journal, attempt)
}

func (c *Client) redeemPendingAgentRegistration(
	ctx context.Context,
	req EnsureAgentRequest,
	journal *EnvRegistrationStore,
	attempt pendingAgentRegistration,
) (*AgentRegistration, error) {
	registered, err := c.registerAgentViaToken(ctx, attempt.AgentToken, attempt.Request)
	if err != nil {
		// Core refuses the same token and request the same way again, so a
		// definitive rejection drops this attempt's record and the next start
		// behaves as before. Any other failure (network, timeout, 408, 429,
		// 5xx or an unreadable success) may follow a committed registration,
		// so the record stays for the next start to replay.
		if journal != nil && definitiveRegistrationRejection(err) {
			_ = journal.clearPendingAgentRegistration(attempt.AgentToken)
		}
		return nil, err
	}
	now := time.Now().UTC()
	registration := &AgentRegistration{
		AgentID: registered.Agent.ID, AgentSlug: registered.Agent.Slug, AgentName: registered.Agent.Name,
		AgentToken: attempt.AgentToken, TokenID: registered.AgentToken.ID, TokenPrefix: registered.AgentToken.Prefix,
		APIBase: req.APIBase, RegisteredAt: now, UpdatedAt: now,
	}
	if err := saveAgentRegistration(req.Store, registration); err != nil {
		// The record stays, so the next start can replay the redemption.
		return nil, err
	}
	if journal != nil {
		// The saved registration takes precedence on every later start, so a
		// record left by a failed cleanup is not consulted again.
		_ = journal.clearPendingAgentRegistration(attempt.AgentToken)
	}
	return registration, nil
}

// definitiveRegistrationRejection reports a 4xx answer that Core would repeat
// for the same token and request. 408 and 429 are retryable.
func definitiveRegistrationRejection(err error) bool {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return apiErr.StatusCode >= 400 && apiErr.StatusCode < 500
}

func registerAgentViaTokenRequestFrom(req EnsureAgentRequest) RegisterAgentViaTokenRequest {
	return RegisterAgentViaTokenRequest{
		Slug: req.Slug, Name: req.Name, Description: req.Description, EndpointURL: req.EndpointURL,
		EndpointAuthHeader: req.EndpointAuthHeader, PricePerCallCents: req.PricePerCallCents,
		Tags: req.Tags, AbilityTags: req.Tags, SkillIDs: req.SkillIDs, Visibility: req.Visibility,
		ConnectionMode: req.ConnectionMode, MCPToolName: req.MCPToolName,
	}
}
