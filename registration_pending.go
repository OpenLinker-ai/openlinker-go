package openlinker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// pendingCleanupTestHooks and pendingWriteTestHooks map an
// *EnvRegistrationStore to a test hook that runs between the read a pending
// record removal or write is based on and the re-read that guards it. They
// live outside the store so the exported struct stays comparable.
var pendingCleanupTestHooks, pendingWriteTestHooks sync.Map

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
// writer stores meanwhile: it renders from one read, re-reads right before the
// rename and replaces the file only if it is unchanged, otherwise it renders
// again from the fresh content, a bounded number of times. There is no
// cross-process lock, so a write between the final re-read and the rename can
// still be lost; that is the same pre-existing limitation as concurrent
// registrations against one env file.
func (s *EnvRegistrationStore) savePendingAgentRegistration(record *pendingAgentRegistration) error {
	if s == nil || record == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir := filepath.Dir(s.Path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
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
		if hook, ok := pendingWriteTestHooks.Load(s); ok {
			hook.(func())()
		}
		replaced, err := replaceRegistrationEnvFile(s.Path, content, func() bool {
			return registrationEnvUnchanged(s.Path, before, missing)
		})
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
// There is no cross-process lock: another process can still write between the
// final re-read and the rename or remove. That is the same pre-existing
// limitation as concurrent registrations against one env file.
func (s *EnvRegistrationStore) clearPendingAgentRegistration(agentToken string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if hook, ok := pendingCleanupTestHooks.Load(s); ok {
		hook.(func())()
	}
	unchanged := func() bool { return registrationEnvUnchanged(s.Path, before, false) }
	if deleteFile {
		if !unchanged() {
			return nil
		}
		return os.Remove(s.Path)
	}
	_, err = replaceRegistrationEnvFile(s.Path, content, unchanged)
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
