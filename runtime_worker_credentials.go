package openlinker

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	runtimeCredentialVersion      = 1
	runtimeCredentialFile         = "runtime-credential.json"
	runtimeCredentialResponseMax  = 64 << 10
	runtimeCredentialHTTPTimeout  = 15 * time.Second
	runtimeCredentialRetryAfter   = 5 * time.Minute
	runtimeCredentialExpiryMargin = 5 * time.Minute
)

type runtimeCredentialDisk struct {
	Version              int       `json:"version"`
	NodeID               string    `json:"node_id"`
	AgentID              string    `json:"agent_id,omitempty"`
	PrivateKeyPEM        string    `json:"private_key_pem"`
	CertificateChainPEM  string    `json:"certificate_chain_pem,omitempty"`
	TrustBundlePEM       string    `json:"trust_bundle_pem,omitempty"`
	CertificateSerial    string    `json:"certificate_serial,omitempty"`
	PublicKeyThumbprint  string    `json:"public_key_thumbprint"`
	CertificateNotBefore time.Time `json:"certificate_not_before,omitempty"`
	CertificateNotAfter  time.Time `json:"certificate_not_after,omitempty"`
	RenewAfter           time.Time `json:"renew_after,omitempty"`
	Checksum             string    `json:"checksum"`
}

type runtimeCredentialIssueRequest struct {
	NodeID                string   `json:"node_id"`
	DisplayName           string   `json:"display_name"`
	NodeVersion           string   `json:"node_version"`
	ProtocolVersion       int      `json:"protocol_version"`
	RuntimeContractID     string   `json:"runtime_contract_id"`
	RuntimeContractDigest string   `json:"runtime_contract_digest"`
	Features              []string `json:"features"`
	Capacity              int64    `json:"capacity"`
	CSRPEM                string   `json:"csr_pem"`
}

type runtimeCredentialIssueResponse struct {
	NodeID                 string    `json:"node_id"`
	AgentID                string    `json:"agent_id"`
	CertificatePEM         string    `json:"certificate_pem"`
	CertificateChainPEM    string    `json:"certificate_chain_pem"`
	TrustBundlePEM         string    `json:"trust_bundle_pem"`
	CertificateSerial      string    `json:"certificate_serial"`
	PublicKeyThumbprint    string    `json:"public_key_thumbprint"`
	NotBefore              time.Time `json:"not_before"`
	NotAfter               time.Time `json:"not_after"`
	RenewAfter             time.Time `json:"renew_after"`
	CertificateLifetimeHrs int       `json:"certificate_lifetime_hours"`
}

type runtimeCredentialManager struct {
	mu                 sync.RWMutex
	renewMu            sync.Mutex
	dataDir            string
	credentialEndpoint string
	agentToken         string
	nodeVersion        string
	capacity           int64
	logger             *log.Logger
	disk               runtimeCredentialDisk
	certificate        tls.Certificate
	rootCAs            *x509.CertPool
	closeIdle          func()
}

func newRuntimeCredentialManager(
	dataDir, credentialEndpoint, agentToken, configuredNodeID, configuredAgentID, nodeVersion string,
	capacity int64,
	logger *log.Logger,
) (*runtimeCredentialManager, error) {
	endpoint, err := validateRuntimeCredentialEndpoint(credentialEndpoint)
	if err != nil {
		return nil, err
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime credential directory: %w", err)
	}
	if err = ensurePrivateDataDir(absDir); err != nil {
		return nil, err
	}
	disk, err := loadOrCreateRuntimeCredential(absDir, strings.TrimSpace(configuredNodeID))
	if err != nil {
		return nil, err
	}
	if configuredNodeID != "" && disk.NodeID != configuredNodeID {
		return nil, errors.New("configured RuntimeWorker ID differs from the key bound to this data directory")
	}
	if configuredAgentID != "" && disk.AgentID != "" && disk.AgentID != configuredAgentID {
		return nil, errors.New("configured Agent ID differs from the credential bound to this data directory")
	}
	manager := &runtimeCredentialManager{
		dataDir:            absDir,
		credentialEndpoint: endpoint,
		agentToken:         strings.TrimSpace(agentToken),
		nodeVersion:        strings.TrimSpace(nodeVersion),
		capacity:           capacity,
		logger:             logger,
		disk:               disk,
	}
	if disk.CertificateChainPEM != "" {
		if err = manager.loadTLSStateLocked(); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *runtimeCredentialManager) Ensure(ctx context.Context, force bool) error {
	if m == nil {
		return errors.New("runtime credential manager is unavailable")
	}
	m.renewMu.Lock()
	defer m.renewMu.Unlock()
	m.mu.RLock()
	needsIssue := force || len(m.certificate.Certificate) == 0 || m.disk.CertificateNotAfter.IsZero() ||
		time.Now().Add(runtimeCredentialExpiryMargin).After(m.disk.CertificateNotAfter) ||
		(!m.disk.RenewAfter.IsZero() && !time.Now().Before(m.disk.RenewAfter))
	m.mu.RUnlock()
	if !needsIssue {
		return nil
	}
	return m.issue(ctx)
}

func (m *runtimeCredentialManager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	go func() {
		for {
			m.mu.RLock()
			renewAfter := m.disk.RenewAfter
			m.mu.RUnlock()
			wait := time.Until(renewAfter)
			if renewAfter.IsZero() || wait < time.Second {
				wait = time.Second
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			renewCtx, cancel := context.WithTimeout(ctx, runtimeCredentialHTTPTimeout)
			err := m.Ensure(renewCtx, false)
			cancel()
			if err != nil {
				if m.logger != nil {
					m.logger.Printf("openlinker: Runtime certificate renewal failed; retrying in %s: %v", runtimeCredentialRetryAfter, err)
				}
				timer = time.NewTimer(runtimeCredentialRetryAfter)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}()
}

func (m *runtimeCredentialManager) TLSConfig() (*tls.Config, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.certificate.Certificate) == 0 || m.rootCAs == nil {
		return nil, errors.New("runtime mTLS credential is unavailable")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    m.rootCAs,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			m.mu.RLock()
			defer m.mu.RUnlock()
			if len(m.certificate.Certificate) == 0 {
				return nil, errors.New("runtime client certificate is unavailable")
			}
			certificate := m.certificate
			return &certificate, nil
		},
	}, nil
}

func (m *runtimeCredentialManager) Identity() (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.disk.NodeID, m.disk.AgentID
}

func (m *runtimeCredentialManager) SetCloseIdleConnections(closeIdle func()) {
	m.mu.Lock()
	m.closeIdle = closeIdle
	m.mu.Unlock()
}

func (m *runtimeCredentialManager) issue(ctx context.Context) error {
	m.mu.RLock()
	disk := m.disk
	m.mu.RUnlock()
	key, err := parseRuntimeCredentialPrivateKey(disk.PrivateKeyPEM)
	if err != nil {
		return err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return fmt.Errorf("create Runtime certificate request: %w", err)
	}
	requestBody, err := json.Marshal(runtimeCredentialIssueRequest{
		NodeID:                disk.NodeID,
		DisplayName:           "runtime-" + strings.ReplaceAll(disk.NodeID, "-", "")[:12],
		NodeVersion:           m.nodeVersion,
		ProtocolVersion:       RuntimeProtocolVersion,
		RuntimeContractID:     RuntimeContractID,
		RuntimeContractDigest: RuntimeContractDigest,
		Features:              RuntimeRequiredFeatures(),
		Capacity:              m.capacity,
		CSRPEM:                string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})),
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, m.credentialEndpoint, bytes.NewReader(requestBody))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+m.agentToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", runtimeWorkerSDKAgent)
	client := newRuntimeCredentialHTTPClient()
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request Runtime certificate: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, runtimeCredentialResponseMax+1))
	if err != nil {
		return fmt.Errorf("read Runtime certificate response: %w", err)
	}
	if len(body) > runtimeCredentialResponseMax {
		return errors.New("Runtime certificate response exceeds 64 KiB")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Runtime certificate request failed with HTTP %d", response.StatusCode)
	}
	var issued runtimeCredentialIssueResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&issued); err != nil {
		return fmt.Errorf("decode Runtime certificate response: %w", err)
	}
	if err = validateIssuedRuntimeCredential(disk, issued); err != nil {
		return err
	}
	disk.AgentID = issued.AgentID
	disk.CertificateChainPEM = issued.CertificateChainPEM
	disk.TrustBundlePEM = issued.TrustBundlePEM
	disk.CertificateSerial = strings.ToLower(issued.CertificateSerial)
	disk.CertificateNotBefore = issued.NotBefore
	disk.CertificateNotAfter = issued.NotAfter
	disk.RenewAfter = issued.RenewAfter
	if err = persistRuntimeCredential(m.dataDir, disk); err != nil {
		return err
	}
	m.mu.Lock()
	m.disk = disk
	err = m.loadTLSStateLocked()
	closeIdle := m.closeIdle
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if closeIdle != nil {
		closeIdle()
	}
	return nil
}

func (m *runtimeCredentialManager) loadTLSStateLocked() error {
	certificate, err := tls.X509KeyPair([]byte(m.disk.CertificateChainPEM), []byte(m.disk.PrivateKeyPEM))
	if err != nil {
		return fmt.Errorf("load automatic Runtime client certificate: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return errors.New("automatic Runtime certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return err
	}
	lifetime := leaf.NotAfter.Sub(leaf.NotBefore)
	if leaf.NotAfter.Before(time.Now()) || lifetime < 23*time.Hour+50*time.Minute ||
		lifetime > 24*time.Hour+10*time.Minute {
		return errors.New("automatic Runtime certificate validity is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(m.disk.TrustBundlePEM)) {
		return errors.New("automatic Runtime trust bundle is invalid")
	}
	m.certificate = certificate
	m.rootCAs = roots
	return nil
}

func loadOrCreateRuntimeCredential(dataDir, configuredNodeID string) (runtimeCredentialDisk, error) {
	path := filepath.Join(dataDir, runtimeCredentialFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		nodeID := configuredNodeID
		if nodeID == "" {
			nodeID, err = newRuntimeUUID()
			if err != nil {
				return runtimeCredentialDisk{}, err
			}
		}
		if !validRuntimeUUID(nodeID) {
			return runtimeCredentialDisk{}, errors.New("RuntimeWorker ID must be a non-zero lowercase UUID")
		}
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			return runtimeCredentialDisk{}, keyErr
		}
		keyDER, keyErr := x509.MarshalPKCS8PrivateKey(key)
		if keyErr != nil {
			return runtimeCredentialDisk{}, keyErr
		}
		spki, keyErr := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if keyErr != nil {
			return runtimeCredentialDisk{}, keyErr
		}
		thumbprint := sha256.Sum256(spki)
		disk := runtimeCredentialDisk{
			Version:             runtimeCredentialVersion,
			NodeID:              nodeID,
			PrivateKeyPEM:       string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
			PublicKeyThumbprint: hex.EncodeToString(thumbprint[:]),
		}
		if err = persistRuntimeCredential(dataDir, disk); err != nil {
			return runtimeCredentialDisk{}, err
		}
		return disk, nil
	}
	if err != nil {
		return runtimeCredentialDisk{}, fmt.Errorf("inspect Runtime credential: %w", err)
	}
	if !info.Mode().IsRegular() || !runtimeFileModeIsPrivate(info.Mode()) || info.Size() <= 0 || info.Size() > runtimeCredentialResponseMax {
		return runtimeCredentialDisk{}, errors.New("Runtime credential file is corrupt or not private")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimeCredentialDisk{}, err
	}
	var disk runtimeCredentialDisk
	if err = decodeStrictJSON(raw, &disk); err != nil || disk.Version != runtimeCredentialVersion ||
		!validRuntimeUUID(disk.NodeID) || !runtimeCredentialChecksumValid(disk) {
		return runtimeCredentialDisk{}, errors.New("Runtime credential file is corrupt")
	}
	key, err := parseRuntimeCredentialPrivateKey(disk.PrivateKeyPEM)
	if err != nil {
		return runtimeCredentialDisk{}, err
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return runtimeCredentialDisk{}, err
	}
	thumbprint := sha256.Sum256(spki)
	if subtleString(disk.PublicKeyThumbprint) != hex.EncodeToString(thumbprint[:]) {
		return runtimeCredentialDisk{}, errors.New("Runtime credential public key does not match its identity")
	}
	return disk, nil
}

func persistRuntimeCredential(dataDir string, disk runtimeCredentialDisk) error {
	disk.Version = runtimeCredentialVersion
	disk.Checksum = ""
	rawForChecksum, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(rawForChecksum)
	disk.Checksum = hex.EncodeToString(digest[:])
	raw, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	if err = atomicWriteDurable(filepath.Join(dataDir, runtimeCredentialFile), raw, 0o600, nil); err != nil {
		return fmt.Errorf("persist Runtime credential: %w", err)
	}
	return nil
}

func runtimeCredentialChecksumValid(disk runtimeCredentialDisk) bool {
	want := disk.Checksum
	disk.Checksum = ""
	raw, err := json.Marshal(disk)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(raw)
	return constantChecksumEqual(want, hex.EncodeToString(digest[:]))
}

func parseRuntimeCredentialPrivateKey(value string) (*ecdsa.PrivateKey, error) {
	block, rest := pem.Decode([]byte(value))
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("Runtime credential private key is invalid")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("Runtime credential private key is invalid")
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, errors.New("Runtime credential private key must use P-256")
	}
	return key, nil
}

func validateIssuedRuntimeCredential(current runtimeCredentialDisk, issued runtimeCredentialIssueResponse) error {
	if issued.NodeID != current.NodeID || !validRuntimeUUID(issued.AgentID) ||
		issued.CertificateChainPEM == "" || issued.TrustBundlePEM == "" ||
		issued.CertificateSerial == "" || issued.PublicKeyThumbprint != current.PublicKeyThumbprint ||
		issued.NotBefore.IsZero() || issued.NotAfter.IsZero() || !issued.NotBefore.Before(issued.NotAfter) ||
		issued.NotAfter.Sub(issued.NotBefore) < 23*time.Hour+50*time.Minute ||
		issued.NotAfter.Sub(issued.NotBefore) > 24*time.Hour+10*time.Minute ||
		issued.RenewAfter.Before(issued.NotBefore) || !issued.RenewAfter.Before(issued.NotAfter) {
		return errors.New("Runtime certificate response is invalid")
	}
	return nil
}

func validateRuntimeCredentialEndpoint(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", errors.New("Runtime credential endpoint is invalid")
	}
	if parsed.Scheme != "https" && (parsed.Scheme != "http" || !isLoopbackHost(parsed.Hostname())) {
		return "", errors.New("Runtime credential endpoint must use HTTPS")
	}
	return parsed.String(), nil
}

func newRuntimeCredentialHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &http.Client{
		Transport: transport,
		Timeout:   runtimeCredentialHTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("Runtime credential endpoint redirects are not allowed")
		},
	}
}

func subtleString(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
