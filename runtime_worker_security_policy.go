package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// Called while the data directory's exclusive store lock is held. Neither
// missing policy nor a failed request permits a previously pinned downgrade.
func (node *RuntimeWorker) resolveRuntimeIdentityPolicy(ctx context.Context, connection runtimeConnectionInformation) (runtimeConnectionInformation, error) {
	var pinned bool
	if node.DataDir != "" {
		_, err := os.Lstat(filepath.Join(node.DataDir, runtimeCredentialFile))
		if err == nil {
			disk, err := loadOrCreateRuntimeCredential(node.DataDir, node.NodeID)
			if err != nil {
				return connection, err
			}
			if node.NodeID != "" && node.NodeID != disk.NodeID {
				return connection, errors.New("Runtime Node identity differs from the stored key")
			}
			if node.AgentID != "" && disk.AgentID != "" && node.AgentID != disk.AgentID {
				return connection, errors.New("Runtime Agent identity differs from the stored key")
			}
			if node.NodeID == "" {
				node.NodeID = disk.NodeID
			}
			pinned = disk.MTLSRequired || disk.CertificateChainPEM != ""
		} else if !errors.Is(err, os.ErrNotExist) {
			return connection, err
		}
	}
	if connection.SecurityPolicyEndpoint != "" {
		platform, err := validatePlatformOrigin(node.PlatformURL)
		if err != nil {
			return connection, err
		}
		// A public manifest cannot direct this Agent Token to a different service.
		if connection.SecurityPolicyEndpoint != platform+"/api/v1/runtime-security-policy" {
			return connection, errors.New("Runtime security policy endpoint must use the platform origin and canonical path")
		}
		if node.NodeID == "" {
			node.NodeID = tokenScopedRuntimeNodeID(node.AgentToken)
		}
		body, _ := json.Marshal(struct {
			NodeID string `json:"node_id"`
		}{node.NodeID})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, connection.SecurityPolicyEndpoint, bytes.NewReader(body))
		if err != nil {
			return connection, err
		}
		req.Header.Set("Authorization", "Bearer "+node.AgentToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		response, err := newOpenLinkerDiscoveryClient().Do(req)
		if err != nil {
			return connection, fmt.Errorf("resolve Runtime security policy: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return connection, fmt.Errorf("Runtime security policy unavailable: HTTP %d", response.StatusCode)
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, openLinkerDiscoveryMaxBytes+1))
		if err != nil {
			return connection, err
		}
		if len(raw) > openLinkerDiscoveryMaxBytes {
			return connection, errors.New("Runtime security policy exceeds 64 KiB")
		}
		var policy struct {
			NodeID             string `json:"node_id"`
			AgentID            string `json:"agent_id"`
			AuthenticationMode string `json:"authentication_mode"`
			RuntimeURL         string `json:"runtime_url"`
			CredentialEndpoint string `json:"credential_endpoint"`
			OperationID        string `json:"operation_id"`
		}
		if err = decodeStrictJSON(raw, &policy); err != nil {
			return connection, err
		}
		if policy.NodeID != node.NodeID || !validRuntimeUUID(policy.AgentID) || (node.AgentID != "" && policy.AgentID != node.AgentID) {
			return connection, errors.New("Runtime security policy identity mismatch")
		}
		if policy.AuthenticationMode != "mtls" && policy.AuthenticationMode != "token_only" {
			return connection, errors.New("unknown Runtime authentication policy")
		}
		if connection.MTLSRequired && policy.AuthenticationMode != "mtls" {
			return connection, errors.New("Runtime identity policy cannot downgrade global mTLS")
		}
		connection.MTLSRequired = policy.AuthenticationMode == "mtls"
		if node.AgentID == "" {
			node.AgentID = policy.AgentID
		}
		if connection.MTLSRequired {
			origin, err := validateRuntimeOrigin(policy.RuntimeURL)
			if err != nil {
				return connection, err
			}
			if policy.CredentialEndpoint != platform+"/api/v1/runtime-credentials" {
				return connection, errors.New("Runtime credential endpoint must use the platform origin and canonical path")
			}
			if policy.OperationID != "" && !validRuntimeUUID(policy.OperationID) {
				return connection, errors.New("invalid Runtime credential operation")
			}
			// The authoritative identity policy wins over a configured transport URL.
			connection.RuntimeURL = origin
			connection.CredentialEndpoint = policy.CredentialEndpoint
			connection.CredentialOperationID = policy.OperationID
		} else if policy.OperationID != "" || policy.CredentialEndpoint != "" || policy.RuntimeURL != "" {
			return connection, errors.New("inconsistent token-only Runtime policy")
		}
	}
	if pinned && !connection.MTLSRequired {
		return connection, errors.New("stored Runtime identity requires mTLS; refusing authentication downgrade")
	}
	return connection, nil
}
