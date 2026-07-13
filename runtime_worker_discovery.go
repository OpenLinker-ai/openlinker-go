package openlinker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	openLinkerDiscoveryPath     = "/.well-known/openlinker.json"
	openLinkerDiscoveryTimeout  = 5 * time.Second
	openLinkerDiscoveryMaxBytes = 64 << 10
)

type openLinkerDiscoveryManifest struct {
	BaseURLs struct {
		Runtime string `json:"runtime"`
	} `json:"base_urls"`
	Runtime struct {
		Enabled      bool `json:"enabled"`
		MTLSRequired bool `json:"mtls_required"`
	} `json:"runtime"`
}

func resolveRuntimeURL(ctx context.Context, platformURL, override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return validateRuntimeOrigin(override)
	}
	platformOrigin, err := validatePlatformOrigin(platformURL)
	if err != nil {
		return "", err
	}
	client := newOpenLinkerDiscoveryClient()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, platformOrigin+openLinkerDiscoveryPath, nil)
	if err != nil {
		return "", fmt.Errorf("build OpenLinker discovery request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", runtimeWorkerSDKAgent)
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("OpenLinker connection information unavailable from %s: %w", platformOrigin, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OpenLinker connection information unavailable from %s: HTTP %d", platformOrigin, response.StatusCode)
	}
	if response.ContentLength > openLinkerDiscoveryMaxBytes {
		return "", errors.New("OpenLinker connection information exceeds 64 KiB")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, openLinkerDiscoveryMaxBytes+1))
	if err != nil {
		return "", fmt.Errorf("read OpenLinker connection information: %w", err)
	}
	if len(body) > openLinkerDiscoveryMaxBytes {
		return "", errors.New("OpenLinker connection information exceeds 64 KiB")
	}
	var manifest openLinkerDiscoveryManifest
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&manifest); err != nil {
		return "", fmt.Errorf("decode OpenLinker connection information: %w", err)
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return "", err
	}
	if !manifest.Runtime.Enabled {
		return "", errors.New("this OpenLinker instance does not provide a Runtime connection address")
	}
	if !manifest.Runtime.MTLSRequired {
		return "", errors.New("OpenLinker connection information does not require the expected mTLS identity")
	}
	if strings.TrimSpace(manifest.BaseURLs.Runtime) == "" {
		return "", errors.New("this OpenLinker instance does not provide a Runtime connection address")
	}
	return validateRuntimeOrigin(manifest.BaseURLs.Runtime)
}

func newOpenLinkerDiscoveryClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Discovery uses the public platform endpoint. It intentionally has no
	// Agent Token and no Runtime client certificate.
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &http.Client{
		Transport: transport,
		Timeout:   openLinkerDiscoveryTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("OpenLinker discovery redirects are not allowed")
		},
	}
}

func validatePlatformOrigin(raw string) (string, error) {
	return validateOrigin(raw, true, "OpenLinker address")
}

func validateRuntimeOrigin(raw string) (string, error) {
	return validateOrigin(raw, false, "Runtime connection address")
}

func validateOrigin(raw string, allowLoopbackHTTP bool, label string) (string, error) {
	value := strings.TrimSpace(raw)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Opaque != "" || parsed.Host == "" {
		return "", fmt.Errorf("%s must be an absolute HTTPS origin", label)
	}
	if parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(value, "#") {
		return "", fmt.Errorf("%s must not include credentials, a path, query, or fragment", label)
	}
	if parsed.Scheme != "https" {
		if !allowLoopbackHTTP || parsed.Scheme != "http" || !isLoopbackHost(parsed.Hostname()) {
			return "", fmt.Errorf("%s must be an absolute HTTPS origin", label)
		}
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("%s must be an absolute HTTPS origin", label)
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return "", fmt.Errorf("%s has an invalid port", label)
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return "", fmt.Errorf("%s has an invalid port", label)
		}
	}
	return parsed.String(), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode OpenLinker connection information: %w", err)
	}
	return errors.New("OpenLinker connection information contains trailing JSON")
}
