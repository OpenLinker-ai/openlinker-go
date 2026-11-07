package openlinker

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const DefaultRegistrationEnvPath = ".env"

type EnvRegistrationStore struct {
	Path string
	mu   sync.Mutex
}

func NewEnvRegistrationStore(path string) *EnvRegistrationStore {
	if strings.TrimSpace(path) == "" {
		path = DefaultRegistrationEnvPath
	}
	return &EnvRegistrationStore{Path: path}
}

func (s *EnvRegistrationStore) LoadAgentRegistration() (*AgentRegistration, error) {
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
	reg := &AgentRegistration{
		AgentID:     values["OPENLINKER_AGENT_ID"],
		AgentSlug:   values["OPENLINKER_AGENT_SLUG"],
		AgentName:   values["OPENLINKER_AGENT_NAME"],
		AgentToken:  values["OPENLINKER_AGENT_TOKEN"],
		TokenID:     values["OPENLINKER_AGENT_TOKEN_ID"],
		TokenPrefix: values["OPENLINKER_AGENT_TOKEN_PREFIX"],
		APIBase:     values["OPENLINKER_API_BASE"],
	}
	reg.RegisteredAt, _ = time.Parse(time.RFC3339, values["OPENLINKER_REGISTERED_AT"])
	reg.UpdatedAt, _ = time.Parse(time.RFC3339, values["OPENLINKER_UPDATED_AT"])
	if reg.AgentID == "" && reg.AgentToken == "" {
		return nil, nil
	}
	return reg, nil
}

func (s *EnvRegistrationStore) SaveAgentRegistration(reg *AgentRegistration) error {
	if s == nil || reg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer lockRegistrationEnvPath(s.Path)()
	values, err := readRegistrationEnv(s.Path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if values == nil {
		values = map[string]string{}
	}
	setRegistrationEnv(values, "OPENLINKER_AGENT_ID", reg.AgentID)
	setRegistrationEnv(values, "OPENLINKER_AGENT_SLUG", reg.AgentSlug)
	setRegistrationEnv(values, "OPENLINKER_AGENT_NAME", reg.AgentName)
	setRegistrationEnv(values, "OPENLINKER_AGENT_TOKEN", reg.AgentToken)
	setRegistrationEnv(values, "OPENLINKER_AGENT_TOKEN_ID", reg.TokenID)
	setRegistrationEnv(values, "OPENLINKER_AGENT_TOKEN_PREFIX", reg.TokenPrefix)
	setRegistrationEnv(values, "OPENLINKER_API_BASE", reg.APIBase)
	if !reg.RegisteredAt.IsZero() {
		values["OPENLINKER_REGISTERED_AT"] = reg.RegisteredAt.Format(time.RFC3339)
	}
	if !reg.UpdatedAt.IsZero() {
		values["OPENLINKER_UPDATED_AT"] = reg.UpdatedAt.Format(time.RFC3339)
	}
	return writeRegistrationEnv(s.Path, values)
}

var registrationEnvKeys = []string{
	"OPENLINKER_API_BASE", "OPENLINKER_AGENT_ID", "OPENLINKER_AGENT_SLUG", "OPENLINKER_AGENT_NAME",
	"OPENLINKER_AGENT_TOKEN", "OPENLINKER_AGENT_TOKEN_ID", "OPENLINKER_AGENT_TOKEN_PREFIX",
	"OPENLINKER_REGISTERED_AT", "OPENLINKER_UPDATED_AT",
}

// registrationEnvPathLocks holds one mutex per absolute env file path. Every
// mutation of a registration env file (completed save, pending-record write
// and pending-record removal) holds it from its read through its final rename
// or remove, so store instances for the same file in this process cannot
// interleave. Processes are not coordinated; that remains the pre-existing
// limitation of sharing one env file between concurrent registrations.
var registrationEnvPathLocks sync.Map

// lockRegistrationEnvPath locks path's mutex and returns its unlock function.
// Callers take their store's own mutex first, so the order is always the same.
func lockRegistrationEnvPath(path string) func() {
	key := filepath.Clean(path)
	if absolute, err := filepath.Abs(path); err == nil {
		key = absolute
	}
	value, _ := registrationEnvPathLocks.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// syncRegistrationEnvDir flushes dir's entries, so a rename or remove inside
// it is durable. It is best effort: platforms that cannot sync a directory,
// such as Windows, skip it.
func syncRegistrationEnvDir(dir string) {
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
}

func readRegistrationEnv(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseRegistrationEnv(content)
}

func parseRegistrationEnv(content []byte) (map[string]string, error) {
	values := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(key) != "" {
			values[strings.TrimSpace(key)] = unquoteRegistrationEnv(strings.TrimSpace(value))
		}
	}
	return values, scanner.Err()
}

func writeRegistrationEnv(path string, values map[string]string) error {
	return writeManagedRegistrationEnv(path, values, registrationEnvKeys)
}

// writeManagedRegistrationEnv rewrites only the given keys and keeps every
// other line of the file as it is.
func writeManagedRegistrationEnv(path string, values map[string]string, keys []string) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	content, err := renderManagedRegistrationEnv(existing, values, keys)
	if err != nil {
		return err
	}
	_, err = replaceRegistrationEnvFile(path, content, nil)
	return err
}

// renderManagedRegistrationEnv returns existing with only the given keys
// rewritten from values; every other line is kept as it is.
func renderManagedRegistrationEnv(existing []byte, values map[string]string, keys []string) (string, error) {
	managed := map[string]struct{}{}
	for _, key := range keys {
		managed[key] = struct{}{}
	}
	seen := map[string]struct{}{}
	var out strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(string(existing)))
	for scanner.Scan() {
		line := scanner.Text()
		key, _, ok := strings.Cut(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export ")), "=")
		key = strings.TrimSpace(key)
		if !ok {
			out.WriteString(line + "\n")
			continue
		}
		if _, ok := managed[key]; !ok {
			out.WriteString(line + "\n")
			continue
		}
		seen[key] = struct{}{}
		if values[key] != "" {
			out.WriteString(key + "=" + fmt.Sprintf("%q", values[key]) + "\n")
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	for _, key := range keys {
		if _, ok := seen[key]; !ok && values[key] != "" {
			out.WriteString(key + "=" + fmt.Sprintf("%q", values[key]) + "\n")
		}
	}
	return out.String(), nil
}

// replaceRegistrationEnvFile atomically replaces path with content in mode
// 0600 and reports whether it did. When beforeRename returns false, path is
// left untouched.
func replaceRegistrationEnvFile(path, content string, beforeRename func() bool) (bool, error) {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	temporary, err := os.CreateTemp(dir, ".openlinker-registration-*.tmp")
	if err != nil {
		return false, err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return false, err
	}
	if _, err = temporary.WriteString(content); err != nil {
		return false, err
	}
	if err = temporary.Sync(); err != nil {
		return false, err
	}
	if err = temporary.Close(); err != nil {
		return false, err
	}
	if beforeRename != nil && !beforeRename() {
		return false, nil
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return false, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		return false, err
	}
	syncRegistrationEnvDir(dir)
	removeTemporary = false
	return true, nil
}

func setRegistrationEnv(values map[string]string, key, value string) {
	if strings.TrimSpace(value) == "" {
		delete(values, key)
	} else {
		values[key] = strings.TrimSpace(value)
	}
}

func unquoteRegistrationEnv(value string) string {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		var decoded string
		if _, err := fmt.Sscanf(value, "%q", &decoded); err == nil {
			return decoded
		}
	}
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1]
	}
	return value
}
