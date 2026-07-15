package openlinker

import (
	"bufio"
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

func readRegistrationEnv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
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
	managed := map[string]struct{}{}
	for _, key := range registrationEnvKeys {
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
		return err
	}
	for _, key := range registrationEnvKeys {
		if _, ok := seen[key]; !ok && values[key] != "" {
			out.WriteString(key + "=" + fmt.Sprintf("%q", values[key]) + "\n")
		}
	}
	if dir == "" {
		dir = "."
	}
	temporary, err := os.CreateTemp(dir, ".openlinker-registration-*.tmp")
	if err != nil {
		return err
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
		return err
	}
	if _, err = temporary.WriteString(out.String()); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		return err
	}
	if directory, openErr := os.Open(dir); openErr == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	removeTemporary = false
	return nil
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
