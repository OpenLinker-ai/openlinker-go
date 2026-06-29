package openlinker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCoreContractsMapToImplementedMethods(t *testing.T) {
	files, err := filepath.Glob("contracts/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no contract files found")
	}

	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var contract struct {
			Scope string `json:"scope"`
			Rules struct {
				ForbiddenPathPrefixes []string `json:"forbidden_path_prefixes"`
			} `json:"rules"`
			Endpoints []struct {
				ClientMethod string `json:"client_method"`
				Path         string `json:"path"`
			} `json:"endpoints"`
		}
		if err := json.Unmarshal(raw, &contract); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(contract.Scope, "core") {
			t.Fatalf("scope = %q", contract.Scope)
		}
		clientType := reflect.TypeOf(&Client{})
		for _, endpoint := range contract.Endpoints {
			if !strings.HasPrefix(endpoint.Path, "/api/v1/") {
				t.Fatalf("endpoint path must be core API path: %s", endpoint.Path)
			}
			for _, prefix := range contract.Rules.ForbiddenPathPrefixes {
				if strings.HasPrefix(endpoint.Path, prefix) {
					t.Fatalf("forbidden endpoint in core contract: %s", endpoint.Path)
				}
			}
			methodName := exportedMethodName(endpoint.ClientMethod)
			if _, ok := clientType.MethodByName(methodName); !ok {
				t.Fatalf("Client missing contract method %s", endpoint.ClientMethod)
			}
		}
	}
}

func exportedMethodName(name string) string {
	if name == "" {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}
