package openlinker

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRuntimePublicSurfaceHasNoGenerationName(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if strings.Contains(strings.ToLower(name), "runtime_v2") {
			t.Fatalf("generation name remains in active filename %q", name)
		}
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(files, name, nil, 0)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && identifier.IsExported() && strings.Contains(identifier.Name, "RuntimeV2") {
				t.Errorf("generation name remains in exported identifier %s (%s)", identifier.Name, files.Position(identifier.Pos()))
			}
			return true
		})
	}
	if _, err = os.Stat("contracts/core-runtime.json"); err != nil {
		t.Fatalf("canonical Runtime contract is missing: %v", err)
	}
	if _, err = os.Stat("contracts/core-runtime.v2.json"); !os.IsNotExist(err) {
		t.Fatal("versioned Runtime contract filename must not exist")
	}
}

func TestNewRuntimeWorkerAcceptsInjectedStoreWithoutDataDir(t *testing.T) {
	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	worker, err := NewRuntimeWorker(RuntimeWorkerConfig{
		RuntimeURL: "https://runtime.example.test",
		NodeID:     testNodeID,
		AgentID:    testAgentID,
		AgentToken: "ol_agent_test",
		MTLS: RuntimeMTLSConfig{
			CertFile: "client.crt",
			KeyFile:  "client.key",
			CAFile:   "ca.crt",
		},
		Store:   store,
		Handler: RuntimeHandlerFunc(func(context.Context, RuntimeContext) (RuntimeResult, error) { return RuntimeResult{}, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.DataDir != "" || worker.Store != store {
		t.Fatalf("injected store was not preserved: %#v", worker)
	}
	if worker.NodeVersion != runtimeWorkerSDKAgent || worker.Transport != RuntimeTransportAuto {
		t.Fatalf("worker defaults = node_version %q transport %q", worker.NodeVersion, worker.Transport)
	}
}

func TestRuntimeWorkerDoesNotExposeTransportInjectionFields(t *testing.T) {
	typeOfWorker := reflect.TypeOf(RuntimeWorker{})
	for _, fieldName := range []string{"RuntimeClient", "RuntimeDialer"} {
		if _, ok := typeOfWorker.FieldByName(fieldName); ok {
			t.Fatalf("RuntimeWorker must not expose the test seam %s", fieldName)
		}
	}
}
