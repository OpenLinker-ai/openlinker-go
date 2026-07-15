package main

import (
	"io"
	"log"
	"path/filepath"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
)

func TestManagedWorkerConfigurationUsesPublicConstructor(t *testing.T) {
	store, err := openlinker.OpenFileRuntimeStore(filepath.Join(t.TempDir(), "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
		RuntimeURL: "https://runtime.example.test", Transport: openlinker.TransportAuto,
		NodeID: runtimetest.NodeID, AgentID: runtimetest.AgentID, AgentToken: runtimetest.AgentToken,
		MTLS:  openlinker.RuntimeMTLSConfig{CertFile: "node.crt", KeyFile: "node.key", CAFile: "ca.crt"},
		Store: store, Handler: managedHandler(), Capacity: 4, Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.Capacity != 4 || worker.Transport != openlinker.TransportAuto || worker.Store != store || worker.Handler == nil {
		t.Fatalf("worker = %#v", worker)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
