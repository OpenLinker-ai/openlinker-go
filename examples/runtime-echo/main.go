package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func durationMilliseconds(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	milliseconds, err := strconv.Atoi(value)
	if err != nil || milliseconds < 1 {
		log.Fatalf("%s must be a positive integer number of milliseconds", name)
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func inputPhase(input any) string {
	var value map[string]any
	switch typed := input.(type) {
	case openlinker.RuntimeJSONMap:
		value = typed
	case map[string]any:
		value = typed
	default:
		return ""
	}
	phase, _ := value["phase"].(string)
	return phase
}

func recordInvocation(path, runID string, execution int64) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = fmt.Fprintf(file, "%s\t%d\n", runID, execution); err != nil {
		return err
	}
	return file.Sync()
}

func waitForRelease(ctx context.Context, path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	transport := openlinker.RuntimeTransportMode(strings.ToLower(strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_TRANSPORT"))))
	if transport == "" {
		transport = openlinker.RuntimeTransportAuto
	}
	var executions atomic.Int64
	worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
		PlatformURL: required("OPENLINKER_URL"),
		RuntimeURL:  strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_URL")),
		NodeID:      strings.TrimSpace(os.Getenv("OPENLINKER_NODE_ID")),
		NodeVersion: "openlinker-go/runtime-worker",
		AgentID:     required("OPENLINKER_AGENT_ID"),
		AgentToken:  required("OPENLINKER_AGENT_TOKEN"),
		Transport:   transport,
		Capacity:    1,
		DataDir:     required("OPENLINKER_RUNTIME_DATA_DIR"),
		MTLS: openlinker.RuntimeMTLSConfig{
			CertFile: strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_MTLS_CERT_FILE")),
			KeyFile:  strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_MTLS_KEY_FILE")),
			CAFile:   strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_MTLS_CA_FILE")),
		},
		RetryMinimum:      durationMilliseconds("OPENLINKER_RUNTIME_RETRY_MIN_MS", 100*time.Millisecond),
		RetryMaximum:      durationMilliseconds("OPENLINKER_RUNTIME_RETRY_MAX_MS", time.Second),
		HeartbeatInterval: durationMilliseconds("OPENLINKER_RUNTIME_HEARTBEAT_MS", 2*time.Second),
		Logger:            log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
		Handler: openlinker.RuntimeHandlerFunc(func(handlerCtx context.Context, run openlinker.RuntimeContext) (openlinker.RuntimeResult, error) {
			execution := executions.Add(1)
			if err := recordInvocation(os.Getenv("OPENLINKER_RUNTIME_INVOCATION_LOG_FILE"), run.RunID, execution); err != nil {
				return openlinker.RuntimeResult{}, err
			}
			if inputPhase(run.Input) == strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_BLOCK_PHASE")) {
				releaseFile := strings.TrimSpace(os.Getenv("OPENLINKER_RUNTIME_BLOCK_UNTIL_FILE"))
				if releaseFile != "" {
					if err := waitForRelease(handlerCtx, releaseFile); err != nil {
						return openlinker.RuntimeResult{}, err
					}
				}
			}
			if err := run.Emit("run.progress", map[string]any{
				"stage":     "handled",
				"sdk":       "go",
				"execution": execution,
			}); err != nil {
				return openlinker.RuntimeResult{}, err
			}
			return openlinker.RuntimeResult{Output: map[string]any{
				"sdk_language":         "go",
				"configured_transport": string(transport),
				"handler_execution":    execution,
				"input":                run.Input,
			}}, nil
		}),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("runtime worker example starting: sdk=go transport=%s\n", transport)
	if err := worker.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
