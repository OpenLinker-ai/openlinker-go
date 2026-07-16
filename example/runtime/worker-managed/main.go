package main

import (
	"context"
	"errors"
	"log"
	"os"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

func main() {
	ctx, stop := exampleutil.SignalContext()
	defer stop()
	config, err := openlinker.LoadRuntimeWorkerConfig()
	if err != nil {
		log.Fatal(err)
	}
	store, err := openlinker.OpenFileRuntimeStore(config.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	config.Store = store
	config.DataDir = ""
	config.Capacity = 4
	config.Transport = openlinker.TransportAuto
	config.Logger = log.New(os.Stderr, "runtime-worker: ", log.LstdFlags)
	config.Handler = managedHandler()
	worker, err := openlinker.NewRuntimeWorker(config)
	if err != nil {
		_ = store.Close()
		log.Fatal(err)
	}
	if err = worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func managedHandler() openlinker.RuntimeHandler {
	return openlinker.RuntimeHandlerFunc(func(_ context.Context, run openlinker.RuntimeContext) (openlinker.RuntimeResult, error) {
		if err := run.Emit("run.message.delta", map[string]any{"text": "managed worker received assignment"}); err != nil {
			return openlinker.RuntimeResult{}, err
		}
		return openlinker.RuntimeResult{Status: "success", Output: map[string]any{"input": run.Input}}, nil
	})
}
