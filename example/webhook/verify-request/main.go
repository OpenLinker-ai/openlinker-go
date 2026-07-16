package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

const maxCallbackBytes = int64(1 << 20)

func main() {
	secret, err := exampleutil.RequiredEnv("OPENLINKER_CALLBACK_SECRET")
	if err != nil {
		log.Fatal(err)
	}
	address := exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_WEBHOOK_ADDR"), ":8080")
	ctx, stop := exampleutil.SignalContext()
	defer stop()
	server := &http.Server{Addr: address, Handler: newHandler(secret, os.Stdout), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("webhook verifier listening on %s", address)
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newHandler(secret string, output io.Writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw, valid, err := openlinker.VerifyTaskCallbackRequest(request, secret, maxCallbackBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !valid {
			http.Error(w, "invalid OpenLinker signature", http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err = json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err = exampleutil.PrintJSON(output, map[string]any{"verified": true, "bytes": len(raw), "payload": payload}); err != nil {
			http.Error(w, fmt.Sprintf("write output: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
