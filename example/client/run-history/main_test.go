package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestRunReadsHistoryAndRetentionMetadata(t *testing.T) {
	var calls []string
	latest := int32(9)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer ol_user_example" {
			t.Fatalf("Authorization = %q", got)
		}
		calls = append(calls, request.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/runs/run-history-1/events":
			if request.URL.Query().Get("after_sequence") != "7" || request.URL.Query().Get("limit") != "50" {
				t.Fatalf("event query = %s", request.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(openlinker.ListRunEventsResponse{
				Items: []openlinker.RunEventResponse{{EventID: "event-8", RunID: "run-history-1", Sequence: 8, EventType: "run.message.delta"}},
				Meta:  openlinker.RunEventPageMeta{RequestedAfterSequence: 7, EffectiveAfterSequence: 7, LatestAvailableSequence: &latest, Terminal: true, StreamComplete: true},
			})
		case "/api/v1/runs/run-history-1/messages":
			_ = json.NewEncoder(w).Encode(openlinker.ListItemsResponse[openlinker.RunMessageResponse]{Items: []openlinker.RunMessageResponse{{ID: "message-1", RunID: "run-history-1", Role: "assistant", Content: "done"}}})
		case "/api/v1/runs/run-history-1/artifacts":
			_ = json.NewEncoder(w).Encode(openlinker.ListItemsResponse[openlinker.RunArtifactResponse]{Items: []openlinker.RunArtifactResponse{{ID: "artifact-1", RunID: "run-history-1", Title: "report"}}})
		case "/api/v1/runs/run-history-1/children":
			_ = json.NewEncoder(w).Encode(openlinker.ListRunChildrenResponse{Items: []openlinker.RunChildResponse{{ChildRunID: "child-1", Status: "success"}}})
		default:
			t.Fatalf("unexpected request %s", request.URL.Path)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	err := run(context.Background(), config{APIBase: server.URL, UserToken: "ol_user_example", RunID: "run-history-1", AfterSequence: 7}, &output)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"requested_after_sequence": 7`, `"latest_available_sequence": 9`, `"message-1"`, `"artifact-1"`, `"child-1"`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("output %q missing %q", output.String(), want)
		}
	}
	if len(calls) != 4 {
		t.Fatalf("calls = %#v", calls)
	}
}
