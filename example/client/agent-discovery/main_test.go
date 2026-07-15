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

func TestRunDiscoversAndReadsAgent(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer ol_user_example" {
			t.Fatalf("Authorization = %q", got)
		}
		calls = append(calls, request.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/agents":
			if request.URL.Query().Get("q") != "data" || request.URL.Query().Get("callable_only") != "true" {
				t.Fatalf("query = %s", request.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(openlinker.MarketListResponse{Items: []openlinker.MarketListItem{{ID: "agent-1", Slug: "data-agent", Name: "Data Agent"}}, Total: 1, Page: 1, Size: 10})
		case "/api/v1/agents/data-agent":
			_ = json.NewEncoder(w).Encode(openlinker.AgentDetailResponse{MarketListItem: openlinker.MarketListItem{ID: "agent-1", Slug: "data-agent", Name: "Data Agent"}})
		case "/api/v1/agents/data-agent/agent-card.json":
			_ = json.NewEncoder(w).Encode(openlinker.AgentCardResponse{Name: "Data Agent", Version: "1.0"})
		default:
			t.Fatalf("unexpected request %s", request.URL.Path)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	err := run(context.Background(), config{APIBase: server.URL, UserToken: "ol_user_example", Slug: "data-agent", Query: "data"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || !strings.Contains(output.String(), `"total_callable_agents": 1`) || !strings.Contains(output.String(), `"name": "Data Agent"`) {
		t.Fatalf("calls=%#v output=%s", calls, output.String())
	}
}
