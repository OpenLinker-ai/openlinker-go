package main

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    config
		wantErr string
	}{
		{
			name: "valid config",
			env: map[string]string{
				"OPENAI_MODEL":    " gpt-test ",
				"OPENAI_API_KEY":  " sk-test ",
				"OPENAI_BASE_URL": " https://example.com/v1 ",
			},
			want: config{
				OpenAIModel:   "gpt-test",
				OpenAIAPIKey:  "sk-test",
				OpenAIBaseURL: "https://example.com/v1",
			},
		},
		{
			name: "missing model",
			env: map[string]string{
				"OPENAI_API_KEY": "sk-test",
			},
			wantErr: "OPENAI_MODEL is required",
		},
		{
			name: "missing api key",
			env: map[string]string{
				"OPENAI_MODEL": "gpt-test",
			},
			wantErr: "OPENAI_API_KEY is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setOpenAIEnv(t, tt.env)

			got, err := loadConfig()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("loadConfig() error = nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadConfig() error = %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("loadConfig() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNewAgent(t *testing.T) {
	agent, err := newAgent(config{
		OpenAIModel:   "gpt-test",
		OpenAIAPIKey:  "sk-test",
		OpenAIBaseURL: "https://example.com/v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if agent == nil {
		t.Fatal("newAgent() = nil")
	}
	if got := agent.Name(); got != "Chat Agent" {
		t.Fatalf("agent.Name() = %q, want %q", got, "Chat Agent")
	}
}

func TestSDKAgentName(t *testing.T) {
	if !strings.Contains(sdkAgent, "agent-blades") {
		t.Fatalf("sdkAgent = %q, want agent-blades example identifier", sdkAgent)
	}
}

func setOpenAIEnv(t *testing.T, env map[string]string) {
	t.Helper()

	for _, key := range []string{"OPENAI_MODEL", "OPENAI_API_KEY", "OPENAI_BASE_URL"} {
		t.Setenv(key, "")
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
}
