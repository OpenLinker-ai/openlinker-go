package openlinker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type NativeTextAgent interface {
	Run(context.Context, string) (string, error)
}

type NativeTextAgentFunc func(context.Context, string) (string, error)

func (f NativeTextAgentFunc) Run(ctx context.Context, input string) (string, error) {
	if f == nil {
		return "", errors.New("openlinker: native text function is nil")
	}
	return f(ctx, input)
}

type NativeAgentRunner struct {
	agent         NativeTextAgent
	model         string
	sdkAgent      string
	fallbackInput string

	runtime      *Runtime
	client       *Client
	apiBase      string
	runtimeToken string
	connector    string
	pullWait     time.Duration
	maxRuns      int
	onReady      func(RuntimeWSServerMessage)
	onError      func(error)
}

func WithAgent(agent NativeTextAgent) *NativeAgentRunner {
	return &NativeAgentRunner{
		agent:         agent,
		sdkAgent:      defaultNativeSDKAgent,
		fallbackInput: "hello",
	}
}

func WithFunc(fn func(context.Context, string) (string, error)) *NativeAgentRunner {
	return WithAgent(NativeTextAgentFunc(fn))
}

func (r *NativeAgentRunner) WithModel(model string) *NativeAgentRunner {
	r.model = strings.TrimSpace(model)
	return r
}

func (r *NativeAgentRunner) WithSDKAgent(agent string) *NativeAgentRunner {
	if strings.TrimSpace(agent) != "" {
		r.sdkAgent = strings.TrimSpace(agent)
	}
	return r
}

func (r *NativeAgentRunner) WithFallbackInput(input string) *NativeAgentRunner {
	r.fallbackInput = strings.TrimSpace(input)
	return r
}

func (r *NativeAgentRunner) WithClient(client *Client) *NativeAgentRunner {
	r.client = client
	return r
}

func (r *NativeAgentRunner) WithRuntime(runtime *Runtime) *NativeAgentRunner {
	r.runtime = runtime
	return r
}

func (r *NativeAgentRunner) WithAPIBase(apiBase string) *NativeAgentRunner {
	r.apiBase = strings.TrimSpace(apiBase)
	return r
}

func (r *NativeAgentRunner) WithRuntimeToken(token string) *NativeAgentRunner {
	r.runtimeToken = strings.TrimSpace(token)
	return r
}

func (r *NativeAgentRunner) WithConnector(connector string) *NativeAgentRunner {
	r.connector = strings.TrimSpace(connector)
	return r
}

func (r *NativeAgentRunner) WithPullWait(wait time.Duration) *NativeAgentRunner {
	r.pullWait = wait
	return r
}

func (r *NativeAgentRunner) WithMaxRuns(maxRuns int) *NativeAgentRunner {
	r.maxRuns = maxRuns
	return r
}

func (r *NativeAgentRunner) WithReadyHandler(fn func(RuntimeWSServerMessage)) *NativeAgentRunner {
	r.onReady = fn
	return r
}

func (r *NativeAgentRunner) WithErrorHandler(fn func(error)) *NativeAgentRunner {
	r.onError = fn
	return r
}

func (r *NativeAgentRunner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("openlinker: native agent runner is nil")
	}
	if r.agent == nil {
		return errors.New("openlinker: native agent is required")
	}
	runner := Native(r.handleRun).
		WithSDKAgent(r.sdkAgent).
		WithAPIBase(r.apiBase).
		WithRuntimeToken(r.runtimeToken).
		WithConnector(r.connector).
		WithPullWait(r.pullWait).
		WithMaxRuns(r.maxRuns).
		WithReadyHandler(r.onReady).
		WithErrorHandler(r.onError)
	if r.client != nil {
		runner.WithClient(r.client)
	}
	if r.runtime != nil {
		runner.WithRuntime(r.runtime)
	}
	return runner.Run(ctx)
}

func (r *NativeAgentRunner) handleRun(ctx context.Context, run NativeRun) (any, error) {
	text := run.Text()
	if text == "" {
		text = r.fallbackInput
	}

	progress := AgentEvent{
		EventType: AgentEventTypeRunMessageDelta,
		Payload:   JSON{"text": fmt.Sprintf("native worker received: %s", text)},
	}
	_ = run.SendEvent(ctx, progress)

	answer, err := r.agent.Run(ContextWithNativeRun(ctx, run), text)
	if err != nil {
		return NativeResult{
			Status: "failed",
			Events: []AgentEvent{
				progress,
			},
			Error: &AgentError{
				Code:    "AGENT_WORKER_ERROR",
				Message: err.Error(),
			},
		}, nil
	}
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return NativeResult{
			Status: "failed",
			Events: []AgentEvent{
				progress,
			},
			Error: &AgentError{
				Code:    "AGENT_WORKER_ERROR",
				Message: "agent returned empty response",
			},
		}, nil
	}

	events := []AgentEvent{{
		EventType: AgentEventTypeRunMessageDelta,
		Payload:   JSON{"text": answer},
	}}
	if !run.SupportsLiveEvents() {
		events = append([]AgentEvent{progress}, events...)
	}

	output := JSON{
		"text": answer,
		"input": JSON{
			"text": text,
			"raw":  run.Assignment.Input,
		},
	}
	if r.model != "" {
		output["llm"] = JSON{
			"text":     answer,
			"run_id":   run.Assignment.RunID,
			"agent_id": run.Assignment.AgentID,
			"model":    r.model,
		}
	}
	if run.Assignment.A2A != nil {
		output["a2a_context"] = run.Assignment.A2A
	}

	return NativeResult{
		Status: "success",
		Output: output,
		Events: events,
	}, nil
}
