package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

const cliSDKAgent = "openlinker-cli/0.1"

type cli struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	getenv func(string) string
}

type globalOptions struct {
	apiBase      string
	userToken    string
	runtimeToken string
	timeout      time.Duration
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
}

func runCLI(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	if getenv == nil {
		getenv = os.Getenv
	}
	c := &cli{stdin: stdin, stdout: stdout, stderr: stderr, getenv: getenv}
	if err := c.run(args); err != nil {
		fmt.Fprintf(stderr, "openlinker: %v\n", err)
		return 1
	}
	return 0
}

func (c *cli) run(args []string) error {
	if len(args) == 0 {
		c.printUsage()
		return nil
	}

	global := flag.NewFlagSet("openlinker", flag.ContinueOnError)
	global.SetOutput(c.stderr)
	opts := globalOptions{}
	global.StringVar(&opts.apiBase, "api", firstNonEmpty(c.getenv("OPENLINKER_API_BASE"), c.getenv("OPENLINKER_API_URL"), "http://localhost:8080"), "OpenLinker Core API base URL")
	global.StringVar(&opts.userToken, "token", firstNonEmpty(c.getenv("OPENLINKER_TOKEN"), c.getenv("OPENLINKER_USER_TOKEN"), c.getenv("OPENLINKER_DEMO_JWT")), "user JWT or API token")
	global.StringVar(&opts.runtimeToken, "runtime-token", firstNonEmpty(c.getenv("OPENLINKER_RUNTIME_TOKEN"), c.getenv("OPENLINKER_AGENT_TOKEN")), "runtime token for delegate commands")
	global.DurationVar(&opts.timeout, "timeout", 60*time.Second, "request timeout")

	if err := global.Parse(args); err != nil {
		return err
	}
	rest := global.Args()
	if len(rest) == 0 {
		c.printUsage()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	switch rest[0] {
	case "help", "-h", "--help":
		c.printUsage()
		return nil
	case "context":
		return c.contextCommand()
	case "agents":
		return c.agentsCommand(ctx, opts, rest[1:])
	case "run":
		return c.runCommand(ctx, opts, rest[1:])
	case "delegate":
		return c.delegateCommand(ctx, opts, rest[1:])
	case "runs":
		return c.runsCommand(ctx, opts, rest[1:])
	default:
		return fmt.Errorf("unknown command %q", rest[0])
	}
}

func (c *cli) printUsage() {
	fmt.Fprintln(c.stderr, `Usage:
  openlinker [global flags] context
  openlinker [global flags] agents search [--query q] [--tag tag] [--callable]
  openlinker [global flags] agents get --slug slug
  openlinker [global flags] agents card --slug slug [--extended]
  openlinker [global flags] run --agent agent_id [--input json|text] [--input-file file]
  openlinker [global flags] delegate --agent agent_id [--parent-run run_id] [--reason text] [--input json|text]
  openlinker [global flags] runs get --id run_id
  openlinker [global flags] runs children --id run_id
  openlinker [global flags] runs events --id run_id [--limit n]
  openlinker [global flags] runs messages --id run_id
  openlinker [global flags] runs artifacts --id run_id

Global flags:
  --api             OpenLinker Core API base URL, default OPENLINKER_API_BASE or http://localhost:8080
  --token           user token, default OPENLINKER_TOKEN / OPENLINKER_USER_TOKEN
  --runtime-token   runtime token for delegate, default OPENLINKER_RUNTIME_TOKEN
  --timeout         request timeout

The CLI always writes JSON to stdout and never prints configured tokens.`)
}

func (c *cli) contextCommand() error {
	return writeJSON(c.stdout, map[string]any{
		"api_base": c.firstEnv("OPENLINKER_API_BASE", "OPENLINKER_API_URL"),
		"run_id":   c.getenv("OPENLINKER_RUN_ID"),
		"agent_id": c.getenv("OPENLINKER_AGENT_ID"),
		"trace_id": c.getenv("OPENLINKER_TRACE_ID"),
	})
}

func (c *cli) agentsCommand(ctx context.Context, opts globalOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("agents requires a subcommand: search, get, card")
	}
	client, err := c.userClient(opts)
	if err != nil {
		return err
	}
	switch args[0] {
	case "search":
		fs := newFlagSet("agents search", c.stderr)
		query := fs.String("query", "", "search query")
		q := fs.String("q", "", "search query")
		var tags stringList
		fs.Var(&tags, "tag", "agent tag or skill id; repeatable")
		page := fs.Int("page", 0, "page number")
		size := fs.Int("size", 20, "page size")
		callable := fs.Bool("callable", false, "only list callable agents")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		search := firstNonEmpty(*query, *q)
		out, err := client.ListAgents(ctx, openlinker.ListAgentsParams{
			Query:        search,
			Tags:         tags,
			Page:         *page,
			Size:         *size,
			CallableOnly: *callable,
		})
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "get":
		fs := newFlagSet("agents get", c.stderr)
		slug := fs.String("slug", "", "agent slug")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		value := firstNonEmpty(*slug, firstArg(fs))
		if value == "" {
			return errors.New("agents get requires --slug or positional slug")
		}
		out, err := client.GetAgent(ctx, value)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "card":
		fs := newFlagSet("agents card", c.stderr)
		slug := fs.String("slug", "", "agent slug")
		extended := fs.Bool("extended", false, "fetch extended agent card")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		value := firstNonEmpty(*slug, firstArg(fs))
		if value == "" {
			return errors.New("agents card requires --slug or positional slug")
		}
		out, err := client.GetAgentCard(ctx, value, *extended)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	default:
		return fmt.Errorf("unknown agents subcommand %q", args[0])
	}
}

func (c *cli) runCommand(ctx context.Context, opts globalOptions, args []string) error {
	fs := newFlagSet("run", c.stderr)
	agentID := fs.String("agent", "", "target agent id")
	input := fs.String("input", "", "JSON payload or plain text")
	inputFile := fs.String("input-file", "", "file containing JSON payload or plain text; use - for stdin")
	text := fs.String("text", "", "plain text input")
	metadata := fs.String("metadata", "", "JSON metadata")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*agentID) == "" {
		return errors.New("run requires --agent")
	}
	payload, err := c.payload(*input, *inputFile, *text)
	if err != nil {
		return err
	}
	meta, err := parseOptionalJSON(*metadata)
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	client, err := c.userClient(opts)
	if err != nil {
		return err
	}
	out, err := client.RunAgent(ctx, openlinker.RunAgentRequest{
		AgentID:  strings.TrimSpace(*agentID),
		Input:    payload,
		Metadata: meta,
	})
	if err != nil {
		return err
	}
	return writeJSON(c.stdout, out)
}

func (c *cli) delegateCommand(ctx context.Context, opts globalOptions, args []string) error {
	fs := newFlagSet("delegate", c.stderr)
	targetAgentID := fs.String("agent", "", "target agent id")
	parentRunID := fs.String("parent-run", c.getenv("OPENLINKER_RUN_ID"), "parent/current run id; defaults to OPENLINKER_RUN_ID")
	reason := fs.String("reason", "", "delegation reason")
	input := fs.String("input", "", "JSON payload or plain text")
	inputFile := fs.String("input-file", "", "file containing JSON payload or plain text; use - for stdin")
	text := fs.String("text", "", "plain text input")
	metadata := fs.String("metadata", "", "JSON metadata")
	contextID := fs.String("context-id", "", "A2A context id")
	traceID := fs.String("trace-id", c.getenv("OPENLINKER_TRACE_ID"), "trace id; defaults to OPENLINKER_TRACE_ID")
	referenceTasks := fs.String("reference-task", "", "comma-separated reference task ids")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*targetAgentID) == "" {
		return errors.New("delegate requires --agent")
	}
	if strings.TrimSpace(*parentRunID) == "" {
		return errors.New("delegate requires --parent-run or OPENLINKER_RUN_ID")
	}
	payload, err := c.payload(*input, *inputFile, *text)
	if err != nil {
		return err
	}
	meta, err := parseOptionalJSON(*metadata)
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	client, err := c.runtimeClient(opts)
	if err != nil {
		return err
	}
	out, err := client.CallAgent(ctx, openlinker.CallAgentRequest{
		ParentRunID:      strings.TrimSpace(*parentRunID),
		TargetAgentID:    strings.TrimSpace(*targetAgentID),
		Reason:           strings.TrimSpace(*reason),
		Input:            payload,
		Metadata:         meta,
		ContextID:        strings.TrimSpace(*contextID),
		TraceID:          strings.TrimSpace(*traceID),
		ReferenceTaskIDs: splitCSV(*referenceTasks),
	})
	if err != nil {
		return err
	}
	return writeJSON(c.stdout, out)
}

func (c *cli) runsCommand(ctx context.Context, opts globalOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("runs requires a subcommand: get, children, events, messages, artifacts")
	}
	client, err := c.userClient(opts)
	if err != nil {
		return err
	}
	switch args[0] {
	case "get":
		fs := newFlagSet("runs get", c.stderr)
		runID := fs.String("id", "", "run id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		id := firstNonEmpty(*runID, firstArg(fs))
		if id == "" {
			return errors.New("runs get requires --id or positional run id")
		}
		out, err := client.GetRun(ctx, id)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "children":
		fs := newFlagSet("runs children", c.stderr)
		runID := fs.String("id", "", "run id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		id := firstNonEmpty(*runID, firstArg(fs))
		if id == "" {
			return errors.New("runs children requires --id or positional run id")
		}
		out, err := client.ListRunChildren(ctx, id)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "events":
		fs := newFlagSet("runs events", c.stderr)
		runID := fs.String("id", "", "run id")
		after := fs.Int("after-sequence", 0, "only return events after this sequence")
		limit := fs.Int("limit", 100, "max events")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		id := firstNonEmpty(*runID, firstArg(fs))
		if id == "" {
			return errors.New("runs events requires --id or positional run id")
		}
		out, err := client.ListRunEvents(ctx, id, openlinker.ListRunEventsParams{
			AfterSequence: int32(*after),
			Limit:         int32(*limit),
		})
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "messages":
		fs := newFlagSet("runs messages", c.stderr)
		runID := fs.String("id", "", "run id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		id := firstNonEmpty(*runID, firstArg(fs))
		if id == "" {
			return errors.New("runs messages requires --id or positional run id")
		}
		out, err := client.ListRunMessages(ctx, id)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	case "artifacts":
		fs := newFlagSet("runs artifacts", c.stderr)
		runID := fs.String("id", "", "run id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		id := firstNonEmpty(*runID, firstArg(fs))
		if id == "" {
			return errors.New("runs artifacts requires --id or positional run id")
		}
		out, err := client.ListRunArtifacts(ctx, id)
		if err != nil {
			return err
		}
		return writeJSON(c.stdout, out)
	default:
		return fmt.Errorf("unknown runs subcommand %q", args[0])
	}
}

func (c *cli) userClient(opts globalOptions) (*openlinker.Client, error) {
	return c.newClient(opts, false)
}

func (c *cli) runtimeClient(opts globalOptions) (*openlinker.Client, error) {
	if strings.TrimSpace(opts.runtimeToken) == "" {
		return nil, errors.New("OPENLINKER_RUNTIME_TOKEN is required for delegate")
	}
	return c.newClient(opts, true)
}

func (c *cli) newClient(opts globalOptions, runtime bool) (*openlinker.Client, error) {
	httpClient := &http.Client{Timeout: opts.timeout}
	options := []openlinker.Option{
		openlinker.WithHTTPClient(httpClient),
		openlinker.WithSDKAgent(cliSDKAgent),
	}
	if runtime {
		options = append(options, openlinker.WithRuntimeToken(opts.runtimeToken))
	} else if strings.TrimSpace(opts.userToken) != "" {
		options = append(options, openlinker.WithUserToken(opts.userToken))
	}
	return openlinker.NewClient(opts.apiBase, options...)
}

func (c *cli) payload(input, inputFile, text string) (any, error) {
	set := 0
	for _, value := range []string{input, inputFile, text} {
		if strings.TrimSpace(value) != "" {
			set++
		}
	}
	if set > 1 {
		return nil, errors.New("use only one of --input, --input-file, or --text")
	}
	switch {
	case strings.TrimSpace(text) != "":
		return openlinker.JSON{"text": text}, nil
	case strings.TrimSpace(inputFile) != "":
		raw, err := c.readInputFile(inputFile)
		if err != nil {
			return nil, err
		}
		return parseInputPayload(raw)
	case strings.TrimSpace(input) != "":
		return parseInputPayload([]byte(input))
	default:
		return openlinker.JSON{}, nil
	}
}

func (c *cli) readInputFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "-" {
		return io.ReadAll(c.stdin)
	}
	return os.ReadFile(path)
}

func (c *cli) firstEnv(keys ...string) string {
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, c.getenv(key))
	}
	return firstNonEmpty(values...)
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func firstArg(fs *flag.FlagSet) string {
	if fs.NArg() == 0 {
		return ""
	}
	return fs.Arg(0)
}

func writeJSON(w io.Writer, value any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func parseInputPayload(raw []byte) (any, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return openlinker.JSON{}, nil
	}
	var payload any
	if err := json.Unmarshal([]byte(text), &payload); err == nil {
		return payload, nil
	}
	return openlinker.JSON{"text": text}, nil
}

func parseOptionalJSON(raw string) (any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func splitCSV(raw string) []string {
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if value := strings.TrimSpace(field); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type stringList []string

func (l *stringList) String() string {
	if l == nil {
		return ""
	}
	return strings.Join(*l, ",")
}

func (l *stringList) Set(value string) error {
	if strings.TrimSpace(value) != "" {
		*l = append(*l, strings.TrimSpace(value))
	}
	return nil
}
