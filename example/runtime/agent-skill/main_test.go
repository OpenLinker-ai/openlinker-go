package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	bladesSkills "github.com/go-kratos/blades/skills"
)

func TestEmbeddedOpenLinkerSkillLoadsScript(t *testing.T) {
	loaded, err := bladesSkills.NewFromEmbed(skillFS)
	if err != nil {
		t.Fatal(err)
	}

	skill := findSkill(t, loaded, "openlinker-cli")
	if !strings.Contains(skill.Instruction(), "run_skill_script") {
		t.Fatal("skill instruction should tell the agent to use run_skill_script")
	}
	if !strings.Contains(skill.Instruction(), "scripts/openlinker") {
		t.Fatal("skill instruction should reference scripts/openlinker")
	}

	resourcesProvider, ok := skill.(bladesSkills.ResourcesProvider)
	if !ok {
		t.Fatal("skill does not expose resources")
	}
	if script, ok := resourcesProvider.Resources().GetScript("openlinker"); ok && len(script) == 0 {
		t.Fatal("scripts/openlinker is empty when present")
	}
}

func TestOpenLinkerSkillScriptContextCommand(t *testing.T) {
	loaded, err := bladesSkills.NewFromEmbed(skillFS)
	if err != nil {
		t.Fatal(err)
	}
	skill := findSkill(t, loaded, "openlinker-cli")
	resourcesProvider, ok := skill.(bladesSkills.ResourcesProvider)
	if !ok {
		t.Fatal("skill does not expose resources")
	}
	if _, ok := resourcesProvider.Resources().GetScript("openlinker"); !ok {
		t.Skip("openlinker CLI binary is not embedded; build it into skills/openlinker-cli/scripts/openlinker to run this test")
	}

	toolset, err := bladesSkills.NewToolset(loaded)
	if err != nil {
		t.Fatal(err)
	}

	runScript := findTool(t, toolset, bladesSkills.ToolRunSkillScriptName)
	input, err := json.Marshal(map[string]any{
		"skill_name":  "openlinker-cli",
		"script_path": "scripts/openlinker",
		"args":        []string{"context"},
		"env": map[string]string{
			"OPENLINKER_API_BASE": "http://core.test",
			"OPENLINKER_RUN_ID":   "run-test",
			"OPENLINKER_AGENT_ID": "agent-test",
			"OPENLINKER_TRACE_ID": "trace-test",
		},
		"timeout_seconds": 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := runScript.Handle(context.Background(), string(input))
	if err != nil {
		t.Fatal(err)
	}

	var scriptResult struct {
		Status   string `json:"status"`
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
		ExitCode int    `json:"exit_code"`
	}
	if err := json.Unmarshal([]byte(resp), &scriptResult); err != nil {
		t.Fatalf("unmarshal script result: %v\n%s", err, resp)
	}
	if scriptResult.Status != "success" || scriptResult.ExitCode != 0 {
		t.Fatalf("script failed status=%q exit=%d stderr=%q", scriptResult.Status, scriptResult.ExitCode, scriptResult.Stderr)
	}

	var contextOut map[string]string
	if err := json.Unmarshal([]byte(scriptResult.Stdout), &contextOut); err != nil {
		t.Fatalf("unmarshal CLI stdout: %v\n%s", err, scriptResult.Stdout)
	}
	if contextOut["api_base"] != "http://core.test" {
		t.Fatalf("api_base = %q", contextOut["api_base"])
	}
	if contextOut["run_id"] != "run-test" {
		t.Fatalf("run_id = %q", contextOut["run_id"])
	}
	if contextOut["agent_id"] != "agent-test" {
		t.Fatalf("agent_id = %q", contextOut["agent_id"])
	}
	if contextOut["trace_id"] != "trace-test" {
		t.Fatalf("trace_id = %q", contextOut["trace_id"])
	}
}

func findSkill(t *testing.T, loaded []bladesSkills.Skill, name string) bladesSkills.Skill {
	t.Helper()
	for _, skill := range loaded {
		if skill.Name() == name {
			return skill
		}
	}
	t.Fatalf("skill %q not found", name)
	return nil
}

type namedTool interface {
	Name() string
	Handle(context.Context, string) (string, error)
}

func findTool(t *testing.T, toolset *bladesSkills.Toolset, name string) namedTool {
	t.Helper()
	for _, tool := range toolset.Tools() {
		if tool.Name() == name {
			return tool
		}
	}
	t.Fatalf("tool %q not found", name)
	return nil
}
