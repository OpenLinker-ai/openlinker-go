package main

import (
	"context"
	"embed"
	"log"
	"os"

	"github.com/go-kratos/blades"
	"github.com/go-kratos/blades/contrib/openai"
	"github.com/go-kratos/blades/skills"
)

//go:embed skills
var skillFS embed.FS

func main() {
	model := openai.NewModel(os.Getenv("OPENAI_MODEL"), openai.Config{
		BaseURL: os.Getenv("OPENAI_BASE_URL"),
		APIKey:  os.Getenv("OPENAI_API_KEY"),
	})

	openlinkerSkills, err := skills.NewFromEmbed(skillFS)
	if err != nil {
		log.Fatal(err)
	}

	agent, err := blades.NewAgent(
		"SkillUserAgent",
		blades.WithModel(model),
		blades.WithInstruction(`Use skills when relevant.
When using openlinker-cli, execute it with run_skill_script.
Do not invent shell commands or local paths.
After receiving a successful OpenLinker CLI JSON response, summarize the result and stop.
Do not repeat the same OpenLinker CLI call unless the user asks for another query.`),
		blades.WithMaxIterations(6),
		blades.WithSkills(openlinkerSkills...),
	)
	if err != nil {
		log.Fatal(err)
	}

	runner := blades.NewRunner(agent)
	output, err := runner.Run(context.Background(), blades.UserMessage("我现在在openlinker中有一个标签为a2a的agent, 我需要你对它下发一次任务，问它一个'你好吗？ by client'"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(output.Text())
}
