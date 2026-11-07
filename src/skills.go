package llmr

import (
	"fmt"
	"strings"
)

func ExpandSkills(cfg *Config, names []string) ([]Tool, string, error) {
	var tools []Tool
	var instructions []string
	for _, name := range names {
		skill, ok := cfg.Skills[name]
		if !ok {
			return nil, "", fmt.Errorf("unknown skill: %s", name)
		}
		tools = append(tools, skill.Tools...)
		if skill.Instructions != "" {
			instructions = append(instructions, skill.Instructions)
		}
	}
	return tools, strings.Join(instructions, "\n"), nil
}
