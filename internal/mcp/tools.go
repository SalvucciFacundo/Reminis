package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fds1288/reminis/pkg/client"
)

// ToolDefinition defines the MCP tool metadata and parameter schema.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// GetToolDefinitions returns all supported Reminis MCP tools.
func GetToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{
			Name:        "reminis_run",
			Description: "Execute a workflow run to achieve a specified goal using Reminis OS-LLM orchestrator",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"goal": map[string]any{
						"type":        "string",
						"description": "High-level goal or objective to plan and execute",
					},
					"model": map[string]any{
						"type":        "string",
						"description": "Optional model override (e.g. gpt-4o-mini)",
					},
					"max_concurrency": map[string]any{
						"type":        "integer",
						"description": "Maximum number of parallel worker goroutines (default 4)",
					},
				},
				"required": []string{"goal"},
			},
		},
		{
			Name:        "reminis_resume",
			Description: "Resume execution of a paused or interrupted run from its SQLite checkpoint",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"run_id": map[string]any{
						"type":        "string",
						"description": "The unique run identifier to resume",
					},
				},
				"required": []string{"run_id"},
			},
		},
		{
			Name:        "reminis_approve",
			Description: "Approve a paused task waiting in WAITING_APPROVAL and resume workflow execution",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"run_id": map[string]any{
						"type":        "string",
						"description": "Unique run ID containing the gated task",
					},
					"task_id": map[string]any{
						"type":        "string",
						"description": "ID of the specific task to approve",
					},
					"scope": map[string]any{
						"type":        "string",
						"description": "Approval scope: 'action' (one-time) or 'session' (grant for all subsequent tasks)",
						"enum":        []string{"action", "session"},
					},
				},
				"required": []string{"run_id", "task_id"},
			},
		},
		{
			Name:        "reminis_facts_search",
			Description: "Search durable architectural facts and conventions by topic and optional scope",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic": map[string]any{
						"type":        "string",
						"description": "Keyword or topic to search for",
					},
					"scope": map[string]any{
						"type":        "string",
						"description": "Optional scope filter: 'project' or 'global'",
					},
				},
				"required": []string{"topic"},
			},
		},
		{
			Name:        "reminis_facts_save",
			Description: "Store an architectural fact, decision, or convention into durable SQLite memory",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic": map[string]any{
						"type":        "string",
						"description": "Topic or category identifier (e.g. 'architecture', 'database')",
					},
					"content": map[string]any{
						"type":        "string",
						"description": "Fact details, rule, or architectural decision",
					},
					"scope": map[string]any{
						"type":        "string",
						"description": "Scope of the fact: 'project' (workspace-level) or 'global' (cross-project)",
					},
				},
				"required": []string{"topic", "content"},
			},
		},
		{
			Name:        "reminis_prefix_show",
			Description: "Audit resolved rules cascade, token count, and SHA-256 PrefixHash for prompt caching",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
}

// CallTool dispatches tool execution to the appropriate Reminis client method.
func CallTool(ctx context.Context, c *client.Client, name string, rawArgs json.RawMessage) (string, error) {
	switch name {
	case "reminis_run":
		var args struct {
			Goal           string `json:"goal"`
			Model          string `json:"model"`
			MaxConcurrency int    `json:"max_concurrency"`
		}
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return "", fmt.Errorf("invalid arguments for reminis_run: %w", err)
			}
		}
		if args.Goal == "" {
			return "", fmt.Errorf("missing required argument 'goal'")
		}

		resp, err := c.Run(ctx, client.RunRequest{
			Goal:           args.Goal,
			Model:          args.Model,
			MaxConcurrency: args.MaxConcurrency,
		})
		if err != nil {
			return "", err
		}
		outBytes, _ := json.MarshalIndent(resp, "", "  ")
		return string(outBytes), nil

	case "reminis_resume":
		var args struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("invalid arguments for reminis_resume: %w", err)
		}
		if args.RunID == "" {
			return "", fmt.Errorf("missing required argument 'run_id'")
		}

		resp, err := c.Resume(ctx, args.RunID)
		if err != nil {
			return "", err
		}
		outBytes, _ := json.MarshalIndent(resp, "", "  ")
		return string(outBytes), nil

	case "reminis_approve":
		var args struct {
			RunID  string `json:"run_id"`
			TaskID string `json:"task_id"`
			Scope  string `json:"scope"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("invalid arguments for reminis_approve: %w", err)
		}
		if args.RunID == "" || args.TaskID == "" {
			return "", fmt.Errorf("missing required arguments 'run_id' and 'task_id'")
		}
		if args.Scope == "" {
			args.Scope = "action"
		}

		if err := c.Approve(ctx, args.RunID, args.TaskID, args.Scope); err != nil {
			return "", err
		}
		res := map[string]any{
			"approved": true,
			"run_id":   args.RunID,
			"task_id":  args.TaskID,
			"scope":    args.Scope,
		}
		outBytes, _ := json.MarshalIndent(res, "", "  ")
		return string(outBytes), nil

	case "reminis_facts_search":
		var args struct {
			Topic string `json:"topic"`
			Scope string `json:"scope"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("invalid arguments for reminis_facts_search: %w", err)
		}
		if args.Topic == "" {
			return "", fmt.Errorf("missing required argument 'topic'")
		}

		facts, err := c.SearchFacts(ctx, args.Topic, args.Scope)
		if err != nil {
			return "", err
		}
		outBytes, _ := json.MarshalIndent(facts, "", "  ")
		return string(outBytes), nil

	case "reminis_facts_save":
		var args struct {
			Topic   string `json:"topic"`
			Content string `json:"content"`
			Scope   string `json:"scope"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("invalid arguments for reminis_facts_save: %w", err)
		}
		if args.Topic == "" || args.Content == "" {
			return "", fmt.Errorf("missing required arguments 'topic' and 'content'")
		}
		if args.Scope == "" {
			args.Scope = "project"
		}

		if err := c.SaveFact(ctx, args.Topic, args.Content, args.Scope); err != nil {
			return "", err
		}
		res := map[string]any{
			"saved":   true,
			"topic":   args.Topic,
			"content": args.Content,
			"scope":   args.Scope,
		}
		outBytes, _ := json.MarshalIndent(res, "", "  ")
		return string(outBytes), nil

	case "reminis_prefix_show":
		prefix, err := c.GetCompiledPrefix(ctx)
		if err != nil {
			return "", err
		}
		outBytes, _ := json.MarshalIndent(prefix, "", "  ")
		return string(outBytes), nil

	default:
		return "", fmt.Errorf("unknown tool %q", name)
	}
}
