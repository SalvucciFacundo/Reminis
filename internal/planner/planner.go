package planner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/prompt"
	"github.com/fds1288/reminis/internal/worker"
)

// MaxReflectionAttempts defines the hard limit for Level 2 self-correction attempts.
const MaxReflectionAttempts = 2

var (
	// ErrReflectionExhausted is returned when Level 2 reflection cannot produce a valid DAG.
	ErrReflectionExhausted = errors.New("planner reflection exhausted: task graph remains invalid")
)

// Planner synthesizes high-level user goals into strictly typed, acyclic DAG task graphs.
type Planner struct {
	client *worker.Client
	model  string
}

// New creates a new Planner instance.
func New(client *worker.Client, model string) *Planner {
	return &Planner{
		client: client,
		model:  model,
	}
}

// NewPlanner is an alias for New.
func NewPlanner(client *worker.Client, model string) *Planner {
	return New(client, model)
}

// Plan calls the LLM with the structured planner prompt, parses the output into a []dag.Task,
// validates graph acyclicity and namespace isolation, and applies Level 2 reflection (max 2 attempts)
// if validation or JSON parsing fails.
func (p *Planner) Plan(ctx context.Context, goal, manifest, rulesContent string) ([]dag.Task, error) {
	if p.client == nil {
		return nil, errors.New("planner worker client cannot be nil")
	}

	messages := BuildPlannerMessages(goal, manifest, rulesContent)

	req := worker.ChatCompletionRequest{
		Model:    p.model,
		Messages: messages,
	}

	resp, err := p.client.CreateChatCompletion(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to generate plan from LLM: %w", err)
	}
	if len(resp.Choices) == 0 {
		return nil, errors.New("empty choices in planner response")
	}

	rawContent := resp.Choices[0].Message.Content
	reflectionAttempts := 0

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		tasks, parseErr := parseAndValidateDAG(rawContent)
		if parseErr == nil {
			return tasks, nil
		}

		if reflectionAttempts >= MaxReflectionAttempts {
			return nil, fmt.Errorf("%w: %v", ErrReflectionExhausted, parseErr)
		}
		reflectionAttempts++

		// Level 2 Reflection: feed error details back to the LLM
		messages = append(messages, prompt.ChatMessage{
			Role:    "assistant",
			Content: rawContent,
		})
		messages = append(messages, BuildReflectionPrompt(parseErr, rawContent))

		req := worker.ChatCompletionRequest{
			Model:    p.model,
			Messages: messages,
		}

		resp, err = p.client.CreateChatCompletion(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("planner reflection request failed (attempt %d): %w", reflectionAttempts, err)
		}
		if len(resp.Choices) == 0 {
			return nil, fmt.Errorf("planner reflection returned empty choices (attempt %d)", reflectionAttempts)
		}

		rawContent = resp.Choices[0].Message.Content
	}
}

// parseAndValidateDAG extracts JSON from raw output, unmarshals []dag.Task,
// and enforces DAG acyclicity, task validity, and namespace isolation.
func parseAndValidateDAG(raw string) ([]dag.Task, error) {
	jsonBytes, err := worker.ExtractJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	var tasks []dag.Task
	if err := json.Unmarshal(jsonBytes, &tasks); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tasks array: %w", err)
	}

	if len(tasks) == 0 {
		return nil, errors.New("plan must contain at least one task")
	}

	// Validate individual task fields
	for _, t := range tasks {
		if strings.TrimSpace(t.ID) == "" {
			return nil, errors.New("task ID cannot be empty")
		}
		if strings.TrimSpace(t.Action) == "" {
			return nil, fmt.Errorf("task %q has empty action", t.ID)
		}

		// Enforce namespace isolation: tasks.<id>.*
		expectedPrefix := fmt.Sprintf("tasks.%s.", t.ID)
		for _, outKey := range t.OutputKeys {
			if !strings.HasPrefix(outKey, expectedPrefix) {
				return nil, fmt.Errorf("task %q output key %q violates namespace isolation: must start with %q",
					t.ID, outKey, expectedPrefix)
			}
		}
	}

	// Validate acyclicity using Kahn's algorithm
	if _, err := dag.ValidateAcyclic(tasks); err != nil {
		return nil, fmt.Errorf("acyclicity validation failed: %w", err)
	}

	return tasks, nil
}

// BuildReflectionPrompt formats a Level 2 reflection prompt explaining why the previous plan failed.
func BuildReflectionPrompt(err error, rawOutput string) prompt.ChatMessage {
	snippet := strings.TrimSpace(rawOutput)
	if len(snippet) > 800 {
		snippet = snippet[:800] + "... [truncated]"
	}

	content := fmt.Sprintf("# Level 2 Resilience: DAG Plan Validation Error\n\n"+
		"Your previous plan could not be validated:\n"+
		"**Validation Error**: %v\n\n"+
		"**Previous Output**:\n```\n%s\n```\n\n"+
		"Please self-correct and output ONLY a valid JSON array of tasks (`[]dag.Task`) satisfying:\n"+
		"1. Strict acyclicity: no circular dependencies in `depends_on`, and all dependencies reference valid task IDs.\n"+
		"2. Valid JSON array: no markdown commentary or formatting outside the JSON array.\n"+
		"3. Namespace isolation: any `output_keys` must begin with `tasks.<task_id>.`.\n"+
		"4. Ensure each task has non-empty `id` and `action`.",
		err, snippet)

	return prompt.ChatMessage{
		Role:    "user",
		Content: content,
	}
}
