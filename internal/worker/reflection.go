package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/prompt"
)

// MaxReflectionAttempts defines the hard budget for Level 2 reflection retries.
const MaxReflectionAttempts = 2

var (
	// ErrReflectionExhausted is returned when Level 2 reflection fails after MaxReflectionAttempts.
	ErrReflectionExhausted = errors.New("level 2 reflection exhausted: output remains invalid")
)

// WorkerOutput captures the structured output contract produced by an LLM worker.
type WorkerOutput struct {
	Status       string          `json:"status,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	YieldedTasks []dag.Task      `json:"yielded_tasks,omitempty"`
	Error        string          `json:"error,omitempty"`
}

// ExtractJSON attempts to clean markdown code fences and parse JSON.
func ExtractJSON(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("empty response content")
	}

	// Strip markdown code fences if present
	if strings.HasPrefix(trimmed, "```") {
		lines := strings.Split(trimmed, "\n")
		if len(lines) >= 2 {
			// Remove first line (``` or ```json)
			lines = lines[1:]
			// Remove last line if closing ```
			if len(lines) > 0 && strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
				lines = lines[:len(lines)-1]
			}
			trimmed = strings.TrimSpace(strings.Join(lines, "\n"))
		}
	}

	if !json.Valid([]byte(trimmed)) {
		return nil, errors.New("invalid JSON syntax: content is not well-formed JSON")
	}

	return []byte(trimmed), nil
}

// ParseWorkerResponse parses the completion text into a result payload and any yielded subtasks.
func ParseWorkerResponse(raw string) (json.RawMessage, []dag.Task, error) {
	jsonBytes, err := ExtractJSON(raw)
	if err != nil {
		return nil, nil, err
	}

	// Attempt structured WorkerOutput unmarshal
	var out WorkerOutput
	if err := json.Unmarshal(jsonBytes, &out); err == nil {
		// If explicit result payload was provided
		if len(out.Result) > 0 {
			return out.Result, out.YieldedTasks, nil
		}
		// If yielded tasks were declared without a separate result payload
		if len(out.YieldedTasks) > 0 {
			return jsonBytes, out.YieldedTasks, nil
		}
	}

	// If it's valid arbitrary JSON, return it directly as the result
	return json.RawMessage(jsonBytes), nil, nil
}

// BuildReflectionPrompt generates a Level 2 reflection prompt targeting a syntax or schema validation error.
func BuildReflectionPrompt(parseErr error, rawOutput string) prompt.ChatMessage {
	snippet := strings.TrimSpace(rawOutput)
	if len(snippet) > 500 {
		snippet = snippet[:500] + "... [truncated]"
	}

	content := fmt.Sprintf("# Level 2 Resilience: Syntax / Schema Validation Error\n\n"+
		"Your previous response could not be parsed or validated:\n"+
		"**Exact Error**: %v\n\n"+
		"**Provided Output**:\n```\n%s\n```\n\n"+
		"Please correct the syntax and return a valid JSON object matching the required schema:\n"+
		"`{\"status\": \"COMPLETED\"|\"YIELD\", \"result\": {...}, \"yielded_tasks\": [...]}`",
		parseErr, snippet)

	return prompt.ChatMessage{
		Role:    "user",
		Content: content,
	}
}

// BuildToolErrorReflectionPrompt generates a Level 2 reflection prompt when tool invocation parameters fail validation.
func BuildToolErrorReflectionPrompt(toolName string, toolErr error, rawArgs string) prompt.ChatMessage {
	snippet := strings.TrimSpace(rawArgs)
	if len(snippet) > 500 {
		snippet = snippet[:500] + "... [truncated]"
	}

	content := fmt.Sprintf("# Level 2 Resilience: Tool Invocation Error\n\n"+
		"The call to tool `%s` failed with an error:\n"+
		"**Error**: %v\n\n"+
		"**Provided Arguments**:\n```\n%s\n```\n\n"+
		"Please reissue the tool call with corrected arguments, or produce your final output.",
		toolName, toolErr, snippet)

	return prompt.ChatMessage{
		Role:    "user",
		Content: content,
	}
}
