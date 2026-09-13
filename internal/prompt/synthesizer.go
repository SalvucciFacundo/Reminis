package prompt

import (
	"fmt"
	"strings"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/rules"
)

// ChatMessage represents a single message in an OpenAI-compatible chat sequence.
type ChatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall represents a structured tool call requested by the model.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function FunctionCallData `json:"function"`
}

// FunctionCallData contains the tool name and serialized JSON arguments.
type FunctionCallData struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// BlackboardReader abstracts reading from working memory.
type BlackboardReader interface {
	Get(key string) (blackboard.MemoryEntry, error)
}

// BuildStaticPrefix constructs the invariant, cacheable system prefix for all workers in a run.
// It combines the compiled universal rules, global project manifest, standard tool schemas,
// and few-shot execution demonstrations.
func BuildStaticPrefix(compiledRules string, manifest string) string {
	var sb strings.Builder

	sb.WriteString("# Role: Ephemeral Reminis Worker Goroutine\n")
	sb.WriteString("You are a stateless, bounded execution worker within a Reminis workflow graph.\n")
	sb.WriteString("Execute the directed action precisely, utilizing available tools within your bounded budget.\n\n")

	// 1. Compiled universal rules (from discovery cascade & compiler)
	if strings.TrimSpace(compiledRules) != "" {
		sb.WriteString("# Universal Project & Operational Rules\n")
		sb.WriteString(strings.TrimSpace(compiledRules))
		sb.WriteString("\n\n")
	} else {
		sb.WriteString(rules.DefaultBaseline)
		sb.WriteString("\n\n")
	}

	// 2. Project manifest (languages, frameworks, repo structure)
	if strings.TrimSpace(manifest) != "" {
		sb.WriteString("# Project Manifest & Repository Topology\n")
		sb.WriteString(strings.TrimSpace(manifest))
		sb.WriteString("\n\n")
	}

	// 3. Tool schemas and constraints
	sb.WriteString(strings.TrimSpace(rules.StandardToolSchemas))
	sb.WriteString("\n\n")

	// 4. Few-shot execution demonstrations
	sb.WriteString(strings.TrimSpace(rules.StandardFewShotExamples))
	sb.WriteString("\n\n")

	// 5. Output contract
	sb.WriteString("# Output Protocol\n")
	sb.WriteString("When your task is complete or when yielding subtasks, output a valid JSON object matching the result contract:\n")
	sb.WriteString("`{\"status\": \"COMPLETED\"|\"YIELD\", \"result\": {...}, \"yielded_tasks\": [...]}`\n")

	return sb.String()
}

// BuildDynamicTail constructs the append-only, non-cached user prompt for a specific task.
// It resolves all task.InputKeys against the Blackboard and formats the task action directive.
func BuildDynamicTail(task dag.Task, bb BlackboardReader) string {
	var sb strings.Builder

	sb.WriteString("# Dynamic Task Context\n\n")

	// 1. Blackboard state injection (resolving Task.InputKeys)
	sb.WriteString("## Working Memory Inputs (Blackboard)\n")
	if len(task.InputKeys) == 0 {
		sb.WriteString("No upstream input keys declared for this task.\n\n")
	} else {
		for _, key := range task.InputKeys {
			if bb == nil {
				sb.WriteString(fmt.Sprintf("- Key `%s`: [Blackboard not attached]\n", key))
				continue
			}

			entry, err := bb.Get(key)
			if err != nil {
				sb.WriteString(fmt.Sprintf("- Key `%s`: [Not found in working memory: %v]\n", key, err))
				continue
			}

			payload, err := blackboard.ReadPayload(entry)
			if err != nil {
				sb.WriteString(fmt.Sprintf("- Key `%s`: [Error reading payload: %v]\n", key, err))
				continue
			}

			if entry.IsSpilled {
				sb.WriteString(fmt.Sprintf("- Key `%s` (Spilled to disk: `%s`, %d bytes):\n```\n%s\n```\n",
					key, entry.SpillPath, len(payload), string(payload)))
			} else {
				sb.WriteString(fmt.Sprintf("- Key `%s` (%d bytes):\n```json\n%s\n```\n",
					key, len(payload), string(payload)))
			}
		}
		sb.WriteString("\n")
	}

	// 2. Current Task action directive
	sb.WriteString("## Task Action Directive\n")
	sb.WriteString(fmt.Sprintf("- **Task ID**: `%s`\n", task.ID))
	sb.WriteString(fmt.Sprintf("- **Action**: %s\n", task.Action))
	if len(task.TargetPaths) > 0 {
		sb.WriteString(fmt.Sprintf("- **Target Paths**: %s\n", strings.Join(task.TargetPaths, ", ")))
	}
	if len(task.OutputKeys) > 0 {
		sb.WriteString(fmt.Sprintf("- **Expected Output Keys**: %s\n", strings.Join(task.OutputKeys, ", ")))
	}
	if task.RequiresApproval {
		sb.WriteString("- **Approval Required**: True (Requires explicit human clearance for mutations)\n")
	}
	if task.TimeoutSeconds > 0 {
		sb.WriteString(fmt.Sprintf("- **Timeout Override**: %d seconds\n", task.TimeoutSeconds))
	}

	return sb.String()
}

// FormatMessages builds the complete []ChatMessage sequence following the
// Enriched Prefix Stability Pattern (SPEC.md Section 3.7):
// - Static Prefix (System message): Invariant and cacheable across all tasks.
// - Dynamic Tail (User message): Append-only task directive and Blackboard state.
func FormatMessages(task dag.Task, bb *blackboard.Blackboard, compiledRules string, manifest string) []ChatMessage {
	var reader BlackboardReader
	if bb != nil {
		reader = bb
	}
	return FormatMessagesWithReader(task, reader, compiledRules, manifest)
}

// FormatMessagesWithReader accepts any BlackboardReader implementation.
func FormatMessagesWithReader(task dag.Task, bb BlackboardReader, compiledRules string, manifest string) []ChatMessage {
	staticPrefix := BuildStaticPrefix(compiledRules, manifest)
	dynamicTail := BuildDynamicTail(task, bb)

	return []ChatMessage{
		{
			Role:    "system",
			Content: staticPrefix,
		},
		{
			Role:    "user",
			Content: dynamicTail,
		},
	}
}

// Synthesizer encapsulates prompt generation for a session.
type Synthesizer struct {
	CompiledPrefix *rules.CompiledPrefix
	Manifest       string
}

// NewSynthesizer creates a Synthesizer with precompiled rules.
func NewSynthesizer(compiledPrefix *rules.CompiledPrefix, manifest string) *Synthesizer {
	if compiledPrefix == nil {
		compiledPrefix = rules.Compile()
	}
	return &Synthesizer{
		CompiledPrefix: compiledPrefix,
		Manifest:       manifest,
	}
}

// FormatMessages formats messages using the precompiled session prefix.
func (s *Synthesizer) FormatMessages(task dag.Task, bb *blackboard.Blackboard) []ChatMessage {
	var content string
	if s.CompiledPrefix != nil {
		content = s.CompiledPrefix.Content
	}
	return FormatMessages(task, bb, content, s.Manifest)
}
