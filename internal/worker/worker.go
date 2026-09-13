package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/prompt"
)

// DefaultMaxToolTurns is the hard limit of tool execution turns per ephemeral worker.
const DefaultMaxToolTurns = 3

// Worker executes a single dag.Task in a bounded, ephemeral context loop.
type Worker struct {
	Client        *Client
	Executor      *Executor
	Blackboard    *blackboard.Blackboard
	CompiledRules string
	Manifest      string
	MaxToolTurns  int
	Tools         []Tool
}

// WorkerOption configures a Worker instance.
type WorkerOption func(*Worker)

// WithMaxToolTurns overrides the default 3-turn tool execution budget.
func WithMaxToolTurns(maxTurns int) WorkerOption {
	return func(w *Worker) {
		if maxTurns > 0 {
			w.MaxToolTurns = maxTurns
		}
	}
}

// WithCustomTools configures custom tools for the worker.
func WithCustomTools(tools []Tool) WorkerOption {
	return func(w *Worker) {
		w.Tools = tools
	}
}

// NewWorker creates an ephemeral worker instance.
func NewWorker(
	client *Client,
	executor *Executor,
	bb *blackboard.Blackboard,
	compiledRules string,
	manifest string,
	opts ...WorkerOption,
) *Worker {
	w := &Worker{
		Client:        client,
		Executor:      executor,
		Blackboard:    bb,
		CompiledRules: compiledRules,
		Manifest:      manifest,
		MaxToolTurns:  DefaultMaxToolTurns,
		Tools:         StandardTools(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// AsTaskHandler adapts the Worker to the dag.TaskHandler signature.
func (w *Worker) AsTaskHandler() dag.TaskHandler {
	return func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
		return w.Execute(ctx, task)
	}
}

// Execute runs the ephemeral worker lifecycle for a single dag.Task.
// Guarantees:
// 1. Hard budget: maximum 3 tool execution turns.
// 2. Level 2 reflection: up to 2 attempts on invalid JSON / schema syntax.
// 3. Complete context destruction upon function exit (zero history leakage).
// 4. Returns (result json.RawMessage, yielded []dag.Task, err error).
func (w *Worker) Execute(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
	if task == nil {
		return nil, nil, errors.New("cannot execute nil task")
	}

	// 1. Synthesize prompt messages following Enriched Prefix Stability Pattern
	messages := prompt.FormatMessages(*task, w.Blackboard, w.CompiledRules, w.Manifest)

	// Context destruction guarantee: ephemeral conversation context is completely discarded on return
	defer func() {
		messages = nil
	}()

	maxToolTurns := w.MaxToolTurns
	if maxToolTurns <= 0 {
		maxToolTurns = DefaultMaxToolTurns
	}

	toolTurnCount := 0
	reflectionAttempts := 0

	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		// Prepare LLM request
		req := ChatCompletionRequest{
			Messages: messages,
			Tools:    w.Tools,
		}

		resp, err := w.Client.CreateChatCompletion(ctx, req)
		if err != nil {
			return nil, nil, fmt.Errorf("worker LLM request failed: %w", err)
		}

		if len(resp.Choices) == 0 {
			return nil, nil, errors.New("empty choices from LLM completion")
		}

		choice := resp.Choices[0]
		assistantMsg := choice.Message

		// Case A: Model requested tool calls
		if len(assistantMsg.ToolCalls) > 0 {
			toolTurnCount++
			if toolTurnCount > maxToolTurns {
				// Hard budget exceeded
				return nil, nil, fmt.Errorf("tool execution budget exhausted (%d turns)", maxToolTurns)
			}

			// Append assistant turn to ephemeral context
			messages = append(messages, assistantMsg)

			// Execute each tool call
			for _, tc := range assistantMsg.ToolCalls {
				output, execErr := w.Executor.ExecuteToolCall(ctx, tc)
				if execErr != nil {
					output = fmt.Sprintf("Error executing tool %s: %v", tc.Function.Name, execErr)
				}

				toolMsg := prompt.ChatMessage{
					Role:       "tool",
					Content:    output,
					Name:       tc.Function.Name,
					ToolCallID: tc.ID,
				}
				messages = append(messages, toolMsg)
			}

			// If budget reached after this execution turn, demand final response without tools
			if toolTurnCount >= maxToolTurns {
				finalPrompt := prompt.ChatMessage{
					Role: "user",
					Content: fmt.Sprintf("Your tool execution budget of %d turns is exhausted. "+
						"Do not call any more tools. Summarize your findings and return your final output as a valid JSON object.", maxToolTurns),
				}
				messages = append(messages, finalPrompt)

				finalReq := ChatCompletionRequest{
					Messages: messages,
					Tools:    nil, // Strip tools to prevent further calls
				}

				finalResp, finalErr := w.Client.CreateChatCompletion(ctx, finalReq)
				if finalErr != nil {
					return nil, nil, fmt.Errorf("failed to obtain final summary after budget exhaustion: %w", finalErr)
				}
				if len(finalResp.Choices) == 0 {
					return nil, nil, errors.New("empty choices in final summary after budget exhaustion")
				}

				rawFinal := finalResp.Choices[0].Message.Content
				result, yielded, pErr := ParseWorkerResponse(rawFinal)
				if pErr != nil {
					// Level 2 reflection on final summary
					if reflectionAttempts < MaxReflectionAttempts {
						reflectionAttempts++
						messages = append(messages, finalResp.Choices[0].Message)
						messages = append(messages, BuildReflectionPrompt(pErr, rawFinal))

						recoveryResp, recErr := w.Client.CreateChatCompletion(ctx, ChatCompletionRequest{Messages: messages})
						if recErr == nil && len(recoveryResp.Choices) > 0 {
							recResult, recYielded, recParseErr := ParseWorkerResponse(recoveryResp.Choices[0].Message.Content)
							if recParseErr == nil {
								w.persistOutputs(task, recResult)
								return recResult, recYielded, nil
							}
						}
					}
					return nil, nil, fmt.Errorf("%w: %v", ErrReflectionExhausted, pErr)
				}

				w.persistOutputs(task, result)
				return result, yielded, nil
			}

			continue
		}

		// Case B: Model returned direct response (no tool calls)
		rawContent := assistantMsg.Content
		result, yielded, parseErr := ParseWorkerResponse(rawContent)
		if parseErr != nil {
			// Level 2 Resilience: Reflection on malformed syntax / schema error
			if reflectionAttempts < MaxReflectionAttempts {
				reflectionAttempts++
				messages = append(messages, assistantMsg)
				messages = append(messages, BuildReflectionPrompt(parseErr, rawContent))
				continue
			}
			return nil, nil, fmt.Errorf("%w: %v", ErrReflectionExhausted, parseErr)
		}

		// Successfully produced output
		w.persistOutputs(task, result)
		return result, yielded, nil
	}
}

// persistOutputs writes the result to any declared output keys in the Blackboard.
func (w *Worker) persistOutputs(task *dag.Task, result json.RawMessage) {
	if w.Blackboard == nil || len(task.OutputKeys) == 0 || len(result) == 0 {
		return
	}
	for _, outKey := range task.OutputKeys {
		_, _ = w.Blackboard.Set(task.ID, outKey, result)
	}
}
