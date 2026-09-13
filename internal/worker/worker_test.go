package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/prompt"
)

func TestLevel1HTTPRetryOn429And500(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := requestCount.Add(1)
		switch count {
		case 1:
			// First attempt: 429 Too Many Requests
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limit exceeded"}`))
		case 2:
			// Second attempt: 500 Internal Server Error
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": "internal error"}`))
		case 3:
			// Third attempt: 200 OK
			w.WriteHeader(http.StatusOK)
			resp := ChatCompletionResponse{
				ID: "resp-retry-ok",
				Choices: []Choice{
					{
						Index: 0,
						Message: prompt.ChatMessage{
							Role:    "assistant",
							Content: `{"status": "COMPLETED", "result": {"success": true}}`,
						},
						FinishReason: "stop",
					},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	client := NewClient(
		server.URL,
		"test-key",
		"test-model",
		WithRetries(3, 10*time.Millisecond, 50*time.Millisecond),
	)

	req := ChatCompletionRequest{
		Messages: []prompt.ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}

	ctx := context.Background()
	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		t.Fatalf("expected successful completion after retries, got err: %v", err)
	}

	if requestCount.Load() != 3 {
		t.Fatalf("expected exactly 3 requests (2 retries), got %d", requestCount.Load())
	}

	if len(resp.Choices) == 0 {
		t.Fatalf("expected at least 1 choice in response")
	}

	// Test retry exhaustion
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error": "bad gateway"}`))
	}))
	defer failServer.Close()

	exhaustClient := NewClient(
		failServer.URL,
		"test-key",
		"test-model",
		WithRetries(2, 5*time.Millisecond, 20*time.Millisecond),
	)

	_, err = exhaustClient.CreateChatCompletion(ctx, req)
	if err == nil {
		t.Fatalf("expected error on retry exhaustion, got nil")
	}
	if !errors.Is(err, ErrMaxRetriesExceeded) {
		t.Fatalf("expected ErrMaxRetriesExceeded, got %v", err)
	}
}

func TestToolExecutionAndSilenceWatchdog(t *testing.T) {
	tempDir := t.TempDir()
	executor := NewExecutor(tempDir, 150*time.Millisecond)

	// 1. Test write_file and read_file
	writeRes, err := executor.WriteFile("test.txt", "hello reminiscence")
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	if !strings.Contains(writeRes, "File written successfully") {
		t.Fatalf("unexpected write file response: %s", writeRes)
	}

	readRes, err := executor.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if readRes != "hello reminiscence" {
		t.Fatalf("expected 'hello reminiscence', got %q", readRes)
	}

	// 2. Test bash fast execution
	ctx := context.Background()
	bashRes, err := executor.RunBash(ctx, "echo -n 'bash works'")
	if err != nil {
		t.Fatalf("bash execution failed: %v", err)
	}
	if bashRes != "bash works" {
		t.Fatalf("expected 'bash works', got %q", bashRes)
	}

	// 3. Test Silence Watchdog killing a deadlocked process
	start := time.Now()
	// Command that hangs with zero output
	_, err = executor.RunBash(ctx, "sleep 10")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected silence timeout error, got nil")
	}
	if !errors.Is(err, ErrSilenceTimeout) {
		t.Fatalf("expected ErrSilenceTimeout, got %v", err)
	}
	// Verify it was killed promptly around ~150ms instead of 10s
	if duration > 1*time.Second {
		t.Fatalf("silence watchdog took too long to kill hanging process: %v", duration)
	}

	// 4. Test periodic output resetting the silence timer
	// Emits output every 40ms, total duration ~200ms (> 150ms silence timeout)
	periodicCmd := `for i in 1 2 3 4; do echo "tick $i"; sleep 0.04; done`
	periodicRes, err := executor.RunBash(ctx, periodicCmd)
	if err != nil {
		t.Fatalf("expected periodic command to succeed, got error: %v", err)
	}
	if !strings.Contains(periodicRes, "tick 4") {
		t.Fatalf("expected periodic output to contain 'tick 4', got: %s", periodicRes)
	}
}

func TestLevel2ReflectionRecoveringFromInvalidJSON(t *testing.T) {
	var callCount atomic.Int32
	var receivedReflection atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		count := callCount.Add(1)
		w.WriteHeader(http.StatusOK)

		if count == 1 {
			// First turn: Return malformed/invalid JSON syntax
			_ = json.NewEncoder(w).Encode(ChatCompletionResponse{
				Choices: []Choice{
					{
						Index: 0,
						Message: prompt.ChatMessage{
							Role:    "assistant",
							Content: `{"status": COMPLETED, invalid json syntax here`,
						},
					},
				},
			})
			return
		}

		// Second turn: Check if prompt contains Level 2 reflection
		for _, m := range req.Messages {
			if strings.Contains(m.Content, "Level 2 Resilience") {
				receivedReflection.Store(true)
			}
		}

		// Return valid recovered JSON
		_ = json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{
				{
					Index: 0,
					Message: prompt.ChatMessage{
						Role:    "assistant",
						Content: `{"status": "COMPLETED", "result": {"recovered": true, "syntax_fixed": true}}`,
					},
				},
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "key", "model")
	executor := NewExecutor(t.TempDir(), DefaultSilenceDuration)
	bb := blackboard.New(t.TempDir())

	worker := NewWorker(client, executor, bb, "Baseline rules", "Manifest")

	task := &dag.Task{
		ID:         "task-reflect",
		Action:     "validate_syntax",
		OutputKeys: []string{"tasks.task-reflect.output"},
	}

	ctx := context.Background()
	result, yielded, err := worker.Execute(ctx, task)
	if err != nil {
		t.Fatalf("expected worker to recover via reflection, got error: %v", err)
	}

	if !receivedReflection.Load() {
		t.Fatalf("expected model to receive Level 2 reflection prompt")
	}

	if len(yielded) != 0 {
		t.Fatalf("expected 0 yielded tasks, got %d", len(yielded))
	}

	var parsed map[string]any
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if parsed["recovered"] != true {
		t.Fatalf("expected recovered: true in result, got %v", parsed)
	}

	// Verify Blackboard state was updated
	entry, err := bb.Get("tasks.task-reflect.output")
	if err != nil {
		t.Fatalf("expected output key in blackboard: %v", err)
	}
	if !strings.Contains(string(entry.Data), `"recovered"`) || !strings.Contains(string(entry.Data), `true`) {
		t.Fatalf("unexpected blackboard entry content: %s", string(entry.Data))
	}
}

func TestBoundedTurnBudgetEnforcementAndContextDestruction(t *testing.T) {
	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "data.txt")
	_ = os.WriteFile(testFilePath, []byte("line of data"), 0644)

	var turnCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		count := turnCount.Add(1)
		w.WriteHeader(http.StatusOK)

		if count <= 3 {
			// Turns 1, 2, 3: request tool execution
			tc := prompt.ToolCall{
				ID:   fmt.Sprintf("call_%d", count),
				Type: "function",
				Function: prompt.FunctionCallData{
					Name:      "read_file",
					Arguments: fmt.Sprintf(`{"path": "%s"}`, testFilePath),
				},
			}
			_ = json.NewEncoder(w).Encode(ChatCompletionResponse{
				Choices: []Choice{
					{
						Index: 0,
						Message: prompt.ChatMessage{
							Role:      "assistant",
							ToolCalls: []prompt.ToolCall{tc},
						},
					},
				},
			})
			return
		}

		// Turn 4: Final summary call after budget reached (tool execution budget exhausted)
		// Model must return final JSON without tools
		_ = json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{
				{
					Index: 0,
					Message: prompt.ChatMessage{
						Role:    "assistant",
						Content: `{"status": "COMPLETED", "result": {"total_turns_used": 3, "status": "bounded_success"}}`,
					},
				},
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "key", "model")
	executor := NewExecutor(tempDir, DefaultSilenceDuration)
	bb := blackboard.New(tempDir)

	worker := NewWorker(client, executor, bb, "Rules", "Manifest", WithMaxToolTurns(3))

	task := &dag.Task{
		ID:     "task-bounded",
		Action: "test_bounds",
	}

	ctx := context.Background()
	result, _, err := worker.Execute(ctx, task)
	if err != nil {
		t.Fatalf("worker failed: %v", err)
	}

	// Verify exactly 3 tool turns + 1 final summary turn = 4 LLM calls
	if turnCount.Load() != 4 {
		t.Fatalf("expected exactly 4 LLM calls (3 tool turns + 1 summary), got %d", turnCount.Load())
	}

	var resMap map[string]any
	if err := json.Unmarshal(result, &resMap); err != nil {
		t.Fatalf("failed to parse result JSON: %v", err)
	}
	if resMap["status"] != "bounded_success" {
		t.Fatalf("expected status 'bounded_success', got %v", resMap["status"])
	}
}
