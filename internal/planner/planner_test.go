package planner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fds1288/reminis/internal/prompt"
	"github.com/fds1288/reminis/internal/worker"
)

func TestPlannerPromptConstruction(t *testing.T) {
	goal := "Refactor payment service and add unit tests"
	manifest := "Languages: Go 1.23\nModules: payments, billing"
	rules := "Follow clean architecture and bounded worker budget"

	promptText := BuildPlannerPrompt(goal, manifest, rules)

	expectedKeywords := []string{
		"id",
		"action",
		"depends_on",
		"input_keys",
		"output_keys",
		"requires_approval",
		"target_paths",
		"timeout_seconds",
		"tasks.<id>.*",
		"Strict DAG Acyclicity",
		"Concurrency & Parallelism",
		"Refactor payment service and add unit tests",
		"Languages: Go 1.23",
		"Follow clean architecture",
	}

	for _, kw := range expectedKeywords {
		if !strings.Contains(promptText, kw) {
			t.Errorf("expected prompt to contain keyword %q, but was missing", kw)
		}
	}

	messages := BuildPlannerMessages(goal, manifest, rules)
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].Role != "user" {
		t.Errorf("expected role 'user', got %s", messages[0].Role)
	}
}

func TestPlannerSuccessfulDAGParsing(t *testing.T) {
	validDAGJSON := `[
		{
			"id": "inspect_code",
			"action": "Inspect payment handler and find outdated patterns",
			"depends_on": [],
			"input_keys": [],
			"output_keys": ["tasks.inspect_code.findings"],
			"requires_approval": false,
			"target_paths": ["pkg/payment/handler.go"],
			"timeout_seconds": 30
		},
		{
			"id": "refactor_handler",
			"action": "Refactor payment handler to use interface ports",
			"depends_on": ["inspect_code"],
			"input_keys": ["tasks.inspect_code.findings"],
			"output_keys": ["tasks.refactor_handler.diff"],
			"requires_approval": false,
			"target_paths": ["pkg/payment/handler.go"],
			"timeout_seconds": 60
		},
		{
			"id": "add_tests",
			"action": "Add unit tests for payment handler",
			"depends_on": ["refactor_handler"],
			"input_keys": ["tasks.refactor_handler.diff"],
			"output_keys": ["tasks.add_tests.output"],
			"requires_approval": false,
			"target_paths": ["pkg/payment/handler_test.go"],
			"timeout_seconds": 45
		}
	]`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := worker.ChatCompletionResponse{
			ID: "test-plan-1",
			Choices: []worker.Choice{
				{
					Message: prompt.ChatMessage{
						Role:    "assistant",
						Content: "```json\n" + validDAGJSON + "\n```",
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := worker.NewClient(server.URL, "dummy-key", "test-model")
	p := New(client, "test-model")

	tasks, err := p.Plan(context.Background(), "Refactor payment", "", "")
	if err != nil {
		t.Fatalf("unexpected error planning DAG: %v", err)
	}

	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
	if tasks[0].ID != "inspect_code" || tasks[1].ID != "refactor_handler" || tasks[2].ID != "add_tests" {
		t.Errorf("unexpected task order or IDs: %+v", tasks)
	}
	if len(tasks[1].DependsOn) != 1 || tasks[1].DependsOn[0] != "inspect_code" {
		t.Errorf("expected refactor_handler to depend on inspect_code: %+v", tasks[1].DependsOn)
	}
}

func TestPlannerCycleRejectionAndReflectionRecovery(t *testing.T) {
	// First response contains a cycle between task_a and task_b
	cyclicJSON := `[
		{
			"id": "task_a",
			"action": "Action A",
			"depends_on": ["task_b"],
			"output_keys": ["tasks.task_a.output"]
		},
		{
			"id": "task_b",
			"action": "Action B",
			"depends_on": ["task_a"],
			"output_keys": ["tasks.task_b.output"]
		}
	]`

	// Corrected response breaks the cycle
	acyclicJSON := `[
		{
			"id": "task_a",
			"action": "Action A",
			"depends_on": [],
			"output_keys": ["tasks.task_a.output"]
		},
		{
			"id": "task_b",
			"action": "Action B",
			"depends_on": ["task_a"],
			"output_keys": ["tasks.task_b.output"]
		}
	]`

	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)

		var content string
		if count == 1 {
			content = cyclicJSON
		} else {
			// Second request should be the Level 2 reflection request
			var reqPayload worker.ChatCompletionRequest
			_ = json.NewDecoder(r.Body).Decode(&reqPayload)

			// Verify reflection message exists
			hasReflection := false
			for _, m := range reqPayload.Messages {
				if strings.Contains(m.Content, "DAG Plan Validation Error") &&
					strings.Contains(m.Content, "acyclicity") {
					hasReflection = true
					break
				}
			}
			if !hasReflection {
				t.Errorf("expected reflection prompt in second request, but was absent")
			}

			content = acyclicJSON
		}

		resp := worker.ChatCompletionResponse{
			ID: "plan-cycle-test",
			Choices: []worker.Choice{
				{
					Message: worker.Choice{}.Message,
				},
			},
		}
		resp.Choices[0].Message.Role = "assistant"
		resp.Choices[0].Message.Content = content

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := worker.NewClient(server.URL, "dummy-key", "test-model")
	p := New(client, "test-model")

	tasks, err := p.Plan(context.Background(), "Break the cycle", "", "")
	if err != nil {
		t.Fatalf("expected plan to recover via reflection, got error: %v", err)
	}

	if atomic.LoadInt32(&requestCount) != 2 {
		t.Errorf("expected exactly 2 requests (1 initial + 1 reflection), got %d", requestCount)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks in recovered DAG, got %d", len(tasks))
	}
	if tasks[0].ID != "task_a" || tasks[1].ID != "task_b" {
		t.Errorf("unexpected task IDs: %+v", tasks)
	}
	if len(tasks[0].DependsOn) != 0 || len(tasks[1].DependsOn) != 1 {
		t.Errorf("expected task_a to have no deps, and task_b to depend on task_a")
	}
}

func TestPlannerInvalidJSONAndReflectionRecovery(t *testing.T) {
	brokenJSON := `[ {"id": "task_1", "action": "Incomplete json...`
	fixedJSON := `[
		{
			"id": "task_1",
			"action": "Fixed JSON action",
			"depends_on": [],
			"output_keys": ["tasks.task_1.output"]
		}
	]`

	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)

		var content string
		if count == 1 {
			content = brokenJSON
		} else {
			content = fixedJSON
		}

		resp := worker.ChatCompletionResponse{
			ID: "plan-json-test",
			Choices: []worker.Choice{
				{
					Message: worker.Choice{}.Message,
				},
			},
		}
		resp.Choices[0].Message.Role = "assistant"
		resp.Choices[0].Message.Content = content

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := worker.NewClient(server.URL, "dummy-key", "test-model")
	p := New(client, "test-model")

	tasks, err := p.Plan(context.Background(), "Fix broken JSON", "", "")
	if err != nil {
		t.Fatalf("expected plan to recover via reflection, got error: %v", err)
	}

	if atomic.LoadInt32(&requestCount) != 2 {
		t.Errorf("expected 2 requests, got %d", requestCount)
	}
	if len(tasks) != 1 || tasks[0].ID != "task_1" {
		t.Errorf("unexpected tasks: %+v", tasks)
	}
}

func TestPlannerReflectionExhausted(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		resp := worker.ChatCompletionResponse{
			Choices: []worker.Choice{
				{
					Message: worker.Choice{}.Message,
				},
			},
		}
		resp.Choices[0].Message.Role = "assistant"
		resp.Choices[0].Message.Content = "not valid json at all"

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := worker.NewClient(server.URL, "dummy-key", "test-model")
	p := New(client, "test-model")

	_, err := p.Plan(context.Background(), "Unrecoverable goal", "", "")
	if err == nil {
		t.Fatalf("expected error due to exhausted reflections, got nil")
	}

	// Initial (1) + MaxReflectionAttempts (2) = 3 total requests
	if atomic.LoadInt32(&requestCount) != 3 {
		t.Errorf("expected 3 requests (1 initial + 2 reflections), got %d", requestCount)
	}
}
