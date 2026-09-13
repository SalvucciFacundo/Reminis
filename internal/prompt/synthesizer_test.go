package prompt

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/rules"
)

func TestEnrichedPrefixStability(t *testing.T) {
	tempDir := t.TempDir()
	bb := blackboard.New(tempDir)

	// Populate blackboard with an inline entry and a spilled entry (>4096 bytes)
	inlineData := []byte(`{"status": "ok", "service": "auth"}`)
	if _, err := bb.Set("task-1", "tasks.task-1.output", inlineData); err != nil {
		t.Fatalf("failed to set inline entry: %v", err)
	}

	largeData := []byte(strings.Repeat("a", 5000))
	if _, err := bb.Set("task-2", "tasks.task-2.large_output", largeData); err != nil {
		t.Fatalf("failed to set large entry: %v", err)
	}

	compiledRules := rules.Compile("Rule: Strict typed interfaces.")
	manifest := "Language: Go 1.23+\nFramework: Standard Library + Pure Go SQLite"

	taskA := dag.Task{
		ID:          "task-a",
		Action:      "inspect_auth",
		InputKeys:   []string{"tasks.task-1.output"},
		OutputKeys:  []string{"tasks.task-a.result"},
		TargetPaths: []string{"internal/auth/auth.go"},
	}

	taskB := dag.Task{
		ID:             "task-b",
		Action:         "analyze_large_log",
		InputKeys:      []string{"tasks.task-2.large_output", "tasks.missing.key"},
		OutputKeys:     []string{"tasks.task-b.summary"},
		TimeoutSeconds: 60,
	}

	messagesA := FormatMessages(taskA, bb, compiledRules.Content, manifest)
	messagesB := FormatMessages(taskB, bb, compiledRules.Content, manifest)

	// 1. Static Prefix must be in the system message
	if len(messagesA) != 2 || len(messagesB) != 2 {
		t.Fatalf("expected 2 messages (system + user), got %d and %d", len(messagesA), len(messagesB))
	}
	if messagesA[0].Role != "system" || messagesB[0].Role != "system" {
		t.Fatalf("expected first message to be role 'system'")
	}
	if messagesA[1].Role != "user" || messagesB[1].Role != "user" {
		t.Fatalf("expected second message to be role 'user'")
	}

	// 2. CRITICAL: Static Prefix must be 100% byte-for-byte identical across distinct tasks
	if messagesA[0].Content != messagesB[0].Content {
		t.Fatalf("expected Static Prefix to be identical across tasks!\nTask A Prefix:\n%s\n\nTask B Prefix:\n%s",
			messagesA[0].Content, messagesB[0].Content)
	}

	// 3. Dynamic Tail must differ and reflect task-specific directives and blackboard inputs
	if messagesA[1].Content == messagesB[1].Content {
		t.Fatalf("expected Dynamic Tail to vary between tasks!")
	}

	// Verify Task A dynamic tail contains auth output and target path
	tailA := messagesA[1].Content
	if !strings.Contains(tailA, "tasks.task-1.output") || !strings.Contains(tailA, "internal/auth/auth.go") {
		t.Fatalf("task A dynamic tail missing expected inputs or target paths:\n%s", tailA)
	}

	// Verify Task B dynamic tail contains spilled note and missing key note
	tailB := messagesB[1].Content
	if !strings.Contains(tailB, "Spilled to disk") || !strings.Contains(tailB, "Not found in working memory") {
		t.Fatalf("task B dynamic tail missing spilled notation or missing key note:\n%s", tailB)
	}
	if !strings.Contains(tailB, "Timeout Override") || !strings.Contains(tailB, "60 seconds") {
		t.Fatalf("task B dynamic tail missing timeout override notation")
	}

	// 4. Token length guarantee: Static Prefix must meet the >1024 token requirement
	tokenEst := rules.EstimateTokens(messagesA[0].Content)
	if tokenEst < rules.MinPromptCacheTokens {
		t.Fatalf("expected Static Prefix token count >= %d, got %d", rules.MinPromptCacheTokens, tokenEst)
	}
}

func TestSynthesizerStruct(t *testing.T) {
	compiled := rules.Compile("Custom rule")
	synth := NewSynthesizer(compiled, "Manifest info")

	task := dag.Task{
		ID:     "task-x",
		Action: "do_something",
		Result: json.RawMessage(`{}`),
	}

	msgs := synth.FormatMessages(task, nil)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if !strings.Contains(msgs[0].Content, "Custom rule") {
		t.Fatalf("expected compiled rules in static prefix")
	}
	if !strings.Contains(msgs[0].Content, "Manifest info") {
		t.Fatalf("expected manifest info in static prefix")
	}
	if !strings.Contains(msgs[1].Content, "task-x") {
		t.Fatalf("expected task id in dynamic tail")
	}
}
