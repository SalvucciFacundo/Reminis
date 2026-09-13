package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/prompt"
	"github.com/fds1288/reminis/internal/store"
	"github.com/fds1288/reminis/internal/worker"
)

// mockPlanner simulates planning without an external HTTP server.
type mockPlanner struct {
	tasks []dag.Task
	err   error
}

func (m *mockPlanner) Plan(ctx context.Context, goal, manifest, rulesContent string) ([]dag.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.tasks, nil
}

func TestEventEmitterBroadcast(t *testing.T) {
	emitter := NewEventEmitter()

	ch1, unsub1 := emitter.Subscribe(10)
	ch2, unsub2 := emitter.Subscribe(10)
	defer unsub2()

	event1 := Event{
		RunID: "run-1",
		Type:  EventRunStarted,
	}
	emitter.Publish(event1)

	select {
	case e := <-ch1:
		if e.RunID != "run-1" || e.Type != EventRunStarted {
			t.Errorf("ch1 received unexpected event: %+v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch1 timed out waiting for event")
	}

	select {
	case e := <-ch2:
		if e.RunID != "run-1" || e.Type != EventRunStarted {
			t.Errorf("ch2 received unexpected event: %+v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch2 timed out waiting for event")
	}

	// Test unsubscribe
	unsub1()

	event2 := Event{
		RunID: "run-1",
		Type:  EventRunFinished,
	}
	emitter.Publish(event2)

	select {
	case e := <-ch2:
		if e.Type != EventRunFinished {
			t.Errorf("ch2 expected EventRunFinished, got %s", e.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch2 timed out waiting for second event")
	}

	// ch1 should not receive event2
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("ch1 received event after unsubscribe")
		}
	default:
	}
}

func TestOrchestratorEndToEndParallelWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	dbStore, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize sqlite store: %v", err)
	}
	defer dbStore.Close()

	// Multi-step DAG with parallel branches:
	// task_prep -> (task_branch_a, task_branch_b) -> task_agg
	tasks := []dag.Task{
		{
			ID:         "task_prep",
			Action:     "Prepare common config",
			DependsOn:  []string{},
			OutputKeys: []string{"tasks.task_prep.config"},
		},
		{
			ID:         "task_branch_a",
			Action:     "Process branch A in parallel",
			DependsOn:  []string{"task_prep"},
			InputKeys:  []string{"tasks.task_prep.config"},
			OutputKeys: []string{"tasks.task_branch_a.output"},
		},
		{
			ID:         "task_branch_b",
			Action:     "Process branch B in parallel",
			DependsOn:  []string{"task_prep"},
			InputKeys:  []string{"tasks.task_prep.config"},
			OutputKeys: []string{"tasks.task_branch_b.output"},
		},
		{
			ID:         "task_agg",
			Action:     "Aggregate results from A and B",
			DependsOn:  []string{"task_branch_a", "task_branch_b"},
			InputKeys:  []string{"tasks.task_branch_a.output", "tasks.task_branch_b.output"},
			OutputKeys: []string{"tasks.task_agg.output"},
		},
	}

	var activeConcurrent int32
	var maxConcurrentObserved int32

	customHandler := func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
		current := atomic.AddInt32(&activeConcurrent, 1)
		for {
			max := atomic.LoadInt32(&maxConcurrentObserved)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrentObserved, max, current) {
				break
			}
		}

		// Small delay to simulate work and allow parallelism overlap
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&activeConcurrent, -1)

		payload := map[string]string{
			"task_id": task.ID,
			"status":  "ok",
		}
		data, _ := json.Marshal(payload)
		return data, nil, nil
	}

	cfg := Config{
		DBStore:        dbStore,
		ArtifactsDir:   filepath.Join(tempDir, "artifacts"),
		MaxConcurrency: 4,
		Planner:        &mockPlanner{tasks: tasks},
		TaskHandler:    customHandler,
	}

	orch := New(cfg)

	// Subscribe to event telemetry
	eventCh, unsub := orch.Events().Subscribe(100)
	defer unsub()

	result, err := orch.Run(context.Background(), "session-42", "Execute parallel pipeline")
	if err != nil {
		t.Fatalf("unexpected error running orchestrator: %v", err)
	}

	if result.Status != dag.RunStatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", result.Status)
	}
	if len(result.CompletedTasks) != 4 {
		t.Fatalf("expected 4 completed tasks, got %d: %+v", len(result.CompletedTasks), result.CompletedTasks)
	}

	// Verify parallel execution took place
	if maxConcurrentObserved < 2 {
		t.Errorf("expected at least 2 concurrent tasks observed, got %d", maxConcurrentObserved)
	}

	// Drain and verify event telemetry
	eventsByType := make(map[EventType]int)
	timeout := time.After(500 * time.Millisecond)
drainEvents:
	for {
		select {
		case e := <-eventCh:
			eventsByType[e.Type]++
		case <-timeout:
			break drainEvents
		}
	}

	if eventsByType[EventRunStarted] != 1 {
		t.Errorf("expected 1 EventRunStarted, got %d", eventsByType[EventRunStarted])
	}
	if eventsByType[EventTaskStarted] != 4 {
		t.Errorf("expected 4 EventTaskStarted, got %d", eventsByType[EventTaskStarted])
	}
	if eventsByType[EventTaskCompleted] != 4 {
		t.Errorf("expected 4 EventTaskCompleted, got %d", eventsByType[EventTaskCompleted])
	}
	if eventsByType[EventRunFinished] != 1 {
		t.Errorf("expected 1 EventRunFinished, got %d", eventsByType[EventRunFinished])
	}

	// Verify database state persistence
	runRec, err := dbStore.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("failed to read run from store: %v", err)
	}
	if runRec.Status != string(dag.RunStatusCompleted) {
		t.Errorf("expected DB run status COMPLETED, got %s", runRec.Status)
	}
	if runRec.CheckpointState == "" {
		t.Error("expected non-empty checkpoint state in runs table")
	}

	dbTasks, err := dbStore.GetTasks(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("failed to read tasks from store: %v", err)
	}
	if len(dbTasks) != 4 {
		t.Fatalf("expected 4 task records in DB, got %d", len(dbTasks))
	}
	for _, dt := range dbTasks {
		if dt.Status != string(dag.StatusCompleted) {
			t.Errorf("task %s has status %s, expected COMPLETED", dt.ID, dt.Status)
		}
		if dt.OutputData == "" {
			t.Errorf("task %s has empty output_data in DB", dt.ID)
		}
	}
}

func TestOrchestratorCheckpointResume(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_resume.db")

	dbStore, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize sqlite store: %v", err)
	}
	defer dbStore.Close()

	// 3-step DAG:
	// step_1 -> step_2 (requires approval) -> step_3
	tasks := []dag.Task{
		{
			ID:         "step_1",
			Action:     "Inspect and stage changes",
			DependsOn:  []string{},
			OutputKeys: []string{"tasks.step_1.output"},
		},
		{
			ID:               "step_2",
			Action:           "Apply migration (destructive)",
			DependsOn:        []string{"step_1"},
			InputKeys:        []string{"tasks.step_1.output"},
			OutputKeys:       []string{"tasks.step_2.output"},
			RequiresApproval: true,
		},
		{
			ID:         "step_3",
			Action:     "Verify migration and cleanup",
			DependsOn:  []string{"step_2"},
			InputKeys:  []string{"tasks.step_2.output"},
			OutputKeys: []string{"tasks.step_3.output"},
		},
	}

	var step1ExecCount int32
	var step2ExecCount int32
	var step3ExecCount int32

	customHandler := func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
		switch task.ID {
		case "step_1":
			atomic.AddInt32(&step1ExecCount, 1)
			return json.RawMessage(`{"step_1": "staged"}`), nil, nil
		case "step_2":
			atomic.AddInt32(&step2ExecCount, 1)
			return json.RawMessage(`{"step_2": "migrated"}`), nil, nil
		case "step_3":
			atomic.AddInt32(&step3ExecCount, 1)
			return json.RawMessage(`{"step_3": "verified"}`), nil, nil
		default:
			return nil, nil, fmt.Errorf("unknown task: %s", task.ID)
		}
	}

	// 1. Initial run: Headless mode (no approval handler).
	// step_1 should complete, step_2 should pause in WAITING_APPROVAL, and run should pause with checkpoint.
	cfg := Config{
		DBStore:        dbStore,
		ArtifactsDir:   filepath.Join(tempDir, "artifacts"),
		MaxConcurrency: 2,
		Planner:        &mockPlanner{tasks: tasks},
		TaskHandler:    customHandler,
	}

	orch := New(cfg)

	initialResult, err := orch.Run(context.Background(), "session-resume", "Run database migration")
	if err != nil {
		t.Fatalf("unexpected error during initial run: %v", err)
	}

	if initialResult.Status != dag.RunStatusWaitingApproval {
		t.Fatalf("expected status WAITING_APPROVAL, got %s", initialResult.Status)
	}
	if len(initialResult.CompletedTasks) != 1 || initialResult.CompletedTasks[0] != "step_1" {
		t.Fatalf("expected step_1 completed, got: %+v", initialResult.CompletedTasks)
	}
	if len(initialResult.WaitingTasks) != 1 || initialResult.WaitingTasks[0] != "step_2" {
		t.Fatalf("expected step_2 waiting approval, got: %+v", initialResult.WaitingTasks)
	}

	// Verify step_1 executed exactly once, and step_2 / step_3 have not executed
	if atomic.LoadInt32(&step1ExecCount) != 1 {
		t.Errorf("expected step_1 executed once, got %d", step1ExecCount)
	}
	if atomic.LoadInt32(&step2ExecCount) != 0 {
		t.Errorf("expected step_2 not executed yet, got %d", step2ExecCount)
	}
	if atomic.LoadInt32(&step3ExecCount) != 0 {
		t.Errorf("expected step_3 not executed yet, got %d", step3ExecCount)
	}

	// Verify checkpoint state in SQLite
	runRec, err := dbStore.GetRun(context.Background(), initialResult.RunID)
	if err != nil {
		t.Fatalf("failed to retrieve run: %v", err)
	}
	if runRec.Status != string(dag.RunStatusWaitingApproval) {
		t.Errorf("expected DB status WAITING_APPROVAL, got %s", runRec.Status)
	}
	if runRec.CheckpointState == "" {
		t.Fatal("expected non-empty checkpoint state in DB")
	}

	// Verify checkpoint contains step_1 output
	var checkpointEntries map[string]blackboard.MemoryEntry
	if err := json.Unmarshal([]byte(runRec.CheckpointState), &checkpointEntries); err != nil {
		t.Fatalf("failed to unmarshal checkpoint state: %v", err)
	}
	if _, ok := checkpointEntries["tasks.step_1.output"]; !ok {
		t.Errorf("expected tasks.step_1.output in checkpoint state: %+v", checkpointEntries)
	}

	// 2. Resume execution: Now provide approval handler approving step_2
	approvedTasks := make(map[string]bool)
	var approvalMu sync.Mutex
	cfg.ApprovalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
		approvalMu.Lock()
		approvedTasks[task.ID] = true
		approvalMu.Unlock()
		return true, nil
	}

	resumedOrch := New(cfg)
	resumedResult, err := resumedOrch.Resume(context.Background(), initialResult.RunID)
	if err != nil {
		t.Fatalf("failed to resume workflow: %v", err)
	}

	if resumedResult.Status != dag.RunStatusCompleted {
		t.Fatalf("expected resumed run status COMPLETED, got %s", resumedResult.Status)
	}
	if len(resumedResult.CompletedTasks) != 3 {
		t.Fatalf("expected all 3 tasks completed after resume, got %d: %+v",
			len(resumedResult.CompletedTasks), resumedResult.CompletedTasks)
	}

	// CRITICAL CHECKPOINT RESUME INVARIANT:
	// step_1 MUST NOT BE RE-EXECUTED! Count remains exactly 1!
	if atomic.LoadInt32(&step1ExecCount) != 1 {
		t.Errorf("CRITICAL: step_1 was re-executed during resume! Count = %d (expected 1)", step1ExecCount)
	}
	if atomic.LoadInt32(&step2ExecCount) != 1 {
		t.Errorf("expected step_2 executed once after approval, got %d", step2ExecCount)
	}
	if atomic.LoadInt32(&step3ExecCount) != 1 {
		t.Errorf("expected step_3 executed once, got %d", step3ExecCount)
	}

	// Verify final state in SQLite
	finalRun, err := dbStore.GetRun(context.Background(), initialResult.RunID)
	if err != nil {
		t.Fatalf("failed to get final run: %v", err)
	}
	if finalRun.Status != string(dag.RunStatusCompleted) {
		t.Errorf("expected final run status COMPLETED, got %s", finalRun.Status)
	}

	finalTasks, err := dbStore.GetTasks(context.Background(), initialResult.RunID)
	if err != nil {
		t.Fatalf("failed to get final tasks: %v", err)
	}
	for _, ft := range finalTasks {
		if ft.Status != string(dag.StatusCompleted) {
			t.Errorf("expected final task %s to be COMPLETED, got %s", ft.ID, ft.Status)
		}
	}
}

func TestOrchestratorEndToEndWithSimulatedLLMServer(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_llm.db")

	dbStore, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize sqlite store: %v", err)
	}
	defer dbStore.Close()

	// Mock server handles both Planner and Worker requests
	plannerDAGJSON := `[
		{
			"id": "code_audit",
			"action": "Audit codebase for insecure endpoints",
			"depends_on": [],
			"output_keys": ["tasks.code_audit.output"]
		},
		{
			"id": "write_report",
			"action": "Write security audit report",
			"depends_on": ["code_audit"],
			"input_keys": ["tasks.code_audit.output"],
			"output_keys": ["tasks.write_report.output"]
		}
	]`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req worker.ChatCompletionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		var content string
		// Determine if this is a planner request or worker request
		isPlanner := false
		for _, m := range req.Messages {
			if m.Role == "user" && (len(req.Messages) == 1) {
				isPlanner = true
				break
			}
		}

		if isPlanner {
			content = "```json\n" + plannerDAGJSON + "\n```"
		} else {
			// Ephemeral worker response
			content = `{"status": "COMPLETED", "result": {"audit": "passed", "findings": []}}`
		}

		resp := worker.ChatCompletionResponse{
			ID: "test-sim-llm",
			Choices: []worker.Choice{
				{
					Message: prompt.ChatMessage{
						Role:    "assistant",
						Content: content,
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := worker.NewClient(server.URL, "mock-key", "mock-model")

	cfg := Config{
		DBStore:        dbStore,
		WorkerClient:   client,
		ArtifactsDir:   filepath.Join(tempDir, "artifacts"),
		MaxConcurrency: 2,
		Model:          "mock-model",
	}

	orch := New(cfg)
	res, err := orch.Run(context.Background(), "session-llm", "Run automated security audit")
	if err != nil {
		t.Fatalf("unexpected error with simulated LLM server: %v", err)
	}

	if res.Status != dag.RunStatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", res.Status)
	}
	if len(res.CompletedTasks) != 2 {
		t.Fatalf("expected 2 tasks completed, got %d", len(res.CompletedTasks))
	}
}
