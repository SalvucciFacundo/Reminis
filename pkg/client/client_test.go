package client

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/fds1288/reminis/internal/dag"
)

type testMockPlanner struct {
	tasks []dag.Task
}

func (p *testMockPlanner) Plan(ctx context.Context, goal, manifest, rulesContent string) ([]dag.Task, error) {
	return p.tasks, nil
}

func TestClientFactsAndPrefix(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "client_facts.db")

	c, err := New(
		WithDBPath(dbPath),
		WithWorkspaceDir(tempDir),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// 1. Test SaveFact
	err = c.SaveFact(ctx, "architecture", "Use hexagonal ports and adapters", "project")
	if err != nil {
		t.Fatalf("failed to save fact: %v", err)
	}

	err = c.SaveFact(ctx, "database", "Use SQLite WAL mode", "global")
	if err != nil {
		t.Fatalf("failed to save second fact: %v", err)
	}

	// 2. Test SearchFacts
	facts, err := c.SearchFacts(ctx, "architecture", "")
	if err != nil {
		t.Fatalf("failed to search facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact matching 'architecture', got %d", len(facts))
	}
	if facts[0].Content != "Use hexagonal ports and adapters" {
		t.Errorf("unexpected content: %s", facts[0].Content)
	}

	// 3. Test ListFacts (all facts)
	allFacts, err := c.ListFacts(ctx, "")
	if err != nil {
		t.Fatalf("failed to list all facts: %v", err)
	}
	if len(allFacts) != 2 {
		t.Fatalf("expected 2 total facts, got %d", len(allFacts))
	}

	// 4. Test GetCompiledPrefix
	prefix, err := c.GetCompiledPrefix(ctx)
	if err != nil {
		t.Fatalf("failed to get compiled prefix: %v", err)
	}
	if prefix.Hash == "" {
		t.Error("expected non-empty prefix hash")
	}
	if prefix.TokenCount <= 0 {
		t.Errorf("expected positive token count, got %d", prefix.TokenCount)
	}
}

func TestClientRunApproveResume(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "client_run.db")

	tasks := []dag.Task{
		{
			ID:               "task_init",
			Action:           "Initialize environment",
			DependsOn:        []string{},
			RequiresApproval: false,
		},
		{
			ID:               "task_migrate",
			Action:           "Run database migration",
			DependsOn:        []string{"task_init"},
			RequiresApproval: true,
		},
		{
			ID:               "task_verify",
			Action:           "Verify migration success",
			DependsOn:        []string{"task_migrate"},
			RequiresApproval: false,
		},
	}

	var initCount, migrateCount, verifyCount int32
	handler := func(ctx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
		switch task.ID {
		case "task_init":
			atomic.AddInt32(&initCount, 1)
			return json.RawMessage(`{"status":"initialized"}`), nil, nil
		case "task_migrate":
			atomic.AddInt32(&migrateCount, 1)
			return json.RawMessage(`{"status":"migrated"}`), nil, nil
		case "task_verify":
			atomic.AddInt32(&verifyCount, 1)
			return json.RawMessage(`{"status":"verified"}`), nil, nil
		}
		return nil, nil, nil
	}

	c, err := New(
		WithDBPath(dbPath),
		WithWorkspaceDir(tempDir),
		WithPlanner(&testMockPlanner{tasks: tasks}),
		WithTaskHandler(handler),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// 1. Initial Run - should pause at task_migrate
	resp, err := c.Run(ctx, RunRequest{
		Goal: "Execute migration workflow",
	})
	if err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	if resp.Status != string(dag.RunStatusWaitingApproval) {
		t.Fatalf("expected WAITING_APPROVAL, got %s", resp.Status)
	}
	if len(resp.WaitingTasks) != 1 || resp.WaitingTasks[0] != "task_migrate" {
		t.Fatalf("expected task_migrate waiting, got %+v", resp.WaitingTasks)
	}
	if atomic.LoadInt32(&initCount) != 1 {
		t.Errorf("expected task_init executed 1 time, got %d", initCount)
	}
	if atomic.LoadInt32(&migrateCount) != 0 {
		t.Errorf("expected task_migrate not executed yet, got %d", migrateCount)
	}

	// 2. Approve task_migrate and resume
	err = c.Approve(ctx, resp.RunID, "task_migrate", "action")
	if err != nil {
		t.Fatalf("approve failed: %v", err)
	}

	// Verify counts: init should NOT re-execute (count stays 1), migrate and verify executed 1
	if atomic.LoadInt32(&initCount) != 1 {
		t.Errorf("task_init re-executed! count = %d", initCount)
	}
	if atomic.LoadInt32(&migrateCount) != 1 {
		t.Errorf("expected task_migrate executed 1 time, got %d", migrateCount)
	}
	if atomic.LoadInt32(&verifyCount) != 1 {
		t.Errorf("expected task_verify executed 1 time, got %d", verifyCount)
	}

	// Check DB state for the run
	runRec, err := c.Store().GetRun(ctx, resp.RunID)
	if err != nil {
		t.Fatalf("failed to get run from store: %v", err)
	}
	if runRec.Status != string(dag.RunStatusCompleted) {
		t.Errorf("expected DB run status COMPLETED, got %s", runRec.Status)
	}
}
