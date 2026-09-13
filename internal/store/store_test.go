package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreRunLifecycleAndCheckpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_reminis.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Create Run
	run := RunRecord{
		ID:        "run-100",
		SessionID: "sess-1",
		Goal:      "Implement core engine",
		Status:    "RUNNING",
		CreatedAt: time.Now(),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	// 2. Fetch Run
	fetched, err := s.GetRun(ctx, "run-100")
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if fetched.ID != "run-100" || fetched.Status != "RUNNING" || fetched.Goal != "Implement core engine" {
		t.Fatalf("unexpected run data: %+v", fetched)
	}
	if fetched.SessionID != "sess-1" {
		t.Fatalf("expected session_id sess-1, got %s", fetched.SessionID)
	}

	// 3. Update Run with Checkpoint State
	checkpointJSON := `{"active_step": 3, "blackboard_keys": ["tasks.t1.out"]}`
	if err := s.UpdateRunStatus(ctx, "run-100", "FAILED_WITH_CHECKPOINT", checkpointJSON, 1500); err != nil {
		t.Fatalf("failed to update run status: %v", err)
	}

	updated, err := s.GetRun(ctx, "run-100")
	if err != nil {
		t.Fatalf("failed to get updated run: %v", err)
	}
	if updated.Status != "FAILED_WITH_CHECKPOINT" {
		t.Fatalf("expected status FAILED_WITH_CHECKPOINT, got %s", updated.Status)
	}
	if updated.CheckpointState != checkpointJSON {
		t.Fatalf("expected checkpoint state %s, got %s", checkpointJSON, updated.CheckpointState)
	}
	if updated.TotalTokens != 1500 {
		t.Fatalf("expected total tokens 1500, got %d", updated.TotalTokens)
	}
	if updated.FinishedAt == nil {
		t.Fatalf("expected FinishedAt to be populated on completion")
	}
}

func TestStoreTaskLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_tasks.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Create run first
	if err := s.CreateRun(ctx, RunRecord{ID: "run-200", Goal: "Build tasks", Status: "RUNNING"}); err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	// Create tasks
	tasks := []TaskRecord{
		{
			ID:               "task-1",
			RunID:            "run-200",
			Action:           "read_file",
			Status:           "PENDING",
			RequiresApproval: false,
			DependsOn:        []string{},
		},
		{
			ID:               "task-2",
			RunID:            "run-200",
			Action:           "write_file",
			Status:           "PENDING",
			RequiresApproval: true,
			DependsOn:        []string{"task-1"},
		},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatalf("failed to create tasks: %v", err)
	}

	fetchedTasks, err := s.GetTasks(ctx, "run-200")
	if err != nil {
		t.Fatalf("failed to get tasks: %v", err)
	}
	if len(fetchedTasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(fetchedTasks))
	}
	if len(fetchedTasks[1].DependsOn) != 1 || fetchedTasks[1].DependsOn[0] != "task-1" {
		t.Fatalf("expected task-2 to depend on task-1, got: %v", fetchedTasks[1].DependsOn)
	}
	if !fetchedTasks[1].RequiresApproval {
		t.Fatalf("expected task-2 RequiresApproval to be true")
	}

	// Update task status
	resultData := json.RawMessage(`{"bytes_written": 1024}`)
	if err := s.UpdateTaskStatus(ctx, "task-2", "COMPLETED", resultData, ""); err != nil {
		t.Fatalf("failed to update task status: %v", err)
	}

	updatedTasks, err := s.GetTasks(ctx, "run-200")
	if err != nil {
		t.Fatalf("failed to get updated tasks: %v", err)
	}
	if updatedTasks[1].Status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %s", updatedTasks[1].Status)
	}
	if updatedTasks[1].OutputData != string(resultData) {
		t.Fatalf("expected output data %s, got %s", string(resultData), updatedTasks[1].OutputData)
	}
}

func TestStoreSessionPrefixSaving(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_session.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	sess := SessionRecord{
		ID:               "session-abc",
		ProjectPath:      "/path/to/project",
		Status:           "ACTIVE",
		PermissionsScope: "session",
		PrefixHash:       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CompiledPrefix:   "# Universal Rules\n- Rule 1\n- Rule 2",
		StartedAt:        time.Now(),
	}

	if err := s.CreateSession(ctx, sess); err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	fetched, err := s.GetSession(ctx, "session-abc")
	if err != nil {
		t.Fatalf("failed to get session: %v", err)
	}
	if fetched.ID != "session-abc" {
		t.Fatalf("expected session ID session-abc, got %s", fetched.ID)
	}
	if fetched.PrefixHash != sess.PrefixHash {
		t.Fatalf("expected prefix hash %s, got %s", sess.PrefixHash, fetched.PrefixHash)
	}
	if fetched.CompiledPrefix != sess.CompiledPrefix {
		t.Fatalf("expected compiled prefix %s, got %s", sess.CompiledPrefix, fetched.CompiledPrefix)
	}
	if fetched.PermissionsScope != "session" {
		t.Fatalf("expected permissions scope session, got %s", fetched.PermissionsScope)
	}
}

func TestStoreConcurrentWriteBursts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_burst.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	runID := "run-burst"
	if err := s.CreateRun(ctx, RunRecord{ID: runID, Goal: "Test Concurrency", Status: "RUNNING"}); err != nil {
		t.Fatalf("failed to create initial run: %v", err)
	}

	// Burst 50 concurrent goroutines writing tasks and updating statuses simultaneously
	numGoroutines := 50
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*2)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()
			taskID := fmt.Sprintf("burst-task-%d", taskNum)

			// Write task
			err := s.CreateTasks(ctx, []TaskRecord{
				{
					ID:     taskID,
					RunID:  runID,
					Action: fmt.Sprintf("action-%d", taskNum),
					Status: "RUNNING",
				},
			})
			if err != nil {
				errCh <- fmt.Errorf("CreateTasks failed for %s: %w", taskID, err)
				return
			}

			// Update task immediately
			result := json.RawMessage(fmt.Sprintf(`{"val":%d}`, taskNum))
			err = s.UpdateTaskStatus(ctx, taskID, "COMPLETED", result, "")
			if err != nil {
				errCh <- fmt.Errorf("UpdateTaskStatus failed for %s: %w", taskID, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent burst error: %v", err)
	}

	tasks, err := s.GetTasks(ctx, runID)
	if err != nil {
		t.Fatalf("GetTasks failed after burst: %v", err)
	}
	if len(tasks) != numGoroutines {
		t.Fatalf("expected %d tasks after burst, found %d", numGoroutines, len(tasks))
	}

	// Verify all tasks reached COMPLETED
	for _, tk := range tasks {
		if tk.Status != "COMPLETED" {
			t.Errorf("task %s has status %s, expected COMPLETED", tk.ID, tk.Status)
		}
	}
}

func TestStoreFactsCRUDAndSearch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_facts.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// 1. Save new fact
	fact1 := FactRecord{
		ID:        "fact-1",
		SessionID: "sess-1",
		Topic:     "database",
		Content:   "SQLite runs in WAL mode with busy_timeout=5000ms",
		Scope:     "project",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := s.SaveFact(ctx, fact1); err != nil {
		t.Fatalf("SaveFact failed: %v", err)
	}

	// 2. GetFact
	got, err := s.GetFact(ctx, "fact-1")
	if err != nil {
		t.Fatalf("GetFact failed: %v", err)
	}
	if got.ID != "fact-1" || got.Topic != "database" || got.Content != fact1.Content {
		t.Fatalf("unexpected fact data: %+v", got)
	}
	if got.SessionID != "sess-1" || got.Scope != "project" {
		t.Fatalf("unexpected metadata: %+v", got)
	}

	// 3. Update existing fact on ID conflict (upsert)
	fact1Updated := FactRecord{
		ID:        "fact-1",
		SessionID: "sess-1",
		Topic:     "database",
		Content:   "SQLite in WAL mode, foreign keys ON, synchronous NORMAL",
		Scope:     "project",
	}
	if err := s.SaveFact(ctx, fact1Updated); err != nil {
		t.Fatalf("SaveFact update failed: %v", err)
	}

	gotUpdated, err := s.GetFact(ctx, "fact-1")
	if err != nil {
		t.Fatalf("GetFact after update failed: %v", err)
	}
	if gotUpdated.Content != fact1Updated.Content {
		t.Fatalf("expected updated content %q, got %q", fact1Updated.Content, gotUpdated.Content)
	}

	// 4. Save second fact in same session
	fact2 := FactRecord{
		ID:        "fact-2",
		SessionID: "sess-1",
		Topic:     "architecture",
		Content:   "Reminis uses Hexagonal Architecture with ports and adapters",
		Scope:     "project",
	}
	if err := s.SaveFact(ctx, fact2); err != nil {
		t.Fatalf("SaveFact fact2 failed: %v", err)
	}

	// 5. Save third fact in different session and scope
	fact3 := FactRecord{
		ID:        "fact-3",
		SessionID: "sess-2",
		Topic:     "database/schema",
		Content:   "Facts table indexed on topic",
		Scope:     "session",
	}
	if err := s.SaveFact(ctx, fact3); err != nil {
		t.Fatalf("SaveFact fact3 failed: %v", err)
	}

	// 6. SearchFacts by topic
	dbFacts, err := s.SearchFacts(ctx, "data", "")
	if err != nil {
		t.Fatalf("SearchFacts failed: %v", err)
	}
	if len(dbFacts) != 2 {
		t.Fatalf("expected 2 database facts, got %d", len(dbFacts))
	}

	// 7. SearchFacts by topic and scope
	sessionScopedFacts, err := s.SearchFacts(ctx, "data", "session")
	if err != nil {
		t.Fatalf("SearchFacts failed: %v", err)
	}
	if len(sessionScopedFacts) != 1 || sessionScopedFacts[0].ID != "fact-3" {
		t.Fatalf("expected fact-3, got %+v", sessionScopedFacts)
	}

	// 8. ListFactsBySession
	sess1Facts, err := s.ListFactsBySession(ctx, "sess-1")
	if err != nil {
		t.Fatalf("ListFactsBySession failed: %v", err)
	}
	if len(sess1Facts) != 2 {
		t.Fatalf("expected 2 facts for sess-1, got %d", len(sess1Facts))
	}

	// 9. Get non-existent fact
	_, err = s.GetFact(ctx, "non-existent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStoreSessionUpdate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_sess_update.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	sess := SessionRecord{
		ID:          "sess-test",
		ProjectPath: "/some/path",
		Status:      "ACTIVE",
	}
	if err := s.CreateSession(ctx, sess); err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	if err := s.UpdateSessionStatus(ctx, "sess-test", "ENDED", "Session completed successfully"); err != nil {
		t.Fatalf("UpdateSessionStatus failed: %v", err)
	}

	fetched, err := s.GetSession(ctx, "sess-test")
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if fetched.Status != "ENDED" {
		t.Fatalf("expected status ENDED, got %s", fetched.Status)
	}
	if fetched.Summary != "Session completed successfully" {
		t.Fatalf("expected summary, got %s", fetched.Summary)
	}
	if fetched.EndedAt == nil {
		t.Fatalf("expected ended_at to be populated")
	}

	// Non-existent session update
	err = s.UpdateSessionStatus(ctx, "non-existent", "ENDED", "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-existent session update, got %v", err)
	}
}

