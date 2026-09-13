package dag

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKahnCycleValidation(t *testing.T) {
	// Case 1: Valid linear
	linearTasks := []Task{
		{ID: "A", Action: "stepA"},
		{ID: "B", Action: "stepB", DependsOn: []string{"A"}},
		{ID: "C", Action: "stepC", DependsOn: []string{"B"}},
	}
	order, err := ValidateAcyclic(linearTasks)
	if err != nil {
		t.Fatalf("expected acyclic, got: %v", err)
	}
	if len(order) != 3 || order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Fatalf("unexpected order: %v", order)
	}

	// Case 2: Valid diamond
	diamondTasks := []Task{
		{ID: "A"},
		{ID: "B", DependsOn: []string{"A"}},
		{ID: "C", DependsOn: []string{"A"}},
		{ID: "D", DependsOn: []string{"B", "C"}},
	}
	order, err = ValidateAcyclic(diamondTasks)
	if err != nil {
		t.Fatalf("expected acyclic diamond, got: %v", err)
	}
	if len(order) != 4 || order[0] != "A" || order[3] != "D" {
		t.Fatalf("unexpected diamond order: %v", order)
	}

	// Case 3: Circular dependency
	cyclicTasks := []Task{
		{ID: "A", DependsOn: []string{"C"}},
		{ID: "B", DependsOn: []string{"A"}},
		{ID: "C", DependsOn: []string{"B"}},
	}
	_, err = ValidateAcyclic(cyclicTasks)
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected, got: %v", err)
	}

	// Case 4: Self loop
	selfCycle := []Task{
		{ID: "A", DependsOn: []string{"A"}},
	}
	_, err = ValidateAcyclic(selfCycle)
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected on self loop, got: %v", err)
	}

	// Case 5: Non-existent dependency
	nonExistent := []Task{
		{ID: "A", DependsOn: []string{"Z"}},
	}
	_, err = ValidateAcyclic(nonExistent)
	if err == nil {
		t.Fatalf("expected error for non-existent dependency")
	}

	// Case 6: Duplicate ID
	duplicate := []Task{
		{ID: "A"},
		{ID: "A"},
	}
	_, err = ValidateAcyclic(duplicate)
	if err == nil {
		t.Fatalf("expected error for duplicate task ID")
	}
}

func TestLinearAndParallelExecution(t *testing.T) {
	// Test bounded concurrency throttling (maxConcurrency = 2)
	var activeWorkers int32
	var peakWorkers int32

	tasks := []Task{
		{ID: "P1", Action: "work"},
		{ID: "P2", Action: "work"},
		{ID: "P3", Action: "work"},
		{ID: "P4", Action: "work"},
		{ID: "Agg", Action: "reduce", DependsOn: []string{"P1", "P2", "P3", "P4"}},
	}

	handler := func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
		current := atomic.AddInt32(&activeWorkers, 1)
		for {
			oldPeak := atomic.LoadInt32(&peakWorkers)
			if current <= oldPeak || atomic.CompareAndSwapInt32(&peakWorkers, oldPeak, current) {
				break
			}
		}

		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&activeWorkers, -1)
		return json.RawMessage(`{"status":"ok"}`), nil, nil
	}

	sched, err := NewScheduler(tasks, handler, WithMaxConcurrency(2))
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	result, err := sched.Run(context.Background())
	if err != nil {
		t.Fatalf("scheduler run failed: %v", err)
	}

	if result.Status != RunStatusCompleted {
		t.Fatalf("expected RunStatusCompleted, got: %s", result.Status)
	}
	if len(result.CompletedTasks) != 5 {
		t.Fatalf("expected 5 completed tasks, got %d", len(result.CompletedTasks))
	}

	// Verify peak workers never exceeded maxConcurrency
	finalPeak := atomic.LoadInt32(&peakWorkers)
	if finalPeak > 2 {
		t.Fatalf("peak concurrency was %d, expected at most 2", finalPeak)
	}
}

func TestDynamicExpand(t *testing.T) {
	tasks := []Task{
		{ID: "parent", Action: "explore"},
		{ID: "downstream", Action: "aggregate", DependsOn: []string{"parent"}},
	}

	executed := make(map[string]bool)
	var mu sync.Mutex

	handler := func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
		mu.Lock()
		executed[task.ID] = true
		mu.Unlock()

		if task.ID == "parent" {
			// Yield 2 subtasks
			yielded := []Task{
				{ID: "sub-1", Action: "sub1"},
				{ID: "sub-2", Action: "sub2"},
			}
			return json.RawMessage(`{"expanded":true}`), yielded, nil
		}

		if task.ID == "downstream" {
			// Check that sub-1 and sub-2 have already executed
			mu.Lock()
			sub1Done := executed["sub-1"]
			sub2Done := executed["sub-2"]
			mu.Unlock()
			if !sub1Done || !sub2Done {
				return nil, nil, errors.New("downstream ran before yielded subtasks completed")
			}
		}

		return json.RawMessage(`{"ok":true}`), nil, nil
	}

	sched, err := NewScheduler(tasks, handler)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	result, err := sched.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if result.Status != RunStatusCompleted {
		t.Fatalf("expected COMPLETED status, got: %s", result.Status)
	}
	if len(result.CompletedTasks) != 4 {
		t.Fatalf("expected 4 completed tasks (parent, sub-1, sub-2, downstream), got %d", len(result.CompletedTasks))
	}

	// Test Expand rejects cycle
	cyclicYield := []Task{
		{ID: "bad-1", DependsOn: []string{"bad-2"}},
		{ID: "bad-2", DependsOn: []string{"bad-1"}},
	}
	err = sched.Expand("downstream", cyclicYield)
	if err == nil {
		t.Fatalf("expected error when dynamically expanding with cyclic dependency")
	}
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected, got: %v", err)
	}
}

func TestCascadingSkipsOnFailure(t *testing.T) {
	tasks := []Task{
		{ID: "failing-task", Action: "step1"},
		{ID: "child-task", Action: "step2", DependsOn: []string{"failing-task"}},
		{ID: "grandchild-task", Action: "step3", DependsOn: []string{"child-task"}},
		{ID: "independent-task", Action: "step4"},
	}

	handler := func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
		if task.ID == "failing-task" {
			return nil, nil, errors.New("something went wrong")
		}
		return json.RawMessage(`{"ok":true}`), nil, nil
	}

	sched, err := NewScheduler(tasks, handler)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	result, err := sched.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if result.Status != RunStatusFailedWithCheckpoint {
		t.Fatalf("expected FAILED_WITH_CHECKPOINT, got %s", result.Status)
	}
	if !result.CanResume {
		t.Fatalf("expected CanResume to be true")
	}
	if result.FailedTask == nil || result.FailedTask.TaskID != "failing-task" {
		t.Fatalf("expected failed task to be 'failing-task'")
	}

	// Verify child-task and grandchild-task were skipped
	child, _ := sched.GetTask("child-task")
	if child.Status != StatusSkipped {
		t.Fatalf("expected child-task to be SKIPPED, got %s", child.Status)
	}
	grandchild, _ := sched.GetTask("grandchild-task")
	if grandchild.Status != StatusSkipped {
		t.Fatalf("expected grandchild-task to be SKIPPED, got %s", grandchild.Status)
	}

	// Verify independent-task completed
	indep, _ := sched.GetTask("independent-task")
	if indep.Status != StatusCompleted {
		t.Fatalf("expected independent-task to be COMPLETED, got %s", indep.Status)
	}
}

func TestResourcePathLocking(t *testing.T) {
	sharedPath := "/workspace/shared_file.go"
	tasks := []Task{
		{ID: "writer-1", TargetPaths: []string{sharedPath}},
		{ID: "writer-2", TargetPaths: []string{sharedPath}},
	}

	var collisionDetected int32
	var inCriticalSection int32

	handler := func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
		if atomic.AddInt32(&inCriticalSection, 1) > 1 {
			atomic.StoreInt32(&collisionDetected, 1)
		}
		time.Sleep(25 * time.Millisecond)
		atomic.AddInt32(&inCriticalSection, -1)
		return json.RawMessage(`{}`), nil, nil
	}

	sched, err := NewScheduler(tasks, handler, WithMaxConcurrency(4))
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	result, err := sched.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if result.Status != RunStatusCompleted {
		t.Fatalf("expected COMPLETED, got: %s", result.Status)
	}

	if atomic.LoadInt32(&collisionDetected) > 0 {
		t.Fatalf("resource path collision detected between concurrent workers")
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	tasks := []Task{
		{
			ID:             "slow-task",
			Action:         "slow",
			TimeoutSeconds: 1, // 1 second timeout
		},
		{
			ID:        "child",
			DependsOn: []string{"slow-task"},
		},
	}

	handler := func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
		select {
		case <-time.After(2 * time.Second):
			return json.RawMessage(`{}`), nil, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	sched, err := NewScheduler(tasks, handler)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	result, err := sched.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if result.Status != RunStatusFailedWithCheckpoint {
		t.Fatalf("expected FAILED_WITH_CHECKPOINT, got %s", result.Status)
	}

	slowTask, _ := sched.GetTask("slow-task")
	if slowTask.Status != StatusTimedOut {
		t.Fatalf("expected slow-task to be TIMED_OUT, got %s", slowTask.Status)
	}

	child, _ := sched.GetTask("child")
	if child.Status != StatusSkipped {
		t.Fatalf("expected child to be SKIPPED, got %s", child.Status)
	}
}

func TestDualModeApproval(t *testing.T) {
	// Mode 1: Interactive approval callback
	t.Run("InteractiveApproval", func(t *testing.T) {
		tasks := []Task{
			{ID: "gate-task", RequiresApproval: true},
		}

		approvedCalled := false
		sched, err := NewScheduler(tasks, func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
			return json.RawMessage(`{"result":"done"}`), nil, nil
		}, WithApprovalHandler(func(ctx context.Context, task *Task) (bool, error) {
			approvedCalled = true
			return true, nil
		}))
		if err != nil {
			t.Fatalf("failed to create scheduler: %v", err)
		}

		res, err := sched.Run(context.Background())
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}
		if !approvedCalled {
			t.Fatalf("expected approval callback to be called")
		}
		if res.Status != RunStatusCompleted {
			t.Fatalf("expected COMPLETED, got %s", res.Status)
		}
	})

	// Mode 2: Headless mode (no approval handler) -> pauses in WAITING_APPROVAL
	t.Run("HeadlessApprovalPause", func(t *testing.T) {
		tasks := []Task{
			{ID: "approved-gate", RequiresApproval: true},
			{ID: "independent", RequiresApproval: false},
		}

		sched, err := NewScheduler(tasks, func(ctx context.Context, task *Task) (json.RawMessage, []Task, error) {
			return json.RawMessage(`{}`), nil, nil
		})
		if err != nil {
			t.Fatalf("failed to create scheduler: %v", err)
		}

		res, err := sched.Run(context.Background())
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}

		if res.Status != RunStatusWaitingApproval {
			t.Fatalf("expected WAITING_APPROVAL, got %s", res.Status)
		}
		if !res.CanResume {
			t.Fatalf("expected CanResume to be true")
		}
		if len(res.WaitingTasks) != 1 || res.WaitingTasks[0] != "approved-gate" {
			t.Fatalf("unexpected waiting tasks: %v", res.WaitingTasks)
		}
		if len(res.CompletedTasks) != 1 || res.CompletedTasks[0] != "independent" {
			t.Fatalf("unexpected completed tasks: %v", res.CompletedTasks)
		}
	})
}
