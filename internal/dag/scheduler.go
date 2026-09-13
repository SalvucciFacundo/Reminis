package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/store"
)

// TaskHandler is the execution hook for a task.
type TaskHandler func(ctx context.Context, task *Task) (result json.RawMessage, yielded []Task, err error)

// ApprovalHandler is invoked when a task requires human/interactive approval.
type ApprovalHandler func(ctx context.Context, task *Task) (approved bool, err error)

// Option configures a Scheduler instance.
type Option func(*Scheduler)

// WithMaxConcurrency configures the maximum concurrent worker goroutines.
func WithMaxConcurrency(n int) Option {
	return func(s *Scheduler) {
		if n > 0 {
			s.maxConcurrency = n
		}
	}
}

// WithApprovalHandler sets the callback for tasks requiring approval.
func WithApprovalHandler(h ApprovalHandler) Option {
	return func(s *Scheduler) {
		s.onWaitingApproval = h
	}
}

// WithBlackboard attaches a Blackboard working memory ledger.
func WithBlackboard(bb *blackboard.Blackboard) Option {
	return func(s *Scheduler) {
		s.blackboard = bb
	}
}

// WithStore attaches SQLite storage and associates the execution with runID.
func WithStore(st *store.Store, runID string) Option {
	return func(s *Scheduler) {
		s.store = st
		s.runID = runID
	}
}

// WithResourceLock attaches a custom ResourceLock table.
func WithResourceLock(rl *ResourceLock) Option {
	return func(s *Scheduler) {
		s.resourceLock = rl
	}
}

// Scheduler orchestrates DAG task execution with bounded concurrency,
// resource path locks, cascading skips, and dynamic expansion.
type Scheduler struct {
	mu                sync.Mutex
	tasks             map[string]*Task
	taskOrder         []string
	maxConcurrency    int
	sem               chan struct{}
	handler           TaskHandler
	onWaitingApproval ApprovalHandler
	resourceLock      *ResourceLock
	blackboard        *blackboard.Blackboard
	store             *store.Store
	runID             string
	notifyCh          chan struct{}
}

// NewScheduler validates the initial task graph for acyclicity and initializes the scheduler.
func NewScheduler(tasks []Task, handler TaskHandler, opts ...Option) (*Scheduler, error) {
	if _, err := ValidateAcyclic(tasks); err != nil {
		return nil, err
	}

	s := &Scheduler{
		tasks:          make(map[string]*Task, len(tasks)),
		taskOrder:      make([]string, 0, len(tasks)),
		maxConcurrency: 4,
		handler:        handler,
		resourceLock:   NewResourceLock(),
		notifyCh:       make(chan struct{}, 512),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.sem = make(chan struct{}, s.maxConcurrency)

	for i := range tasks {
		t := tasks[i]
		if t.Status == "" {
			t.Status = StatusPending
		}
		taskCopy := t
		s.tasks[t.ID] = &taskCopy
		s.taskOrder = append(s.taskOrder, t.ID)
	}

	return s, nil
}

func (s *Scheduler) notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// GetTasks returns a snapshot copy of all tasks in execution order.
func (s *Scheduler) GetTasks() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make([]Task, 0, len(s.taskOrder))
	for _, id := range s.taskOrder {
		if t, ok := s.tasks[id]; ok {
			res = append(res, *t)
		}
	}
	return res
}

// GetTask returns a copy of a task by ID.
func (s *Scheduler) GetTask(id string) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	taskCopy := *t
	return &taskCopy, true
}

// Expand dynamically injects yielded subtasks into the active DAG with incremental Kahn cycle validation.
func (s *Scheduler) Expand(parentID string, yielded []Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.expandLocked(parentID, yielded)
	if err == nil {
		s.notify()
	}
	return err
}

func (s *Scheduler) expandLocked(parentID string, yielded []Task) error {
	if len(yielded) == 0 {
		return nil
	}

	yieldedMap := make(map[string]*Task, len(yielded))
	for i := range yielded {
		yt := &yielded[i]
		if yt.ID == "" {
			return fmt.Errorf("yielded task must have non-empty ID")
		}
		if _, exists := s.tasks[yt.ID]; exists {
			return fmt.Errorf("yielded task ID %q already exists in task graph", yt.ID)
		}
		if _, exists := yieldedMap[yt.ID]; exists {
			return fmt.Errorf("duplicate yielded task ID: %q", yt.ID)
		}
		yieldedMap[yt.ID] = yt
	}

	// Identify leaf tasks among yielded
	isDepInYielded := make(map[string]bool)
	for _, yt := range yielded {
		for _, dep := range yt.DependsOn {
			if _, inYielded := yieldedMap[dep]; inYielded {
				isDepInYielded[dep] = true
			}
		}
	}
	var leafIDs []string
	for _, yt := range yielded {
		if !isDepInYielded[yt.ID] {
			leafIDs = append(leafIDs, yt.ID)
		}
	}
	sort.Strings(leafIDs)

	// If a yielded task does not specify dependencies, have it depend on parent
	for i := range yielded {
		if len(yielded[i].DependsOn) == 0 && parentID != "" {
			yielded[i].DependsOn = []string{parentID}
		}
	}

	// Rewire any existing pending downstream tasks that depend on parentID
	// to also depend on leaf yielded tasks
	downstreamModified := make(map[string][]string)
	for id, t := range s.tasks {
		if id == parentID || t.Status != StatusPending {
			continue
		}
		containsParent := false
		for _, dep := range t.DependsOn {
			if dep == parentID {
				containsParent = true
				break
			}
		}
		if containsParent {
			newDeps := append([]string(nil), t.DependsOn...)
			for _, leafID := range leafIDs {
				alreadyPresent := false
				for _, d := range newDeps {
					if d == leafID {
						alreadyPresent = true
						break
					}
				}
				if !alreadyPresent {
					newDeps = append(newDeps, leafID)
				}
			}
			downstreamModified[id] = newDeps
		}
	}

	// Build hypothetical graph for incremental Kahn cycle validation
	var hypothetical []Task
	for _, id := range s.taskOrder {
		t := s.tasks[id]
		taskCopy := *t
		if newDeps, modified := downstreamModified[t.ID]; modified {
			taskCopy.DependsOn = newDeps
		}
		hypothetical = append(hypothetical, taskCopy)
	}
	for _, yt := range yielded {
		taskCopy := yt
		if taskCopy.Status == "" {
			taskCopy.Status = StatusPending
		}
		hypothetical = append(hypothetical, taskCopy)
	}

	if _, err := ValidateAcyclic(hypothetical); err != nil {
		return fmt.Errorf("incremental cycle validation failed: %w", err)
	}

	// Commit expansion
	for id, newDeps := range downstreamModified {
		s.tasks[id].DependsOn = newDeps
	}
	for i := range yielded {
		yt := yielded[i]
		if yt.Status == "" {
			yt.Status = StatusPending
		}
		taskCopy := yt
		s.tasks[yt.ID] = &taskCopy
		s.taskOrder = append(s.taskOrder, yt.ID)
	}

	return nil
}

func (s *Scheduler) cascadeSkipsLocked(failedID string) {
	queue := []string{failedID}
	visited := make(map[string]bool)
	visited[failedID] = true

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		for _, id := range s.taskOrder {
			t := s.tasks[id]
			if t.Status != StatusPending && t.Status != StatusWaitingApproval {
				continue
			}
			for _, dep := range t.DependsOn {
				if dep == curr {
					t.Status = StatusSkipped
					t.Error = fmt.Sprintf("skipped due to dependency failure: %s", curr)
					if !visited[t.ID] {
						visited[t.ID] = true
						queue = append(queue, t.ID)
					}
					break
				}
			}
		}
	}
}

func (s *Scheduler) isTaskReadyLocked(t *Task) bool {
	for _, depID := range t.DependsOn {
		depTask, ok := s.tasks[depID]
		if !ok || depTask.Status != StatusCompleted {
			return false
		}
	}
	return true
}

func (s *Scheduler) buildRunResultLocked() *RunResult {
	res := &RunResult{
		RunID:          s.runID,
		Status:         RunStatusCompleted,
		CompletedTasks: []string{},
		SkippedTasks:   []string{},
		WaitingTasks:   []string{},
	}

	hasWaiting := false
	var firstFailed *TaskFailure

	for _, id := range s.taskOrder {
		t := s.tasks[id]
		switch t.Status {
		case StatusCompleted:
			res.CompletedTasks = append(res.CompletedTasks, t.ID)
		case StatusSkipped:
			res.SkippedTasks = append(res.SkippedTasks, t.ID)
		case StatusWaitingApproval:
			res.WaitingTasks = append(res.WaitingTasks, t.ID)
			hasWaiting = true
		case StatusFailed, StatusTimedOut:
			if firstFailed == nil {
				firstFailed = &TaskFailure{
					TaskID: t.ID,
					Action: t.Action,
					Error:  t.Error,
				}
			}
		}
	}

	if hasWaiting {
		res.Status = RunStatusWaitingApproval
		res.CanResume = true
	} else if firstFailed != nil {
		res.Status = RunStatusFailedWithCheckpoint
		res.FailedTask = firstFailed
		res.CanResume = true
	} else {
		res.Status = RunStatusCompleted
		res.CanResume = false
	}

	if s.blackboard != nil {
		res.BlackboardKeys = s.blackboard.ListKeys()
	}

	return res
}

func (s *Scheduler) runTask(ctx context.Context, task *Task) {
	// Handle approval gate
	if task.RequiresApproval {
		if s.onWaitingApproval != nil {
			s.mu.Lock()
			task.Status = StatusWaitingApproval
			s.mu.Unlock()
			s.notify()

			approved, err := s.onWaitingApproval(ctx, task)
			if err != nil {
				s.mu.Lock()
				task.Status = StatusFailed
				task.Error = fmt.Sprintf("approval error: %v", err)
				s.cascadeSkipsLocked(task.ID)
				s.mu.Unlock()
				return
			}
			if !approved {
				s.mu.Lock()
				task.Status = StatusWaitingApproval
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			task.Status = StatusRunning
			s.mu.Unlock()
		} else {
			// Headless / async mode: pause in WAITING_APPROVAL
			s.mu.Lock()
			task.Status = StatusWaitingApproval
			s.mu.Unlock()
			return
		}
	}

	// Cooperative path locking
	var unlockPaths func() = func() {}
	if len(task.TargetPaths) > 0 && s.resourceLock != nil {
		unlockPaths = s.resourceLock.LockPaths(task.TargetPaths)
	}
	defer unlockPaths()

	var taskCtx context.Context
	var cancel context.CancelFunc
	if task.TimeoutSeconds > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(task.TimeoutSeconds)*time.Second)
	} else {
		taskCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	var result json.RawMessage
	var yielded []Task
	var err error

	if s.handler != nil {
		result, yielded, err = s.handler(taskCtx, task)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		if errors.Is(taskCtx.Err(), context.DeadlineExceeded) || (task.TimeoutSeconds > 0 && taskCtx.Err() != nil) {
			task.Status = StatusTimedOut
			task.Error = "task execution timed out"
		} else {
			task.Status = StatusFailed
			task.Error = err.Error()
		}
		s.cascadeSkipsLocked(task.ID)
		return
	}

	task.Result = result
	task.Status = StatusCompleted

	if len(yielded) > 0 {
		if expErr := s.expandLocked(task.ID, yielded); expErr != nil {
			task.Status = StatusFailed
			task.Error = fmt.Sprintf("failed to expand yielded tasks: %v", expErr)
			s.cascadeSkipsLocked(task.ID)
		}
	}
}

// Run orchestrates execution of the DAG until all reachable tasks reach a terminal state,
// pause for approval, or context cancels.
func (s *Scheduler) Run(ctx context.Context) (*RunResult, error) {
	runningCount := 0

	for {
		s.mu.Lock()

		// Cascade skips for any tasks whose dependencies failed or were skipped
		for _, id := range s.taskOrder {
			t := s.tasks[id]
			if t.Status == StatusPending {
				for _, depID := range t.DependsOn {
					if depTask, ok := s.tasks[depID]; ok {
						if depTask.Status == StatusFailed || depTask.Status == StatusTimedOut || depTask.Status == StatusSkipped {
							t.Status = StatusSkipped
							t.Error = fmt.Sprintf("skipped due to dependency: %s", depID)
							s.cascadeSkipsLocked(t.ID)
							break
						}
					}
				}
			}
		}

		// Find ready pending tasks
		var readyTasks []*Task
		for _, id := range s.taskOrder {
			t := s.tasks[id]
			if t.Status == StatusPending && s.isTaskReadyLocked(t) {
				readyTasks = append(readyTasks, t)
			}
		}

		// Handle tasks requiring approval without an interactive callback in headless mode
		var runnableTasks []*Task
		for _, t := range readyTasks {
			if t.RequiresApproval && s.onWaitingApproval == nil {
				t.Status = StatusWaitingApproval
			} else {
				runnableTasks = append(runnableTasks, t)
			}
		}

		// Termination check: no active workers and no runnable tasks
		if runningCount == 0 && len(runnableTasks) == 0 {
			result := s.buildRunResultLocked()
			s.mu.Unlock()

			if s.store != nil && s.runID != "" {
				_ = s.store.UpdateRunStatus(ctx, s.runID, string(result.Status), "", 0)
			}
			return result, nil
		}

		// Dispatch runnable tasks up to bounded concurrency limit
		for _, t := range runnableTasks {
			select {
			case s.sem <- struct{}{}:
				t.Status = StatusRunning
				runningCount++
				taskToRun := t
				go func() {
					defer func() {
						<-s.sem
						s.mu.Lock()
						runningCount--
						s.mu.Unlock()
						s.notify()
					}()

					s.runTask(ctx, taskToRun)
				}()
			default:
				// Semaphore capacity reached
				break
			}
		}

		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.mu.Lock()
			result := s.buildRunResultLocked()
			s.mu.Unlock()
			return result, ctx.Err()
		case <-s.notifyCh:
			// Woken up by worker completion or dynamic task injection
		}
	}
}
