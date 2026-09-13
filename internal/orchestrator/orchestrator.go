package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/planner"
	"github.com/fds1288/reminis/internal/rules"
	"github.com/fds1288/reminis/internal/store"
	"github.com/fds1288/reminis/internal/worker"
)

// PlanRunner defines the interface for generating DAG execution plans.
type PlanRunner interface {
	Plan(ctx context.Context, goal, manifest, rulesContent string) ([]dag.Task, error)
}

// Config specifies the runtime configuration for the Orchestrator.
type Config struct {
	DBStore         *store.Store
	WorkerClient    *worker.Client
	ArtifactsDir    string
	MaxConcurrency  int
	Model           string
	Manifest        string
	RulesOpts       rules.DiscoveryOptions
	ApprovalHandler dag.ApprovalHandler
	Executor        *worker.Executor
	Planner         PlanRunner
	TaskHandler     dag.TaskHandler // Optional execution override for tests
}

// taskMetadata is serialized into tasks.input_data to preserve task DAG metadata across restarts.
type taskMetadata struct {
	InputKeys      []string `json:"input_keys,omitempty"`
	OutputKeys     []string `json:"output_keys,omitempty"`
	TargetPaths    []string `json:"target_paths,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

// Orchestrator coordinates the lifecycle of agent workflows, integrating
// storage, memory (Blackboard), execution graph (DAG), planner, and live telemetry.
type Orchestrator struct {
	config       Config
	store        *store.Store
	events       *EventEmitter
	planner      PlanRunner
	workerClient *worker.Client
	executor     *worker.Executor
}

// New creates a new Orchestrator instance with sensible defaults.
func New(cfg Config) *Orchestrator {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.ArtifactsDir == "" {
		cfg.ArtifactsDir = filepath.Join(os.TempDir(), "reminis_artifacts")
	}

	p := cfg.Planner
	if p == nil && cfg.WorkerClient != nil {
		p = planner.New(cfg.WorkerClient, cfg.Model)
	}

	exec := cfg.Executor
	if exec == nil {
		exec = worker.NewExecutor("", worker.DefaultSilenceDuration)
	}

	return &Orchestrator{
		config:       cfg,
		store:        cfg.DBStore,
		events:       NewEventEmitter(),
		planner:      p,
		workerClient: cfg.WorkerClient,
		executor:     exec,
	}
}

// Events returns the thread-safe event broadcaster for execution telemetry.
func (o *Orchestrator) Events() *EventEmitter {
	return o.events
}

// generateRunID generates a globally unique identifier for a workflow run.
func generateRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("run_%d_%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// Run executes the full Reminis pipeline:
// 1. Discovers and compiles rules via rules.DiscoverAndCompile.
// 2. Calls planner.Plan to generate the DAG.
// 3. Creates RunRecord and TaskRecords in store.Store.
// 4. Initializes blackboard.Blackboard with ArtifactsDir.
// 5. Initializes dag.Scheduler with MaxConcurrency and TaskHandler (wrapping worker.Worker).
// 6. Wires event subscriptions to emit telemetry and flushes state snapshots to SQLite.
// 7. Returns dag.RunResult.
func (o *Orchestrator) Run(ctx context.Context, sessionID, goal string) (*dag.RunResult, error) {
	if o.store == nil {
		return nil, errors.New("orchestrator store cannot be nil")
	}
	if o.planner == nil {
		return nil, errors.New("orchestrator planner cannot be nil")
	}

	// 1. Discover and compile rules
	compiledPrefix, err := rules.DiscoverAndCompile(o.config.RulesOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to discover and compile rules: %w", err)
	}

	// 2. Generate DAG via Planner
	tasks, err := o.planner.Plan(ctx, goal, o.config.Manifest, compiledPrefix.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to generate plan: %w", err)
	}
	if len(tasks) == 0 {
		return nil, errors.New("planner generated empty task list")
	}

	runID := generateRunID()

	// 3. Create RunRecord and TaskRecords in store
	runRecord := store.RunRecord{
		ID:              runID,
		SessionID:       sessionID,
		Goal:            goal,
		Status:          string(dag.RunStatusRunning),
		TotalTokens:     0,
		CheckpointState: "",
		CreatedAt:       time.Now().UTC(),
	}
	if err := o.store.CreateRun(ctx, runRecord); err != nil {
		return nil, fmt.Errorf("failed to create run record: %w", err)
	}

	for i := range tasks {
		if tasks[i].TimeoutSeconds > 0 && tasks[i].TimeoutSeconds < 60 {
			tasks[i].TimeoutSeconds = 60
		}
	}

	taskRecords := make([]store.TaskRecord, len(tasks))
	for i, t := range tasks {
		metaJSON, _ := json.Marshal(taskMetadata{
			InputKeys:      t.InputKeys,
			OutputKeys:     t.OutputKeys,
			TargetPaths:    t.TargetPaths,
			TimeoutSeconds: t.TimeoutSeconds,
		})
		taskRecords[i] = store.TaskRecord{
			ID:               t.ID,
			RunID:            runID,
			Action:           t.Action,
			Status:           string(dag.StatusPending),
			RequiresApproval: t.RequiresApproval,
			DependsOn:        t.DependsOn,
			InputData:        string(metaJSON),
			CreatedAt:        time.Now().UTC(),
		}
	}
	if err := o.store.CreateTasks(ctx, taskRecords); err != nil {
		return nil, fmt.Errorf("failed to persist initial task records: %w", err)
	}

	// Emit EventRunStarted
	o.events.Publish(Event{
		RunID:   runID,
		Type:    EventRunStarted,
		Payload: map[string]any{"goal": goal, "total_tasks": len(tasks)},
	})

	// 4. Initialize Blackboard with run-scoped artifacts directory
	runArtifactsDir := filepath.Join(o.config.ArtifactsDir, runID, "artifacts")
	bb := blackboard.New(runArtifactsDir)

	// 5 & 6. Execute DAG via Scheduler with telemetry and continuous checkpointing
	return o.executeDAG(ctx, runID, tasks, bb, compiledPrefix.Content)
}

// executeDAG executes the scheduled DAG, wiring task execution, telemetry,
// approval gates, and SQLite state snapshots.
func (o *Orchestrator) executeDAG(
	ctx context.Context,
	runID string,
	tasks []dag.Task,
	bb *blackboard.Blackboard,
	compiledRules string,
) (*dag.RunResult, error) {
	// Base worker execution function
	var baseHandler dag.TaskHandler
	if o.config.TaskHandler != nil {
		baseHandler = o.config.TaskHandler
	} else {
		w := worker.NewWorker(
			o.workerClient,
			o.executor,
			bb,
			compiledRules,
			o.config.Manifest,
		)
		baseHandler = w.AsTaskHandler()
	}

	// Wrap worker execution to stream telemetry and flush checkpoints
	wrappedTaskHandler := func(tCtx context.Context, task *dag.Task) (json.RawMessage, []dag.Task, error) {
		// Emit EventTaskStarted
		o.events.Publish(Event{
			RunID:   runID,
			TaskID:  task.ID,
			Type:    EventTaskStarted,
			Payload: map[string]any{"action": task.Action},
		})
		_ = o.store.UpdateTaskStatus(tCtx, task.ID, string(dag.StatusRunning), nil, "")

		startTime := time.Now()
		result, yielded, err := baseHandler(tCtx, task)
		durationMs := time.Since(startTime).Milliseconds()

		if err != nil {
			// Emit EventTaskFailed
			o.events.Publish(Event{
				RunID:   runID,
				TaskID:  task.ID,
				Type:    EventTaskFailed,
				Payload: map[string]any{"error": err.Error(), "duration_ms": durationMs},
			})
			_ = o.store.UpdateTaskStatus(tCtx, task.ID, string(dag.StatusFailed), nil, err.Error())
			return nil, nil, err
		}

		// Ensure output keys are recorded in Blackboard
		if len(task.OutputKeys) > 0 && len(result) > 0 && bb != nil {
			for _, outKey := range task.OutputKeys {
				if _, getErr := bb.Get(outKey); getErr != nil {
					_, _ = bb.Set(task.ID, outKey, result)
				}
			}
		}

		// Emit EventTaskCompleted
		o.events.Publish(Event{
			RunID:   runID,
			TaskID:  task.ID,
			Type:    EventTaskCompleted,
			Payload: map[string]any{"duration_ms": durationMs, "result": string(result)},
		})

		// Update task in SQLite store (tasks.output_data)
		_ = o.store.UpdateTaskStatus(tCtx, task.ID, string(dag.StatusCompleted), result, "")

		// Record dynamic subtasks if yielded
		if len(yielded) > 0 {
			yieldedRecords := make([]store.TaskRecord, len(yielded))
			for i, yt := range yielded {
				yMeta, _ := json.Marshal(taskMetadata{
					InputKeys:      yt.InputKeys,
					OutputKeys:     yt.OutputKeys,
					TargetPaths:    yt.TargetPaths,
					TimeoutSeconds: yt.TimeoutSeconds,
				})
				yieldedRecords[i] = store.TaskRecord{
					ID:               yt.ID,
					RunID:            runID,
					Action:           yt.Action,
					Status:           string(dag.StatusPending),
					RequiresApproval: yt.RequiresApproval,
					DependsOn:        yt.DependsOn,
					InputData:        string(yMeta),
					CreatedAt:        time.Now().UTC(),
				}
			}
			_ = o.store.CreateTasks(tCtx, yieldedRecords)
		}

		// Flush working memory state snapshot to SQLite: runs.checkpoint_state
		if bb != nil {
			snap := bb.Snapshot()
			if snapBytes, mErr := json.Marshal(snap); mErr == nil {
				_ = o.store.UpdateRunStatus(tCtx, runID, string(dag.RunStatusRunning), string(snapBytes), 0)
			}
		}

		return result, yielded, nil
	}

	// Human-in-the-loop approval hook
	approvalHook := func(aCtx context.Context, task *dag.Task) (bool, error) {
		o.events.Publish(Event{
			RunID:   runID,
			TaskID:  task.ID,
			Type:    EventTaskWaitingApproval,
			Payload: map[string]any{"action": task.Action, "target_paths": task.TargetPaths},
		})
		_ = o.store.UpdateTaskStatus(aCtx, task.ID, string(dag.StatusWaitingApproval), nil, "")

		// Flush checkpoint when entering waiting approval
		if bb != nil {
			snap := bb.Snapshot()
			if snapBytes, mErr := json.Marshal(snap); mErr == nil {
				_ = o.store.UpdateRunStatus(aCtx, runID, string(dag.RunStatusWaitingApproval), string(snapBytes), 0)
			}
		}

		if o.config.ApprovalHandler != nil {
			return o.config.ApprovalHandler(aCtx, task)
		}
		// Headless pause
		return false, nil
	}

	// Initialize Scheduler
	sched, err := dag.NewScheduler(
		tasks,
		wrappedTaskHandler,
		dag.WithMaxConcurrency(o.config.MaxConcurrency),
		dag.WithBlackboard(bb),
		dag.WithStore(o.store, runID),
		dag.WithApprovalHandler(approvalHook),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DAG scheduler: %w", err)
	}

	// Run execution loop
	result, schedErr := sched.Run(ctx)

	// Mark cascading skipped tasks in store
	if result != nil {
		for _, skippedID := range result.SkippedTasks {
			_ = o.store.UpdateTaskStatus(ctx, skippedID, string(dag.StatusSkipped), nil, "skipped due to dependency failure")
		}
		for _, waitingID := range result.WaitingTasks {
			_ = o.store.UpdateTaskStatus(ctx, waitingID, string(dag.StatusWaitingApproval), nil, "")
		}

		// Flush final state snapshot and run status
		var snapBytes []byte
		if bb != nil {
			snapBytes, _ = json.Marshal(bb.Snapshot())
		}
		totalTokens := 0
		if o.workerClient != nil {
			totalTokens = o.workerClient.TotalTokens()
			result.TotalTokens = totalTokens
			result.PromptTokens = o.workerClient.PromptTokens()
			result.CompletionTokens = o.workerClient.CompletionTokens()
			result.CachedTokens = o.workerClient.CachedTokens()
		}
		_ = o.store.UpdateRunStatus(ctx, runID, string(result.Status), string(snapBytes), totalTokens)

		// Emit EventRunFinished
		o.events.Publish(Event{
			RunID:   runID,
			Type:    EventRunFinished,
			Payload: result,
		})
	}

	return result, schedErr
}
