package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/fds1288/reminis/internal/blackboard"
	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/rules"
)

// Resume reconstitutes a paused or interrupted workflow run from its persisted
// SQLite checkpoint and resumes execution of remaining tasks without repeating completed ones.
//
// Steps:
// 1. Loads RunRecord and TaskRecords from store.
// 2. Reconstructs blackboard.Blackboard by restoring from checkpoint_state.
// 3. Rebuilds the DAG scheduler containing only non-completed tasks (skipping COMPLETED and SKIPPED).
// 4. Resumes execution and checkpoints progress to SQLite until completion.
func (o *Orchestrator) Resume(ctx context.Context, runID string) (*dag.RunResult, error) {
	if o.store == nil {
		return nil, errors.New("orchestrator store cannot be nil")
	}

	// 1. Load RunRecord and TaskRecords from SQLite store
	run, err := o.store.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve run %q: %w", runID, err)
	}

	taskRecords, err := o.store.GetTasks(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve tasks for run %q: %w", runID, err)
	}
	if len(taskRecords) == 0 {
		return nil, fmt.Errorf("no tasks found for run %q", runID)
	}

	// 2. Reconstruct blackboard.Blackboard by restoring from checkpoint_state
	runArtifactsDir := filepath.Join(o.config.ArtifactsDir, runID, "artifacts")
	bb := blackboard.New(runArtifactsDir)

	if run.CheckpointState != "" {
		var entries map[string]blackboard.MemoryEntry
		if err := json.Unmarshal([]byte(run.CheckpointState), &entries); err == nil {
			bb.Restore(entries)
		}
	}

	// 3. Rebuild the DAG scheduler containing tasks:
	// Tasks marked COMPLETED or SKIPPED retain their terminal status so they are NOT re-executed.
	// Tasks in PENDING, WAITING_APPROVAL, or FAILED are set to StatusPending for re-evaluation.
	dagTasks := make([]dag.Task, len(taskRecords))
	for i, tr := range taskRecords {
		var meta taskMetadata
		if tr.InputData != "" {
			_ = json.Unmarshal([]byte(tr.InputData), &meta)
		}

		timeoutSec := meta.TimeoutSeconds
		if timeoutSec > 0 && timeoutSec < 60 {
			timeoutSec = 60
		}

		t := dag.Task{
			ID:               tr.ID,
			Action:           tr.Action,
			DependsOn:        tr.DependsOn,
			RequiresApproval: tr.RequiresApproval,
			InputKeys:        meta.InputKeys,
			OutputKeys:       meta.OutputKeys,
			TargetPaths:      meta.TargetPaths,
			TimeoutSeconds:   timeoutSec,
		}
		if tr.OutputData != "" {
			t.Result = json.RawMessage(tr.OutputData)
		}

		switch tr.Status {
		case string(dag.StatusCompleted):
			t.Status = dag.StatusCompleted
		case string(dag.StatusSkipped):
			t.Status = dag.StatusSkipped
		default:
			t.Status = dag.StatusPending
		}

		dagTasks[i] = t
	}

	// Resolve frozen rules prefix if session exists, else re-discover
	var compiledRules string
	if run.SessionID != "" {
		if sess, sErr := o.store.GetSession(ctx, run.SessionID); sErr == nil && sess.CompiledPrefix != "" {
			compiledRules = sess.CompiledPrefix
		}
	}
	if compiledRules == "" {
		compiled, err := rules.DiscoverAndCompile(o.config.RulesOpts)
		if err == nil {
			compiledRules = compiled.Content
		} else {
			compiledRules = rules.DefaultBaseline
		}
	}

	// Update run status in store to RUNNING
	_ = o.store.UpdateRunStatus(ctx, runID, string(dag.RunStatusRunning), run.CheckpointState, run.TotalTokens)

	// Emit EventRunStarted for resumption
	o.events.Publish(Event{
		RunID:   runID,
		Type:    EventRunStarted,
		Payload: map[string]any{"goal": run.Goal, "resumed": true, "total_tasks": len(dagTasks)},
	})

	// 4. Resume execution and checkpoint progress to SQLite until completion
	return o.executeDAG(ctx, runID, dagTasks, bb, compiledRules)
}
