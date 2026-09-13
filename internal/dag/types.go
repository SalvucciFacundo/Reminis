package dag

import "encoding/json"

// TaskStatus represents the lifecycle state of a task in the graph.
type TaskStatus string

const (
	StatusPending         TaskStatus = "PENDING"
	StatusRunning         TaskStatus = "RUNNING"
	StatusWaitingApproval TaskStatus = "WAITING_APPROVAL"
	StatusCompleted       TaskStatus = "COMPLETED"
	StatusFailed          TaskStatus = "FAILED"
	StatusTimedOut        TaskStatus = "TIMED_OUT"
	StatusSkipped         TaskStatus = "SKIPPED"
)

// RunStatus represents the aggregate state of a workflow run.
type RunStatus string

const (
	RunStatusCompleted            RunStatus = "COMPLETED"
	RunStatusFailedWithCheckpoint RunStatus = "FAILED_WITH_CHECKPOINT"
	RunStatusWaitingApproval      RunStatus = "WAITING_APPROVAL"
	RunStatusRunning              RunStatus = "RUNNING"
	RunStatusFailed               RunStatus = "FAILED"
)

// TaskFailure captures failure context for a task.
type TaskFailure struct {
	TaskID string `json:"task_id"`
	Action string `json:"action"`
	Error  string `json:"error"`
}

// Task defines an executable unit of work within the DAG.
type Task struct {
	ID               string          `json:"id"`
	Action           string          `json:"action"`
	DependsOn        []string        `json:"depends_on"`
	InputKeys        []string        `json:"input_keys"`
	OutputKeys       []string        `json:"output_keys"`
	RequiresApproval bool            `json:"requires_approval"`
	TimeoutSeconds   int             `json:"timeout_seconds,omitempty"` // Hard deadline override (0 = silence watchdog)
	TargetPaths      []string        `json:"target_paths,omitempty"`    // Paths locked during execution
	Status           TaskStatus      `json:"status"`
	Result           json.RawMessage `json:"result,omitempty"`
	YieldedTasks     []Task          `json:"yielded_tasks,omitempty"`   // Dynamic sub-tasks for scheduler.Expand
	Error            string          `json:"error,omitempty"`
}

// RunResult summarizes the execution outcome of the task graph.
type RunResult struct {
	RunID          string       `json:"run_id"`
	Status         RunStatus    `json:"status"`
	CompletedTasks []string     `json:"completed_tasks"`
	SkippedTasks   []string     `json:"skipped_tasks"`
	WaitingTasks   []string     `json:"waiting_tasks,omitempty"`
	FailedTask     *TaskFailure `json:"failed_task,omitempty"`
	CanResume      bool         `json:"can_resume"`
	BlackboardKeys []string     `json:"blackboard_keys"`
}
