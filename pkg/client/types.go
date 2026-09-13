package client

import (
	"time"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/orchestrator"
	"github.com/fds1288/reminis/internal/rules"
	"github.com/fds1288/reminis/internal/store"
)

// RunRequest defines the execution request parameters for a workflow run.
type RunRequest struct {
	Goal           string `json:"goal"`
	SessionID      string `json:"session_id,omitempty"`
	Model          string `json:"model,omitempty"`
	MaxConcurrency int    `json:"max_concurrency,omitempty"`
	RulesPath      string `json:"rules_path,omitempty"`
	AutoApprove    bool   `json:"auto_approve,omitempty"`
	Manifest       string `json:"manifest,omitempty"`
}

// FailedTaskInfo describes the failed task details if a run terminates with failure.
type FailedTaskInfo struct {
	TaskID string `json:"task_id"`
	Action string `json:"action"`
	Error  string `json:"error"`
}

// RunResponse captures the execution outcome of a workflow run.
type RunResponse struct {
	RunID          string          `json:"run_id"`
	Status         string          `json:"status"`
	CompletedTasks []string        `json:"completed_tasks"`
	SkippedTasks   []string        `json:"skipped_tasks"`
	WaitingTasks   []string        `json:"waiting_tasks,omitempty"`
	FailedTask     *FailedTaskInfo `json:"failed_task,omitempty"`
	CanResume      bool            `json:"can_resume"`
	BlackboardKeys []string        `json:"blackboard_keys,omitempty"`
	TotalTokens    int             `json:"total_tokens"`
	Error          string          `json:"error,omitempty"`
}

// Fact represents an architectural memory fact or learned convention.
type Fact struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id,omitempty"`
	Topic     string    `json:"topic"`
	Content   string    `json:"content"`
	Scope     string    `json:"scope"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CompiledPrefixInfo contains discovery audit details and caching metadata for rules.
type CompiledPrefixInfo struct {
	Hash       string `json:"hash"`
	Content    string `json:"content"`
	TokenCount int    `json:"token_count"`
	Source     string `json:"source"`
	Path       string `json:"path"`
}

// Option configures Reminis client settings.
type Option func(*options)

type options struct {
	dbPath         string
	model          string
	maxConcurrency int
	artifactsDir   string
	rulesPath      string
	workspaceDir   string
	globalDir      string
	baseURL        string
	apiKey         string
	autoApprove    bool
	customStore           *store.Store
	customPlanner         orchestrator.PlanRunner
	customHandler         dag.TaskHandler
	customApprovalHandler dag.ApprovalHandler
}

// WithDBPath sets the path to the SQLite database file.
func WithDBPath(path string) Option {
	return func(o *options) {
		o.dbPath = path
	}
}

// WithModel sets the default LLM model identifier.
func WithModel(model string) Option {
	return func(o *options) {
		o.model = model
	}
}

// WithMaxConcurrency sets the maximum number of parallel worker goroutines.
func WithMaxConcurrency(concurrency int) Option {
	return func(o *options) {
		o.maxConcurrency = concurrency
	}
}

// WithArtifactsDir sets the filesystem directory for large spilled Blackboard artifacts.
func WithArtifactsDir(dir string) Option {
	return func(o *options) {
		o.artifactsDir = dir
	}
}

// WithRulesPath sets an explicit file path to user rules.
func WithRulesPath(path string) Option {
	return func(o *options) {
		o.rulesPath = path
	}
}

// WithWorkspaceDir sets the workspace root directory for rule discovery.
func WithWorkspaceDir(dir string) Option {
	return func(o *options) {
		o.workspaceDir = dir
	}
}

// WithGlobalDir sets the global config directory for rule discovery.
func WithGlobalDir(dir string) Option {
	return func(o *options) {
		o.globalDir = dir
	}
}

// WithBaseURL sets the base URL for the OpenAI-compatible worker API endpoint.
func WithBaseURL(url string) Option {
	return func(o *options) {
		o.baseURL = url
	}
}

// WithAPIKey sets the API key for the OpenAI-compatible worker API endpoint.
func WithAPIKey(key string) Option {
	return func(o *options) {
		o.apiKey = key
	}
}

// WithAutoApprove sets whether approval gates should be automatically approved.
func WithAutoApprove(auto bool) Option {
	return func(o *options) {
		o.autoApprove = auto
	}
}

// WithStore configures an existing store.Store instance (useful for testing).
func WithStore(s *store.Store) Option {
	return func(o *options) {
		o.customStore = s
	}
}

// WithPlanner configures a custom PlanRunner (useful for testing or customized planning).
func WithPlanner(p orchestrator.PlanRunner) Option {
	return func(o *options) {
		o.customPlanner = p
	}
}

// WithTaskHandler configures a custom dag.TaskHandler (useful for testing).
func WithTaskHandler(h dag.TaskHandler) Option {
	return func(o *options) {
		o.customHandler = h
	}
}

// WithApprovalHandler configures a custom dag.ApprovalHandler (e.g. for interactive TTY approvals).
func WithApprovalHandler(h dag.ApprovalHandler) Option {
	return func(o *options) {
		o.customApprovalHandler = h
	}
}

// Helper to convert internal store.FactRecord to client.Fact
func fromStoreFact(r store.FactRecord) Fact {
	return Fact{
		ID:        r.ID,
		SessionID: r.SessionID,
		Topic:     r.Topic,
		Content:   r.Content,
		Scope:     r.Scope,
		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
	}
}

// Helper to build rules.DiscoveryOptions from client options
func (o *options) toDiscoveryOptions() rules.DiscoveryOptions {
	return rules.DiscoveryOptions{
		CLIPath:      o.rulesPath,
		WorkspaceDir: o.workspaceDir,
		GlobalDir:    o.globalDir,
	}
}
