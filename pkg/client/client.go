package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/memory"
	"github.com/fds1288/reminis/internal/orchestrator"
	"github.com/fds1288/reminis/internal/rules"
	"github.com/fds1288/reminis/internal/session"
	"github.com/fds1288/reminis/internal/store"
	"github.com/fds1288/reminis/internal/worker"
)

// Client provides a clean, thread-safe public Go SDK for Reminis orchestration,
// session management, rules discovery, and long-term memory.
type Client struct {
	store      *store.Store
	ownsStore  bool
	sessionMgr *session.Manager
	memService *memory.MemoryService
	orch       *orchestrator.Orchestrator
	opts       options
	mu         sync.RWMutex
}

// New creates and initializes a new Client instance with functional options.
func New(opts ...Option) (*Client, error) {
	cfg := options{
		maxConcurrency: 4,
		artifactsDir:   filepath.Join(os.TempDir(), "reminis_artifacts"),
		model:          os.Getenv("OPENAI_MODEL"),
		baseURL:        os.Getenv("OPENAI_BASE_URL"),
		apiKey:         os.Getenv("OPENAI_API_KEY"),
	}
	if cfg.model == "" {
		cfg.model = "gpt-4o-mini"
	}
	if cfg.baseURL == "" {
		cfg.baseURL = "https://api.openai.com/v1"
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	var dbStore *store.Store
	var ownsStore bool

	if cfg.customStore != nil {
		dbStore = cfg.customStore
		ownsStore = false
	} else {
		dbPath := cfg.dbPath
		if dbPath == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				dbPath = ".reminis.db"
			} else {
				dbDir := filepath.Join(homeDir, ".reminis")
				_ = os.MkdirAll(dbDir, 0755)
				dbPath = filepath.Join(dbDir, "reminis.db")
			}
		} else {
			dir := filepath.Dir(dbPath)
			if dir != "" && dir != "." {
				_ = os.MkdirAll(dir, 0755)
			}
		}

		var err error
		dbStore, err = store.New(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open sqlite store at %s: %w", dbPath, err)
		}
		ownsStore = true
	}

	sessionMgr := session.NewManager(dbStore)
	memService := memory.NewMemoryService(dbStore)

	var workerOpts []worker.ClientOption
	if len(cfg.headers) > 0 {
		workerOpts = append(workerOpts, worker.WithHeaders(cfg.headers))
	}
	workerClient := worker.NewClient(cfg.baseURL, cfg.apiKey, cfg.model, workerOpts...)

	var approvalHandler dag.ApprovalHandler
	if cfg.customApprovalHandler != nil {
		approvalHandler = cfg.customApprovalHandler
	} else if cfg.autoApprove {
		approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
			return true, nil
		}
	} else {
		approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
			return false, nil // headless pause in WAITING_APPROVAL
		}
	}

	orchConfig := orchestrator.Config{
		DBStore:         dbStore,
		WorkerClient:    workerClient,
		ArtifactsDir:    cfg.artifactsDir,
		MaxConcurrency:  cfg.maxConcurrency,
		Model:           cfg.model,
		RulesOpts:       cfg.toDiscoveryOptions(),
		ApprovalHandler: approvalHandler,
		Planner:         cfg.customPlanner,
		TaskHandler:     cfg.customHandler,
	}

	orch := orchestrator.New(orchConfig)

	return &Client{
		store:      dbStore,
		ownsStore:  ownsStore,
		sessionMgr: sessionMgr,
		memService: memService,
		orch:       orch,
		opts:       cfg,
	}, nil
}

// Run executes a goal workflow end-to-end within an active or newly created session.
func (c *Client) Run(ctx context.Context, req RunRequest) (*RunResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sessionID := req.SessionID
	if sessionID == "" {
		workspaceDir := c.opts.workspaceDir
		if workspaceDir == "" {
			workspaceDir = "."
		}
		sess, err := c.sessionMgr.StartSession(ctx, workspaceDir, "action")
		if err != nil {
			return nil, fmt.Errorf("failed to start session: %w", err)
		}
		sessionID = sess.ID
	}

	orch := c.orch
	// Handle per-request overrides if necessary
	if req.AutoApprove != c.opts.autoApprove || (req.MaxConcurrency > 0 && req.MaxConcurrency != c.opts.maxConcurrency) || (req.Model != "" && req.Model != c.opts.model) {
		concurrency := c.opts.maxConcurrency
		if req.MaxConcurrency > 0 {
			concurrency = req.MaxConcurrency
		}
		model := c.opts.model
		if req.Model != "" {
			model = req.Model
		}

		var approvalHandler dag.ApprovalHandler
		if req.AutoApprove || c.opts.autoApprove {
			approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
				return true, nil
			}
		} else if c.opts.customApprovalHandler != nil {
			approvalHandler = c.opts.customApprovalHandler
		} else {
			approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
				return false, nil
			}
		}

		rulesOpts := c.opts.toDiscoveryOptions()
		if req.RulesPath != "" {
			rulesOpts.CLIPath = req.RulesPath
		}

		var workerOpts []worker.ClientOption
		if len(c.opts.headers) > 0 {
			workerOpts = append(workerOpts, worker.WithHeaders(c.opts.headers))
		}
		workerClient := worker.NewClient(c.opts.baseURL, c.opts.apiKey, model, workerOpts...)

		orch = orchestrator.New(orchestrator.Config{
			DBStore:         c.store,
			WorkerClient:    workerClient,
			ArtifactsDir:    c.opts.artifactsDir,
			MaxConcurrency:  concurrency,
			Model:           model,
			Manifest:        req.Manifest,
			RulesOpts:       rulesOpts,
			ApprovalHandler: approvalHandler,
			Planner:         c.opts.customPlanner,
			TaskHandler:     c.opts.customHandler,
		})
	}

	dagResult, err := orch.Run(ctx, sessionID, req.Goal)
	if err != nil && dagResult == nil {
		return nil, err
	}

	return toRunResponse(dagResult, err), nil
}

// Resume restarts execution of a paused or interrupted run from its SQLite checkpoint.
func (c *Client) Resume(ctx context.Context, runID string) (*RunResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dagResult, err := c.orch.Resume(ctx, runID)
	if err != nil && dagResult == nil {
		return nil, err
	}

	return toRunResponse(dagResult, err), nil
}

// Approve records human approval for a paused task and triggers run resumption.
func (c *Client) Approve(ctx context.Context, runID string, taskID string, scope string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if scope == "" {
		scope = "action"
	}

	if err := c.store.ApproveTask(ctx, runID, taskID, scope); err != nil {
		return fmt.Errorf("failed to record task approval: %w", err)
	}

	_, err := c.orch.Resume(ctx, runID)
	return err
}

// SearchFacts queries durable architectural facts matching topic and scope.
func (c *Client) SearchFacts(ctx context.Context, topic string, scope string) ([]Fact, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	internalFacts, err := c.memService.SearchFacts(ctx, topic, scope)
	if err != nil {
		return nil, err
	}

	result := make([]Fact, len(internalFacts))
	for i, f := range internalFacts {
		result[i] = Fact{
			ID:        f.ID,
			SessionID: f.SessionID,
			Topic:     f.Topic,
			Content:   f.Content,
			Scope:     f.Scope,
			CreatedAt: f.CreatedAt,
			UpdatedAt: f.UpdatedAt,
		}
	}
	return result, nil
}

// SaveFact persists or updates an architectural fact in durable SQLite storage.
func (c *Client) SaveFact(ctx context.Context, topic string, content string, scope string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if scope == "" {
		scope = "project"
	}

	return c.memService.SaveFact(ctx, memory.Fact{
		Topic:   topic,
		Content: content,
		Scope:   scope,
	})
}

// ListFacts lists facts for a specific session, or all facts if sessionID is empty.
func (c *Client) ListFacts(ctx context.Context, sessionID string) ([]Fact, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var records []store.FactRecord
	var err error

	if sessionID != "" {
		records, err = c.store.ListFactsBySession(ctx, sessionID)
	} else {
		records, err = c.store.SearchFacts(ctx, "", "")
	}

	if err != nil {
		return nil, err
	}

	result := make([]Fact, len(records))
	for i, r := range records {
		result[i] = fromStoreFact(r)
	}
	return result, nil
}

// GetCompiledPrefix resolves the rules cascade and returns compiled prefix metadata.
func (c *Client) GetCompiledPrefix(ctx context.Context) (*CompiledPrefixInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	disc, err := rules.Discover(c.opts.toDiscoveryOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to discover rules: %w", err)
	}

	compiled := rules.CompileDiscovered(disc)

	return &CompiledPrefixInfo{
		Hash:       compiled.Hash,
		Content:    compiled.Content,
		TokenCount: compiled.TokenCount,
		Source:     string(disc.Source),
		Path:       disc.Path,
	}, nil
}

// Events returns the thread-safe telemetry event broadcaster.
func (c *Client) Events() *orchestrator.EventEmitter {
	return c.orch.Events()
}

// Store returns the underlying store.Store (useful for inspection or testing).
func (c *Client) Store() *store.Store {
	return c.store
}

// Close closes the underlying storage connection if owned by this client.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ownsStore && c.store != nil {
		return c.store.Close()
	}
	return nil
}

func toRunResponse(dagResult *dag.RunResult, err error) *RunResponse {
	resp := &RunResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	if dagResult != nil {
		resp.RunID = dagResult.RunID
		resp.Status = string(dagResult.Status)
		resp.CompletedTasks = dagResult.CompletedTasks
		resp.SkippedTasks = dagResult.SkippedTasks
		resp.WaitingTasks = dagResult.WaitingTasks
		resp.CanResume = dagResult.CanResume
		resp.BlackboardKeys = dagResult.BlackboardKeys
		if dagResult.FailedTask != nil {
			resp.FailedTask = &FailedTaskInfo{
				TaskID: dagResult.FailedTask.TaskID,
				Action: dagResult.FailedTask.Action,
				Error:  dagResult.FailedTask.Error,
			}
		}
	} else if err != nil {
		resp.Status = string(dag.RunStatusFailed)
	}
	return resp
}
