# Reminis Architecture Specification

**Version:** 0.5.0  
**Status:** Approved Specification  
**Language:** Go (1.23+)  
**Module:** `github.com/fds1288/reminis`  

---

## 1. Motivation & Problem Statement

Modern LLM agents suffer from the **quadratic context problem**:
1. **Context Bloat:** Traditional ReAct loops accumulate chat histories (`[turn_1, turn_2, ... turn_N]`). By turn 30, each request uploads tens of thousands of tokens.
2. **Attention Degradation:** As context balloons, LLMs lose focus (*needle-in-a-haystack* degradation), contradict earlier instructions, and hallucinate.
3. **TPM / Rate Limit Exhaustion:** Re-sending 30,000 tokens on every step rapidly exhausts API tokens-per-minute (TPM) quotas or saturates local CPU memory bandwidth.

### The Solution: The OS-LLM Architecture
- **CPU:** LLM (pure stateless compute).
- **RAM / L1 (Working Memory):** Thread-safe Blackboard (scoped strictly to the active task graph).
- **Disk (Archival / Long-term Memory):** Embedded SQLite database (sessions, facts, task ledger).
- **Workers:** Ephemeral goroutines whose LLM context windows are destroyed immediately upon task completion.

---

## 2. Core Architecture

```
                       ┌────────────────────────┐
                       │    User / Objective    │
                       └───────────┬────────────┘
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │     Planner Engine     │
                       │  (1 single LLM call)   │
                       └───────────┬────────────┘
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │   Task Graph (DAG)     │
                       │  Dependency Resolution │
                       └───────────┬────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                                         ▼
   ┌──────────────────────┐                  ┌──────────────────────┐
   │  Worker (Task A)     │                  │  Worker (Task B)     │
   │  • Micro-prompt      │                  │  • Micro-prompt      │
   │  • Goroutine 1       │                  │  • Goroutine 2       │
   │  • Max 3 Tool Calls  │                  │  • Max 3 Tool Calls  │
   │  • Context discarded │                  │  • Context discarded │
   └──────────┬───────────┘                  └──────────┬───────────┘
              │                                         │
              └────────────────────┬────────────────────┘
                                   ▼
                       ┌────────────────────────┐
                       │   Thread-safe State    │
                       │     (Blackboard)       │
                       └───────────┬────────────┘
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │     Pure-Go SQLite     │
                       │   (WAL Mode Storage)   │
                       └────────────────────────┘
```

---

## 3. Core Components & Resilience Engine

### 3.1 Blackboard (Working Memory)
A concurrent, thread-safe memory ledger representing the ground truth of the active workflow run.
- **Implementation:** `sync.RWMutex` guarding an in-memory key-value map.
- **Isolation:** Workers receive an immutable sub-slice containing strictly the keys declared in their `InputKeys`.
- **Checkpointing:** State snapshots are flushed to SQLite after every task transition to enable resume-on-failure without recalculating completed tasks.

### 3.2 Task Graph & Failure Handling (3-Level Resilience)
1. **Level 1 (Transient Infra Errors):** 429 rate limits, connection timeouts, or 500 API errors are retried up to 3 times in pure Go using exponential backoff with jitter (zero LLM tokens burned).
2. **Level 2 (Semantic / Validation Errors):** If the LLM generates invalid JSON, broken syntax, or fails an assertion, a local reflection prompt (max 2 attempts) is sent to that specific worker with the exact error.
3. **Level 3 (Hard Blockers & Checkpoint Freezing):**
   - If retries and reflection fail, the task is marked `FAILED`.
   - Dependent child tasks are cascadingly marked `SKIPPED`.
   - Independent, parallel branches continue to completion.
   - The run transitions to `FAILED_WITH_CHECKPOINT`.
   - All state is preserved in SQLite, allowing the host agent or user to resolve the blocker and run `reminis resume <run_id>`.
   - *Optional:* When `--auto-recover` is enabled, Reminis executes one final surgical call to the Planner with the full post-run Blackboard state to formulate an alternative branch.

### 3.3 Human-in-the-Loop Approval Gates
Potentially destructive or high-impact actions (file deletion, database drops, git force-pushes, deployments) can be gated by an approval requirement:
- **Task Flag:** `RequiresApproval: true`.
- **State Transition:** The task pauses in `WAITING_APPROVAL`, while non-dependent tasks continue execution.
- **Granular Permission Scopes:**
  1. **Action (`action`):** Approves only this single task execution.
  2. **Session (`session`):** Approves all subsequent actions of the same capability category (e.g. `fs:delete`, `git:push`) for the duration of the active session without further prompting.
  3. **Permanent (`permanent` / allowlist):** Configured via CLI flag (`--auto-approve=all`) or configuration file for autonomous environments.

### 3.4 Ephemeral Workers & Bounded Tool Execution (Max 3 Turns)
Workers are permitted to execute real-world tools (`bash`, `read_file`, `write_file`, `git`), governed by strict bounding rules:
1. **Hard Budget:** Maximum **3 tool execution turns** per worker.
2. **Map-Reduce / Fan-Out Fan-In Pattern:**
   - If an inspection requires more than 3 tool calls (e.g. analyzing 10 different files or multiple commits), the task is chunked across multiple parallel workers.
   - Each worker inspects its assigned partition and writes partial observations to the Blackboard.
   - An **Aggregator / Reducer Worker** reads the partial Blackboard entries and synthesizes the unified conclusion.
3. **Context Destruction:** Large tool outputs (e.g., 50,000 tokens of raw `git diff` or build logs) exist only within the ephemeral worker's temporary process. Once the worker summarizes its findings into the Blackboard, its entire conversation context is garbage collected.

### 3.5 Structured Telemetry & Task Structs
```go
type TaskStatus string

const (
    StatusPending         TaskStatus = "PENDING"
    StatusRunning         TaskStatus = "RUNNING"
    StatusWaitingApproval TaskStatus = "WAITING_APPROVAL"
    StatusCompleted       TaskStatus = "COMPLETED"
    StatusFailed          TaskStatus = "FAILED"
    StatusSkipped         TaskStatus = "SKIPPED"
)

type Task struct {
    ID               string     `json:"id"`
    Action           string     `json:"action"`
    DependsOn        []string   `json:"depends_on"`
    InputKeys        []string   `json:"input_keys"`
    OutputKeys       []string   `json:"output_keys"`
    RequiresApproval bool       `json:"requires_approval"`
    Status           TaskStatus `json:"status"`
    Result           any        `json:"result,omitempty"`
    Error            string     `json:"error,omitempty"`
}

type RunResult struct {
    RunID           string       `json:"run_id"`
    Status          RunStatus    `json:"status"` // COMPLETED, FAILED_WITH_CHECKPOINT, WAITING_APPROVAL
    CompletedTasks  []string     `json:"completed_tasks"`
    SkippedTasks    []string     `json:"skipped_tasks"`
    WaitingTasks    []string     `json:"waiting_tasks,omitempty"`
    FailedTask      *TaskFailure `json:"failed_task,omitempty"`
    CanResume       bool         `json:"can_resume"`
    BlackboardKeys  []string     `json:"blackboard_keys"`
}
```

### 3.6 Archival Store (SQLite Pure-Go)
- **Driver:** `modernc.org/sqlite` (100% CGO-free, cross-compilable to any OS/architecture).
- **Concurrency Settings:** `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=5000;`.
- **Database Schema:**

```sql
-- Active and past workflow runs
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    goal TEXT NOT NULL,
    status TEXT NOT NULL, -- PENDING, RUNNING, WAITING_APPROVAL, COMPLETED, FAILED_WITH_CHECKPOINT
    total_tokens INTEGER DEFAULT 0,
    checkpoint_state TEXT, -- Serialized Blackboard JSON
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME
);

-- Discrete tasks within a DAG
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    status TEXT NOT NULL, -- PENDING, RUNNING, WAITING_APPROVAL, COMPLETED, FAILED, SKIPPED
    requires_approval BOOLEAN DEFAULT FALSE,
    depends_on TEXT,      -- JSON array of task IDs
    input_data TEXT,      -- JSON payload of injected inputs
    output_data TEXT,     -- JSON payload of worker outputs
    tool_calls_count INTEGER DEFAULT 0,
    duration_ms INTEGER DEFAULT 0,
    tokens INTEGER DEFAULT 0,
    error TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Sessions (Engram-compatible session tracking)
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    project_path TEXT NOT NULL,
    status TEXT NOT NULL, -- ACTIVE, CLOSED
    summary TEXT,
    permissions_scope TEXT DEFAULT 'action', -- action, session, permanent
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME
);

-- Persistent facts / semantic memory
CREATE TABLE IF NOT EXISTS facts (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    topic TEXT NOT NULL,
    content TEXT NOT NULL,
    scope TEXT DEFAULT 'project', -- project, user, global
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_facts_topic ON facts(topic);
CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON tasks(run_id);
```

---

## 4. Interfaces & Consumption Models

1. **Go Library (`pkg/client`):** Directly imported into Go applications (like AGIS) for zero-latency, embedded agent orchestration.
2. **Model Context Protocol (`reminis mcp`):** Exposes JSON-RPC over stdio so external IDEs and agents (Antigravity, Hermes, Cursor) can invoke Reminis memory and DAG tools natively:
   - `reminis_session_start(project_path)`
   - `reminis_save_fact(topic, content)`
   - `reminis_query_facts(query)`
   - `reminis_run_workflow(goal)`
   - `reminis_resume_workflow(run_id)`
   - `reminis_approve_task(run_id, task_id, scope)`
3. **CLI (`cmd/reminis`):** Standalone terminal command for running tasks, inspecting memory, and managing sessions:
   - `reminis run "Crear endpoint de login"`
   - `reminis resume <run_id>`
   - `reminis approve <run_id> <task_id> [--scope=action|session|permanent]`
   - `reminis mem search "sqlite"`

---

## 5. Technical Specifications & Dependencies

| Component | Choice | Rationale |
| :--- | :--- | :--- |
| **Language** | Go 1.23+ | Native goroutines, fast startup, single static binary. |
| **Database Driver** | `modernc.org/sqlite` | Pure Go, zero CGO requirements, runs on any ARM64/AMD64 OS. |
| **Concurrency** | `sync.RWMutex`, `golang.org/x/sync/errgroup` | Race-free concurrent DAG dispatch. |
| **LLM Protocol** | OpenAI-compatible HTTP | Native compatibility with local `llama-server`, Ollama, Gemini, Groq, OpenRouter. |

---

## 6. Implementation Roadmap

- [ ] **Milestone 1: Core Engine & Resilience**
  - Implement `internal/blackboard` with concurrent snapshotting and checkpoint serialization.
  - Implement `internal/dag` with dependency resolution, cycle checking, cascading skip on failure, and `WAITING_APPROVAL` pause.
  - Implement `internal/store` with pure-Go SQLite migrations and CRUD.
- [ ] **Milestone 2: Worker & Tool Execution (Max 3 Turns)**
  - Implement HTTP client for OpenAI-compatible endpoints with Level 1 exponential backoff.
  - Tool execution loop capped at 3 turns with context destruction.
  - Level 2 reflection loop.
- [ ] **Milestone 3: Planner, Map-Reduce & Orchestrator**
  - LLM-based DAG generator with automatic chunking for tasks exceeding 3 tool calls.
  - Aggregator / Reducer worker pattern for combining partial findings into Blackboard.
  - Concurrent DAG execution pipeline with checkpointing and structured error reporting.
  - Implement `reminis resume <run_id>` and `reminis approve <run_id> <task_id>`.
- [ ] **Milestone 4: Sessions & Fact Retrieval (Engram-parity)**
  - `sessions` lifecycle and automatic fact extraction/indexing.
  - Topic-based and keyword-based fast search.
- [ ] **Milestone 5: Interfaces**
  - CLI commands (`reminis run`, `reminis resume`, `reminis approve`, `reminis mem`, `reminis status`).
  - Native MCP Server (`reminis mcp`).

---

## 7. Future Vision & Extensibility

### 7.1 Deep Integration with CodeGraph (Deterministic Dependency Discovery)
Currently, the Planner uses LLM reasoning to decompose a goal into a DAG. In future iterations, Reminis can connect natively to **CodeGraph**:
1. **CodeGraph-Directed Planning:** When tasked with a codebase refactor or bugfix, Reminis queries CodeGraph CLI/MCP (`codegraph callers`, `codegraph impact`, `codegraph affected`) before prompting the Planner.
2. **True Deterministic DAGs:** Instead of hallucinating dependencies, the DAG is constructed directly from the AST and symbol call graph:
   - Node 1: Target interface / symbol modification.
   - Nodes 2..N: Parallel worker goroutines updating affected callers identified by CodeGraph.
3. **Blast-Radius Verification:** Post-execution verification queries CodeGraph to confirm no broken references remain across the workspace.

### 7.2 Scaling to a Unified Cognitive Layer (All-in-One Engine)
While Reminis currently focuses on L1 Working Memory and DAG orchestration, its pure-Go SQLite persistence architecture allows seamless expansion into a self-contained cognitive memory engine:
1. **Vector & Full-Text Search (Pure-Go SQLite FTS5 / sqlite-vec):** Embedding support for local semantic similarity searches without external vector databases.
2. **Episodic Memory Clustering:** Automatically clustering past successful runs into reusable execution templates ("How I previously solved migration X").
3. **Ecosystem Unification:** Serving as the unified memory and execution backbone for lightweight Go agents (like AGIS), eliminating the need for separate Python-based memory sidecars.
