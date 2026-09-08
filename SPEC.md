# Reminis Architecture Specification

**Version:** 0.3.0  
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
When a task encounters an error during execution, Reminis applies a tiered resolution strategy:

1. **Level 1 (Transient Infra Errors):** 429 rate limits, connection timeouts, or 500 API errors are retried up to 3 times in pure Go using exponential backoff with jitter (zero LLM tokens burned).
2. **Level 2 (Semantic / Validation Errors):** If the LLM generates invalid JSON, broken syntax, or fails an assertion, a local reflection prompt (max 2 attempts) is sent to that specific worker with the exact error.
3. **Level 3 (Hard Blockers & Checkpoint Freezing):**
   - If retries and reflection fail, the task is marked `FAILED`.
   - Dependent child tasks are cascadingly marked `SKIPPED`.
   - Independent, parallel branches continue to completion.
   - The run transitions to `FAILED_WITH_CHECKPOINT`.
   - All state is preserved in SQLite, allowing the host agent or user to resolve the blocker and run `reminis resume <run_id>`.
   - *Optional:* When `--auto-recover` is enabled, Reminis executes one final surgical call to the Planner with the full post-run Blackboard state to formulate an alternative branch.

### 3.3 Structured Failure Telemetry
Reminis guarantees explicit, structured error reporting back to the host agent:
```go
type RunResult struct {
    RunID           string       `json:"run_id"`
    Status          RunStatus    `json:"status"` // COMPLETED, FAILED_WITH_CHECKPOINT
    CompletedTasks  []string     `json:"completed_tasks"`
    SkippedTasks    []string     `json:"skipped_tasks"`
    FailedTask      *TaskFailure `json:"failed_task,omitempty"`
    CanResume       bool         `json:"can_resume"`
    BlackboardKeys  []string     `json:"blackboard_keys"`
}

type TaskFailure struct {
    TaskID      string `json:"task_id"`
    Action      string `json:"action"`
    Error       string `json:"error"`
    Attempts    int    `json:"attempts"`
    LastPayload string `json:"last_payload,omitempty"`
}
```

### 3.4 Ephemeral Worker Engine
- **Stateless Request:** Combines:
  1. System Prompt (Task role).
  2. Blackboard inputs (strict JSON slice).
  3. Action Directive.
- **Budget:** 300 to 800 tokens total.
- **Destruction:** Response is parsed into typed outputs and written to the Blackboard. The worker's LLM conversation context is completely discarded.

### 3.5 Archival Store (SQLite Pure-Go)
- **Driver:** `modernc.org/sqlite` (100% CGO-free, cross-compilable to any OS/architecture).
- **Concurrency Settings:** `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=5000;`.
- **Database Schema:**

```sql
-- Active and past workflow runs
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    goal TEXT NOT NULL,
    status TEXT NOT NULL, -- PENDING, RUNNING, COMPLETED, FAILED_WITH_CHECKPOINT
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
    status TEXT NOT NULL, -- PENDING, RUNNING, COMPLETED, FAILED, SKIPPED
    depends_on TEXT,      -- JSON array of task IDs
    input_data TEXT,      -- JSON payload of injected inputs
    output_data TEXT,     -- JSON payload of worker outputs
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
3. **CLI (`cmd/reminis`):** Standalone terminal command for running tasks, inspecting memory, and managing sessions:
   - `reminis run "Crear endpoint de login"`
   - `reminis resume <run_id>`
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
  - Implement `internal/dag` with dependency resolution, cycle checking, and cascading skip on failure.
  - Implement `internal/store` with pure-Go SQLite migrations and CRUD.
- [ ] **Milestone 2: Worker & LLM Client**
  - Implement HTTP client for OpenAI-compatible endpoints with Level 1 exponential backoff.
  - Ephemeral prompt synthesizer and Level 2 reflection loop.
- [ ] **Milestone 3: Planner & Orchestrator**
  - LLM-based DAG generator (Goal -> Task list with dependencies).
  - Concurrent DAG execution pipeline with checkpointing and structured error reporting.
  - Implement `reminis resume <run_id>`.
- [ ] **Milestone 4: Sessions & Fact Retrieval (Engram-parity)**
  - `sessions` lifecycle and automatic fact extraction/indexing.
  - Topic-based and keyword-based fast search.
- [ ] **Milestone 5: Interfaces**
  - CLI commands (`reminis run`, `reminis resume`, `reminis mem`, `reminis status`).
  - Native MCP Server (`reminis mcp`).
