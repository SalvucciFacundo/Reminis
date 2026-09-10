# Reminis Architecture Specification

**Version:** 0.6.0  
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
- **Namespace Isolation:** Each task writes strictly to its dedicated namespace (`tasks.<task_id>.output`). Multiple parallel tasks cannot write to the same key.
- **4KB Size Ceiling (Pass-by-Reference):** Individual Blackboard values must not exceed 4KB. Large payloads (diffs, CSVs, build logs) are spilled to a run-scoped directory (`~/.reminis/runs/<run_id>/artifacts/`), storing only the file path in the Blackboard.
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

### 3.3 Human-in-the-Loop Approval Gates
Potentially destructive or high-impact actions (file deletion, database drops, git force-pushes, deployments) are gated by an approval requirement:
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
3. **Context Destruction:** Large tool outputs exist only within the ephemeral worker's temporary process. Once the worker summarizes its findings into the Blackboard, its entire conversation context is garbage collected.

### 3.5 Cold-Start Prevention (Global Project Manifest)
To prevent workers from hallucinating architectural conventions (e.g., using `Gin` when the project uses `net/http`), Reminis automatically injects an invariant **Project Manifest** (~100 tokens) into the Static Prefix:
```json
{
  "project_name": "example-app",
  "language": "Go 1.23",
  "frameworks": ["net/http", "modernc.org/sqlite"],
  "conventions": "standard clean architecture, idiomatic error handling"
}
```

### 3.6 Event-Driven Telemetry (Live Streaming UX)
Reminis avoids the "black box" syndrome by streaming execution state in real time via Go channels (`chan Event`) and SSE (Server-Sent Events) over MCP/HTTP:
- `EventRunStarted(run_id, total_tasks)`
- `EventTaskStarted(task_id, action)`
- `EventTaskWaitingApproval(task_id, action, diff)`
- `EventTaskCompleted(task_id, duration_ms, tokens)`
- `EventTaskFailed(task_id, error, can_retry)`
- `EventRunFinished(run_id, status, total_tokens)`

### 3.7 Prompt Caching Optimization (Prefix Stability Pattern)
Providers (Google Gemini, Anthropic Claude, OpenAI) offer prompt caching with 50%–90% cost and latency discounts when prompt prefixes remain static.

Reminis maximizes cache hits while maintaining strict context isolation by enforcing the **Prefix Stability Pattern**:
1. **Invariant Static Prefix (Cached):** Placed at the very top of every worker prompt:
   - System persona & operational rules.
   - Global Project Manifest.
   - Tool schemas & JSON definitions.
   - Standard output format specifications.
2. **Dynamic Tail (Non-Cached Append-Only):** Placed strictly at the bottom of the prompt:
   - Blackboard injected slice (`InputKeys`).
   - Specific action directive for the current node.

### 3.8 Concurrency & Storage Guardrails
1. **Topological Cycle Validation:** The DAG scheduler verifies graph acyclicity using Kahn's algorithm prior to execution. Circular dependencies are rejected immediately.
2. **SQLite Lock Contention Prevention:** Although SQLite WAL mode allows concurrent readers, writes must be serialized. Reminis manages database writes through a dedicated single-writer channel in Go with `busy_timeout=5000ms`, preventing `SQLITE_BUSY` errors during parallel worker completion bursts.
3. **Run Artifact Lifecycle:** Files spilled to `~/.reminis/runs/<run_id>/artifacts/` are tracked in the `runs` table. Ephemeral scratch data is purged upon successful run termination, while designated artifacts are retained for user inspection.

---

## 4. Structured Telemetry & Data Schemas

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

---

## 5. Database Schema (SQLite Pure-Go)

```sql
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    total_tokens INTEGER DEFAULT 0,
    checkpoint_state TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    requires_approval BOOLEAN DEFAULT FALSE,
    depends_on TEXT,
    input_data TEXT,
    output_data TEXT,
    tool_calls_count INTEGER DEFAULT 0,
    duration_ms INTEGER DEFAULT 0,
    tokens INTEGER DEFAULT 0,
    error TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    project_path TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT,
    permissions_scope TEXT DEFAULT 'action',
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME
);

CREATE TABLE IF NOT EXISTS facts (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    topic TEXT NOT NULL,
    content TEXT NOT NULL,
    scope TEXT DEFAULT 'project',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_facts_topic ON facts(topic);
CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON tasks(run_id);
```

---

## 6. Implementation Roadmap

- [ ] **Milestone 1: Core Engine & Concurrency Guardrails**
  - Implement `internal/blackboard` (thread-safe, 4KB spillover, namespacing).
  - Implement `internal/dag` (Kahn's cycle validation, dependency resolution, cascading skips, approval pauses).
  - Implement `internal/store` (pure-Go SQLite with single-writer channel and WAL mode).
- [ ] **Milestone 2: Ephemeral Worker Engine**
  - OpenAI-compatible HTTP client with Level 1 exponential backoff.
  - Bounded tool execution loop (max 3 turns) with scratch context destruction.
  - Level 2 local reflection.
  - Prefix stability prompt builder with Project Manifest.
- [ ] **Milestone 3: Planner & Orchestrator Pipeline**
  - Planner prompt producing strictly typed DAGs.
  - Map-Reduce worker chunking for large inspections.
  - Live event streaming channel (`chan Event`).
  - Checkpoint resume engine (`reminis resume <run_id>`).
- [ ] **Milestone 4: Sessions, Approvals & Long-term Memory**
  - Session lifecycle management and approval scopes (`action`, `session`, `permanent`).
  - Fact extraction and keyword/topic retrieval.
- [ ] **Milestone 5: Interfaces & Ecosystem Integration**
  - CLI application (`reminis run`, `reminis resume`, `reminis approve`, `reminis mem`).
  - Stdio Model Context Protocol server (`reminis mcp`).
  - Go SDK (`pkg/client`) for AGIS integration.
