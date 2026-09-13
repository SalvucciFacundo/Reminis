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
- **Implementation:** `sync.RWMutex` guarding an in-memory map of typed `MemoryEntry` records.
- **Namespace Isolation:** Each task writes strictly to its dedicated namespace (`tasks.<task_id>.output`). Multiple parallel tasks cannot write to the same key.
- **Typed Value & 4KB Size Ceiling (Pass-by-Reference):** Values are stored as `json.RawMessage`. Individual Blackboard values must not exceed 4KB (`len(data) <= 4096`). Large payloads (diffs, CSVs, build logs) are spilled to a run-scoped directory (`~/.reminis/runs/<run_id>/artifacts/`), setting `IsSpilled: true` and storing the file path in `SpillPath`. Downstream tasks inspect metadata to decide whether to read in-memory JSON or stream the artifact from disk.
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
- **Dual Execution Modes:**
  1. **Interactive Mode (TTY):** When Reminis runs connected to an active terminal, it prompts the user in real time (`[y/N]`) with diff and risk metadata. Upon approval, the worker spawns immediately without interrupting running peer tasks.
  2. **Headless / Async Mode (MCP / Background):** When running non-interactively or via MCP, Reminis serializes checkpoint state to SQLite and idles or yields process execution. External approval via `reminis approve <run_id> <task_id>` loads the checkpoint and re-triggers the DAG scheduler seamlessly.

### 3.4 Ephemeral Workers & Bounded Tool Execution (Max 3 Turns)
Workers are permitted to execute real-world tools (`bash`, `read_file`, `write_file`, `git`), governed by strict bounding rules:
1. **Hard Budget:** Maximum **3 tool execution turns** per worker.
2. **Dynamic Sub-DAG Expansion (`scheduler.Expand`):**
   - If an inspection or mutation exceeds the 3-turn budget (e.g., discovering 15 files to analyze during an initial exploration step), the worker yields subtasks (`YieldedTasks []Task`).
   - The scheduler suspends the parent task and dynamically injects the yielded subtasks into the active DAG using incremental Kahn validation to guarantee acyclicity.
   - Child tasks execute concurrently across goroutines, write their partial outputs to the Blackboard, and unblock the designated aggregator/reducer task upon completion.
3. **Context Destruction:** Large tool outputs exist only within the ephemeral worker's temporary process. Once the worker summarizes its findings into the Blackboard, its entire conversation context is garbage collected.
4. **Execution Watchdog & Silence Timeout:**
   - Static short timeouts risk prematurely terminating legitimate long-running commands (e.g. heavy Go compilations, test suites, large dependency downloads).
   - Tool execution streams `stdout`/`stderr` through an **inactivity watchdog**. The silence timer resets upon receiving any output byte.
   - Execution is aborted only if zero output is emitted for `SilenceTimeout` (default: 90s), cleanly trapping deadlocks or unhandled interactive `stdin` prompts.
   - Tasks may declare a hard deadline override (`TimeoutSeconds`) when executing known long-duration batch jobs.

### 3.5 Universal Rules Discovery & Cold-Start Prevention
To ensure workers adhere to user-defined conventions without manual configuration or breaking existing setups (Gentle AI, Cursor, Windsurf, Claude Code, GitHub Copilot), Reminis implements a **Hierarchical Rules Discovery Engine** that resolves at session initialization:

1. **Precedence Cascade (Strict Priority Order):**
   - **CLI / Env Overrides (Highest Priority):** `--rules <path>` or `REMINIS_RULES="..."`.
   - **Project / Workspace Level:** Auto-discovered repository rules files: `.reminis.yaml`, `AGENTS.md`, `CLAUDE.md`, `.cursorrules`, `.windsurfrules`, `.github/copilot-instructions.md`.
   - **User Global Config:** Global user guidelines: `~/.config/reminis/rules.md`, `~/.config/gentle-ai/`, or host persona configurations.
   - **Reminis Baseline (Fallback):** Injected only if no upstream rules exist (standard clean architecture, idiomatic error handling, bounded worker protocols).
2. **Deterministic Merge & Session Freeze:**
   - At session boot, Reminis compiles detected rules into a normalized, immutable byte-sequence and computes a `PrefixHash` (SHA-256).
   - This compiled prefix is recorded in SQLite (`sessions.compiled_prefix`) and frozen for the run's lifecycle.
   - All ephemeral workers inherit this identical prefix, guaranteeing a **100% prompt cache hit rate** without modifying or ignoring the user's personal/team configs.
3. **Auditability:** Users can inspect the exact resolved prefix at any time via `reminis prefix show`.

### 3.6 Event-Driven Telemetry & MCP Protocol Hygiene
Reminis avoids the "black box" syndrome by streaming execution state in real time via Go channels (`chan Event`) and SSE (Server-Sent Events) over MCP/HTTP:
- `EventRunStarted(run_id, total_tasks)`
- `EventTaskStarted(task_id, action)`
- `EventTaskWaitingApproval(task_id, action, diff)`
- `EventTaskCompleted(task_id, duration_ms, tokens)`
- `EventTaskFailed(task_id, error, can_retry)`
- `EventRunFinished(run_id, status, total_tokens)`

**MCP Stdout Protocol Hygiene:** When operating as an MCP server over stdio (`reminis mcp`), `os.Stdout` is strictly isolated for JSON-RPC protocol framing. All internal engine logging, telemetry, and child-process tool output are routed exclusively to `os.Stderr` or persisted to SQLite, preventing JSON parse corruption in host environments (Antigravity, Claude Desktop, Cursor, Codex).

### 3.7 Prompt Caching Optimization (Enriched Prefix Stability Pattern)
Modern providers (Anthropic Claude, Google Gemini, OpenAI) require minimum token thresholds (e.g., Anthropic enforces a strict 1,024-token minimum) to activate prompt caching discounts (50%–90% cost/latency reduction).

Reminis enforces an **Enriched Static Prefix** designed to reliably exceed provider thresholds while maintaining strict context isolation:
1. **Invariant Static Prefix (>1,024 Tokens, Cached):** Placed at the very top of every worker prompt:
   - Compiled Universal Rules Prefix (from Section 3.5 discovery cascade).
   - Global Project Manifest and repo conventions.
   - Comprehensive tool schemas and argument constraints.
   - Few-shot execution examples demonstrating expected tool calls and Blackboard state reporting.
   - Strict output schema definitions.
2. **Dynamic Tail (Non-Cached Append-Only):** Placed strictly at the bottom of the prompt:
   - Injected Blackboard slice (`InputKeys` resolved from working memory).
   - Specific action directive for the current node.

### 3.8 Concurrency & Storage Guardrails
1. **Topological Cycle Validation:** The DAG scheduler verifies graph acyclicity using Kahn's algorithm prior to execution. Circular dependencies are rejected immediately.
2. **SQLite Lock Contention Prevention:** Although SQLite WAL mode allows concurrent readers, writes must be serialized. Reminis manages database writes through a dedicated single-writer channel in Go with `busy_timeout=5000ms`, preventing `SQLITE_BUSY` errors during parallel worker completion bursts.
3. **Run Artifact Lifecycle:** Files spilled to `~/.reminis/runs/<run_id>/artifacts/` are tracked in the `runs` table. Ephemeral scratch data is purged upon successful run termination, while designated artifacts are retained for user inspection.
4. **Process Group Isolation & Context Cancellation:** Every tool sub-process is spawned in its own process group (`Setpgid: true`). When the governing `context.Context` cancels (due to silence timeout, hard task deadline, or `SIGINT`/`SIGTERM`), Reminis signals the entire process tree (`syscall.Kill(-pgid, syscall.SIGKILL)`), eliminating orphan/zombie processes.
5. **Bounded Worker Pool (Concurrency Throttling):** To prevent 429 RPM/TPM rate-limit exhaustion against commercial APIs and compute/VRAM starvation on local models (Ollama, llama-server), the scheduler limits concurrent goroutines via a configurable semaphore (`--max-concurrency`, default: 4 for cloud endpoints, 1–2 for local engines).
6. **Workspace Resource Locks (File Collision Prevention):** Parallel workers targeting the same files or git staging area acquire cooperative locks via an in-memory path mutex registry (`internal/dag.ResourceLock`). If parallel tasks target overlapping file paths, execution is serialized automatically to prevent race conditions and corrupted source trees.

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
    StatusTimedOut        TaskStatus = "TIMED_OUT"
    StatusSkipped         TaskStatus = "SKIPPED"
)

type MemoryEntry struct {
    Key        string          `json:"key"`
    Data       json.RawMessage `json:"data"`                 // In-memory JSON payload
    IsSpilled  bool            `json:"is_spilled"`            // True if size exceeded 4KB
    SpillPath  string          `json:"spill_path,omitempty"`  // Artifact path on disk when spilled
    ProducerID string          `json:"producer_id"`           // Originating task ID
}

type Task struct {
    ID               string          `json:"id"`
    Action           string          `json:"action"`
    DependsOn        []string        `json:"depends_on"`
    InputKeys        []string        `json:"input_keys"`
    OutputKeys       []string        `json:"output_keys"`
    RequiresApproval bool            `json:"requires_approval"`
    TimeoutSeconds   int             `json:"timeout_seconds,omitempty"` // Hard deadline override (0 = silence watchdog)
    TargetPaths      []string        `json:"target_paths,omitempty"`    // Paths locked during execution to prevent disk race conditions
    Status           TaskStatus      `json:"status"`
    Result           json.RawMessage `json:"result,omitempty"`
    YieldedTasks     []Task          `json:"yielded_tasks,omitempty"`   // Dynamic sub-tasks for scheduler.Expand
    Error            string          `json:"error,omitempty"`
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
    prefix_hash TEXT,
    compiled_prefix TEXT,
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

- [x] **Milestone 1: Core Engine & Concurrency Guardrails**
  - Implement `internal/blackboard` (thread-safe, `MemoryEntry`, 4KB spillover pass-by-reference).
  - Implement `internal/dag` (Kahn's cycle validation, dynamic `scheduler.Expand`, dependency resolution, cascading skips, dual-mode approval pauses, context propagation & cancellation, bounded worker pool semaphore, workspace resource lock table).
  - Implement `internal/store` (pure-Go SQLite with single-writer channel and WAL mode).
- [x] **Milestone 2: Ephemeral Worker Engine & Universal Rules Discovery**
  - OpenAI-compatible HTTP client with Level 1 exponential backoff.
  - Bounded tool execution loop (max 3 turns) with scratch context destruction, silence watchdog (inactivity timeout), and process group isolation.
  - Level 2 local reflection.
  - Hierarchical Rules Discovery Engine (precedence cascade: CLI -> project files like `.cursorrules`, `AGENTS.md`, `CLAUDE.md` -> global configs like `gentle-ai` -> baseline).
  - Enriched Prefix prompt synthesizer (>1,024 tokens) with session freeze (`sessions.compiled_prefix`) and CLI inspection (`reminis prefix show`).
- [x] **Milestone 3: Planner & Orchestrator Pipeline**
  - Planner prompt producing strictly typed DAGs with subtask yield support.
  - Dynamic fan-out / fan-in map-reduce execution via `scheduler.Expand`.
  - Live event streaming channel (`chan Event`).
  - Checkpoint resume engine (`reminis resume <run_id>`).
- [x] **Milestone 4: Sessions, Approvals & Long-term Memory**
  - Session lifecycle management and approval scopes (`action`, `session`, `permanent`).
  - Fact extraction and keyword/topic retrieval.
- [x] **Milestone 5: Interfaces & Ecosystem Integration**
  - CLI application (`reminis run`, `reminis resume`, `reminis approve`, `reminis prefix show`, `reminis mem`).
  - Stdio Model Context Protocol server (`reminis mcp`) with strict `stdout` protocol hygiene.
  - Go SDK (`pkg/client`) for AGIS integration.
