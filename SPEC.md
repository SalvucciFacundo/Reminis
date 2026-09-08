# Reminis Architecture Specification

**Version:** 0.2.0  
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

## 3. Core Components

### 3.1 Blackboard (Working Memory)
A concurrent, thread-safe memory ledger representing the ground truth of the active workflow run.
- **Implementation:** `sync.RWMutex` guarding an in-memory key-value map.
- **Isolation:** Workers receive an immutable sub-slice containing strictly the keys declared in their `InputKeys`.
- **Checkpointing:** State snapshots are periodically flushed to SQLite for resume-on-failure.

### 3.2 Task Graph (DAG Scheduler)
- **Topological Sorting:** Resolves task execution order and detects circular dependencies prior to execution.
- **Concurrency:** Uses `golang.org/x/sync/errgroup` to execute independent tasks in parallel goroutines.
- **Resilience:** Per-task configurable retries (exponential backoff) and timeouts (`context.WithTimeout`).

### 3.3 Ephemeral Worker Engine
- **Stateless Request:** Combines:
  1. System Prompt (Task role).
  2. Blackboard inputs (strict JSON slice).
  3. Action Directive.
- **Budget:** 300 to 800 tokens total.
- **Destruction:** Response is parsed into typed outputs and written to the Blackboard. The worker's LLM conversation context is completely discarded.

### 3.4 Archival Store (SQLite Pure-Go)
- **Driver:** `modernc.org/sqlite` (100% CGO-free, cross-compilable to any OS/architecture).
- **Concurrency Settings:** `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=5000;`.
- **Database Schema:**

```sql
-- Active and past workflow runs
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    goal TEXT NOT NULL,
    status TEXT NOT NULL, -- PENDING, RUNNING, COMPLETED, FAILED
    total_tokens INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME
);

-- Discrete tasks within a DAG
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
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

Reminis is designed to be consumed in three distinct modalities:

1. **Go Library (`pkg/client`):** Directly imported into Go applications (like AGIS) for zero-latency, embedded agent orchestration.
2. **Model Context Protocol (`reminis mcp`):** Exposes JSON-RPC over stdio so external IDEs and agents (Antigravity, Hermes, Cursor) can invoke Reminis memory and DAG tools natively:
   - `reminis_session_start(project_path)`
   - `reminis_save_fact(topic, content)`
   - `reminis_query_facts(query)`
   - `reminis_run_workflow(goal)`
3. **CLI (`cmd/reminis`):** Standalone terminal command for running tasks, inspecting memory, and managing sessions.

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

- [ ] **Milestone 1: Core Engine**
  - Implement `internal/blackboard` with concurrent snapshotting.
  - Implement `internal/dag` with dependency resolution and cycle checking.
  - Implement `internal/store` with pure-Go SQLite migrations and CRUD.
- [ ] **Milestone 2: Worker & LLM Client**
  - Implement HTTP client for OpenAI-compatible endpoints.
  - Ephemeral prompt synthesizer and token consumption logger.
- [ ] **Milestone 3: Planner & Orchestrator**
  - LLM-based DAG generator (Goal -> Task list with dependencies).
  - Concurrent DAG execution pipeline with auto-recovery and Blackboard updates.
- [ ] **Milestone 4: Sessions & Fact Retrieval (Engram-parity)**
  - `sessions` lifecycle and automatic fact extraction/indexing.
  - Topic-based and keyword-based fast search.
- [ ] **Milestone 5: Interfaces**
  - CLI commands (`reminis run`, `reminis mem`, `reminis status`).
  - Native MCP Server (`reminis mcp`).
