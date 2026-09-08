# Reminis Architecture Specification

**Version:** 0.1.0  
**Status:** Draft / Prototype  
**Language:** Go (1.23+)  
**Repository:** `github.com/fds1288/reminis`  

---

## 1. Motivation & Problem Statement

Modern LLM agents suffer from the **quadratic context problem**:
1. **Context Bloat:** Traditional ReAct loops accumulate complete chat histories (`[turn_1, turn_2, ... turn_N]`). By turn 30, each request uploads tens of thousands of tokens.
2. **Attention Degradation:** As context balloons, LLMs lose focus (*needle-in-a-haystack* degradation), contradict earlier instructions, and hallucinate.
3. **TPM / Rate Limit Exhaustion:** Re-sending 30,000 tokens on every step rapidly exhausts API tokens-per-minute (TPM) quotas or saturates local CPU memory bandwidth.

### The Solution: The OS-LLM Architecture
In traditional computing:
- **CPU:** Does not remember past programs; it processes current instructions in registers.
- **RAM / Working Memory:** Fast, bounded, holds only active variables.
- **Disk:** Durable, infinite, queried on demand.

**Reminis** applies this architecture to LLM agents:
- **The LLM is the CPU.**
- **The Blackboard is the RAM (Working Memory).**
- **The Knowledge Store is the Disk (Archival Memory).**
- **Tasks are executed by ephemeral workers whose contexts are destroyed immediately upon completion.**

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
                       │   Reducer / Assembler  │
                       │   (Final Synthesis)    │
                       └────────────────────────┘
```

---

## 3. Core Components

### 3.1 Blackboard (Working Memory)
A concurrent, thread-safe memory ledger representing the ground truth of the active workflow.

- **Storage:** Key-value pairs protected by `sync.RWMutex`.
- **Scope:** Scoped to the active workflow run.
- **Behavior:**
  - Workers read only specific keys declared in their dependencies.
  - Workers write outputs back to designated keys.
  - Generates immutable snapshots for worker injection.

### 3.2 Task Graph (DAG Scheduler)
A Directed Acyclic Graph orchestrating task execution.

- **Task Definition:**
  ```go
  type TaskStatus string

  const (
      StatusPending   TaskStatus = "PENDING"
      StatusRunning   TaskStatus = "RUNNING"
      StatusCompleted TaskStatus = "COMPLETED"
      StatusFailed    TaskStatus = "FAILED"
  )

  type Task struct {
      ID          string            `json:"id"`
      Action      string            `json:"action"`
      DependsOn   []string          `json:"depends_on"`
      InputKeys   []string          `json:"input_keys"`
      OutputKeys  []string          `json:"output_keys"`
      Status      TaskStatus        `json:"status"`
      Result      any               `json:"result,omitempty"`
      Error       string            `json:"error,omitempty"`
  }
  ```
- **Scheduler Logic:**
  - Identifies all tasks where `Status == Pending` and all `DependsOn` tasks are `Status == Completed`.
  - Dispatches ready tasks concurrently using `golang.org/x/sync/errgroup` or managed worker pools.

### 3.3 Ephemeral Worker Engine
Executes an isolated, stateless request to an LLM provider:
- **Input Prompt Structure:**
  1. System Prompt (Task-specific execution role).
  2. Injected State (Strictly the values of `InputKeys` from the Blackboard).
  3. Action Directive (The specific task action to execute).
- **Token Budget:** Typically 300 to 800 tokens total per request.
- **Lifecycle:** Upon completion, the worker updates the Blackboard and its context is discarded. No conversation history is retained between tasks.

### 3.4 Archival Store (Persistence)
Durable storage for completed workflows, task results, and long-term project facts.
- **Driver Options:** Pure-Go SQLite (`modernc.org/sqlite`) or local JSON/WAL ledger.
- **Interface:**
  ```go
  type Store interface {
      SaveRun(ctx context.Context, run *Run) error
      GetRun(ctx context.Context, runID string) (*Run, error)
      SaveFact(ctx context.Context, key string, value []byte) error
      QueryFact(ctx context.Context, key string) ([]byte, error)
  }
  ```

---

## 4. Technical Specifications & Stack

| Component | Choice | Rationale |
| :--- | :--- | :--- |
| **Language** | Go 1.23+ | Static binary, native concurrency (goroutines), minimal memory footprint. |
| **Concurrency** | `sync.RWMutex`, `errgroup` | Race-free concurrent execution of independent DAG tasks. |
| **LLM Provider Interface** | OpenAI-compatible HTTP client | Compatible with local `llama-server`, Ollama, vLLM, Groq, OpenRouter, Google AI Studio. |
| **Serialization** | `encoding/json` | Standard, strongly typed schemas. |
| **Storage Driver** | Pure-Go SQLite / Local WAL | Zero external CGO dependencies; portable cross-platform binaries. |

---

## 5. Security & Isolation

1. **No Shared Chat Contexts:** Workers cannot pollute or inspect neighboring workers' context windows unless explicitly wired via Blackboard keys.
2. **Context Leak Prevention:** Prompt templates strip credentials, secrets, or unreferenced Blackboard variables.
3. **Execution Timeouts:** Each task has an individual `context.WithTimeout` to prevent stalled LLM calls from freezing the DAG.

---

## 6. Implementation Milestones

- [ ] **Milestone 1: Core Blackboard & DAG Engine**
  - Implement `internal/blackboard` (thread-safe store + snapshots).
  - Implement `internal/dag` (topological sort, ready task detection, cycle validation).
- [ ] **Milestone 2: LLM Client & Ephemeral Worker**
  - Implement `internal/worker` (OpenAI-compatible HTTP provider).
  - Benchmark atomic token consumption per micro-task.
- [ ] **Milestone 3: Planner & End-to-End Orchestrator**
  - Implement decomposition prompt (Goal -> DAG).
  - Concurrent execution of multi-step task graphs with live telemetry.
