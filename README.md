<div align="center">

![Reminis Banner](docs/assets/banner.png)

# Reminis

**High-performance, concurrent OS-LLM agent memory layer and DAG orchestrator in pure Go.**

[![Go Report Card](https://goreportcard.com/badge/github.com/SalvucciFacundo/Reminis)](https://goreportcard.com/report/github.com/SalvucciFacundo/Reminis)
[![GitHub Release](https://img.shields.io/github/v/release/SalvucciFacundo/Reminis)](https://github.com/SalvucciFacundo/Reminis/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/SalvucciFacundo/Reminis)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![MCP Protocol](https://img.shields.io/badge/MCP-JSON--RPC%202.0-orange.svg)](https://modelcontextprotocol.io)

[Quickstart](#quickstart) • [Architecture](#the-os-llm-architecture) • [MCP Server](#model-context-protocol-mcp) • [Go SDK](#go-sdk-pkgclient) • [Documentation](#documentation)

</div>

---

## The Problem: Quadratic Context Bloat

Traditional ReAct agent loops append turns indefinitely into a single conversational window:

$$\text{Tokens}(N) \propto \sum_{i=1}^N \text{turn}_i \approx O(N^2)$$

By turn 30, re-sending tens of thousands of tokens exhausts API rate limits (TPM), degrades model attention (*lost-in-the-middle* failures), and spikes latency and costs.

## The Solution: The OS-LLM Architecture

Reminis decouples compute from state by mapping agent operations to classic operating system primitives:

| Primitive | Operating System | Reminis Implementation |
| :--- | :--- | :--- |
| **Compute** | **CPU** | **Stateless LLM**: Pure computation engine. Zero chat history carried between steps. |
| **Working Memory** | **RAM / L1** | **Blackboard**: Thread-safe key-value ledger (`sync.RWMutex`) with 4KB disk spillover. |
| **Persistence** | **Disk / File System** | **Pure-Go SQLite**: WAL-mode archival storage with serialized single-writer channel. |
| **Processes** | **Threads / Subprocesses** | **Ephemeral Workers**: Goroutines bounded to 3 tool calls; context discarded on return. |

```mermaid
flowchart TD
    User([User / Objective]) --> Planner[Planner Engine]
    Planner -->|Generates Acyclic Graph| DAG[Task Graph Scheduler]
    
    subgraph Execution ["Parallel Ephemeral Workers (Goroutines)"]
        DAG -->|Dispatches| W1[Worker A<br/>• Static Prefix<br/>• Dynamic Tail<br/>• Max 3 Tools]
        DAG -->|Dispatches| W2[Worker B<br/>• Static Prefix<br/>• Dynamic Tail<br/>• Max 3 Tools]
    end

    W1 -.->|1. Write Result| BB[(Blackboard RAM)]
    W2 -.->|1. Write Result| BB
    W1 -.->|2. Discard Context| Trash1[Context Destroyed]
    W2 -.->|2. Discard Context| Trash2[Context Destroyed]
    
    BB -->|Flush Checkpoints| SQL[(Pure-Go SQLite WAL)]
    
    classDef main fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    classDef worker fill:#0f172a,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;
    classDef memory fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#f8fafc;
    classDef destroy fill:#450a0a,stroke:#ef4444,stroke-width:1px,color:#fca5a5;

    class User,Planner,DAG main;
    class W1,W2 worker;
    class BB,SQL memory;
    class Trash1,Trash2 destroy;
```

---

## Quickstart

### 1. Installation

#### Precompiled Binaries
Download standalone binaries for Linux, macOS, and Windows directly from [GitHub Releases](https://github.com/SalvucciFacundo/Reminis/releases/latest).

#### Or via `go install`
```bash
go install github.com/SalvucciFacundo/Reminis/cmd/reminis@latest
```

### 2. Choose Your Integration

```mermaid
graph LR
    User[Start Here] --> A[Claude Desktop / Antigravity / Cursor]
    User --> B[Terminal / Developer CLI]
    User --> C[Custom Go Application]

    A -->|"reminis mcp"| MCP[Model Context Protocol]
    B -->|"reminis run <goal>"| CLI[Standalone CLI]
    C -->|"import pkg/client"| SDK[Go Client SDK]

    classDef choice fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    class User,A,B,C,MCP,CLI,SDK choice;
```

#### Option A: Run as an MCP Server (Antigravity, Claude Desktop, Cursor)
Add to your `claude_desktop_config.json` or Antigravity MCP settings:
```json
{
  "mcpServers": {
    "reminis": {
      "command": "reminis",
      "args": ["mcp"],
      "env": {
        "OPENAI_API_KEY": "sk-your-key"
      }
    }
  }
}
```

#### Option B: Standalone CLI
```bash
# Execute a goal with parallel workers
reminis run "Audit internal/store and generate benchmark tests" \
  --model gpt-4o \
  --max-concurrency 4

# Inspect the compiled rules prefix and prompt cache qualification
reminis prefix show

# Resume a checkpointed workflow
reminis resume run_a1b2c3d4
```

#### Option C: Go Library SDK
```go
c, _ := client.New(client.WithDBPath("reminis.db"), client.WithModel("gpt-4o"))
defer c.Close()

res, _ := c.Run(ctx, client.RunRequest{
    Goal: "Analyze dependency graph and detect cycles",
})
fmt.Printf("Run ID: %s | Status: %s\n", res.RunID, res.Status)
```

---

## How It Works

### 1. 3-Level Resilience Pipeline
Reminis protects long-running workflows from network flakes, syntax malformations, and host crashes:

```mermaid
flowchart TD
    Run[Task Execution] --> Attempt{Is Error?}
    Attempt -->|No| Success[Task COMPLETED]
    Attempt -->|HTTP 429 / 500 / Timeout| L1[Level 1: Exponential Backoff + Jitter<br/>Zero LLM Tokens Burned]
    L1 -->|Retry <= 3| Run
    L1 -->|Exhausted| L3[Level 3: Freeze Run Checkpoint in SQLite]

    Attempt -->|Invalid JSON / Syntax Error| L2[Level 2: Targeted Local Reflection<br/>Feeds exact error back to Worker]
    L2 -->|Attempt <= 2| Run
    L2 -->|Exhausted| L3

    L3 --> Resume[Atomic Resumption<br/>reminis resume run_id]

    classDef pass fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#f8fafc;
    classDef retry fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    classDef freeze fill:#450a0a,stroke:#ef4444,stroke-width:2px,color:#fca5a5;

    class Success pass;
    class L1,L2 retry;
    class L3,Resume freeze;
```

### 2. Enriched Prefix Stability (Prompt Caching)
Providers (Anthropic, Gemini, OpenAI) grant **50%–90% discounts** when prompt prefixes remain static and exceed minimum lengths (e.g. 1,024 tokens for Claude).

Reminis enforces the **Prefix Stability Pattern**:

```mermaid
graph TD
    subgraph CachedPrefix ["Invariant Static Prefix (CACHED > 1024 Tokens)"]
        direction TB
        R[Universal Rules: CLI -> Repo -> Global -> Baseline]
        M[Project Manifest: Language, Frameworks, Architecture]
        T[Tool Schemas & Argument Constraints]
        F[Few-Shot Demonstrations]
    end

    subgraph DynamicTail ["Dynamic Tail (NON-CACHED Append-Only)"]
        direction TB
        B[Injected Blackboard Slice: Task.InputKeys]
        A[Specific Task Action Directive]
    end

    CachedPrefix --> DynamicTail
    DynamicTail --> LLM[Stateless LLM Execution]

    classDef cached fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#f8fafc;
    classDef dynamic fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;
    classDef exec fill:#0f172a,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;

    class CachedPrefix cached;
    class DynamicTail dynamic;
    class LLM exec;
```

### 3. Dynamic Sub-DAG Expansion (`scheduler.Expand`)
When a worker discovers work that exceeds its 3-turn budget (e.g. discovering 20 files to analyze), it yields subtasks. The scheduler dynamically mutates the DAG in runtime using **incremental Kahn validation**:

```mermaid
sequenceDiagram
    autonumber
    participant S as DAG Scheduler
    participant W as Explorer Worker
    participant B as Blackboard
    participant R as Reducer Worker

    S->>W: Dispatches Exploration Task
    Note over W: Discovers 15 files to inspect<br/>(Exceeds 3-turn budget)
    W->>S: Yields Subtasks [Child 1...N]
    Note over S: Incremental Kahn Check<br/>Wired before Reducer
    par Parallel Sub-Tasks
        S->>W: Run Child 1
        S->>W: Run Child 2
    end
    W->>B: Write Partial Observations
    S->>R: Unblocks Reducer Task
    B->>R: Reads Partial Observations
    R->>B: Writes Synthesized Conclusion
```

---

## Model Context Protocol (MCP)

Reminis runs a stdio JSON-RPC 2.0 server with **strict `stdout` protocol hygiene**—preventing JSON-RPC parse crashes in host agents by isolating diagnostic logs to `os.Stderr`.

### Available MCP Tools

| Tool | Parameters | Purpose |
| :--- | :--- | :--- |
| `reminis_run` | `goal`, `model?`, `max_concurrency?` | Plans and executes a goal via dynamic DAG and ephemeral workers |
| `reminis_resume` | `run_id` | Resumes an interrupted or paused run from its SQLite checkpoint |
| `reminis_approve` | `run_id`, `task_id`, `scope?` | Grants approval (`action` or `session`) to a paused gated task |
| `reminis_facts_search` | `topic?`, `scope?` | Queries long-term memory for architectural conventions |
| `reminis_facts_save` | `topic`, `content`, `scope?` | Records a durable architectural observation into SQLite |
| `reminis_prefix_show` | *(none)* | Audits active universal rules cascade, token count, and PrefixHash |

---

## CLI Command Overview

```bash
# Execute a goal
reminis run <goal> [--model <m>] [--max-concurrency <n>] [--auto-approve all]

# Resume execution from checkpoint
reminis resume <run_id>

# Approve a task awaiting human authorization
reminis approve <run_id> <task_id> [--scope action|session]

# Inspect rules cascade and prompt caching compliance
reminis prefix show

# Manage long-term memory facts
reminis mem search <topic>
reminis mem save <topic> <content>
reminis mem list [session_id]

# Start stdio MCP server
reminis mcp
```

---

## Documentation

For comprehensive guides and technical deep dives:

- 🏛️ **[Architecture & Deep Dive](docs/architecture.md):** OS-LLM design, Blackboard spillover, concurrency semaphores, and Kahn's algorithm.
- 💻 **[CLI Reference Guide](docs/cli.md):** Command options, environment flags, and interactive approval flows.
- 🔌 **[Model Context Protocol (MCP) Guide](docs/mcp.md):** Configuration for Claude Desktop, Antigravity, Cursor, and Codex.
- 📦 **[Go Client SDK Guide](docs/sdk.md):** Embedding Reminis into custom Go applications with streaming events.
- 📋 **[Architecture Specification (SPEC.md)](SPEC.md):** Formal production specification (v0.6.0).

---

## Roadmap & Milestones

- [x] **Milestone 1: Core Engine & Concurrency Guardrails** (`internal/blackboard`, `internal/dag`, `internal/store`)
- [x] **Milestone 2: Ephemeral Worker Engine & Universal Rules Discovery** (`internal/worker`, `internal/rules`, `internal/prompt`)
- [x] **Milestone 3: Planner & Orchestrator Pipeline** (`internal/planner`, `internal/orchestrator`)
- [x] **Milestone 4: Sessions, Approvals & Long-term Memory** (`internal/session`, `internal/memory`)
- [x] **Milestone 5: Interfaces & Ecosystem Integration** (`cmd/reminis`, `internal/mcp`, `pkg/client`)

## Contributing

Contributions are warmly welcome! Whether you found a bug, want to improve documentation, propose a performance optimization, or build a new integration:

- 🐛 **Bug Reports & Issues:** Found unexpected behavior or an edge-case error? Open an issue on [GitHub Issues](https://github.com/SalvucciFacundo/Reminis/issues) with reproduction steps.
- 💡 **Feature Requests & Ideas:** Open an issue with the `enhancement` label to discuss architectural ideas.
- 🛠️ **Pull Requests:** Review our [Contributing Guide](CONTRIBUTING.md) for local development setup, testing standards, and conventional commit guidelines.

---

## License

MIT License. See [LICENSE](LICENSE) for details.

