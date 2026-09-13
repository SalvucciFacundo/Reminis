# Reminis CLI Reference Guide

The `reminis` command-line tool provides full control over workflow orchestration, checkpoint resumption, approval gates, rules auditing, and long-term architectural memory.

---

## Global Syntax

```bash
reminis <command> [arguments] [flags]
```

### Global Flags
| Flag | Default | Description |
| :--- | :--- | :--- |
| `--db` | `~/.reminis/reminis.db` | Path to SQLite database file |
| `--model` | `gpt-4o` | LLM model identifier for planner and workers |
| `--base-url` | (Provider default) | OpenAI-compatible endpoint URL (e.g. `http://localhost:11434/v1`) |
| `--api-key` | `$OPENAI_API_KEY` | API authentication key |
| `--max-concurrency` | `4` | Maximum parallel worker goroutines |
| `--rules` | (Auto-discovered) | Path to explicit rules markdown or YAML file |
| `--auto-approve` | `""` | Set to `all` to bypass all human approval gates |

---

## Subcommands

### 1. `reminis run`
Executes a high-level goal through DAG planning, ephemeral workers, and bounded tools.

```bash
reminis run <goal> [flags]
```

#### Example
```bash
reminis run "Refactor internal/store to support pagination and add tests" \
  --model gpt-4o \
  --max-concurrency 4
```

#### Telemetry Output
Events are streamed in real time to `os.Stderr`:
```
[15:04:12] [RUN_STARTED] Run run_9f81a2 started with 4 tasks
[15:04:13] [TASK_STARTED] [task_inspect] Action: Inspect internal/store/store.go
[15:04:16] [TASK_COMPLETED] [task_inspect] Duration: 3.2s | Tokens: 420
[15:04:17] [TASK_WAITING_APPROVAL] [task_write] Modifying internal/store/store.go
[?] Approve task task_write? (y/N): y
[15:04:21] [TASK_COMPLETED] [task_write] Duration: 4.1s | Tokens: 680
[15:04:22] [RUN_FINISHED] Status: COMPLETED | Total Tokens: 1100
```

---

### 2. `reminis resume`
Resumes an interrupted or paused run from its persisted SQLite checkpoint.

```bash
reminis resume <run_id> [flags]
```

#### Example
```bash
reminis resume run_9f81a2
```

- Completed and skipped tasks are never re-executed.
- Incomplete branches continue execution from their last snapshot.

---

### 3. `reminis approve`
Approves a task currently paused in `WAITING_APPROVAL`.

```bash
reminis approve <run_id> <task_id> [--scope action|session]
```

#### Scopes
- `--scope action` (default): Approves this single task execution.
- `--scope session`: Approves this task and caches authorization for all subsequent tasks in the same capability category (`fs:delete`, `git:push`, etc.) for the remainder of the session.

#### Example
```bash
reminis approve run_9f81a2 task_git_push --scope session
```

---

### 4. `reminis prefix show`
Audits the resolved Universal Rules discovery cascade, estimated token count, and SHA-256 `PrefixHash`.

```bash
reminis prefix show [--rules <path>]
```

#### Output
```
=== Reminis Rules Prefix Audit ===
Source Origin:       project (.cursorrules)
Discovered Path:     /path/to/project/.cursorrules
SHA-256 PrefixHash:  e1ac670dc76a5a47c8abcc674f35ab3f9ff46a4cd87d864ee40acf10b842a8ea
Estimated Tokens:    ~1502
Provider Cacheable:  YES (Cache Active) (Threshold: 1024 tokens)
----------------------------------
```

---

### 5. `reminis mem`
Manages durable architectural facts and conventions in SQLite.

#### Subcommands
```bash
# Search facts by topic or keyword
reminis mem search <topic> [--scope project|session|global]

# Save a manual architectural fact
reminis mem save <topic> <content> [--scope project]

# List all facts recorded in a session
reminis mem list [session_id]
```

#### Examples
```bash
reminis mem save "database" "Pure-Go modernc.org/sqlite with WAL mode"
reminis mem search "database"
```

---

### 6. `reminis mcp`
Starts the stdio Model Context Protocol (JSON-RPC 2.0) server for agent host environments (Antigravity, Claude Desktop, Cursor, Codex).

```bash
reminis mcp
```

- `os.Stdout` is strictly isolated for JSON-RPC messages.
- All diagnostics, logs, and telemetry route to `os.Stderr`.
