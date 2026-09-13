# Model Context Protocol (MCP) Server Guide

Reminis provides a native **Model Context Protocol (MCP)** server over standard input and output (`stdio`), enabling autonomous agent environments (Antigravity, Claude Desktop, Cursor, Codex) to drive Reminis as an external tool execution engine.

---

## Protocol Hygiene: Strict Stdout Isolation

Agent host systems communicate with MCP servers using JSON-RPC 2.0 framing over standard I/O:
- Any extraneous text emitted to `stdout` (such as logger messages, child command outputs, or database warnings) will corrupt the host's JSON parser (`Unexpected token in JSON at position...`).
- Reminis enforces **Strict Stdout Hygiene**: `os.Stdout` is strictly isolated for framed JSON-RPC 2.0 messages only.
- All internal runtime logs, background telemetry, and command outputs are safely redirected to `os.Stderr` or SQLite.

---

## Configuration

### Claude Desktop Configuration
Add the server entry to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "reminis": {
      "command": "reminis",
      "args": ["mcp"],
      "env": {
        "OPENAI_API_KEY": "sk-your-api-key"
      }
    }
  }
}
```

### Antigravity / Cursor Configuration
In your project or user MCP settings:
```json
{
  "name": "reminis",
  "command": "reminis",
  "args": ["mcp"]
}
```

---

## Available MCP Tools

### 1. `reminis_run`
Executes an objective through automated DAG planning, bounded ephemeral workers, and Blackboard state passing.

**Parameters:**
- `goal` (*string, required*): The high-level objective to accomplish.
- `model` (*string, optional*): The model to use (default: `gpt-4o`).
- `max_concurrency` (*integer, optional*): Maximum concurrent worker goroutines (default: `4`).

---

### 2. `reminis_resume`
Resumes an interrupted or paused run from its SQLite checkpoint.

**Parameters:**
- `run_id` (*string, required*): The identifier of the run to resume.

---

### 3. `reminis_approve`
Approves a task that paused in `WAITING_APPROVAL`.

**Parameters:**
- `run_id` (*string, required*): The run identifier.
- `task_id` (*string, required*): The task identifier.
- `scope` (*string, optional*): Scope of approval (`action` or `session`, default: `action`).

---

### 4. `reminis_facts_search`
Queries the long-term memory layer for architectural decisions, conventions, and facts.

**Parameters:**
- `topic` (*string, optional*): Search query or topic filter.
- `scope` (*string, optional*): Scope filter (`project`, `session`, `global`).

---

### 5. `reminis_facts_save`
Persists a durable architectural decision or convention into SQLite memory.

**Parameters:**
- `topic` (*string, required*): The category or symbol name.
- `content` (*string, required*): The factual observation or decision details.
- `scope` (*string, optional*): Scope level (default: `project`).

---

### 6. `reminis_prefix_show`
Audits the resolved Universal Rules discovery cascade, estimated token count, and SHA-256 `PrefixHash`.

**Parameters:** None.
