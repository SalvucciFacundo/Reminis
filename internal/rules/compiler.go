package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// MinPromptCacheTokens is the minimum token count required by major providers
// (notably Anthropic's strict 1,024 token minimum) to trigger prompt caching.
const MinPromptCacheTokens = 1024

// CompiledPrefix represents the deterministic frozen rules prefix.
type CompiledPrefix struct {
	Hash       string `json:"hash"`
	Content    string `json:"content"`
	TokenCount int    `json:"token_count"`
}

// StandardToolSchemas defines the invariant tool schemas and parameter contracts.
const StandardToolSchemas = `
## Standardized Tool Schemas

### 1. ` + "`" + `read_file` + "`" + `
- **Description**: Reads the contents of a file from the workspace filesystem.
- **Parameters**:
  - ` + "`" + `path` + "`" + ` (string, required): Relative or absolute path of the file to inspect.
- **Constraints**:
  - Non-existent files return an explicit error.
  - Payloads exceeding in-memory limits are preserved and referenced.

### 2. ` + "`" + `write_file` + "`" + `
- **Description**: Atomically creates a new file or overwrites an existing file with the provided content.
- **Parameters**:
  - ` + "`" + `path` + "`" + ` (string, required): Destination file path.
  - ` + "`" + `content` + "`" + ` (string, required): Complete text content to write into the file.
- **Constraints**:
  - Missing parent directories are created automatically with 0755 permissions.
  - Writes are atomic to prevent partially written corruption on disk.

### 3. ` + "`" + `bash` + "`" + `
- **Description**: Executes a shell command inside an isolated bash sub-process.
- **Parameters**:
  - ` + "`" + `command` + "`" + ` (string, required): The shell command string to execute.
- **Constraints**:
  - **Process Group Isolation**: Spawned with ` + "`" + `Setpgid: true` + "`" + `.
  - **Silence Watchdog**: Execution streams stdout/stderr; resets watchdog timer on each output byte. Zero output for the silence timeout terminates the process group via ` + "`" + `SIGKILL` + "`" + `.
  - No interactive stdin blocking is permitted.

### 4. ` + "`" + `git` + "`" + `
- **Description**: Executes git version control commands against the active repository.
- **Parameters**:
  - ` + "`" + `command` + "`" + ` (string, required): The git subcommand and flags to execute.
- **Constraints**:
  - Destructive or forced operations require explicit human approval.
`

// StandardFewShotExamples defines few-shot demonstrations for tool calling and state reporting.
const StandardFewShotExamples = `
## Few-Shot Execution Demonstrations

### Example 1: Targeted File Inspection and Structural Analysis
**Task Directive**: Analyze ` + "`" + `pkg/logger/logger.go` + "`" + ` to verify if thread-safe log rotation is implemented.
**Worker Turn 1**:
- Thought: Inspect the logger implementation to review synchronization primitives and file descriptor handling.
- Tool Call: ` + "`" + `read_file` + "`" + ` with ` + "`" + `{"path": "pkg/logger/logger.go"}` + "`" + `
- Tool Result:
  ` + "```go" + `
  package logger
  type Logger struct { f *os.File }
  func (l *Logger) Write(p []byte) (int, error) { return l.f.Write(p) }
  ` + "```" + `
**Worker Turn 2**:
- Thought: The Logger struct contains an un-synchronized *os.File handle without mutex locks or rotation logic.
- Final Output:
  ` + "```json" + `
  {
    "findings": "Logger is not thread-safe. Concurrent writes may interleave or corrupt file descriptors.",
    "thread_safe": false,
    "recommendations": ["Add sync.Mutex to guard Write operations", "Introduce size/time-based log rotation"]
  }
  ` + "```" + `

### Example 2: Safe Code Mutation with Atomic Verification
**Task Directive**: Implement a thread-safe counter in ` + "`" + `pkg/counter/counter.go` + "`" + `.
**Worker Turn 1**:
- Thought: I will write the counter implementation with sync.RWMutex guarding value access.
- Tool Call: ` + "`" + `write_file` + "`" + ` with:
  ` + "```json" + `
  {
    "path": "pkg/counter/counter.go",
    "content": "package counter\n\nimport \"sync\"\n\ntype SafeCounter struct {\n\tmu sync.RWMutex\n\tval int\n}\n\nfunc (c *SafeCounter) Inc() {\n\tc.mu.Lock()\n\tdefer c.mu.Unlock()\n\tc.val++\n}\n\nfunc (c *SafeCounter) Value() int {\n\tc.mu.RLock()\n\tdefer c.mu.RUnlock()\n\treturn c.val\n}\n"
  }
  ` + "```" + `
- Tool Result: File written successfully: pkg/counter/counter.go (234 bytes)
**Worker Turn 2**:
- Thought: I will run tests via bash to verify compilation and test coverage.
- Tool Call: ` + "`" + `bash` + "`" + ` with ` + "`" + `{"command": "go test -v ./pkg/counter/..."}` + "`" + `
- Tool Result: PASS\nok github.com/fds1288/reminis/pkg/counter 0.012s
**Worker Turn 3**:
- Thought: The counter implementation is verified and compiles cleanly.
- Final Output:
  ` + "```json" + `
  {
    "status": "COMPLETED",
    "files_created": ["pkg/counter/counter.go"],
    "tests_passed": true
  }
  ` + "```" + `

### Example 3: Dynamic Sub-DAG Expansion (Yielding Tasks on Exploration Budget)
**Task Directive**: Audit all 25 modules in the codebase for SQL injection vulnerabilities.
**Worker Turn 1**:
- Thought: Listing all repository modules to determine the scope of SQL operations.
- Tool Call: ` + "`" + `bash` + "`" + ` with ` + "`" + `{"command": "find . -name '*_repository.go'"}` + "`" + `
- Tool Result: 18 files found across 6 distinct subpackages.
**Worker Turn 2**:
- Thought: Auditing 18 files exceeds the 3-turn ephemeral budget for a single worker. Per Reminis Section 3.4, I must yield subtasks to the DAG scheduler for parallel fan-out execution.
- Final Output:
  ` + "```json" + `
  {
    "status": "YIELD",
    "yielded_tasks": [
      {"id": "audit-user-repo", "action": "audit_sql", "target_paths": ["internal/user/user_repository.go"]},
      {"id": "audit-order-repo", "action": "audit_sql", "target_paths": ["internal/order/order_repository.go"]},
      {"id": "audit-billing-repo", "action": "audit_sql", "target_paths": ["internal/billing/billing_repository.go"]}
    ]
  }
  ` + "```" + `
`

// EstimateTokens calculates an estimated token count using the standard ~4 chars/token heuristic.
func EstimateTokens(text string) int {
	if len(text) == 0 {
		return 0
	}
	tokens := len(text) / 4
	if tokens == 0 {
		return 1
	}
	return tokens
}

// Compile merges discovered rule strings deterministically, enforces minimum token padding
// (>= 1,024 tokens) for prompt caching, and computes the stable SHA-256 PrefixHash.
func Compile(fragments ...string) *CompiledPrefix {
	var cleaned []string
	for _, f := range fragments {
		trimmed := strings.TrimSpace(f)
		if trimmed != "" {
			// Normalize Windows CRLF to standard LF
			normalized := strings.ReplaceAll(trimmed, "\r\n", "\n")
			cleaned = append(cleaned, normalized)
		}
	}

	merged := strings.Join(cleaned, "\n\n---\n\n")
	if merged == "" {
		merged = strings.TrimSpace(DefaultBaseline)
	}

	tokenCount := EstimateTokens(merged)

	// If below 1,024 tokens, pad with standardized tool schemas and few-shot examples
	// so that Anthropic/provider prompt caching threshold is guaranteed.
	if tokenCount < MinPromptCacheTokens {
		padding := fmt.Sprintf("\n\n---\n\n# Universal Tool Execution & Prompt Caching Anchor\n%s\n%s",
			strings.TrimSpace(StandardToolSchemas),
			strings.TrimSpace(StandardFewShotExamples),
		)
		merged += padding
		tokenCount = EstimateTokens(merged)

		// If still below 1024 tokens under conservative estimation, add deterministic safety padding
		for tokenCount < MinPromptCacheTokens {
			merged += "\n# Architectural Invariant: Strict ephemeral isolation, bounded 3-turn budget, atomic disk state."
			tokenCount = EstimateTokens(merged)
		}
	}

	// Calculate deterministic SHA-256 hash
	sum := sha256.Sum256([]byte(merged))
	hashHex := hex.EncodeToString(sum[:])

	return &CompiledPrefix{
		Hash:       hashHex,
		Content:    merged,
		TokenCount: tokenCount,
	}
}

// CompileDiscovered compiles a single DiscoveredRule into a CompiledPrefix.
func CompileDiscovered(rule *DiscoveredRule) *CompiledPrefix {
	if rule == nil {
		return Compile()
	}
	return Compile(rule.Content)
}
