package planner

import (
	"strings"

	"github.com/fds1288/reminis/internal/prompt"
	"github.com/fds1288/reminis/internal/rules"
)

// BuildPlannerPrompt constructs the full prompt for the LLM planner.
// It instructs the LLM to output a strictly typed JSON array of dag.Task,
// enforce Blackboard namespace isolation (tasks.<id>.*), and maximize parallelism.
func BuildPlannerPrompt(goal, manifest, rulesContent string) string {
	var sb strings.Builder

	sb.WriteString("# Role: Reminis DAG Workflow Planner\n")
	sb.WriteString("You are the planning engine of Reminis. Your job is to analyze the user's high-level goal ")
	sb.WriteString("and decompose it into an optimal Directed Acyclic Graph (DAG) of executable tasks.\n\n")

	sb.WriteString("## Execution Model\n")
	sb.WriteString("Each task in the DAG is executed by an ephemeral, bounded worker goroutine with access to standard tools ")
	sb.WriteString("(read_file, write_file, bash, git). Workers share state through a thread-safe Blackboard working memory ledger.\n\n")

	sb.WriteString("## Task Schema Specification\n")
	sb.WriteString("You MUST output a strictly typed JSON array of tasks matching this exact schema:\n")
	sb.WriteString("```json\n")
	sb.WriteString("[\n")
	sb.WriteString("  {\n")
	sb.WriteString("    \"id\": \"unique_task_id\",\n")
	sb.WriteString("    \"action\": \"Detailed and actionable imperative instruction for the worker\",\n")
	sb.WriteString("    \"depends_on\": [\"upstream_task_id\"],\n")
	sb.WriteString("    \"input_keys\": [\"tasks.upstream_task_id.output\"],\n")
	sb.WriteString("    \"output_keys\": [\"tasks.unique_task_id.output\"],\n")
	sb.WriteString("    \"requires_approval\": false,\n")
	sb.WriteString("    \"target_paths\": [\"path/to/target_file.go\"],\n")
	sb.WriteString("    \"timeout_seconds\": 60\n")
	sb.WriteString("  }\n")
	sb.WriteString("]\n")
	sb.WriteString("```\n\n")

	sb.WriteString("## Core Rules and Constraints\n")
	sb.WriteString("1. **Strict DAG Acyclicity**:\n")
	sb.WriteString("   - The task graph MUST be strictly acyclic. Circular dependencies are rejected immediately.\n")
	sb.WriteString("   - Every entry in `depends_on` must reference a valid `id` of another task defined in the same array.\n")
	sb.WriteString("   - Tasks without dependencies (`\"depends_on\": []`) execute immediately in parallel.\n\n")

	sb.WriteString("2. **Concurrency & Parallelism**:\n")
	sb.WriteString("   - Identify independent steps that do not depend on each other and schedule them to run in parallel.\n")
	sb.WriteString("   - Do NOT serialize tasks unless there is an authentic data or temporal dependency.\n\n")

	sb.WriteString("3. **Blackboard Namespace Isolation**:\n")
	sb.WriteString("   - All working memory keys in `input_keys` and `output_keys` MUST strictly adhere to the namespace: `tasks.<id>.*`.\n")
	sb.WriteString("   - A task may ONLY declare `output_keys` under its own ID (e.g., `tasks.<id>.output`).\n")
	sb.WriteString("   - Upstream outputs must be consumed via `input_keys` referencing the producer (e.g., `tasks.<upstream_id>.output`).\n\n")

	sb.WriteString("4. **Human-in-the-Loop Approval Gates**:\n")
	sb.WriteString("   - Set `requires_approval: true` for tasks performing destructive, irreversible, or high-risk mutations ")
	sb.WriteString("(e.g., file deletions, database drops, git force-pushes, production deployments).\n")
	sb.WriteString("   - Read-only inspections or safe additive file writes should have `requires_approval: false`.\n\n")

	sb.WriteString("5. **Workspace Resource Locking**:\n")
	sb.WriteString("   - In `target_paths`, list workspace files or directories that the task modifies.\n")
	sb.WriteString("   - Overlapping target paths are locked cooperatively to prevent concurrent file corruption.\n\n")

	sb.WriteString("6. **Timeout Safeguard**:\n")
	sb.WriteString("   - `timeout_seconds`: Hard deadline in seconds (0 = rely on default silence watchdog).\n\n")

	sb.WriteString("7. **Output Hygiene**:\n")
	sb.WriteString("   - Output ONLY the raw JSON array (or wrapped in ```json ... ```). No extra commentary or preamble.\n\n")

	if strings.TrimSpace(rulesContent) != "" {
		sb.WriteString("## Universal Rules & Guidelines\n")
		sb.WriteString(strings.TrimSpace(rulesContent))
		sb.WriteString("\n\n")
	} else {
		sb.WriteString("## Universal Rules & Guidelines\n")
		sb.WriteString(strings.TrimSpace(rules.DefaultBaseline))
		sb.WriteString("\n\n")
	}

	if strings.TrimSpace(manifest) != "" {
		sb.WriteString("## Project Manifest & Repository Topology\n")
		sb.WriteString(strings.TrimSpace(manifest))
		sb.WriteString("\n\n")
	}

	sb.WriteString("## User Objective / Goal\n")
	sb.WriteString(strings.TrimSpace(goal))
	sb.WriteString("\n")

	return sb.String()
}

// BuildPlannerMessages creates the ChatMessage sequence for planning.
func BuildPlannerMessages(goal, manifest, rulesContent string) []prompt.ChatMessage {
	return []prompt.ChatMessage{
		{
			Role:    "user",
			Content: BuildPlannerPrompt(goal, manifest, rulesContent),
		},
	}
}
