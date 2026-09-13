package rules

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RuleSource indicates the origin of discovered rules.
type RuleSource string

const (
	SourceCLI       RuleSource = "cli"
	SourceWorkspace RuleSource = "workspace"
	SourceGlobal    RuleSource = "global"
	SourceBaseline  RuleSource = "baseline"
)

// DiscoveredRule represents a resolved rule set and its origin.
type DiscoveredRule struct {
	Source  RuleSource `json:"source"`
	Path    string     `json:"path"`
	Content string     `json:"content"`
}

// DiscoveryOptions configures the rule discovery engine.
type DiscoveryOptions struct {
	CLIPath      string // Path passed explicitly via CLI (e.g. --rules <path>)
	EnvRules     string // Explicit string/path override; falls back to REMINIS_RULES env var
	WorkspaceDir string // Root of workspace repo (defaults to ".")
	GlobalDir    string // Directory for global configuration (defaults to ~/.config)
	HomeDir      string // User home directory (defaults to os.UserHomeDir)
}

// RepoRulesPriority defines repository rule files in descending priority order.
var RepoRulesPriority = []string{
	".reminis.yaml",
	"AGENTS.md",
	"CLAUDE.md",
	".cursorrules",
	".windsurfrules",
	filepath.Join(".github", "copilot-instructions.md"),
}

// DefaultBaseline is the Reminis baseline fallback rules text.
const DefaultBaseline = `# Reminis Universal Baseline Rules

## 1. Clean & Hexagonal Architecture
- Enforce strict separation of concerns: domain core, application use cases, and infrastructure adapters.
- Dependencies must point inward: domain logic never depends on infrastructure, database, or network protocols.
- Keep components loosely coupled through interfaces (ports) and swappable implementations (adapters).

## 2. Idiomatic Go Error Handling
- Never ignore errors; check all returned errors explicitly.
- Wrap errors with contextual information using fmt.Errorf("%w: ...", err) to preserve the error chain.
- Fail fast and return early to maintain low cyclomatic complexity.

## 3. Bounded Tool Execution Protocols
- Every ephemeral worker must operate within a strict hard budget of maximum 3 tool execution turns.
- If exploration or mutation exceeds 3 turns, yield subtasks to the DAG scheduler instead of continuing the loop.
- Ephemeral conversation contexts are destroyed immediately upon task completion; state is persisted exclusively to the Blackboard or SQLite.
- All file mutations must be atomic and non-destructive without prior authorization.
`

// Discover resolves universal rules following the hierarchical cascade:
// 1. CLI / Environment flags (e.g. CLI path, REMINIS_RULES).
// 2. Workspace repository files in priority order:
//    .reminis.yaml, AGENTS.md, CLAUDE.md, .cursorrules, .windsurfrules, .github/copilot-instructions.md.
// 3. User global config: ~/.config/reminis/rules.md, ~/.config/gentle-ai/, or host global persona.
// 4. Reminis baseline fallback.
func Discover(opts DiscoveryOptions) (*DiscoveredRule, error) {
	// 1. CLI / Environment flags
	if opts.CLIPath != "" {
		data, err := os.ReadFile(opts.CLIPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CLI rules from %q: %w", opts.CLIPath, err)
		}
		return &DiscoveredRule{
			Source:  SourceCLI,
			Path:    opts.CLIPath,
			Content: string(data),
		}, nil
	}

	envRules := opts.EnvRules
	if envRules == "" {
		envRules = os.Getenv("REMINIS_RULES")
	}
	if envRules != "" {
		// If envRules points to an existing file, read it; otherwise treat as raw content
		if fi, err := os.Stat(envRules); err == nil && !fi.IsDir() {
			data, err := os.ReadFile(envRules)
			if err != nil {
				return nil, fmt.Errorf("failed to read REMINIS_RULES file %q: %w", envRules, err)
			}
			return &DiscoveredRule{
				Source:  SourceCLI,
				Path:    envRules,
				Content: string(data),
			}, nil
		}
		return &DiscoveredRule{
			Source:  SourceCLI,
			Path:    "ENV:REMINIS_RULES",
			Content: envRules,
		}, nil
	}

	// 2. Workspace repository files in priority order
	workspaceDir := opts.WorkspaceDir
	if workspaceDir == "" {
		workspaceDir = "."
	}

	for _, candidate := range RepoRulesPriority {
		targetPath := filepath.Join(workspaceDir, candidate)
		if fi, err := os.Stat(targetPath); err == nil && !fi.IsDir() {
			data, err := os.ReadFile(targetPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read workspace rules file %q: %w", targetPath, err)
			}
			return &DiscoveredRule{
				Source:  SourceWorkspace,
				Path:    targetPath,
				Content: string(data),
			}, nil
		}
	}

	// 3. User global config
	globalCandidates, err := resolveGlobalCandidates(opts)
	if err == nil {
		for _, targetPath := range globalCandidates {
			if fi, err := os.Stat(targetPath); err == nil && !fi.IsDir() {
				data, err := os.ReadFile(targetPath)
				if err != nil {
					return nil, fmt.Errorf("failed to read global rules file %q: %w", targetPath, err)
				}
				return &DiscoveredRule{
					Source:  SourceGlobal,
					Path:    targetPath,
					Content: string(data),
				}, nil
			}
		}
	}

	// 4. Reminis baseline fallback
	return &DiscoveredRule{
		Source:  SourceBaseline,
		Path:    "baseline",
		Content: DefaultBaseline,
	}, nil
}

func resolveGlobalCandidates(opts DiscoveryOptions) ([]string, error) {
	var configBase string
	if opts.GlobalDir != "" {
		configBase = opts.GlobalDir
	} else {
		homeDir := opts.HomeDir
		if homeDir == "" {
			var err error
			homeDir, err = os.UserHomeDir()
			if err != nil {
				return nil, err
			}
		}
		configBase = filepath.Join(homeDir, ".config")
	}

	candidates := []string{
		filepath.Join(configBase, "reminis", "rules.md"),
		filepath.Join(configBase, "reminis", "rules.yaml"),
		filepath.Join(configBase, "gentle-ai", "rules.md"),
		filepath.Join(configBase, "gentle-ai", "persona.md"),
		filepath.Join(configBase, "gentle-ai", "CLAUDE.md"),
		filepath.Join(configBase, "gentle-ai", "AGENTS.md"),
	}

	// Check if gentle-ai directory contains any markdown or rule files
	gentleDir := filepath.Join(configBase, "gentle-ai")
	if entries, err := os.ReadDir(gentleDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".md") || strings.HasSuffix(entry.Name(), ".yaml")) {
				candidates = append(candidates, filepath.Join(gentleDir, entry.Name()))
			}
		}
	}

	return candidates, nil
}

// DiscoverAndCompile discovers rules using the hierarchical cascade and compiles them
// into a deterministic CompiledPrefix.
func DiscoverAndCompile(opts DiscoveryOptions) (*CompiledPrefix, error) {
	rule, err := Discover(opts)
	if err != nil {
		return nil, err
	}
	return CompileDiscovered(rule), nil
}
