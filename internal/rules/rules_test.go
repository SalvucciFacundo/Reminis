package rules

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCascadePrecedence(t *testing.T) {
	tempDir := t.TempDir()
	workspaceDir := filepath.Join(tempDir, "workspace")
	globalDir := filepath.Join(tempDir, "global")
	if err := os.MkdirAll(workspaceDir, 0755); err != nil {
		t.Fatalf("failed to create workspace dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(globalDir, "reminis"), 0755); err != nil {
		t.Fatalf("failed to create global dir: %v", err)
	}

	// 1. When nothing is present, fallback to baseline
	discovered, err := Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error discovering baseline: %v", err)
	}
	if discovered.Source != SourceBaseline {
		t.Fatalf("expected SourceBaseline, got %s", discovered.Source)
	}

	// 2. Global config present -> SourceGlobal overrides baseline
	globalRulesPath := filepath.Join(globalDir, "reminis", "rules.md")
	if err := os.WriteFile(globalRulesPath, []byte("global rules content"), 0644); err != nil {
		t.Fatalf("failed to write global rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error discovering global: %v", err)
	}
	if discovered.Source != SourceGlobal || discovered.Content != "global rules content" {
		t.Fatalf("expected SourceGlobal with content, got %s: %s", discovered.Source, discovered.Content)
	}

	// 3. Workspace repository files in priority order
	// Create .windsurfrules (lower priority than .cursorrules)
	windsurfPath := filepath.Join(workspaceDir, ".windsurfrules")
	if err := os.WriteFile(windsurfPath, []byte("windsurf rules"), 0644); err != nil {
		t.Fatalf("failed to write windsurf rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Source != SourceWorkspace || discovered.Content != "windsurf rules" {
		t.Fatalf("expected windsurf rules, got %s: %s", discovered.Source, discovered.Content)
	}

	// Create .cursorrules (higher priority than .windsurfrules)
	cursorPath := filepath.Join(workspaceDir, ".cursorrules")
	if err := os.WriteFile(cursorPath, []byte("cursor rules"), 0644); err != nil {
		t.Fatalf("failed to write cursor rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Content != "cursor rules" {
		t.Fatalf("expected cursor rules to take precedence, got %s", discovered.Content)
	}

	// Create CLAUDE.md (higher priority than .cursorrules)
	claudePath := filepath.Join(workspaceDir, "CLAUDE.md")
	if err := os.WriteFile(claudePath, []byte("claude rules"), 0644); err != nil {
		t.Fatalf("failed to write claude rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Content != "claude rules" {
		t.Fatalf("expected claude rules to take precedence, got %s", discovered.Content)
	}

	// Create AGENTS.md (higher priority than CLAUDE.md)
	agentsPath := filepath.Join(workspaceDir, "AGENTS.md")
	if err := os.WriteFile(agentsPath, []byte("agents rules"), 0644); err != nil {
		t.Fatalf("failed to write agents rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Content != "agents rules" {
		t.Fatalf("expected agents rules to take precedence, got %s", discovered.Content)
	}

	// Create .reminis.yaml (highest priority workspace file)
	reminisPath := filepath.Join(workspaceDir, ".reminis.yaml")
	if err := os.WriteFile(reminisPath, []byte("reminis yaml rules"), 0644); err != nil {
		t.Fatalf("failed to write reminis yaml rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Content != "reminis yaml rules" {
		t.Fatalf("expected .reminis.yaml to take precedence, got %s", discovered.Content)
	}

	// 4. Env flags take precedence over workspace
	discovered, err = Discover(DiscoveryOptions{
		EnvRules:     "env rules inline",
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Source != SourceCLI || discovered.Content != "env rules inline" {
		t.Fatalf("expected env rules override, got %s: %s", discovered.Source, discovered.Content)
	}

	// 5. CLI flag takes precedence over everything
	cliPath := filepath.Join(tempDir, "cli_rules.md")
	if err := os.WriteFile(cliPath, []byte("cli rules explicit"), 0644); err != nil {
		t.Fatalf("failed to write cli rules: %v", err)
	}
	discovered, err = Discover(DiscoveryOptions{
		CLIPath:      cliPath,
		EnvRules:     "env rules inline",
		WorkspaceDir: workspaceDir,
		GlobalDir:    globalDir,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered.Source != SourceCLI || discovered.Content != "cli rules explicit" {
		t.Fatalf("expected CLI rules explicit override, got %s: %s", discovered.Source, discovered.Content)
	}
}

func TestDeterministicSHA256HashStability(t *testing.T) {
	content1 := "Rule A: All functions must be pure.\nRule B: Log structured JSON."
	compiled1 := Compile(content1)
	compiled2 := Compile(content1)

	if compiled1.Hash != compiled2.Hash {
		t.Fatalf("expected identical hashes, got %s vs %s", compiled1.Hash, compiled2.Hash)
	}
	if compiled1.Content != compiled2.Content {
		t.Fatalf("expected identical compiled content")
	}
	if compiled1.TokenCount != compiled2.TokenCount {
		t.Fatalf("expected identical token counts, got %d vs %d", compiled1.TokenCount, compiled2.TokenCount)
	}

	// Same content with CRLF should normalize to same hash
	contentCRLF := "Rule A: All functions must be pure.\r\nRule B: Log structured JSON."
	compiledCRLF := Compile(contentCRLF)
	if compiledCRLF.Hash != compiled1.Hash {
		t.Fatalf("expected CRLF normalization to produce identical hash, got %s vs %s", compiledCRLF.Hash, compiled1.Hash)
	}
}

func TestTokenPaddingGuarantee(t *testing.T) {
	shortRule := "Short rule: fail fast."
	compiled := Compile(shortRule)

	if compiled.TokenCount < MinPromptCacheTokens {
		t.Fatalf("expected TokenCount >= %d, got %d", MinPromptCacheTokens, compiled.TokenCount)
	}
	if len(compiled.Content) < MinPromptCacheTokens*4 {
		t.Fatalf("expected content length >= %d, got %d", MinPromptCacheTokens*4, len(compiled.Content))
	}
	if compiled.Hash == "" {
		t.Fatalf("expected non-empty hash")
	}
}
