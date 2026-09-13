package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"--help"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0 for --help, got %d", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "reminis <command>") {
		t.Errorf("expected usage string in help output, got: %s", out)
	}
	if !strings.Contains(out, "prefix show") || !strings.Contains(out, "mem <subcommand>") || !strings.Contains(out, "mcp") {
		t.Errorf("missing subcommands in help output: %s", out)
	}
}

func TestCLIPrefixShow(t *testing.T) {
	tempDir := t.TempDir()
	var stdout, stderr bytes.Buffer

	code := runCLI([]string{"prefix", "show", "--workspace", tempDir}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0 for prefix show, got %d. stderr: %s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "Reminis Rules Prefix Audit") {
		t.Errorf("expected audit header in output, got: %s", out)
	}
	if !strings.Contains(out, "SHA-256 PrefixHash:") {
		t.Errorf("expected PrefixHash in output, got: %s", out)
	}
	if !strings.Contains(out, "Estimated Tokens:") {
		t.Errorf("expected Estimated Tokens in output, got: %s", out)
	}
}

func TestCLIMemSubcommands(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "cli_mem.db")

	// 1. mem save
	{
		var stdout, stderr bytes.Buffer
		code := runCLI([]string{
			"mem", "save", "architecture", "Modular Hexagonal Ports and Adapters",
			"--scope", "project",
			"--db", dbPath,
		}, nil, &stdout, &stderr)

		if code != 0 {
			t.Fatalf("mem save failed (code %d): %s", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "Saved fact [architecture]") {
			t.Errorf("unexpected stdout: %s", stdout.String())
		}
	}

	// 2. mem search
	{
		var stdout, stderr bytes.Buffer
		code := runCLI([]string{
			"mem", "search", "architecture",
			"--db", dbPath,
		}, nil, &stdout, &stderr)

		if code != 0 {
			t.Fatalf("mem search failed (code %d): %s", code, stderr.String())
		}
		out := stdout.String()
		if !strings.Contains(out, "Modular Hexagonal Ports and Adapters") {
			t.Errorf("expected fact in search output, got: %s", out)
		}
	}

	// 3. mem list
	{
		var stdout, stderr bytes.Buffer
		code := runCLI([]string{
			"mem", "list",
			"--db", dbPath,
		}, nil, &stdout, &stderr)

		if code != 0 {
			t.Fatalf("mem list failed (code %d): %s", code, stderr.String())
		}
		out := stdout.String()
		if !strings.Contains(out, "Listing 1 fact(s)") {
			t.Errorf("expected listing 1 fact, got: %s", out)
		}
	}
}

func TestCLIRunMissingGoal(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"run"}, nil, &stdout, &stderr)
	if code == 0 {
		t.Error("expected non-zero exit code when goal is missing")
	}
	if !strings.Contains(stderr.String(), "missing required workflow goal") {
		t.Errorf("expected error message on stderr, got: %s", stderr.String())
	}
}

func TestCLIResumeMissingRunID(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"resume"}, nil, &stdout, &stderr)
	if code == 0 {
		t.Error("expected non-zero exit code when run_id is missing")
	}
	if !strings.Contains(stderr.String(), "missing required run_id to resume") {
		t.Errorf("expected error message on stderr, got: %s", stderr.String())
	}
}

func TestCLIApproveMissingArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"approve", "only_one_arg"}, nil, &stdout, &stderr)
	if code == 0 {
		t.Error("expected non-zero exit code when args missing")
	}
	if !strings.Contains(stderr.String(), "missing required run_id and task_id") {
		t.Errorf("expected error message on stderr, got: %s", stderr.String())
	}
}
