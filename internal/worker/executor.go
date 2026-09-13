package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fds1288/reminis/internal/prompt"
)

var (
	// ErrSilenceTimeout is returned when an executed command produces no output for the silence duration.
	ErrSilenceTimeout = errors.New("command terminated by silence watchdog: output inactivity timeout")
	// ErrUnknownTool is returned when a tool call references an unrecognized tool name.
	ErrUnknownTool = errors.New("unknown tool requested")
)

// DefaultSilenceDuration is the standard 90-second inactivity timeout.
const DefaultSilenceDuration = 90 * time.Second

// Executor manages tool dispatch, filesystem isolation, process groups, and silence watchdogs.
type Executor struct {
	WorkDir         string
	SilenceDuration time.Duration
}

// NewExecutor creates a tool executor.
func NewExecutor(workDir string, silenceDuration time.Duration) *Executor {
	if workDir == "" {
		workDir = "."
	}
	if silenceDuration <= 0 {
		silenceDuration = DefaultSilenceDuration
	}
	return &Executor{
		WorkDir:         workDir,
		SilenceDuration: silenceDuration,
	}
}

// StandardTools returns the OpenAI Tool definitions for Reminis built-in tools.
func StandardTools() []Tool {
	return []Tool{
		{
			Type: "function",
			Function: FunctionDeclaration{
				Name:        "read_file",
				Description: "Reads the complete contents of a file from the workspace filesystem.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{
							"type":        "string",
							"description": "The relative or absolute file path to read.",
						},
					},
					"required": []string{"path"},
				},
			},
		},
		{
			Type: "function",
			Function: FunctionDeclaration{
				Name:        "write_file",
				Description: "Atomically writes or overwrites content to a specified file.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{
							"type":        "string",
							"description": "Destination file path.",
						},
						"content": map[string]any{
							"type":        "string",
							"description": "The full text content to write.",
						},
					},
					"required": []string{"path", "content"},
				},
			},
		},
		{
			Type: "function",
			Function: FunctionDeclaration{
				Name:        "bash",
				Description: "Executes a shell command in an isolated process group monitored by a silence watchdog.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"command": map[string]any{
							"type":        "string",
							"description": "The shell command line to run.",
						},
					},
					"required": []string{"command"},
				},
			},
		},
		{
			Type: "function",
			Function: FunctionDeclaration{
				Name:        "git",
				Description: "Executes a git command in the repository workspace.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"command": map[string]any{
							"type":        "string",
							"description": "The git subcommand and arguments (e.g. status, diff, commit).",
						},
					},
					"required": []string{"command"},
				},
			},
		},
	}
}

// Execute invokes a tool by name with JSON-encoded arguments.
func (e *Executor) Execute(ctx context.Context, name string, argsJSON string) (string, error) {
	switch name {
	case "read_file":
		var args struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
			return "", fmt.Errorf("invalid read_file arguments: %w", err)
		}
		if args.Path == "" {
			return "", errors.New("read_file requires non-empty path")
		}
		return e.ReadFile(args.Path)

	case "write_file":
		var args struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
			return "", fmt.Errorf("invalid write_file arguments: %w", err)
		}
		if args.Path == "" {
			return "", errors.New("write_file requires non-empty path")
		}
		return e.WriteFile(args.Path, args.Content)

	case "bash":
		var args struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
			return "", fmt.Errorf("invalid bash arguments: %w", err)
		}
		if args.Command == "" {
			return "", errors.New("bash requires non-empty command")
		}
		return e.RunBash(ctx, args.Command)

	case "git":
		var args struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
			return "", fmt.Errorf("invalid git arguments: %w", err)
		}
		if args.Command == "" {
			return "", errors.New("git requires non-empty command")
		}
		return e.RunGit(ctx, args.Command)

	default:
		return "", fmt.Errorf("%w: %s", ErrUnknownTool, name)
	}
}

// ExecuteToolCall dispatches a prompt.ToolCall directly.
func (e *Executor) ExecuteToolCall(ctx context.Context, tc prompt.ToolCall) (string, error) {
	return e.Execute(ctx, tc.Function.Name, tc.Function.Arguments)
}

// ReadFile reads a file relative to WorkDir.
func (e *Executor) ReadFile(filePath string) (string, error) {
	target := filePath
	if !filepath.IsAbs(target) {
		target = filepath.Join(e.WorkDir, target)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		return "", fmt.Errorf("read_file failed: %w", err)
	}
	return string(data), nil
}

// WriteFile atomically writes content to a file relative to WorkDir.
func (e *Executor) WriteFile(filePath string, content string) (string, error) {
	target := filePath
	if !filepath.IsAbs(target) {
		target = filepath.Join(e.WorkDir, target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return "", fmt.Errorf("failed to create directories for %q: %w", target, err)
	}

	// Write atomically using temporary file in the same directory
	tmpFile, err := os.CreateTemp(filepath.Dir(target), ".reminis_write_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.WriteString(content); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("failed to write content: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("failed to sync content: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("failed to close temporary file: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("failed to atomic rename %s -> %s: %w", tmpName, target, err)
	}

	return fmt.Sprintf("File written successfully: %s (%d bytes)", filePath, len(content)), nil
}

// RunBash executes a bash command with Process Group Isolation and a Silence Watchdog.
func (e *Executor) RunBash(ctx context.Context, command string) (string, error) {
	return e.runIsolatedCommand(ctx, "bash", "-c", command)
}

// RunGit executes a git command with Process Group Isolation and a Silence Watchdog.
func (e *Executor) RunGit(ctx context.Context, command string) (string, error) {
	return e.runIsolatedCommand(ctx, "bash", "-c", "git "+command)
}

// watchdogStream wraps a buffer and signals an activity channel on each byte written.
type watchdogStream struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	activity chan struct{}
}

func (w *watchdogStream) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	w.buf.Write(p)
	w.mu.Unlock()

	select {
	case w.activity <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (w *watchdogStream) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// runIsolatedCommand launches the given binary with:
// 1. Process Group Isolation (Setpgid: true).
// 2. Silence Watchdog (kills process group if no output is received for SilenceDuration).
// 3. Context cancellation handling (kills process group on ctx.Done()).
func (e *Executor) runIsolatedCommand(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = e.WorkDir

	// Process group isolation
	setProcessGroup(cmd)

	stream := &watchdogStream{
		activity: make(chan struct{}, 64),
	}
	cmd.Stdout = stream
	cmd.Stderr = stream

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start process %s: %w", name, err)
	}

	doneCh := make(chan struct{})
	var silenceKilled atomic.Bool

	// Silence watchdog & cancellation supervisor
	go func() {
		timer := time.NewTimer(e.SilenceDuration)
		defer timer.Stop()

		for {
			select {
			case <-doneCh:
				return

			case <-ctx.Done():
				killProcessGroup(cmd)
				return

			case <-stream.activity:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(e.SilenceDuration)

			case <-timer.C:
				silenceKilled.Store(true)
				killProcessGroup(cmd)
				return
			}
		}
	}()

	waitErr := cmd.Wait()
	close(doneCh)

	output := stream.String()

	if silenceKilled.Load() {
		return output, ErrSilenceTimeout
	}

	if ctx.Err() != nil {
		return output, ctx.Err()
	}

	if waitErr != nil {
		return output, fmt.Errorf("command exited with error: %w", waitErr)
	}

	return output, nil
}

