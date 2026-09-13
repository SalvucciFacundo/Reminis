package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/fds1288/reminis/internal/dag"
	"github.com/fds1288/reminis/internal/mcp"
	"github.com/fds1288/reminis/internal/orchestrator"
	"github.com/fds1288/reminis/pkg/client"
	"github.com/mattn/go-isatty"
)

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Reminis - Bounded Ephemeral OS-LLM Workflow Orchestrator

Usage:
  reminis <command> [arguments] [flags]

Commands:
  run <goal>           Execute a goal through DAG planning, ephemeral workers, and bounded tools
  resume <run_id>      Resume an interrupted or paused run from its SQLite checkpoint
  approve <run_id> <task_id> [--scope action|session]
                       Approve a task waiting in WAITING_APPROVAL and resume execution
  prefix show          Audit resolved universal rules cascade, token count, and PrefixHash
  mem <subcommand>     Manage architectural memory facts (search, save, list)
  mcp                  Start stdio Model Context Protocol (JSON-RPC 2.0) server

Run 'reminis <command> --help' for more details on a command.`)
}

func isTTY(r io.Reader) bool {
	if f, ok := r.(*os.File); ok {
		return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return false
}

// parseFlagSet separates flag arguments from positional arguments and parses flags,
// allowing flags to appear in any order relative to positional arguments.
func parseFlagSet(fs *flag.FlagSet, args []string) ([]string, error) {
	var flagArgs []string
	var posArgs []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
			if !strings.Contains(arg, "=") {
				flagName := strings.TrimLeft(arg, "-")
				f := fs.Lookup(flagName)
				if f != nil {
					if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); !ok || !bf.IsBoolFlag() {
						if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
							i++
							flagArgs = append(flagArgs, args[i])
						}
					}
				}
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}
	return posArgs, nil
}

func runCLI(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 1
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "help", "--help", "-h":
		printUsage(stdout)
		return 0

	case "run":
		return executeRun(cmdArgs, stdin, stdout, stderr)

	case "resume":
		return executeResume(cmdArgs, stdin, stdout, stderr)

	case "approve":
		return executeApprove(cmdArgs, stdin, stdout, stderr)

	case "prefix":
		return executePrefix(cmdArgs, stdout, stderr)

	case "mem":
		return executeMem(cmdArgs, stdout, stderr)

	case "mcp":
		return executeMCP(cmdArgs, stdin, stdout, stderr)

	default:
		fmt.Fprintf(stderr, "Unknown command: %s\n\n", cmd)
		printUsage(stderr)
		return 1
	}
}

func executeRun(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		model          string
		baseURL        string
		apiKey         string
		maxConcurrency int
		rulesPath      string
		dbPath         string
		autoApprove    bool
	)

	fs.StringVar(&model, "model", "", "LLM model override (defaults to env OPENAI_MODEL or gpt-4o-mini)")
	fs.StringVar(&baseURL, "base-url", "", "API Base URL override (defaults to env OPENAI_BASE_URL)")
	fs.StringVar(&apiKey, "api-key", "", "API Key override (defaults to env OPENAI_API_KEY)")
	fs.IntVar(&maxConcurrency, "max-concurrency", 4, "Maximum parallel worker goroutines")
	fs.StringVar(&rulesPath, "rules", "", "Path to explicit custom rules file")
	fs.StringVar(&dbPath, "db", "", "Path to SQLite database file")
	fs.BoolVar(&autoApprove, "auto-approve", false, "Automatically approve gated actions without prompting")

	remaining, err := parseFlagSet(fs, args)
	if err != nil {
		return 1
	}

	if len(remaining) == 0 {
		fmt.Fprintln(stderr, "Error: missing required workflow goal")
		fmt.Fprintln(stderr, "Usage: reminis run [flags] <goal>")
		return 1
	}
	goal := strings.Join(remaining, " ")

	tty := isTTY(stdin)
	var approvalHandler dag.ApprovalHandler
	if autoApprove {
		approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
			fmt.Fprintf(stderr, "[APPROVAL] Auto-approving task %s: %s\n", task.ID, task.Action)
			return true, nil
		}
	} else if tty {
		reader := bufio.NewReader(stdin)
		approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
			fmt.Fprintf(stderr, "\n[APPROVAL REQUIRED] Task %s requires human approval:\n  Action: %s\n", task.ID, task.Action)
			if len(task.TargetPaths) > 0 {
				fmt.Fprintf(stderr, "  Target Paths: %v\n", task.TargetPaths)
			}
			fmt.Fprint(stderr, "Approve execution? [y/N]: ")
			ans, err := reader.ReadString('\n')
			if err != nil {
				return false, nil
			}
			ans = strings.TrimSpace(strings.ToLower(ans))
			if ans == "y" || ans == "yes" {
				return true, nil
			}
			return false, nil
		}
	} else {
		// Headless pause
		approvalHandler = func(ctx context.Context, task *dag.Task) (bool, error) {
			fmt.Fprintf(stderr, "[APPROVAL REQUIRED] Task %s paused (non-interactive stdin).\n", task.ID)
			return false, nil
		}
	}

	opts := []client.Option{
		client.WithMaxConcurrency(maxConcurrency),
	}
	if dbPath != "" {
		opts = append(opts, client.WithDBPath(dbPath))
	}
	if model != "" {
		opts = append(opts, client.WithModel(model))
	}
	if baseURL != "" {
		opts = append(opts, client.WithBaseURL(baseURL))
	}
	if apiKey != "" {
		opts = append(opts, client.WithAPIKey(apiKey))
	}
	if rulesPath != "" {
		opts = append(opts, client.WithRulesPath(rulesPath))
	}
	if autoApprove {
		opts = append(opts, client.WithAutoApprove(true))
	} else if approvalHandler != nil {
		opts = append(opts, client.WithApprovalHandler(approvalHandler))
	}

	c, err := client.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "Error initializing client: %v\n", err)
		return 1
	}
	defer c.Close()

	// Stream telemetry events to stderr
	var wg sync.WaitGroup
	subCh, unsub := c.Events().Subscribe(100)
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-subCh:
				if !ok {
					return
				}
				printTelemetryEvent(stderr, ev)
			}
		}
	}()

	resp, err := c.Run(ctx, client.RunRequest{
		Goal:           goal,
		Model:          model,
		MaxConcurrency: maxConcurrency,
		RulesPath:      rulesPath,
		AutoApprove:    autoApprove,
	})

	cancel()
	wg.Wait()

	if err != nil && resp == nil {
		fmt.Fprintf(stderr, "\nRun failed with error: %v\n", err)
		return 1
	}

	printRunSummary(stdout, stderr, resp)
	if resp != nil && (resp.Status == string(dag.RunStatusCompleted)) {
		return 0
	}
	return 2
}

func executeResume(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dbPath  string
		baseURL string
		apiKey  string
		model   string
	)
	fs.StringVar(&dbPath, "db", "", "Path to SQLite database file")
	fs.StringVar(&baseURL, "base-url", "", "API Base URL override (defaults to env OPENAI_BASE_URL)")
	fs.StringVar(&apiKey, "api-key", "", "API Key override (defaults to env OPENAI_API_KEY)")
	fs.StringVar(&model, "model", "", "LLM model override (defaults to env OPENAI_MODEL)")

	remaining, err := parseFlagSet(fs, args)
	if err != nil {
		return 1
	}

	if len(remaining) == 0 {
		fmt.Fprintln(stderr, "Error: missing required run_id to resume")
		fmt.Fprintln(stderr, "Usage: reminis resume [flags] <run_id>")
		return 1
	}
	runID := remaining[0]

	opts := []client.Option{}
	if dbPath != "" {
		opts = append(opts, client.WithDBPath(dbPath))
	}
	if baseURL != "" {
		opts = append(opts, client.WithBaseURL(baseURL))
	}
	if apiKey != "" {
		opts = append(opts, client.WithAPIKey(apiKey))
	}
	if model != "" {
		opts = append(opts, client.WithModel(model))
	}

	c, err := client.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "Error initializing client: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	subCh, unsub := c.Events().Subscribe(100)
	defer unsub()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-subCh:
				if !ok {
					return
				}
				printTelemetryEvent(stderr, ev)
			}
		}
	}()

	resp, err := c.Resume(ctx, runID)
	cancel()
	wg.Wait()

	if err != nil && resp == nil {
		fmt.Fprintf(stderr, "Resume failed: %v\n", err)
		return 1
	}

	printRunSummary(stdout, stderr, resp)
	if resp != nil && resp.Status == string(dag.RunStatusCompleted) {
		return 0
	}
	return 2
}

func executeApprove(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("approve", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		scope   string
		dbPath  string
		baseURL string
		apiKey  string
		model   string
	)
	fs.StringVar(&scope, "scope", "action", "Approval scope: 'action' or 'session'")
	fs.StringVar(&dbPath, "db", "", "Path to SQLite database file")
	fs.StringVar(&baseURL, "base-url", "", "API Base URL override (defaults to env OPENAI_BASE_URL)")
	fs.StringVar(&apiKey, "api-key", "", "API Key override (defaults to env OPENAI_API_KEY)")
	fs.StringVar(&model, "model", "", "LLM model override (defaults to env OPENAI_MODEL)")

	remaining, err := parseFlagSet(fs, args)
	if err != nil {
		return 1
	}

	if len(remaining) < 2 {
		fmt.Fprintln(stderr, "Error: missing required run_id and task_id")
		fmt.Fprintln(stderr, "Usage: reminis approve [--scope action|session] [--db path] <run_id> <task_id>")
		return 1
	}
	runID := remaining[0]
	taskID := remaining[1]

	opts := []client.Option{}
	if dbPath != "" {
		opts = append(opts, client.WithDBPath(dbPath))
	}
	if baseURL != "" {
		opts = append(opts, client.WithBaseURL(baseURL))
	}
	if apiKey != "" {
		opts = append(opts, client.WithAPIKey(apiKey))
	}
	if model != "" {
		opts = append(opts, client.WithModel(model))
	}

	c, err := client.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "Error initializing client: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	subCh, unsub := c.Events().Subscribe(100)
	defer unsub()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-subCh:
				if !ok {
					return
				}
				printTelemetryEvent(stderr, ev)
			}
		}
	}()

	fmt.Fprintf(stderr, "[APPROVE] Granting scope %q approval for task %s (run %s)...\n", scope, taskID, runID)
	err = c.Approve(ctx, runID, taskID, scope)
	cancel()
	wg.Wait()

	if err != nil {
		fmt.Fprintf(stderr, "Approve failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "Approval recorded and run %s resumed successfully.\n", runID)
	return 0
}

func executePrefix(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("prefix", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		rulesPath    string
		workspaceDir string
	)
	fs.StringVar(&rulesPath, "rules", "", "Explicit path to rules file")
	fs.StringVar(&workspaceDir, "workspace", ".", "Workspace directory to inspect")

	if _, err := parseFlagSet(fs, args); err != nil {
		return 1
	}

	opts := []client.Option{
		client.WithWorkspaceDir(workspaceDir),
	}
	if rulesPath != "" {
		opts = append(opts, client.WithRulesPath(rulesPath))
	}

	c, err := client.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "Error initializing client: %v\n", err)
		return 1
	}
	defer c.Close()

	info, err := c.GetCompiledPrefix(context.Background())
	if err != nil {
		fmt.Fprintf(stderr, "Failed to resolve rules prefix: %v\n", err)
		return 1
	}

	cacheThreshold := 1024
	meetsThreshold := "NO"
	if info.TokenCount >= cacheThreshold {
		meetsThreshold = "YES (Cache Active)"
	}

	fmt.Fprintln(stdout, "=== Reminis Rules Prefix Audit ===")
	fmt.Fprintf(stdout, "Source Origin:       %s\n", info.Source)
	fmt.Fprintf(stdout, "Discovered Path:     %s\n", info.Path)
	fmt.Fprintf(stdout, "SHA-256 PrefixHash:  %s\n", info.Hash)
	fmt.Fprintf(stdout, "Estimated Tokens:    ~%d\n", info.TokenCount)
	fmt.Fprintf(stdout, "Provider Cacheable:  %s (Threshold: %d tokens)\n", meetsThreshold, cacheThreshold)
	fmt.Fprintln(stdout, "----------------------------------")
	return 0
}

func executeMem(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "Usage: reminis mem <search|save|list> [arguments] [flags]")
		return 1
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "search":
		fs := flag.NewFlagSet("mem search", flag.ContinueOnError)
		var (
			scope  string
			dbPath string
		)
		fs.StringVar(&scope, "scope", "", "Scope filter: project or global")
		fs.StringVar(&dbPath, "db", "", "Path to SQLite database")
		rem, err := parseFlagSet(fs, subArgs)
		if err != nil {
			return 1
		}
		if len(rem) == 0 {
			fmt.Fprintln(stderr, "Usage: reminis mem search <topic> [--scope project|global]")
			return 1
		}
		topic := rem[0]

		opts := []client.Option{}
		if dbPath != "" {
			opts = append(opts, client.WithDBPath(dbPath))
		}
		c, err := client.New(opts...)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return 1
		}
		defer c.Close()

		facts, err := c.SearchFacts(context.Background(), topic, scope)
		if err != nil {
			fmt.Fprintf(stderr, "Search error: %v\n", err)
			return 1
		}
		if len(facts) == 0 {
			fmt.Fprintf(stdout, "No facts found matching topic %q\n", topic)
			return 0
		}
		fmt.Fprintf(stdout, "Found %d fact(s) matching %q:\n", len(facts), topic)
		for _, f := range facts {
			fmt.Fprintf(stdout, "- [%s] (%s): %s\n", f.Topic, f.Scope, f.Content)
		}
		return 0

	case "save":
		fs := flag.NewFlagSet("mem save", flag.ContinueOnError)
		var (
			scope  string
			dbPath string
		)
		fs.StringVar(&scope, "scope", "project", "Scope: project or global")
		fs.StringVar(&dbPath, "db", "", "Path to SQLite database")
		rem, err := parseFlagSet(fs, subArgs)
		if err != nil {
			return 1
		}
		if len(rem) < 2 {
			fmt.Fprintln(stderr, "Usage: reminis mem save <topic> <content> [--scope project|global]")
			return 1
		}
		topic := rem[0]
		content := strings.Join(rem[1:], " ")

		opts := []client.Option{}
		if dbPath != "" {
			opts = append(opts, client.WithDBPath(dbPath))
		}
		c, err := client.New(opts...)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return 1
		}
		defer c.Close()

		if err := c.SaveFact(context.Background(), topic, content, scope); err != nil {
			fmt.Fprintf(stderr, "Save error: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "Saved fact [%s] (%s)\n", topic, scope)
		return 0

	case "list":
		fs := flag.NewFlagSet("mem list", flag.ContinueOnError)
		var dbPath string
		fs.StringVar(&dbPath, "db", "", "Path to SQLite database")
		rem, err := parseFlagSet(fs, subArgs)
		if err != nil {
			return 1
		}
		var sessionID string
		if len(rem) > 0 {
			sessionID = rem[0]
		}

		opts := []client.Option{}
		if dbPath != "" {
			opts = append(opts, client.WithDBPath(dbPath))
		}
		c, err := client.New(opts...)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return 1
		}
		defer c.Close()

		facts, err := c.ListFacts(context.Background(), sessionID)
		if err != nil {
			fmt.Fprintf(stderr, "List error: %v\n", err)
			return 1
		}
		if len(facts) == 0 {
			fmt.Fprintln(stdout, "No facts stored.")
			return 0
		}
		fmt.Fprintf(stdout, "Listing %d fact(s):\n", len(facts))
		for _, f := range facts {
			fmt.Fprintf(stdout, "- [%s] (%s): %s\n", f.Topic, f.Scope, f.Content)
		}
		return 0

	default:
		fmt.Fprintf(stderr, "Unknown mem subcommand %q (expected search, save, list)\n", sub)
		return 1
	}
}

func executeMCP(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dbPath         string
		model          string
		maxConcurrency int
	)
	fs.StringVar(&dbPath, "db", "", "Path to SQLite database")
	fs.StringVar(&model, "model", "", "Model identifier")
	fs.IntVar(&maxConcurrency, "max-concurrency", 4, "Concurrency limit")

	if _, err := parseFlagSet(fs, args); err != nil {
		return 1
	}

	opts := []client.Option{
		client.WithMaxConcurrency(maxConcurrency),
	}
	if dbPath != "" {
		opts = append(opts, client.WithDBPath(dbPath))
	}
	if model != "" {
		opts = append(opts, client.WithModel(model))
	}

	c, err := client.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to initialize MCP client: %v\n", err)
		return 1
	}
	defer c.Close()

	// Strict stdout hygiene: os.Stdout (passed as stdout) is strictly reserved
	// for JSON-RPC 2.0 framing. All diagnostics go to stderr.
	server := mcp.NewServer(c, stdin, stdout, stderr)
	if err := server.Serve(context.Background()); err != nil {
		fmt.Fprintf(stderr, "MCP server exited with error: %v\n", err)
		return 1
	}
	return 0
}

func printTelemetryEvent(w io.Writer, ev orchestrator.Event) {
	switch ev.Type {
	case orchestrator.EventRunStarted:
		fmt.Fprintf(w, "[EVENT:RUN_STARTED] Run %s | Payload: %v\n", ev.RunID, ev.Payload)
	case orchestrator.EventTaskStarted:
		fmt.Fprintf(w, "[EVENT:TASK_STARTED] %s | Task %s\n", ev.RunID, ev.TaskID)
	case orchestrator.EventTaskWaitingApproval:
		fmt.Fprintf(w, "[EVENT:WAITING_APPROVAL] Task %s paused awaiting approval\n", ev.TaskID)
	case orchestrator.EventTaskCompleted:
		fmt.Fprintf(w, "[EVENT:TASK_COMPLETED] Task %s finished successfully\n", ev.TaskID)
	case orchestrator.EventTaskFailed:
		fmt.Fprintf(w, "[EVENT:TASK_FAILED] Task %s failed: %v\n", ev.TaskID, ev.Payload)
	case orchestrator.EventRunFinished:
		fmt.Fprintf(w, "[EVENT:RUN_FINISHED] Run %s finished\n", ev.RunID)
	}
}

func printRunSummary(stdout, stderr io.Writer, resp *client.RunResponse) {
	if resp == nil {
		return
	}
	fmt.Fprintf(stdout, "\n=== Run Execution Summary ===\n")
	fmt.Fprintf(stdout, "Run ID:          %s\n", resp.RunID)
	fmt.Fprintf(stdout, "Status:          %s\n", resp.Status)
	fmt.Fprintf(stdout, "Completed Tasks: %d %v\n", len(resp.CompletedTasks), resp.CompletedTasks)
	if len(resp.WaitingTasks) > 0 {
		fmt.Fprintf(stdout, "Waiting Tasks:   %d %v\n", len(resp.WaitingTasks), resp.WaitingTasks)
		fmt.Fprintf(stdout, "Resume Command:  reminis approve %s <task_id>\n", resp.RunID)
	}
	if len(resp.SkippedTasks) > 0 {
		fmt.Fprintf(stdout, "Skipped Tasks:   %d %v\n", len(resp.SkippedTasks), resp.SkippedTasks)
	}
	if resp.TotalTokens > 0 {
		if resp.CachedTokens > 0 {
			fmt.Fprintf(stdout, "Total Tokens:    %d (Prompt: %d, Completion: %d, Cached: %d)\n", resp.TotalTokens, resp.PromptTokens, resp.CompletionTokens, resp.CachedTokens)
		} else {
			fmt.Fprintf(stdout, "Total Tokens:    %d (Prompt: %d, Completion: %d)\n", resp.TotalTokens, resp.PromptTokens, resp.CompletionTokens)
		}
	}
	if resp.FailedTask != nil {
		fmt.Fprintf(stderr, "Failed Task:     %s (%s): %s\n", resp.FailedTask.TaskID, resp.FailedTask.Action, resp.FailedTask.Error)
	}
	fmt.Fprintf(stdout, "Can Resume:      %v\n", resp.CanResume)
}
