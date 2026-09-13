# Reminis Go Client SDK (`pkg/client`)

The `pkg/client` package provides a programmatic Go interface for embedding Reminis into custom agents, backend services, or developer CLI tools.

---

## Installation

```bash
go get github.com/SalvucciFacundo/Reminis/pkg/client
```

---

## Quick Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/SalvucciFacundo/Reminis/pkg/client"
)

func main() {
    ctx := context.Background()

    // 1. Initialize client with functional options
    c, err := client.New(
        client.WithDBPath("reminis.db"),
        client.WithModel("gpt-4o"),
        client.WithMaxConcurrency(4),
    )
    if err != nil {
        log.Fatalf("failed to initialize client: %v", err)
    }
    defer c.Close()

    // 2. Subscribe to real-time execution events
    events, unsubscribe := c.Events().Subscribe(100)
    defer unsubscribe()

    go func() {
        for ev := range events {
            fmt.Printf("[%s] Task %s: %s\n", ev.Type, ev.TaskID, ev.RunID)
        }
    }()

    // 3. Execute workflow goal
    res, err := c.Run(ctx, client.RunRequest{
        Goal: "Analyze internal/dag and generate test cases for edge cases",
    })
    if err != nil {
        log.Fatalf("run execution failed: %v", err)
    }

    fmt.Printf("Run ID: %s | Status: %s\n", res.RunID, res.Status)
    fmt.Printf("Completed Tasks: %d\n", len(res.CompletedTasks))
}
```

---

## Functional Options

The `client.New` constructor accepts functional configuration options:

| Option | Description |
| :--- | :--- |
| `WithDBPath(path string)` | Sets the SQLite database path (default: `reminis.db`) |
| `WithModel(model string)` | Sets the default LLM model for planning and worker steps |
| `WithMaxConcurrency(n int)`| Bounded worker pool size (default: `4`) |
| `WithBaseURL(url string)` | Custom OpenAI-compatible endpoint URL |
| `WithAPIKey(key string)` | Authentication key for inference provider |
| `WithRulesPath(path string)`| Explicit file path for rules discovery override |
| `WithAutoApprove(val string)`| Pass `"all"` to bypass interactive approval gates |
| `WithArtifactsDir(dir string)`| Directory for spilled >4KB working memory artifacts |

---

## Managing Long-Term Memory

```go
// Save an architectural observation
err := c.SaveFact(ctx, "architecture", "Uses Kahn algorithm for acyclicity checks", "project")
if err != nil {
    log.Printf("failed to save fact: %v", err)
}

// Search recorded facts
facts, err := c.SearchFacts(ctx, "architecture", "project")
if err != nil {
    log.Printf("search error: %v", err)
}
for _, f := range facts {
    fmt.Printf("Fact [%s]: %s\n", f.Topic, f.Content)
}
```

---

## Handling Approval Pauses & Resumption

When a task requires human authorization, `Run` will pause with status `WAITING_APPROVAL`:

```go
res, err := c.Run(ctx, client.RunRequest{
    Goal: "Deploy staging database migrations",
})
if err != nil {
    log.Fatal(err)
}

if res.Status == "WAITING_APPROVAL" {
    fmt.Println("Tasks awaiting approval:", res.WaitingTasks)

    // Approve the task with session scope
    err = c.Approve(ctx, res.RunID, res.WaitingTasks[0], "session")
    if err != nil {
        log.Fatal(err)
    }

    // Resume execution from checkpoint
    res, err = c.Resume(ctx, res.RunID)
    if err != nil {
        log.Fatal(err)
    }
}
```
