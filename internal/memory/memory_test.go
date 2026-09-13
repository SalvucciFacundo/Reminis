package memory

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/fds1288/reminis/internal/store"
)

func TestFactSavingAndRetrieval(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_memory_saving.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mem := NewMemoryService(s)
	ctx := context.Background()

	// 1. Save fact
	fact1 := Fact{
		ID:        "fact-arch-1",
		SessionID: "sess-100",
		Topic:     "architecture",
		Content:   "Reminis adheres to Hexagonal Architecture with inward-pointing dependencies",
		Scope:     "project",
	}
	if err := mem.SaveFact(ctx, fact1); err != nil {
		t.Fatalf("SaveFact failed: %v", err)
	}

	// 2. Search facts by topic
	results, err := mem.SearchFacts(ctx, "architecture", "project")
	if err != nil {
		t.Fatalf("SearchFacts failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(results))
	}
	if results[0].Content != fact1.Content {
		t.Fatalf("expected content %q, got %q", fact1.Content, results[0].Content)
	}

	// 3. Upsert fact with updated content using same ID
	fact1Updated := Fact{
		ID:        "fact-arch-1",
		SessionID: "sess-100",
		Topic:     "architecture",
		Content:   "Reminis uses Hexagonal Architecture: core domain, ports, and adapters",
		Scope:     "project",
	}
	if err := mem.SaveFact(ctx, fact1Updated); err != nil {
		t.Fatalf("SaveFact upsert failed: %v", err)
	}

	resultsAfterUpdate, err := mem.SearchFacts(ctx, "architecture", "")
	if err != nil {
		t.Fatalf("SearchFacts after update failed: %v", err)
	}
	if len(resultsAfterUpdate) != 1 {
		t.Fatalf("expected 1 fact after upsert, got %d", len(resultsAfterUpdate))
	}
	if resultsAfterUpdate[0].Content != fact1Updated.Content {
		t.Fatalf("expected updated content %q, got %q", fact1Updated.Content, resultsAfterUpdate[0].Content)
	}
}

func TestTopicSearchAndScoping(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_memory_search.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mem := NewMemoryService(s)
	ctx := context.Background()

	testFacts := []Fact{
		{Topic: "database", Content: "SQLite WAL mode with busy_timeout=5000ms", Scope: "project"},
		{Topic: "database/migration", Content: "Automated DDL migrations on store init", Scope: "project"},
		{Topic: "architecture", Content: "Ephemeral workers discard context on completion", Scope: "project"},
		{Topic: "session/override", Content: "Temporary debug log verbosity enabled", Scope: "session"},
	}

	for _, f := range testFacts {
		if err := mem.SaveFact(ctx, f); err != nil {
			t.Fatalf("failed to save fact: %v", err)
		}
	}

	// Search partial topic match: "data" matches "database" and "database/migration"
	dataFacts, err := mem.SearchFacts(ctx, "data", "")
	if err != nil {
		t.Fatalf("SearchFacts failed: %v", err)
	}
	if len(dataFacts) != 2 {
		t.Fatalf("expected 2 database facts, got %d", len(dataFacts))
	}

	// Search by scope "session"
	sessionFacts, err := mem.SearchFacts(ctx, "", "session")
	if err != nil {
		t.Fatalf("SearchFacts by scope failed: %v", err)
	}
	if len(sessionFacts) != 1 || sessionFacts[0].Topic != "session/override" {
		t.Fatalf("expected 1 session fact, got: %+v", sessionFacts)
	}

	// Search all (empty topic and empty scope)
	allFacts, err := mem.SearchFacts(ctx, "", "")
	if err != nil {
		t.Fatalf("SearchFacts all failed: %v", err)
	}
	if len(allFacts) != 4 {
		t.Fatalf("expected 4 total facts, got %d", len(allFacts))
	}
}

func TestFactExtractionFromRunOutputs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_memory_extract.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mem := NewMemoryService(s)
	ctx := context.Background()

	taskOutputs := map[string]string{
		// 1. Explicit JSON facts array
		"tasks.audit.output": `{
			"facts": [
				{"topic": "security", "content": "Strict process group isolation via Setpgid: true"},
				{"topic": "runtime", "content": "Silence watchdog timeout set to 90 seconds"}
			]
		}`,
		// 2. Structured architectural JSON keys
		"tasks.database.output": `{
			"database": "Pure-Go SQLite engine without CGO dependencies",
			"conventions": [
				"Always check errors and wrap with fmt.Errorf",
				"Bounded 3-turn tool budget per worker"
			]
		}`,
		// 3. Plain text / Markdown patterns
		"tasks.summary.output": `
Execution summary:
Decision: Use Go channels for real-time SSE telemetry streaming
- **[storage]**: Blackboard spills payloads exceeding 4KB to disk
- [concurrency]: Cooperative file locking prevents disk race conditions
		`,
	}

	sessionID := "sess-extract-1"
	persisted, err := mem.ExtractAndPersist(ctx, sessionID, taskOutputs)
	if err != nil {
		t.Fatalf("ExtractAndPersist failed: %v", err)
	}

	if len(persisted) < 6 {
		t.Fatalf("expected at least 6 extracted facts, got %d", len(persisted))
	}

	// Verify all persisted facts belong to the session in SQLite
	sessionFacts, err := mem.ListFactsBySession(ctx, sessionID)
	if err != nil {
		t.Fatalf("ListFactsBySession failed: %v", err)
	}
	if len(sessionFacts) != len(persisted) {
		t.Fatalf("expected %d session facts in store, got %d", len(persisted), len(sessionFacts))
	}

	// Verify specific extracted topics exist
	foundSecurity := false
	foundStorage := false
	foundDecision := false
	for _, f := range sessionFacts {
		if f.Topic == "security" && strings.Contains(f.Content, "Setpgid") {
			foundSecurity = true
		}
		if f.Topic == "storage" && strings.Contains(f.Content, "4KB") {
			foundStorage = true
		}
		if f.Topic == "decision" && strings.Contains(f.Content, "Go channels") {
			foundDecision = true
		}
	}

	if !foundSecurity {
		t.Errorf("expected to find security fact about Setpgid")
	}
	if !foundStorage {
		t.Errorf("expected to find storage fact about 4KB")
	}
	if !foundDecision {
		t.Errorf("expected to find decision fact about Go channels")
	}
}

func TestFormatContext(t *testing.T) {
	// 1. Empty facts
	if formatted := FormatContext(nil); formatted != "" {
		t.Fatalf("expected empty string for nil facts, got %q", formatted)
	}
	if formatted := FormatContext([]Fact{}); formatted != "" {
		t.Fatalf("expected empty string for empty slice, got %q", formatted)
	}

	// 2. Formatted markdown
	facts := []Fact{
		{Topic: "architecture", Content: "Clean Hexagonal Architecture"},
		{Topic: "database", Content: "SQLite WAL mode with busy_timeout=5000ms"},
	}
	ctxStr := FormatContext(facts)
	if !strings.Contains(ctxStr, "## Architectural Memory & Learned Conventions") {
		t.Fatalf("missing header in formatted context: %s", ctxStr)
	}
	if !strings.Contains(ctxStr, "- **[architecture]**: Clean Hexagonal Architecture") {
		t.Fatalf("missing architecture bullet: %s", ctxStr)
	}
	if !strings.Contains(ctxStr, "- **[database]**: SQLite WAL mode with busy_timeout=5000ms") {
		t.Fatalf("missing database bullet: %s", ctxStr)
	}

	// 3. MemoryService GetFactContext
	dbPath := filepath.Join(t.TempDir(), "test_context.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mem := NewMemoryService(s)
	ctx := context.Background()
	for _, f := range facts {
		_ = mem.SaveFact(ctx, f)
	}

	archCtx, err := mem.GetFactContext(ctx, "architecture")
	if err != nil {
		t.Fatalf("GetFactContext failed: %v", err)
	}
	if !strings.Contains(archCtx, "**[architecture]**") {
		t.Fatalf("expected architecture fact in GetFactContext: %s", archCtx)
	}
	if strings.Contains(archCtx, "**[database]**") {
		t.Fatalf("unexpected database fact in architecture-only context: %s", archCtx)
	}
}

func TestConcurrentMemoryOperations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_concurrent_mem.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mem := NewMemoryService(s)
	ctx := context.Background()

	workers := 15
	iterations := 20
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				fact := Fact{
					Topic:   "concurrency",
					Content: "Thread safety verified under heavy concurrent access",
					Scope:   "project",
				}
				_ = mem.SaveFact(ctx, fact)
				_, _ = mem.SearchFacts(ctx, "concurrency", "")
			}
		}(i)
	}

	wg.Wait()

	results, err := mem.SearchFacts(ctx, "concurrency", "")
	if err != nil {
		t.Fatalf("SearchFacts failed after concurrent writes: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least one fact persisted")
	}
}
