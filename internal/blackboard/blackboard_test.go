package blackboard

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNamespaceIsolation(t *testing.T) {
	tempDir := t.TempDir()
	bb := New(tempDir)

	// Valid namespace
	entry, err := bb.Set("worker-1", "tasks.worker-1.output", []byte(`{"status":"ok"}`))
	if err != nil {
		t.Fatalf("expected success for valid namespace, got: %v", err)
	}
	if entry.Key != "tasks.worker-1.output" {
		t.Fatalf("unexpected entry key: %s", entry.Key)
	}

	// Invalid namespace - different worker ID
	_, err = bb.Set("worker-1", "tasks.worker-2.output", []byte(`{"status":"ok"}`))
	if !errors.Is(err, ErrInvalidNamespace) {
		t.Fatalf("expected ErrInvalidNamespace, got: %v", err)
	}

	// Invalid namespace - missing tasks prefix
	_, err = bb.Set("worker-1", "global.config", []byte(`{"status":"ok"}`))
	if !errors.Is(err, ErrInvalidNamespace) {
		t.Fatalf("expected ErrInvalidNamespace, got: %v", err)
	}
}

func TestInlineAndSpillover(t *testing.T) {
	tempDir := t.TempDir()
	bb := New(tempDir)

	// Case 1: Under 4KB (inline)
	smallData := []byte(`{"message":"hello world"}`)
	entry, err := bb.Set("worker-1", "tasks.worker-1.small", smallData)
	if err != nil {
		t.Fatalf("unexpected error setting small entry: %v", err)
	}
	if entry.IsSpilled {
		t.Fatalf("expected entry to be inline, but IsSpilled was true")
	}
	if entry.SpillPath != "" {
		t.Fatalf("expected empty SpillPath, got: %s", entry.SpillPath)
	}
	readSmall, err := ReadPayload(entry)
	if err != nil {
		t.Fatalf("ReadPayload failed: %v", err)
	}
	if !bytes.Equal(readSmall, smallData) {
		t.Fatalf("payload mismatch for inline entry")
	}

	// Case 2: Over 4KB (spillover)
	largeData := bytes.Repeat([]byte("A"), 5000)
	spillEntry, err := bb.Set("worker-2", "tasks.worker-2.large", largeData)
	if err != nil {
		t.Fatalf("unexpected error setting large entry: %v", err)
	}
	if !spillEntry.IsSpilled {
		t.Fatalf("expected entry to be spilled, but IsSpilled was false")
	}
	if spillEntry.SpillPath == "" {
		t.Fatalf("expected non-empty SpillPath for spilled entry")
	}
	if spillEntry.Data != nil {
		t.Fatalf("expected Data to be nil for spilled entry")
	}

	// Check file on disk
	contentOnDisk, err := os.ReadFile(spillEntry.SpillPath)
	if err != nil {
		t.Fatalf("failed to read spilled file from disk: %v", err)
	}
	if !bytes.Equal(contentOnDisk, largeData) {
		t.Fatalf("spilled content on disk mismatch")
	}

	// Test ReadPayload helper
	readLarge, err := ReadPayload(spillEntry)
	if err != nil {
		t.Fatalf("ReadPayload failed for spilled entry: %v", err)
	}
	if !bytes.Equal(readLarge, largeData) {
		t.Fatalf("ReadPayload content mismatch for spilled entry")
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	tempDir := t.TempDir()
	bb := New(tempDir)

	_, err := bb.Set("w1", "tasks.w1.data", []byte(`"val1"`))
	if err != nil {
		t.Fatalf("failed to set entry: %v", err)
	}
	_, err = bb.Set("w2", "tasks.w2.data", []byte(`"val2"`))
	if err != nil {
		t.Fatalf("failed to set entry: %v", err)
	}

	snap := bb.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected snapshot size 2, got %d", len(snap))
	}

	newBB := New(filepath.Join(tempDir, "new"))
	newBB.Restore(snap)

	keys := newBB.ListKeys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys in restored blackboard, got %d", len(keys))
	}
	if keys[0] != "tasks.w1.data" || keys[1] != "tasks.w2.data" {
		t.Fatalf("unexpected keys in restored blackboard: %v", keys)
	}

	entry1, err := newBB.Get("tasks.w1.data")
	if err != nil {
		t.Fatalf("failed to get restored entry: %v", err)
	}
	if string(entry1.Data) != `"val1"` {
		t.Fatalf("expected 'val1', got %s", string(entry1.Data))
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	tempDir := t.TempDir()
	bb := New(tempDir)

	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func(wid string, id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("tasks.%s.metric-%d", wid, j)
				data := []byte(fmt.Sprintf(`{"iteration":%d}`, j))
				// Some large to test concurrent spillover
				if id%5 == 0 && j%10 == 0 {
					data = bytes.Repeat([]byte(fmt.Sprintf("X%d", j)), 2500)
				}
				_, err := bb.Set(wid, key, data)
				if err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}

				// Concurrent read
				entry, err := bb.Get(key)
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
				payload, err := ReadPayload(entry)
				if err != nil {
					t.Errorf("ReadPayload failed: %v", err)
					return
				}
				if !bytes.Equal(payload, data) {
					t.Errorf("payload mismatch in worker %s, iteration %d", wid, j)
					return
				}
			}
		}(workerID, i)
	}

	// Concurrent snapshot readers
	for k := 0; k < 5; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := 0; m < iterations; m++ {
				_ = bb.Snapshot()
				_ = bb.ListKeys()
			}
		}()
	}

	wg.Wait()

	totalExpectedKeys := workers * iterations
	keys := bb.ListKeys()
	if len(keys) != totalExpectedKeys {
		t.Fatalf("expected %d total keys, got %d", totalExpectedKeys, len(keys))
	}
}
