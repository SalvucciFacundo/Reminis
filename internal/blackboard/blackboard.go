package blackboard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrInvalidNamespace is returned when a key does not match tasks.<producer_id>.*
	ErrInvalidNamespace = errors.New("key violates namespace isolation: must start with tasks.<producer_id>.")
	// ErrKeyNotFound is returned when a key does not exist in the blackboard.
	ErrKeyNotFound = errors.New("key not found in blackboard")
)

const MaxInlineSize = 4096

var sanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// Blackboard is a thread-safe working memory ledger for workflow runs.
type Blackboard struct {
	mu           sync.RWMutex
	entries      map[string]MemoryEntry
	artifactsDir string
}

// New creates a new Blackboard instance.
func New(artifactsDir string) *Blackboard {
	if artifactsDir == "" {
		artifactsDir = filepath.Join(os.TempDir(), "reminis_artifacts")
	}
	return &Blackboard{
		entries:      make(map[string]MemoryEntry),
		artifactsDir: artifactsDir,
	}
}

// Set stores a key-value entry in the blackboard.
// It enforces namespace isolation (tasks.<producer_id>.) and spills payloads > 4096 bytes to disk.
func (b *Blackboard) Set(producerID, key string, data []byte) (MemoryEntry, error) {
	expectedPrefix := fmt.Sprintf("tasks.%s.", producerID)
	if !strings.HasPrefix(key, expectedPrefix) {
		return MemoryEntry{}, fmt.Errorf("%w: key %q does not start with %q", ErrInvalidNamespace, key, expectedPrefix)
	}

	var entry MemoryEntry
	if len(data) > MaxInlineSize {
		if err := os.MkdirAll(b.artifactsDir, 0755); err != nil {
			return MemoryEntry{}, fmt.Errorf("failed to create artifacts dir: %w", err)
		}

		safeName := sanitizeRegex.ReplaceAllString(key, "_")
		spillPath := filepath.Join(b.artifactsDir, safeName+".bin")

		if err := os.WriteFile(spillPath, data, 0644); err != nil {
			return MemoryEntry{}, fmt.Errorf("failed to spill payload to %s: %w", spillPath, err)
		}

		entry = MemoryEntry{
			Key:        key,
			Data:       nil,
			IsSpilled:  true,
			SpillPath:  spillPath,
			ProducerID: producerID,
		}
	} else {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		entry = MemoryEntry{
			Key:        key,
			Data:       dataCopy,
			IsSpilled:  false,
			ProducerID: producerID,
		}
	}

	b.mu.Lock()
	b.entries[key] = entry
	b.mu.Unlock()

	return entry, nil
}

// Get retrieves an entry by key.
func (b *Blackboard) Get(key string) (MemoryEntry, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entry, ok := b.entries[key]
	if !ok {
		return MemoryEntry{}, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	return entry, nil
}

// ListKeys returns all keys currently stored in the blackboard in sorted order.
func (b *Blackboard) ListKeys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.entries))
	for k := range b.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Snapshot returns a copy of all current memory entries.
func (b *Blackboard) Snapshot() map[string]MemoryEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res := make(map[string]MemoryEntry, len(b.entries))
	for k, v := range b.entries {
		res[k] = v
	}
	return res
}

// Restore overwrites the blackboard with the provided snapshot entries.
func (b *Blackboard) Restore(entries map[string]MemoryEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = make(map[string]MemoryEntry, len(entries))
	for k, v := range entries {
		b.entries[k] = v
	}
}
