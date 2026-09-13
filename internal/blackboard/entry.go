package blackboard

import (
	"encoding/json"
	"errors"
	"os"
)

var (
	// ErrSpillPathEmpty is returned when an entry is marked spilled but has no spill path.
	ErrSpillPathEmpty = errors.New("spilled entry has empty spill path")
)

// MemoryEntry represents a typed value in working memory.
type MemoryEntry struct {
	Key        string          `json:"key"`
	Data       json.RawMessage `json:"data"`
	IsSpilled  bool            `json:"is_spilled"`
	SpillPath  string          `json:"spill_path,omitempty"`
	ProducerID string          `json:"producer_id"`
}

// ReadPayload reads the payload of a MemoryEntry. If IsSpilled is true,
// it reads the spilled content from SpillPath on disk. Otherwise, it returns Data.
func ReadPayload(entry MemoryEntry) ([]byte, error) {
	if entry.IsSpilled {
		if entry.SpillPath == "" {
			return nil, ErrSpillPathEmpty
		}
		return os.ReadFile(entry.SpillPath)
	}
	if entry.Data == nil {
		return nil, nil
	}
	res := make([]byte, len(entry.Data))
	copy(res, entry.Data)
	return res, nil
}
