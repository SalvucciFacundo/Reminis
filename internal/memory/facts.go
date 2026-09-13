package memory

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/fds1288/reminis/internal/store"
)

// Fact represents a durable architectural fact, decision, or convention.
type Fact struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id,omitempty"`
	Topic     string    `json:"topic"`
	Content   string    `json:"content"`
	Scope     string    `json:"scope"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// toStoreFact converts a memory Fact to a store FactRecord.
func toStoreFact(f Fact) store.FactRecord {
	return store.FactRecord{
		ID:        f.ID,
		SessionID: f.SessionID,
		Topic:     f.Topic,
		Content:   f.Content,
		Scope:     f.Scope,
		CreatedAt: f.CreatedAt,
		UpdatedAt: f.UpdatedAt,
	}
}

// fromStoreFact converts a store FactRecord to a memory Fact.
func fromStoreFact(r store.FactRecord) Fact {
	return Fact{
		ID:        r.ID,
		SessionID: r.SessionID,
		Topic:     r.Topic,
		Content:   r.Content,
		Scope:     r.Scope,
		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
	}
}

// FormatContext formats a slice of facts into clean markdown for prompt injection.
// If facts is empty, it returns an empty string.
func FormatContext(facts []Fact) string {
	if len(facts) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("## Architectural Memory & Learned Conventions\n")
	for _, f := range facts {
		content := strings.TrimSpace(f.Content)
		if content == "" {
			continue
		}
		topic := strings.TrimSpace(f.Topic)
		if topic != "" {
			sb.WriteString(fmt.Sprintf("- **[%s]**: %s\n", strings.ToLower(topic), content))
		} else {
			sb.WriteString(fmt.Sprintf("- %s\n", content))
		}
	}
	return sb.String()
}

func generateFactID(topic string, content string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(topic)) + ":" + strings.TrimSpace(content)))
	return fmt.Sprintf("fact_%s", hex.EncodeToString(h[:8]))
}
