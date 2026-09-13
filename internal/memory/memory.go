package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fds1288/reminis/internal/store"
)

// MemoryService provides long-term memory management for architectural facts,
// decisions, and conventions backed by pure-Go SQLite storage.
type MemoryService struct {
	store     *store.Store
	extractor *FactExtractor
}

// Option configures MemoryService options.
type Option func(*MemoryService)

// WithExtractor configures a custom FactExtractor.
func WithExtractor(extractor *FactExtractor) Option {
	return func(s *MemoryService) {
		if extractor != nil {
			s.extractor = extractor
		}
	}
}

// NewMemoryService creates a new MemoryService backed by store.Store.
func NewMemoryService(s *store.Store, opts ...Option) *MemoryService {
	ms := &MemoryService{
		store:     s,
		extractor: NewFactExtractor(),
	}
	for _, opt := range opts {
		opt(ms)
	}
	return ms
}

// SaveFact persists or updates a durable fact in SQLite.
func (s *MemoryService) SaveFact(ctx context.Context, fact Fact) error {
	if s.store == nil {
		return errors.New("store is not initialized")
	}
	if fact.Content == "" {
		return errors.New("fact content cannot be empty")
	}
	if fact.Topic == "" {
		fact.Topic = "general"
	}
	if fact.Scope == "" {
		fact.Scope = "project"
	}
	if fact.ID == "" {
		fact.ID = generateFactID(fact.Topic, fact.Content)
	}
	now := time.Now().UTC()
	if fact.CreatedAt.IsZero() {
		fact.CreatedAt = now
	}
	if fact.UpdatedAt.IsZero() {
		fact.UpdatedAt = now
	}

	return s.store.SaveFact(ctx, toStoreFact(fact))
}

// SearchFacts queries facts matching the topic (partial match) and/or scope (exact match).
func (s *MemoryService) SearchFacts(ctx context.Context, topic string, scope string) ([]Fact, error) {
	if s.store == nil {
		return nil, errors.New("store is not initialized")
	}
	records, err := s.store.SearchFacts(ctx, topic, scope)
	if err != nil {
		return nil, err
	}
	facts := make([]Fact, len(records))
	for i, r := range records {
		facts[i] = fromStoreFact(r)
	}
	return facts, nil
}

// ExtractAndPersist analyzes task outputs from a session, extracts architectural facts,
// persists them to SQLite, and returns the extracted facts.
func (s *MemoryService) ExtractAndPersist(ctx context.Context, sessionID string, taskOutputs map[string]string) ([]Fact, error) {
	if s.store == nil {
		return nil, errors.New("store is not initialized")
	}
	extracted := s.extractor.Extract(sessionID, taskOutputs)
	if len(extracted) == 0 {
		return []Fact{}, nil
	}

	persisted := make([]Fact, 0, len(extracted))
	for _, fact := range extracted {
		if err := s.SaveFact(ctx, fact); err != nil {
			return nil, fmt.Errorf("failed to persist fact %s: %w", fact.ID, err)
		}
		persisted = append(persisted, fact)
	}

	return persisted, nil
}

// GetFactContext retrieves facts matching the given topic and formats them into
// clean markdown suitable for system prompt injection.
func (s *MemoryService) GetFactContext(ctx context.Context, topic string) (string, error) {
	facts, err := s.SearchFacts(ctx, topic, "")
	if err != nil {
		return "", err
	}
	return FormatContext(facts), nil
}

// ListFactsBySession retrieves all facts created within a specific session.
func (s *MemoryService) ListFactsBySession(ctx context.Context, sessionID string) ([]Fact, error) {
	if s.store == nil {
		return nil, errors.New("store is not initialized")
	}
	records, err := s.store.ListFactsBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	facts := make([]Fact, len(records))
	for i, r := range records {
		facts[i] = fromStoreFact(r)
	}
	return facts, nil
}
