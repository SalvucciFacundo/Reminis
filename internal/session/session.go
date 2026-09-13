package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fds1288/reminis/internal/rules"
	"github.com/fds1288/reminis/internal/store"
)

// Session lifecycle status constants.
const (
	StatusActive    = "ACTIVE"
	StatusEnded     = "ENDED"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Manager coordinates session lifecycles, rule discovery freezing, and approval scopes.
type Manager struct {
	store     *store.Store
	approvals *ApprovalManager
	mu        sync.RWMutex
}

// Option configures Manager options.
type Option func(*Manager)

// WithApprovalManager configures a custom ApprovalManager.
func WithApprovalManager(am *ApprovalManager) Option {
	return func(m *Manager) {
		if am != nil {
			m.approvals = am
		}
	}
}

// WithAllowlist initializes the approval manager with permanent capabilities.
func WithAllowlist(capabilities ...string) Option {
	return func(m *Manager) {
		m.approvals = NewApprovalManager(capabilities...)
	}
}

// NewManager creates a new session Manager backed by store.Store.
func NewManager(s *store.Store, opts ...Option) *Manager {
	m := &Manager{
		store:     s,
		approvals: NewApprovalManager(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Approvals returns the underlying ApprovalManager.
func (m *Manager) Approvals() *ApprovalManager {
	return m.approvals
}

// ApprovalManager returns the underlying ApprovalManager.
func (m *Manager) ApprovalManager() *ApprovalManager {
	return m.approvals
}

// StartSession initiates a new session:
// 1. Validates project path and permission scope (defaults to "action").
// 2. Discovers and compiles universal rules for the project path, freezing PrefixHash and CompiledPrefix.
// 3. Persists the new SessionRecord into SQLite.
func (m *Manager) StartSession(ctx context.Context, projectPath string, scope string) (*store.SessionRecord, error) {
	if m.store == nil {
		return nil, errors.New("store is not initialized")
	}
	if projectPath == "" {
		projectPath = "."
	}
	if scope == "" {
		scope = string(ScopeAction)
	}

	// Discover and compile rules for deterministic prefix caching
	var prefixHash, compiledPrefix string
	compiled, err := rules.DiscoverAndCompile(rules.DiscoveryOptions{
		WorkspaceDir: projectPath,
	})
	if err == nil && compiled != nil {
		prefixHash = compiled.Hash
		compiledPrefix = compiled.Content
	} else {
		// Fallback to baseline compilation
		def := rules.Compile()
		prefixHash = def.Hash
		compiledPrefix = def.Content
	}

	sessionID := generateSessionID()
	now := time.Now().UTC()

	sess := store.SessionRecord{
		ID:               sessionID,
		ProjectPath:      projectPath,
		Status:           StatusActive,
		PermissionsScope: scope,
		PrefixHash:       prefixHash,
		CompiledPrefix:   compiledPrefix,
		StartedAt:        now,
	}

	if err := m.store.CreateSession(ctx, sess); err != nil {
		return nil, fmt.Errorf("failed to persist session: %w", err)
	}

	return &sess, nil
}

// EndSession marks a session as ended, updates its summary, and records ended_at.
func (m *Manager) EndSession(ctx context.Context, sessionID string, summary string) error {
	if m.store == nil {
		return errors.New("store is not initialized")
	}
	if sessionID == "" {
		return errors.New("sessionID cannot be empty")
	}

	// Verify session exists
	sess, err := m.store.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess.Status == StatusEnded {
		return nil
	}

	if err := m.store.UpdateSessionStatus(ctx, sessionID, StatusEnded, summary); err != nil {
		return fmt.Errorf("failed to update session status: %w", err)
	}

	// Reset any session-scoped grants on end
	m.approvals.ResetSession()

	return nil
}

// GetSession retrieves an existing session by ID from SQLite.
func (m *Manager) GetSession(ctx context.Context, sessionID string) (*store.SessionRecord, error) {
	if m.store == nil {
		return nil, errors.New("store is not initialized")
	}
	return m.store.GetSession(ctx, sessionID)
}

func generateSessionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("sess_%d_%s", time.Now().UnixNano(), hex.EncodeToString(b))
}
