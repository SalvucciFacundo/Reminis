package session

import (
	"sync"
)

// ApprovalScope defines the validity window of a human approval.
type ApprovalScope string

const (
	// ScopeAction approves only this single task execution.
	ScopeAction ApprovalScope = "action"
	// ScopeSession approves all subsequent actions of the same capability category
	// for the duration of the active session without further prompting.
	ScopeSession ApprovalScope = "session"
	// ScopePermanent records the capability permanently in the allowlist.
	ScopePermanent ApprovalScope = "permanent"
)

// Standard granular capability categories per SPEC.md Section 3.3.
const (
	CapFSDelete        = "fs:delete"
	CapGitPush         = "git:push"
	CapDBDrop          = "db:drop"
	CapBashDestructive = "bash:destructive"
	CapDeploy          = "deploy"
)

// ApprovalManager manages permission grants across action, session, and permanent scopes.
// It is fully thread-safe and guarded by sync.RWMutex.
type ApprovalManager struct {
	mu            sync.RWMutex
	allowlist     map[string]bool // permanent allowlist
	sessionGrants map[string]bool // session-scoped grants (cached for session duration)
	actionGrants  map[string]int  // one-time action grants (consumed on use)
}

// NewApprovalManager creates a new ApprovalManager with an optional initial allowlist.
func NewApprovalManager(allowlist ...string) *ApprovalManager {
	mgr := &ApprovalManager{
		allowlist:     make(map[string]bool),
		sessionGrants: make(map[string]bool),
		actionGrants:  make(map[string]int),
	}
	for _, cap := range allowlist {
		mgr.allowlist[cap] = true
	}
	return mgr
}

// Grant grants permission for a given capability under the specified scope.
// - ScopeAction: records a single-use approval for the capability.
// - ScopeSession: subsequent checks for that capability return true without prompting.
// - ScopePermanent: records the capability permanently in the allowlist.
func (m *ApprovalManager) Grant(capability string, scope ApprovalScope) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch scope {
	case ScopePermanent:
		m.allowlist[capability] = true
	case ScopeSession:
		m.sessionGrants[capability] = true
	case ScopeAction:
		m.actionGrants[capability]++
	default:
		m.actionGrants[capability]++
	}
}

// IsApproved checks whether a capability is approved.
// Returns true if:
// 1. The capability is in the permanent allowlist, or wildcard "*" / "all" is set.
// 2. The capability was granted for the active session (ScopeSession).
// 3. The capability was granted for a single action (ScopeAction), which consumes the grant.
func (m *ApprovalManager) IsApproved(capability string) bool {
	m.mu.RLock()
	if m.allowlist["*"] || m.allowlist["all"] || m.allowlist[capability] || m.sessionGrants[capability] {
		m.mu.RUnlock()
		return true
	}
	if m.actionGrants[capability] == 0 {
		m.mu.RUnlock()
		return false
	}
	m.mu.RUnlock()

	// Upgrade lock to consume one-time action grant
	m.mu.Lock()
	defer m.mu.Unlock()

	// Recheck permanent and session grants in case of race during lock upgrade
	if m.allowlist["*"] || m.allowlist["all"] || m.allowlist[capability] || m.sessionGrants[capability] {
		return true
	}
	if m.actionGrants[capability] > 0 {
		m.actionGrants[capability]--
		return true
	}
	return false
}

// Allowlist returns a copy of all capabilities in the permanent allowlist.
func (m *ApprovalManager) Allowlist() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]string, 0, len(m.allowlist))
	for cap, ok := range m.allowlist {
		if ok {
			res = append(res, cap)
		}
	}
	return res
}

// SessionGrants returns a copy of all capabilities granted for the current session.
func (m *ApprovalManager) SessionGrants() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]string, 0, len(m.sessionGrants))
	for cap, ok := range m.sessionGrants {
		if ok {
			res = append(res, cap)
		}
	}
	return res
}

// ResetSession clears all session-scoped grants and pending action grants.
func (m *ApprovalManager) ResetSession() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessionGrants = make(map[string]bool)
	m.actionGrants = make(map[string]int)
}
