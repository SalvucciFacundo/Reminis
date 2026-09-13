package session

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fds1288/reminis/internal/store"
)

func TestSessionLifecycleAndStatusTransitions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_session_mgr.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	mgr := NewManager(s)
	ctx := context.Background()

	// 1. Start Session
	sess, err := mgr.StartSession(ctx, t.TempDir(), string(ScopeSession))
	if err != nil {
		t.Fatalf("StartSession failed: %v", err)
	}
	if sess.ID == "" {
		t.Fatalf("expected non-empty session ID")
	}
	if sess.Status != StatusActive {
		t.Fatalf("expected status %s, got %s", StatusActive, sess.Status)
	}
	if sess.PermissionsScope != string(ScopeSession) {
		t.Fatalf("expected scope %s, got %s", ScopeSession, sess.PermissionsScope)
	}
	if sess.PrefixHash == "" || sess.CompiledPrefix == "" {
		t.Fatalf("expected frozen prefix hash and compiled prefix")
	}

	// 2. Get Session
	fetched, err := mgr.GetSession(ctx, sess.ID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if fetched.ID != sess.ID || fetched.Status != StatusActive {
		t.Fatalf("unexpected fetched session: %+v", fetched)
	}

	// 3. End Session with summary
	summary := "Run completed: all tests passed and artifacts saved."
	if err := mgr.EndSession(ctx, sess.ID, summary); err != nil {
		t.Fatalf("EndSession failed: %v", err)
	}

	// 4. Verify ended session
	endedSess, err := mgr.GetSession(ctx, sess.ID)
	if err != nil {
		t.Fatalf("GetSession after EndSession failed: %v", err)
	}
	if endedSess.Status != StatusEnded {
		t.Fatalf("expected status %s, got %s", StatusEnded, endedSess.Status)
	}
	if endedSess.Summary != summary {
		t.Fatalf("expected summary %q, got %q", summary, endedSess.Summary)
	}
	if endedSess.EndedAt == nil {
		t.Fatalf("expected non-nil EndedAt")
	}

	// 5. End already ended session is idempotent
	if err := mgr.EndSession(ctx, sess.ID, "another summary"); err != nil {
		t.Fatalf("subsequent EndSession should be idempotent: %v", err)
	}

	// 6. End non-existent session
	err = mgr.EndSession(ctx, "non-existent-sess", "summary")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-existent session, got %v", err)
	}
}

func TestApprovalScopeEvaluation(t *testing.T) {
	mgr := NewApprovalManager()

	// 1. Initial state: not approved
	if mgr.IsApproved(CapFSDelete) {
		t.Fatalf("expected fs:delete to be unapproved initially")
	}
	if mgr.IsApproved(CapGitPush) {
		t.Fatalf("expected git:push to be unapproved initially")
	}
	if mgr.IsApproved(CapDBDrop) {
		t.Fatalf("expected db:drop to be unapproved initially")
	}

	// 2. Action Scope: single-use check
	mgr.Grant(CapFSDelete, ScopeAction)
	if !mgr.IsApproved(CapFSDelete) {
		t.Fatalf("expected first fs:delete check to be approved under ScopeAction")
	}
	// Subsequent check must return false (prompting required again)
	if mgr.IsApproved(CapFSDelete) {
		t.Fatalf("expected second fs:delete check to fail after single action consumption")
	}

	// 3. Session Scope: cached without prompting
	mgr.Grant(CapGitPush, ScopeSession)
	for i := 0; i < 5; i++ {
		if !mgr.IsApproved(CapGitPush) {
			t.Fatalf("expected git:push check #%d to succeed under ScopeSession without prompting", i+1)
		}
	}
	sessionGrants := mgr.SessionGrants()
	if len(sessionGrants) != 1 || sessionGrants[0] != CapGitPush {
		t.Fatalf("expected sessionGrants to contain git:push, got: %v", sessionGrants)
	}

	// 4. Permanent Scope: recorded in allowlist
	mgr.Grant(CapDeploy, ScopePermanent)
	for i := 0; i < 5; i++ {
		if !mgr.IsApproved(CapDeploy) {
			t.Fatalf("expected deploy check #%d to succeed under ScopePermanent", i+1)
		}
	}
	allowlist := mgr.Allowlist()
	if len(allowlist) != 1 || allowlist[0] != CapDeploy {
		t.Fatalf("expected allowlist to contain deploy, got: %v", allowlist)
	}

	// 5. ResetSession: clears session and action grants but preserves allowlist
	mgr.Grant(CapBashDestructive, ScopeSession)
	if !mgr.IsApproved(CapBashDestructive) {
		t.Fatalf("expected bash:destructive to be approved")
	}

	mgr.ResetSession()

	if mgr.IsApproved(CapBashDestructive) {
		t.Fatalf("expected bash:destructive to be unapproved after ResetSession")
	}
	if mgr.IsApproved(CapGitPush) {
		t.Fatalf("expected git:push to be unapproved after ResetSession")
	}
	if !mgr.IsApproved(CapDeploy) {
		t.Fatalf("expected deploy in permanent allowlist to remain approved after ResetSession")
	}
}

func TestApprovalAllowlistAndWildcard(t *testing.T) {
	// Pre-configured allowlist
	mgr := NewApprovalManager(CapFSDelete, CapGitPush)
	if !mgr.IsApproved(CapFSDelete) {
		t.Fatalf("expected fs:delete to be approved via initial allowlist")
	}
	if !mgr.IsApproved(CapGitPush) {
		t.Fatalf("expected git:push to be approved via initial allowlist")
	}
	if mgr.IsApproved(CapDBDrop) {
		t.Fatalf("expected db:drop to be unapproved")
	}

	// Wildcard allowlist (e.g. --auto-approve=all)
	wildcardMgr := NewApprovalManager("*")
	if !wildcardMgr.IsApproved("anything:unknown") {
		t.Fatalf("expected wildcard to approve all capabilities")
	}
}

func TestApprovalConcurrency(t *testing.T) {
	mgr := NewApprovalManager()
	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	// Concurrent grants and checks across multiple scopes
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if workerID%3 == 0 {
					mgr.Grant(CapGitPush, ScopeSession)
					_ = mgr.IsApproved(CapGitPush)
				} else if workerID%3 == 1 {
					mgr.Grant(CapDeploy, ScopePermanent)
					_ = mgr.IsApproved(CapDeploy)
				} else {
					mgr.Grant(CapFSDelete, ScopeAction)
					_ = mgr.IsApproved(CapFSDelete)
				}
			}
		}(i)
	}

	wg.Wait()

	if !mgr.IsApproved(CapGitPush) {
		t.Fatalf("expected git:push to be approved after concurrent session grants")
	}
	if !mgr.IsApproved(CapDeploy) {
		t.Fatalf("expected deploy to be approved after concurrent permanent grants")
	}
}
