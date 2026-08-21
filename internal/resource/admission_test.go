package resource

import (
	"path/filepath"
	"testing"
	"task106/internal/model"
	"task106/internal/storage"
)

func mustRegister(t *testing.T, m *Manager, path, owner string) {
	t.Helper()
	if _, err := m.Register(model.ResourceCreateRequest{Path: path, Owner: owner}); err != nil {
		t.Fatalf("register %s: %v", path, err)
	}
}

func mustSetPolicy(t *testing.T, m *Manager, path string, maxLease int) {
	t.Helper()
	if _, err := m.SetPolicy(path, model.ResourcePolicy{MaxLeaseSec: maxLease}); err != nil {
		t.Fatalf("set policy %s: %v", path, err)
	}
}

func TestParentMaxLeaseInheritsToChild(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer store.Close()
	m := NewManager(store)
	if err := m.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	mustRegister(t, m, "a", "ownerA")
	mustRegister(t, m, "a/b", "ownerB")
	mustRegister(t, m, "a/b/c", "ownerC")
	mustSetPolicy(t, m, "a", 30)

	// child without own policy inherits parent's max lease bound of 30
	if err := m.BeforeAcquire("a/b", "h1", 40); err == nil {
		t.Fatalf("expected child a/b rejected for lease 40 under parent max 30")
	}
	if err := m.BeforeAcquire("a/b", "h1", 30); err != nil {
		t.Fatalf("expected child a/b lease 30 allowed, got %v", err)
	}
	// grandchild is also constrained by nearest ancestor with a policy
	if err := m.BeforeAcquire("a/b/c", "h1", 31); err == nil {
		t.Fatalf("expected grandchild a/b/c rejected for lease 31 under parent max 30")
	}
	// child overrides the parent policy with its own max of 50
	mustSetPolicy(t, m, "a/b", 50)
	if err := m.BeforeAcquire("a/b", "h1", 45); err != nil {
		t.Fatalf("expected child a/b override max=50 allow 45, got %v", err)
	}
	if err := m.BeforeAcquire("a/b", "h1", 60); err == nil {
		t.Fatalf("expected child a/b override max=50 reject 60")
	}
	// grandchild now inherits the nearest ancestor policy (a/b max=50), not the root (a max=30)
	if err := m.BeforeAcquire("a/b/c", "h1", 45); err != nil {
		t.Fatalf("expected grandchild a/b/c inherit a/b max=50 allow 45, got %v", err)
	}
	if err := m.BeforeAcquire("a/b/c", "h1", 51); err == nil {
		t.Fatalf("expected grandchild a/b/c reject 51 under inherited a/b max=50")
	}
}
