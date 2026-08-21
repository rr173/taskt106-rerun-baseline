package fencing

import (
	"path/filepath"
	"task106/internal/storage"
	"testing"
	"time"
)

func TestFencingSequencesPersistAndRevocationIsObservable(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	now := time.Now().UTC()
	first, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("fencing tokens must be unique")
	}
	if !manager.Validate(second, "prod/db", "worker-a", now.Add(time.Second)).Valid {
		t.Fatal("new token should validate")
	}
	if manager.Validate(first, "prod/db", "worker-a", now.Add(time.Second)).Valid {
		t.Fatal("older token should be stale after a newer token is issued")
	}
	if manager.Validate(first, "prod/cache", "worker-a", now).Valid {
		t.Fatal("cross-resource token should fail")
	}
	if err := manager.Revoke(second, "handover", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if manager.Validate(second, "prod/db", "worker-a", now.Add(3*time.Second)).Valid {
		t.Fatal("revoked token should fail")
	}
}

func TestRevokedTokenTakesPrecedenceOverStale(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	now := time.Now().UTC()

	stale, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Issue("prod/db", "worker-a", 60, now); err != nil {
		t.Fatal(err)
	}
	if err := manager.Revoke(stale, "handover", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// The revoked token's sequence is now stale too; the result must report
	// revocation rather than staleness, since revocation is the stronger state.
	got := manager.Validate(stale, "prod/db", "worker-a", now.Add(2*time.Second))
	if got.Valid {
		t.Fatalf("revoked stale token must not validate")
	}
	if got.Reason != ErrTokenRevoked.Error() {
		t.Fatalf("expected revoked reason, got %q", got.Reason)
	}
}
