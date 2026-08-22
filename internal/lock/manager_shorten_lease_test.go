package lock

import (
	"path/filepath"
	"testing"
	"time"

	"task106/internal/storage"
)

// TestShortenLeaseClampsDurationToRemaining reproduces BUG28: when the remaining
// lease is under half a minute and the caller requests to shorten it to one
// minute, the returned and persisted lease_sec must reflect the actually
// effective duration (capped at the real remaining hold time), not the
// requested 60s.
func TestShortenLeaseClampsDurationToRemaining(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "manager.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatalf("manager.Start failed: %v", err)
	}
	defer manager.Stop()

	// Acquire with a short lease so remaining is well under a minute.
	acquired, err := manager.AcquireLock("clamp-lock", "holder-a", 20, false)
	if err != nil || acquired == nil || !acquired.Acquired {
		t.Fatalf("AcquireLock failed: result=%+v err=%v", acquired, err)
	}

	// Simulate the caller requesting a one-minute lease while the remaining
	// lease is < 30s (certainly < 60s here).
	shortened, err := manager.ShortenLease("clamp-lock", 60)
	if err != nil {
		t.Fatalf("ShortenLease failed: %v", err)
	}

	// The effective duration must not exceed the real remaining hold time.
	if shortened.LeaseSec > 20 {
		t.Fatalf("returned lease_sec %d exceeds real remaining hold time (~20s)", shortened.LeaseSec)
	}
	if shortened.LeaseSec == 60 {
		t.Fatalf("returned lease_sec was the requested 60s, not the clamped effective duration")
	}
	if shortened.RemainingSec > 20 {
		t.Fatalf("returned remaining_sec %.1f exceeds real hold time", shortened.RemainingSec)
	}

	// The persisted lease must agree: reload from storage and re-check.
	persisted, err := manager.GetActiveLease("clamp-lock")
	if err != nil {
		t.Fatalf("GetActiveLease failed: %v", err)
	}
	if persisted == nil {
		t.Fatalf("persisted lease is nil")
	}
	if persisted.LeaseSec > 20 {
		t.Fatalf("persisted lease_sec %d exceeds real remaining hold time (~20s)", persisted.LeaseSec)
	}
	if persisted.LeaseSec == 60 {
		t.Fatalf("persisted lease_sec was the requested 60s, not the clamped effective duration")
	}
	// remaining and lease_sec must be consistent with each other.
	if persisted.LeaseSec > int(persisted.RemainingSec)+1 {
		t.Fatalf("persisted lease_sec %d inconsistent with remaining_sec %.1f", persisted.LeaseSec, persisted.RemainingSec)
	}
}

// TestShortenLeaseHonorsRequestWhenFits verifies that when the requested
// duration fits within the remaining hold time, the effective duration equals
// the request (the clamp must not shorten leases unnecessarily).
func TestShortenLeaseHonorsRequestWhenFits(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "manager.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatalf("manager.Start failed: %v", err)
	}
	defer manager.Stop()

	if _, err := manager.AcquireLock("fit-lock", "holder-b", 120, false); err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	shortened, err := manager.ShortenLease("fit-lock", 30)
	if err != nil {
		t.Fatalf("ShortenLease failed: %v", err)
	}
	if shortened.LeaseSec != 30 {
		t.Fatalf("expected effective lease_sec=30 when request fits within remaining, got %d", shortened.LeaseSec)
	}
	if got := int(time.Until(shortened.ExpiresAt).Seconds()); got < 28 || got > 31 {
		t.Fatalf("expires_at drifts from requested 30s: ~%ds", got)
	}

	persisted, _ := manager.GetActiveLease("fit-lock")
	if persisted == nil || persisted.LeaseSec != 30 {
		t.Fatalf("persisted lease_sec mismatch: %+v", persisted)
	}
}
