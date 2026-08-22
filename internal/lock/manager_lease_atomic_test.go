package lock

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"task106/internal/model"
	"task106/internal/storage"
)

// failingFencingIssuer is a FencingIssuer that fails the first N Issue calls
// so we can reproduce a lease-setup failure without corrupting storage.
type failingFencingIssuer struct {
	failCount int32
	calls     int32
}

func (f *failingFencingIssuer) Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error) {
	n := atomic.AddInt32(&f.calls, 1)
	if n <= atomic.LoadInt32(&f.failCount) {
		return "", errors.New("simulated lease write failure")
	}
	// Return a deterministic token once issuing succeeds; the manager only
	// needs the token string to persist alongside the lease.
	return "tok-" + resourcePath + "-" + holder, nil
}

type succeedFencingIssuer struct{}

func (succeedFencingIssuer) Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error) {
	return "seed-" + resourcePath + "-" + holder, nil
}

// TestAcquireLeaseFailureLeavesNoHeldLock reproduces BUG26: when the lease
// setup step fails (here simulated by a fencing issuer that errors before the
// lock/lease are persisted), the lock must NOT be left in the held state.
// Otherwise every other worker sees a held lock with no lease and no expiry
// timer, and can never make progress — the exact hang the task describes.
func TestAcquireLeaseFailureLeavesNoHeldLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lease-failure.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	manager := NewManager(store)
	manager.SetFencingIssuer(&failingFencingIssuer{failCount: 1})
	if err := manager.Start(); err != nil {
		t.Fatalf("manager.Start failed: %v", err)
	}
	defer manager.Stop()

	const lockName = "lease-fail-lock"

	// First acquire fails because the fencing issuer (lease setup step) errors.
	if _, err := manager.AcquireLock(lockName, "worker-a", 30, false); err == nil {
		t.Fatalf("expected first acquire to fail, but it succeeded")
	}

	// The lock must not be stranded in the held state with no lease.
	lock, err := store.GetLock(lockName)
	if err != nil {
		t.Fatalf("GetLock failed: %v", err)
	}
	if lock != nil && lock.Status == model.LockStatusHeld {
		t.Fatalf("lock was left held after lease setup failure: %+v", lock)
	}
	lease, err := store.GetActiveLease(lockName)
	if err != nil {
		t.Fatalf("GetActiveLease failed: %v", err)
	}
	if lease != nil {
		t.Fatalf("expected no active lease after lease setup failure, got %+v", lease)
	}

	// A second worker must be able to acquire the lock immediately — this is
	// the scenario that previously blocked forever.
	result, err := manager.AcquireLock(lockName, "worker-b", 30, false)
	if err != nil || result == nil || !result.Acquired {
		t.Fatalf("second worker could not acquire lock after failed lease setup: result=%+v err=%v", result, err)
	}
}

// TestGrantLeaseFailureReleasesLock exercises the wait-queue grant path
// (tryGrantNextLocked). When granting a dequeued waiter fails at lease setup,
// the lock must not be left held by that waiter without a lease.
func TestGrantLeaseFailureReleasesLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "grant-failure.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	manager := NewManager(store)
	manager.SetFencingIssuer(succeedFencingIssuer{})
	if err := manager.Start(); err != nil {
		t.Fatalf("manager.Start failed: %v", err)
	}
	defer manager.Stop()

	const lockName = "grant-fail-lock"

	// worker-a holds the lock; worker-b is queued behind it.
	if _, err := manager.AcquireLock(lockName, "worker-a", 30, false); err != nil {
		t.Fatalf("seed acquire failed: %v", err)
	}
	if _, err := manager.AcquireLock(lockName, "worker-b", 30, false); err != nil {
		t.Fatalf("queue worker-b failed: %v", err)
	}

	// Swap in a failing issuer so the grant of worker-b fails at lease setup.
	failIssuer := &failingFencingIssuer{failCount: 1}
	manager.SetFencingIssuer(failIssuer)

	// Release worker-a; the manager tries to grant worker-b, which fails. The
	// returned error propagates the grant failure, but worker-a's lock must
	// already be free and worker-b must not hold a lease-less lock.
	release, err := manager.ReleaseLock(lockName, "worker-a")
	if err == nil {
		t.Fatalf("expected grant failure to surface from ReleaseLock, got: %+v", release)
	}

	lock, err := store.GetLock(lockName)
	if err != nil {
		t.Fatalf("GetLock after failed grant failed: %v", err)
	}
	if lock == nil {
		t.Fatal("lock row disappeared after failed grant")
	}
	if lock.Status == model.LockStatusHeld && lock.Holder == "worker-b" {
		t.Fatalf("grant left lock held by worker-b without a lease: %+v", lock)
	}
	lease, _ := store.GetActiveLease(lockName)
	if lease != nil && lease.Holder == "worker-b" {
		t.Fatalf("grant left active lease for worker-b after failure: %+v", lease)
	}
}

// TestAcquireLockAndLeaseRollsBackOnLeaseWriteFailure proves the storage
// primitive that backs the fix: when the lease INSERT fails (here forced by
// dropping the leases table), the held-lock INSERT done in the same
// transaction is rolled back too. This is the exact "lease record write failed
// → must not leave a held lock" invariant.
func TestAcquireLockAndLeaseRollsBackOnLeaseWriteFailure(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	// Force every subsequent lease INSERT to fail. The lock INSERT and lease
	// INSERT share one transaction; the lease failure must roll the lock back.
	if _, err := store.DB().Exec(`DROP TABLE leases`); err != nil {
		t.Fatalf("drop leases table failed: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	lock := &model.Lock{
		Name:      "rollback-lock",
		Status:    model.LockStatusHeld,
		Holder:    "owner",
		Reentrant: false,
		Count:     1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	lease := &model.Lease{
		LockName:   "rollback-lock",
		Holder:     "owner",
		LeaseSec:   30,
		AcquiredAt: now,
		ExpiresAt:  now.Add(30 * time.Second),
		Active:     true,
	}

	if err := store.AcquireLockAndLease(lock, lease); err == nil {
		t.Fatal("expected AcquireLockAndLease to fail when the lease write fails")
	}

	// The lock must NOT exist as held: the whole transaction was rolled back.
	got, err := store.GetLock("rollback-lock")
	if err != nil {
		t.Fatalf("GetLock failed: %v", err)
	}
	if got != nil && got.Status == model.LockStatusHeld {
		t.Fatalf("held lock survived a failed lease write: %+v", got)
	}
}

// TestAcquireLockAndLeasePersistsBoth is the positive control: a successful
// atomic acquire writes both the held lock and the active lease together.
func TestAcquireLockAndLeasePersistsBoth(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "atomic-ok.db"))
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	lock := &model.Lock{
		Name:      "atomic-lock",
		Status:    model.LockStatusHeld,
		Holder:    "owner",
		Reentrant: false,
		Count:     1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	lease := &model.Lease{
		LockName:     "atomic-lock",
		Holder:       "owner",
		LeaseSec:     30,
		AcquiredAt:   now,
		ExpiresAt:    now.Add(30 * time.Second),
		Active:       true,
		FencingToken: "token-1",
	}

	if err := store.AcquireLockAndLease(lock, lease); err != nil {
		t.Fatalf("AcquireLockAndLease failed: %v", err)
	}
	gotLock, err := store.GetLock("atomic-lock")
	if err != nil || gotLock == nil {
		t.Fatalf("GetLock failed: %v", err)
	}
	if gotLock.Status != model.LockStatusHeld || gotLock.Holder != "owner" {
		t.Fatalf("lock not persisted as held: %+v", gotLock)
	}
	gotLease, err := store.GetActiveLease("atomic-lock")
	if err != nil || gotLease == nil || !gotLease.Active {
		t.Fatalf("active lease not persisted: %+v %v", gotLease, err)
	}
	if gotLease.FencingToken != "token-1" {
		t.Fatalf("lease fencing token mismatch: %+v", gotLease)
	}
}
