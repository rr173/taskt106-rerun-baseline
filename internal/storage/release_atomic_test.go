package storage

import (
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
)

// TestReleaseLockAndDeactivateLeaseIsAtomic guards against the half-released
// state described by BUG27: if deactivating the lease succeeds but writing the
// lock row fails (or vice-versa), neither side must be left mutated. Here we
// force the lock-row UPDATE to fail mid-transaction with a CHECK constraint and
// assert the lock is still held and the lease is still active.
func TestReleaseLockAndDeactivateLeaseIsAtomic(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "atomic.db")
	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)

	lock := &model.Lock{
		Name:      "guarded-resource",
		Status:    model.LockStatusHeld,
		Holder:    "owner-a",
		Count:     1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.UpsertLock(lock); err != nil {
		t.Fatalf("seed UpsertLock failed: %v", err)
	}
	if err := store.CreateLease(&model.Lease{
		LockName:   "guarded-resource",
		Holder:     "owner-a",
		LeaseSec:   60,
		AcquiredAt:  now,
		ExpiresAt:   now.Add(60 * time.Second),
		Active:     true,
	}); err != nil {
		t.Fatalf("seed CreateLease failed: %v", err)
	}

	// Add a CHECK constraint that rejects the "free" status, so the lock-row
	// UPDATE inside ReleaseLockAndDeactivateLease fails and the transaction
	// must roll back entirely.
	if _, err := store.DB().Exec(
		`ALTER TABLE locks ADD CONSTRAINT no_free CHECK (status <> 'free')`,
	); err != nil {
		// SQLite parses but ignores named CHECK on ALTER; emulate via a
		// trigger that raises an error when freeing the lock.
		if _, err := store.DB().Exec(`
			CREATE TRIGGER prevent_free BEFORE UPDATE OF status ON locks
			FOR EACH ROW WHEN NEW.status = 'free'
			BEGIN
				SELECT RAISE(ABORT, 'status free not allowed');
			END
		`); err != nil {
			t.Fatalf("install failure trigger failed: %v", err)
		}
	}

	lock.Status = model.LockStatusFree
	lock.Holder = ""
	lock.Count = 0
	if err := store.ReleaseLockAndDeactivateLease(lock); err == nil {
		t.Fatalf("expected ReleaseLockAndDeactivateLease to fail when lock-row write is rejected")
	}

	gotLock, err := store.GetLock("guarded-resource")
	if err != nil {
		t.Fatalf("GetLock after failed release failed: %v", err)
	}
	if gotLock.Status != model.LockStatusHeld || gotLock.Holder != "owner-a" {
		t.Fatalf("lock row was mutated despite transaction failure: %+v", gotLock)
	}

	lease, err := store.GetActiveLease("guarded-resource")
	if err != nil {
		t.Fatalf("GetActiveLease after failed release failed: %v", err)
	}
	if lease == nil || !lease.Active || lease.Holder != "owner-a" {
		t.Fatalf("active lease was deactivated despite transaction failure: %+v", lease)
	}
}
