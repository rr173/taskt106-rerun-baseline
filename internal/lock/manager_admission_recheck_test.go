package lock

import (
	"path/filepath"
	"testing"
	"time"

	"task106/internal/controlplane"
	"task106/internal/model"
	"task106/internal/storage"
)

// TestQueuedRequestRechecksAdmissionBeforeGrant covers the scenario where a
// maintenance window becomes active while a request is waiting in the queue.
// Before being granted, the queued request must re-check the current admission
// conditions. With the window active the request must NOT be granted, and it
// must stay in the queue (rather than being dropped or skipped past).
func TestQueuedRequestRechecksAdmissionBeforeGrant(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "admission.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	defer store.Close()

	coord := controlplane.NewManager(store)
	if err := coord.Start(); err != nil {
		t.Fatalf("coordination start failed: %v", err)
	}
	mgr := NewManager(store)
	mgr.SetAdmissionGuard(coord)
	if err := mgr.Start(); err != nil {
		t.Fatalf("manager start failed: %v", err)
	}
	defer mgr.Stop()

	const lockName = "queue-recheck-lock"

	// First holder acquires the lock; a second requester is queued.
	if _, err := mgr.AcquireLock(lockName, "alice", 60, false); err != nil {
		t.Fatalf("acquire by alice failed: %v", err)
	}
	queued, err := mgr.AcquireLock(lockName, "bob", 30, false)
	if err != nil || queued == nil || !queued.Queued {
		t.Fatalf("bob should be queued: result=%+v err=%v", queued, err)
	}

	// Sanity: bob is at the head of the queue.
	before, err := store.ListWaitQueue(lockName)
	if err != nil {
		t.Fatalf("list wait queue failed: %v", err)
	}
	if len(before) != 1 || before[0].Holder != "bob" {
		t.Fatalf("expected bob queued, got %+v", before)
	}

	// Open a maintenance window that is already active for the lock's resource.
	now := time.Now().UTC()
	window, err := coord.Maintenance().Create(model.MaintenanceCreateRequest{
		ResourcePath: lockName,
		Mode:         model.MaintenanceDrain,
		StartAt:      now.Add(-time.Minute),
		EndAt:        now.Add(time.Hour),
		Reason:       "requeue recheck",
		Operator:     "ops",
	})
	if err != nil {
		t.Fatalf("create maintenance window failed: %v", err)
	}
	windowID := window.ID

	// Releasing the lock should attempt to grant bob, but the active window
	// blocks the grant and bob must remain queued.
	rel, err := mgr.ReleaseLock(lockName, "alice")
	if err != nil {
		t.Fatalf("release by alice failed: %v", err)
	}
	if rel == nil || rel.Released != true {
		t.Fatalf("expected release to succeed: %+v", rel)
	}
	if rel.Granted != nil {
		t.Fatalf("lock should not be granted from queue while maintenance is active, granted to %s", rel.Granted.Holder)
	}

	// The lock must be free (not handed to bob).
	lockState, err := store.GetLock(lockName)
	if err != nil {
		t.Fatalf("get lock failed: %v", err)
	}
	if lockState.Status != model.LockStatusFree {
		t.Fatalf("expected lock free after blocked grant, status=%s holder=%s", lockState.Status, lockState.Holder)
	}

	// bob must still be in the queue, at the head, preserving his position.
	after, err := store.ListWaitQueue(lockName)
	if err != nil {
		t.Fatalf("list wait queue after release failed: %v", err)
	}
	if len(after) != 1 || after[0].Holder != "bob" {
		t.Fatalf("bob should still be queued after blocked grant, got %+v", after)
	}
	if after[0].ID != before[0].ID {
		t.Fatalf("queued item id changed: before=%d after=%d (request did not stay in place)", before[0].ID, after[0].ID)
	}

	// Cancel the maintenance window and trigger the grant again. Now bob should
	// finally be granted, proving the block was due to the window, not a hard
	// rejection of bob.
	if err := coord.Maintenance().Cancel(windowID, "ops"); err != nil {
		t.Fatalf("cancel maintenance window failed: %v", err)
	}
	if _, err := mgr.tryGrantNextLocked(lockName); err != nil {
		t.Fatalf("grant next after window cancelled failed: %v", err)
	}
	lockState, err = store.GetLock(lockName)
	if err != nil {
		t.Fatalf("get lock after unblock failed: %v", err)
	}
	if lockState.Status != model.LockStatusHeld || lockState.Holder != "bob" {
		t.Fatalf("expected bob to hold the lock after maintenance cleared, status=%s holder=%s", lockState.Status, lockState.Holder)
	}
	if queueLen, _ := store.WaitQueueLen(lockName); queueLen != 0 {
		t.Fatalf("queue should be empty after bob granted, len=%d", queueLen)
	}
}
