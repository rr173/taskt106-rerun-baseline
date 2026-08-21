package recovery_test

import (
	"path/filepath"
	"task106/internal/controlplane"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func TestRecoveryCheckpointRecordsHealthyLeaseState(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	coord := controlplane.NewManager(store)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	mgr := lock.NewManager(store)
	mgr.SetAdmissionGuard(coord)
	mgr.SetFencingIssuer(coord)
	if err := mgr.Start(); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()
	result, err := mgr.AcquireLock("recovery-resource", "worker", 30, false)
	if err != nil || result == nil || !result.Acquired {
		t.Fatalf("acquire failed: %+v %v", result, err)
	}
	checkpoint, err := coord.RunRecovery("test")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Status != "healthy" {
		t.Fatalf("unexpected checkpoint: %+v", checkpoint)
	}
}

// TestRecoveryContinuesWhenResourceMissing ensures that an active lease whose
// resource no longer exists is recorded as an attention issue rather than
// aborting the whole recovery checkpoint.
func TestRecoveryContinuesWhenResourceMissing(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	coord := controlplane.NewManager(store)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	// Plant an active lease for a resource that was never registered, simulating
	// a resource that has since been removed from the registry.
	if err := store.CreateLease(&model.Lease{
		LockName:     "ghost-resource",
		Holder:       "worker",
		LeaseSec:     30,
		AcquiredAt:   now,
		ExpiresAt:    now.Add(30 * time.Second),
		Active:       true,
		FencingToken: "token-ghost",
	}); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := coord.RunRecovery("missing-resource")
	if err != nil {
		t.Fatalf("recovery should not fail on a missing resource: %v", err)
	}
	if checkpoint.Status != "attention" {
		t.Fatalf("expected status attention, got %q (issues=%v)", checkpoint.Status, checkpoint.Issues)
	}
	if len(checkpoint.Issues) != 1 {
		t.Fatalf("expected exactly one issue, got %v", checkpoint.Issues)
	}
	if checkpoint.Issues[0] != "active lease ghost-resource references missing resource" {
		t.Fatalf("unexpected issue text: %q", checkpoint.Issues[0])
	}
	if checkpoint.FinishedAt == nil {
		t.Fatal("expected checkpoint to be finished")
	}
}
