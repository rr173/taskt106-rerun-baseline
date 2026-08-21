package recovery_test

import (
	"path/filepath"
	"task106/internal/controlplane"
	"task106/internal/lock"
	"task106/internal/storage"
	"testing"
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
