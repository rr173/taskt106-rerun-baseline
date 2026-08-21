package lock

import (
	"path/filepath"
	"testing"

	"task106/internal/storage"
)

func TestManagerRebuildsActiveLeaseAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "manager.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		store.Close()
		t.Fatalf("manager.Start failed: %v", err)
	}
	result, err := manager.AcquireLock("restartable", "holder-a", 60, false)
	if err != nil || result == nil || !result.Acquired {
		manager.Stop()
		store.Close()
		t.Fatalf("AcquireLock failed: result=%+v err=%v", result, err)
	}
	manager.Stop()
	if err := store.Close(); err != nil {
		t.Fatalf("close initial storage failed: %v", err)
	}

	reopened, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("reopen storage failed: %v", err)
	}
	defer reopened.Close()
	restarted := NewManager(reopened)
	if err := restarted.Start(); err != nil {
		t.Fatalf("restarted manager failed to rebuild timers: %v", err)
	}
	defer restarted.Stop()

	lease, err := restarted.GetActiveLease("restartable")
	if err != nil {
		t.Fatalf("GetActiveLease failed: %v", err)
	}
	if lease == nil || lease.Holder != "holder-a" || !lease.Active {
		t.Fatalf("active lease was not restored: %+v", lease)
	}
}
