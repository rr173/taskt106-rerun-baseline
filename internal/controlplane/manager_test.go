package controlplane

import (
	"path/filepath"
	"task106/internal/lock"
	"task106/internal/storage"
	"testing"
)

func TestControlPlaneGatesLocksAndIssuesToken(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "controlplane.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	coord := NewManager(store)
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
	result, err := mgr.AcquireLock("service-a", "worker-a", 10, false)
	if err != nil || result.Lease == nil || result.Lease.FencingToken == "" {
		t.Fatalf("expected fenced lease: %+v %v", result, err)
	}
	validation := coord.ValidateToken(result.Lease.FencingToken, "service-a", "worker-a", result.Lease.AcquiredAt)
	if !validation.Valid {
		t.Fatalf("token validation failed: %+v", validation)
	}
}
