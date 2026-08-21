package maintenance

import (
	"path/filepath"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func TestOverlappingWindowsAreRejectedAndCancellationUnblocks(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	first, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceDrain, StartAt: now, EndAt: now.Add(time.Hour), Reason: "schema change", Operator: "ops"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceForce, StartAt: now.Add(10 * time.Minute), EndAt: now.Add(2 * time.Hour), Reason: "overlap", Operator: "ops"}); err != ErrWindowOverlap {
		t.Fatalf("expected overlap error, got %v", err)
	}
	blocked, _, err := manager.IsBlocked("prod/child", now.Add(2*time.Minute))
	if err != nil || !blocked {
		t.Fatalf("expected active maintenance block: %v %v", blocked, err)
	}
	if err := manager.Cancel(first.ID, "ops"); err != nil {
		t.Fatal(err)
	}
	blocked, _, err = manager.IsBlocked("prod/child", now.Add(2*time.Minute))
	if err != nil || blocked {
		t.Fatalf("expected cancellation to unblock: %v %v", blocked, err)
	}
}
