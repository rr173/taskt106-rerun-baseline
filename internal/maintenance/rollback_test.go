package maintenance

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
	"task106/internal/storage"
)

// failingStore wraps the real storage so we can force the transactional
// window+event write to fail and assert that no orphan window is left behind.
type failingStore struct {
	*storage.Storage
	failWithEvent error
}

func (f *failingStore) CreateMaintenanceWindowWithEvent(window *model.MaintenanceWindow, eventType, resourcePath, holder, detail string) error {
	if f.failWithEvent != nil {
		return f.failWithEvent
	}
	return f.Storage.CreateMaintenanceWindowWithEvent(window, eventType, resourcePath, holder, detail)
}

// rollingBackStore asserts that the atomic path never persists a partial window
// by delegating to the real store and verifying no row was left on failure.
func TestCreateRollsBackWhenEventWriteFails(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(&failingStore{Storage: store, failWithEvent: errors.New("event write failed")})
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	window, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceDrain, StartAt: now, EndAt: now.Add(time.Hour), Reason: "schema change", Operator: "ops"})
	if err == nil {
		t.Fatalf("expected create to fail when event write fails, got window %+v", window)
	}

	// No orphan window in memory: the resource must not be blocked.
	if blocked, _, berr := manager.IsBlocked("prod/child", now.Add(2*time.Minute)); berr != nil || blocked {
		t.Fatalf("expected no block after rollback, got blocked=%v err=%v", blocked, berr)
	}
	// No orphan window persisted on disk either.
	persisted, err := store.ListMaintenanceWindows("prod")
	if err != nil {
		t.Fatalf("list windows: %v", err)
	}
	if len(persisted) != 0 {
		t.Fatalf("expected zero persisted windows after rollback, got %d: %+v", len(persisted), persisted)
	}
	if got := len(manager.ActiveWindows(now.Add(2 * time.Minute))); got != 0 {
		t.Fatalf("expected zero active windows after rollback, got %d", got)
	}
}

// A successful create still persists both the window and the audit event.
func TestCreateWithEventPersistsWindowAndEvent(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "success.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	window, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceDrain, StartAt: now, EndAt: now.Add(time.Hour), Reason: "schema change", Operator: "ops"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	persisted, err := store.ListMaintenanceWindows("prod")
	if err != nil {
		t.Fatalf("list windows: %v", err)
	}
	if len(persisted) != 1 || persisted[0].ID != window.ID {
		t.Fatalf("expected one persisted window matching id %d, got %+v", window.ID, persisted)
	}
	events, err := store.ListCoordinationEvents("prod", 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	found := false
	for _, ev := range events {
		if ev.EventType == "maintenance_created" && ev.Holder == "ops" && ev.Detail == "schema change" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected maintenance_created event to be recorded, got %v", events)
	}
}
