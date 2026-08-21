package maintenance

import (
	"errors"
	"path/filepath"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

// failingStore wraps a real store so the coordination-event write can be forced to fail.
type failingStore struct {
	Store
	failEvents bool
	eventCalls int
}

func (f *failingStore) RecordCoordinationEvent(eventType, resourcePath, holder, detail string) error {
	f.eventCalls++
	if f.failEvents {
		return errors.New("coordination event store unavailable")
	}
	return f.Store.RecordCoordinationEvent(eventType, resourcePath, holder, detail)
}

func TestCancelRollsBackStatusWhenCoordinationEventFails(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	failing := &failingStore{Store: store, failEvents: true}
	manager := NewManager(failing)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	window, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceDrain, StartAt: now, EndAt: now.Add(time.Hour), Reason: "schema change", Operator: "ops"})
	if err != nil {
		t.Fatal(err)
	}
	previousStatus := window.Status

	if err := manager.Cancel(window.ID, "ops"); err == nil {
		t.Fatal("expected cancel to fail when coordination event write fails")
	}

	// In-memory state must be rolled back.
	manager.mu.RLock()
	current, ok := manager.windows[window.ID]
	manager.mu.RUnlock()
	if !ok {
		t.Fatal("window missing from manager cache")
	}
	if current.Status != previousStatus {
		t.Fatalf("in-memory status not rolled back: want %q got %q", previousStatus, current.Status)
	}

	// Persisted state must be rolled back too.
	persisted, err := store.ListMaintenanceWindows("prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 {
		t.Fatalf("expected one persisted window, got %d", len(persisted))
	}
	if persisted[0].Status != previousStatus {
		t.Fatalf("persisted status not rolled back: want %q got %q", previousStatus, persisted[0].Status)
	}

	// The window must still be cancellable once the event store recovers.
	failing.failEvents = false
	if err := manager.Cancel(window.ID, "ops"); err != nil {
		t.Fatalf("cancel should succeed after event store recovers: %v", err)
	}
}
