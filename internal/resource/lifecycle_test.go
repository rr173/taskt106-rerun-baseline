package resource

import (
	"fmt"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
)

// failingEventStore wraps the real storage so individual writes succeed, but
// the atomic SetResourceStateWithEvent fails — simulating a coordination-event
// write failure that must roll the whole state change back.
type failingEventStore struct {
	*storage.Storage
	atomicCalls int
	failAtomic  bool
}

func (f *failingEventStore) SetResourceStateWithEvent(item *model.Resource, eventType, holder, detail string) error {
	f.atomicCalls++
	if f.failAtomic {
		return fmt.Errorf("simulated coordination event write failure")
	}
	return f.Storage.SetResourceStateWithEvent(item, eventType, holder, detail)
}

// countStateChangeEvents counts only the "resource_state_changed" events for a
// resource, ignoring unrelated "resource_registered" events written by Register.
func countStateChangeEvents(t *testing.T, store *storage.Storage, path string) int {
	t.Helper()
	events, err := store.ListCoordinationEvents(path, 500)
	if err != nil {
		t.Fatalf("ListCoordinationEvents returned error: %v", err)
	}
	var n int
	for _, e := range events {
		if e.EventType == "resource_state_changed" {
			n++
		}
	}
	return n
}

// TestSetStateRollsBackWhenEventWriteFails is the regression test for the
// atomicity contract: a resource state change must never be persisted on its
// own. When the coordination event cannot be written, the whole operation has
// to roll back — the on-disk state must be unchanged and the in-memory cache
// must keep the previous state, with no state-changed event recorded.
func TestSetStateRollsBackWhenEventWriteFails(t *testing.T) {
	store, err := storage.New(t.TempDir() + "/resource-rollback.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	failingStore := &failingEventStore{Storage: store, failAtomic: true}
	manager := NewManager(failingStore)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "prod", Owner: "platform"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "prod/payments", Owner: "payments"}); err != nil {
		t.Fatal(err)
	}

	// Register uses UpsertResource, so it must not go through the atomic path.
	if failingStore.atomicCalls != 0 {
		t.Fatalf("Register unexpectedly used the atomic path: calls=%d", failingStore.atomicCalls)
	}

	// Event write fails → the state change must roll back entirely.
	_, err = manager.SetState("prod/payments", model.ResourceDraining, "planned maintenance")
	if err == nil {
		t.Fatal("expected SetState to fail when the event write fails")
	}
	if failingStore.atomicCalls != 1 {
		t.Fatalf("expected SetResourceStateWithEvent to be called once, got %d", failingStore.atomicCalls)
	}

	// In-memory cache must not have advanced to the new state/generation.
	cached, err := manager.Get("prod/payments")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if cached.State != model.ResourceActive || cached.Generation != 1 {
		t.Fatalf("in-memory state advanced to %s/generation=%d; expected active/1 (rolled back)",
			cached.State, cached.Generation)
	}

	// On-disk state must also be unchanged — no lone state persisted without its event.
	persisted, err := store.GetResource("prod/payments")
	if err != nil {
		t.Fatalf("GetResource returned error: %v", err)
	}
	if persisted == nil || persisted.State != model.ResourceActive || persisted.Generation != 1 {
		t.Fatalf("on-disk state persisted without event: %+v; expected active/1 (rolled back)", persisted)
	}
	if got := countStateChangeEvents(t, store, "prod/payments"); got != 0 {
		t.Fatalf("expected no state-changed event after rollback, got %d", got)
	}

	// After unblocking, the same transition must succeed and write both the
	// state change and the event together atomically.
	failingStore.failAtomic = false
	if _, err := manager.SetState("prod/payments", model.ResourceDraining, "planned maintenance"); err != nil {
		t.Fatalf("expected SetState to succeed after unblocking event write: %v", err)
	}
	if failingStore.atomicCalls != 2 {
		t.Fatalf("expected two atomic calls total, got %d", failingStore.atomicCalls)
	}
	confirmed, err := store.GetResource("prod/payments")
	if err != nil {
		t.Fatalf("final GetResource returned error: %v", err)
	}
	if confirmed.State != model.ResourceDraining || confirmed.Generation != 2 {
		t.Fatalf("expected draining/generation=2 after successful atomic write, got %s/%d",
			confirmed.State, confirmed.Generation)
	}
	if got := countStateChangeEvents(t, store, "prod/payments"); got != 1 {
		t.Fatalf("expected one state-changed event persisted alongside the state change, got %d", got)
	}
}
