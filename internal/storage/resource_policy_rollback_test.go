package storage

import (
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
)

// TestSetPolicyWithEventRollsBackPolicyOnEventFailure proves that when the
// coordination event write fails, the resource policy change is rolled back
// as a whole and is not persisted on its own.
func TestSetPolicyWithEventRollsBackPolicyOnEventFailure(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer store.Close()

	if err := store.UpsertResource(&model.Resource{Path: "queue", Owner: "platform", State: "active", Generation: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("UpsertResource returned error: %v", err)
	}

	// Break the coordination_events table so the event INSERT inside the
	// shared transaction fails. The policy upsert still succeeds on its own,
	// so without a transactional rollback the policy would be left persisted.
	if _, err := store.DB().Exec(`DROP TABLE coordination_events`); err != nil {
		t.Fatalf("failed to drop coordination_events: %v", err)
	}

	policy := &model.ResourcePolicy{Path: "queue", MaxLeaseSec: 30, RequiredHolder: "worker-a", UpdatedAt: time.Now().UTC()}
	if err := store.SetPolicyWithEvent(policy, "resource_policy_changed", "worker-a", "policy updated"); err == nil {
		t.Fatal("expected SetPolicyWithEvent to fail when the event write fails")
	}

	// The policy must not have been persisted on its own.
	persisted, err := store.GetResourcePolicy("queue")
	if err != nil {
		t.Fatalf("GetResourcePolicy returned error: %v", err)
	}
	if persisted != nil {
		t.Fatalf("policy was persisted despite event-write failure: %+v", persisted)
	}
}

// TestSetPolicyWithEventPersistsBothOnSuccess proves that on success the policy
// and its coordination event are both committed atomically.
func TestSetPolicyWithEventPersistsBothOnSuccess(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "commit.db"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer store.Close()

	if err := store.UpsertResource(&model.Resource{Path: "queue", Owner: "platform", State: "active", Generation: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("UpsertResource returned error: %v", err)
	}

	policy := &model.ResourcePolicy{Path: "queue", MaxLeaseSec: 45, RequiredHolder: "worker-a", RequireFencing: true, UpdatedAt: time.Now().UTC()}
	if err := store.SetPolicyWithEvent(policy, "resource_policy_changed", "worker-a", "policy updated"); err != nil {
		t.Fatalf("SetPolicyWithEvent returned error: %v", err)
	}

	persisted, err := store.GetResourcePolicy("queue")
	if err != nil {
		t.Fatalf("GetResourcePolicy returned error: %v", err)
	}
	if persisted == nil || persisted.MaxLeaseSec != 45 || persisted.RequiredHolder != "worker-a" || !persisted.RequireFencing {
		t.Fatalf("persisted policy mismatch: %+v", persisted)
	}

	events, err := store.ListCoordinationEvents("queue", 10)
	if err != nil {
		t.Fatalf("ListCoordinationEvents returned error: %v", err)
	}
	var sawEvent bool
	for _, e := range events {
		if e.EventType == "resource_policy_changed" {
			sawEvent = true
			break
		}
	}
	if !sawEvent {
		t.Fatal("expected a resource_policy_changed coordination event to be recorded")
	}
}
