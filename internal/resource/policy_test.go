package resource

import (
	"errors"
	"testing"

	"task106/internal/model"
	"task106/internal/storage"
	"path/filepath"
)

// failingPolicyStore records calls into SetPolicyWithEvent so we can assert the
// manager relies on the atomic path and never falls back to persisting the
// policy alone when the event write fails.
type failingPolicyStore struct {
	*storage.Storage
	policyCalls  int
	policyRecord *model.ResourcePolicy
	failing      bool
}

func (s *failingPolicyStore) SetPolicyWithEvent(policy *model.ResourcePolicy, eventType, holder, detail string) error {
	s.policyCalls++
	s.policyRecord = policy
	if s.failing {
		return errors.New("event write failed")
	}
	return s.Storage.SetPolicyWithEvent(policy, eventType, holder, detail)
}

func TestSetPolicyRollsBackWhenEventWriteFails(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "policy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "queue", Owner: "platform"}); err != nil {
		t.Fatal(err)
	}

	failing := &failingPolicyStore{Storage: store, failing: true}
	manager.store = failing

	policy := model.ResourcePolicy{MaxLeaseSec: 30, RequiredHolder: "worker-a"}
	if _, err := manager.SetPolicy("queue", policy); err == nil {
		t.Fatal("expected SetPolicy to surface the event-write failure")
	}

	// The in-memory cache must not be updated when the atomic write failed,
	// otherwise the process would believe the policy is active even though it
	// was rolled back.
	if got, err := manager.GetPolicy("queue"); err != ErrNotFound || got != nil {
		t.Fatalf("in-memory policy should be absent after rollback, got %+v err=%v", got, err)
	}

	// The policy must not have been persisted on its own either.
	persisted, err := store.GetResourcePolicy("queue")
	if err != nil {
		t.Fatalf("GetResourcePolicy returned error: %v", err)
	}
	if persisted != nil {
		t.Fatalf("policy was persisted despite event-write failure: %+v", persisted)
	}

	if failing.policyCalls != 1 {
		t.Fatalf("expected SetPolicyWithEvent to be invoked once, got %d", failing.policyCalls)
	}
}

func TestSetPolicyPersistsPolicyAndEventAtomicallyOnSuccess(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "policy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "queue", Owner: "platform"}); err != nil {
		t.Fatal(err)
	}

	policy := model.ResourcePolicy{MaxLeaseSec: 45, RequiredHolder: "worker-a", RequireFencing: true}
	if _, err := manager.SetPolicy("queue", policy); err != nil {
		t.Fatalf("SetPolicy failed: %v", err)
	}

	if got, err := manager.GetPolicy("queue"); err != nil || got.MaxLeaseSec != 45 {
		t.Fatalf("in-memory policy mismatch: %+v err=%v", got, err)
	}
	if persisted, err := store.GetResourcePolicy("queue"); err != nil || persisted == nil || persisted.MaxLeaseSec != 45 {
		t.Fatalf("persisted policy mismatch: %+v err=%v", persisted, err)
	}

	events, err := store.ListCoordinationEvents("queue", 10)
	if err != nil {
		t.Fatal(err)
	}
	var sawPolicyEvent bool
	for _, e := range events {
		if e.EventType == "resource_policy_changed" {
			sawPolicyEvent = true
		}
	}
	if !sawPolicyEvent {
		t.Fatal("expected a resource_policy_changed coordination event to be recorded")
	}
}
