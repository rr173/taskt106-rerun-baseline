package resource

import (
	"path/filepath"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func TestResourceLifecycleAndPolicySurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "resource.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "prod", Owner: "platform"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(model.ResourceCreateRequest{Path: "prod/payments", Owner: "payments"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.SetPolicy("prod/payments", model.ResourcePolicy{MaxLeaseSec: 20, RequiredHolder: "worker-a", RequireFencing: true}); err != nil {
		t.Fatal(err)
	}
	decision, err := manager.Decide("prod/payments", "worker-a", 10, time.Now().UTC())
	if err != nil || !decision.Allowed {
		t.Fatalf("expected allow: %+v %v", decision, err)
	}
	if _, err := manager.SetState("prod/payments", model.ResourceDraining, "planned maintenance"); err != nil {
		t.Fatal(err)
	}
	if err := manager.BeforeAcquire("prod/payments", "worker-a", 10); err == nil {
		t.Fatal("draining resource should reject acquisition")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewManager(reopened)
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	item, err := restarted.Get("prod/payments")
	if err != nil || item.State != model.ResourceDraining {
		t.Fatalf("resource state was not restored: %+v %v", item, err)
	}
	policy, err := restarted.GetPolicy("prod/payments")
	if err != nil || policy.MaxLeaseSec != 20 {
		t.Fatalf("resource policy was not restored: %+v %v", policy, err)
	}
}
