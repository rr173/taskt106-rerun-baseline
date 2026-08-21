package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestParentPolicyAppliesToChild(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "policy.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod", Owner:"ops"}); err != nil { t.Fatal(err) }
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod/api", Owner:"api"}); err != nil { t.Fatal(err) }
    if _, err := m.SetPolicy("prod", model.ResourcePolicy{MaxLeaseSec:5}); err != nil { t.Fatal(err) }
    decision, err := m.Decide("prod/api", "worker", 10, time.Now().UTC()); if err != nil { t.Fatal(err) }
    if decision.Allowed { t.Fatalf("parent lease policy was not inherited: %#v", decision) }
}
