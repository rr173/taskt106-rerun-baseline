package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestListPoliciesIsStable(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "policy.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    for _, path := range []string{"a", "b", "c"} { if _, err := m.Register(model.ResourceCreateRequest{Path:path, Owner:"ops"}); err != nil { t.Fatal(err) }; if _, err := m.SetPolicy(path, model.ResourcePolicy{MaxLeaseSec:10}); err != nil { t.Fatal(err) } }
    for i := 0; i < 100; i++ { got := m.ListPolicies(); for j := 1; j < len(got); j++ { if got[j-1].Path > got[j].Path { t.Fatalf("policies are not ordered: %#v", got) } } }
}
