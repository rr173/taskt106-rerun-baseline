package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestChildrenNormalizesPath(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "tree.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod", Owner:"ops"}); err != nil { t.Fatal(err) }
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod/api", Owner:"api"}); err != nil { t.Fatal(err) }
    if got := m.Children(" prod "); len(got) != 1 || got[0].Path != "prod/api" { t.Fatalf("children=%v", got) }
}
