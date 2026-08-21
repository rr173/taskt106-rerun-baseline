package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestResourceCopiesLabelsOnGet(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "labels.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod", Owner:"ops", Labels:map[string]string{"tier":"gold"}}); err != nil { t.Fatal(err) }
    got, err := m.Get("prod"); if err != nil { t.Fatal(err) }; got.Labels["tier"] = "silver"
    again, err := m.Get("prod"); if err != nil { t.Fatal(err) }
    if again.Labels["tier"] != "gold" { t.Fatalf("Get exposed internal labels map: %#v", again.Labels) }
}
