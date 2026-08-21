package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestResourceCopiesLabelsOnRegister(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "labels.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    labels := map[string]string{"tier":"gold"}
    if _, err := m.Register(model.ResourceCreateRequest{Path:"prod", Owner:"ops", Labels:labels}); err != nil { t.Fatal(err) }
    labels["tier"] = "silver"
    got, err := m.Get("prod"); if err != nil { t.Fatal(err) }
    if got.Labels["tier"] != "gold" { t.Fatalf("stored labels changed through request alias: %#v", got.Labels) }
}
