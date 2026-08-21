package maintenance

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestAncestorMaintenanceOverlap(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    start := time.Now().UTC().Add(time.Minute); end := start.Add(time.Hour)
    _, err = m.Create(model.MaintenanceCreateRequest{ResourcePath:"prod", Mode:model.MaintenanceDrain, StartAt:start, EndAt:end, Reason:"parent", Operator:"ops"}); if err != nil { t.Fatal(err) }
    _, err = m.Create(model.MaintenanceCreateRequest{ResourcePath:"prod/api", Mode:model.MaintenanceForce, StartAt:start.Add(10*time.Minute), EndAt:end, Reason:"child", Operator:"ops"})
    if err != ErrWindowOverlap { t.Fatalf("expected ancestor overlap, got %v", err) }
}
