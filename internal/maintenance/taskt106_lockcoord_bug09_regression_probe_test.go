package maintenance

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestCancelRequiresOperator(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    now := time.Now().UTC().Add(time.Minute)
    w, err := m.Create(model.MaintenanceCreateRequest{ResourcePath:"prod", Mode:model.MaintenanceDrain, StartAt:now, EndAt:now.Add(time.Hour), Reason:"planned", Operator:"ops"}); if err != nil { t.Fatal(err) }
    if err := m.Cancel(w.ID, ""); err == nil { t.Fatal("empty cancel operator was accepted") }
}
