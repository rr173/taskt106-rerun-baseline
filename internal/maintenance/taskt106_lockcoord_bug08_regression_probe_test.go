package maintenance

import (
    "path/filepath"
    "sync"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestConcurrentMaintenanceCreateIsAtomic(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); if err := m.Start(); err != nil { t.Fatal(err) }
    start := time.Now().UTC().Add(time.Minute); end := start.Add(time.Hour)
    req := model.MaintenanceCreateRequest{ResourcePath:"prod", Mode:model.MaintenanceDrain, StartAt:start, EndAt:end, Reason:"same window", Operator:"ops"}
    var wg sync.WaitGroup; var mu sync.Mutex; success := 0
    for i := 0; i < 20; i++ { wg.Add(1); go func() { defer wg.Done(); if _, err := m.Create(req); err == nil { mu.Lock(); success++; mu.Unlock() } }() }
    wg.Wait()
    if success != 1 { t.Fatalf("overlapping concurrent creates succeeded %d times", success) }
}
