package fencing

import (
    "path/filepath"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestIssueRejectsInvalidResourcePath(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store)
    if _, err := m.Issue("prod//db", "worker", 10, time.Now().UTC()); err == nil { t.Fatal("invalid resource path accepted") }
}
