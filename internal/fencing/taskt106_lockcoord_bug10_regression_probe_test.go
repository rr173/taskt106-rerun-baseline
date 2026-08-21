package fencing

import (
    "path/filepath"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestRevokedTokenReasonWinsOverStale(t *testing.T) {
    store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db")); if err != nil { t.Fatal(err) }; defer store.Close()
    m := NewManager(store); now := time.Now().UTC()
    first, err := m.Issue("prod/db", "worker", 60, now); if err != nil { t.Fatal(err) }
    if _, err := m.Issue("prod/db", "worker", 60, now); err != nil { t.Fatal(err) }
    if err := m.Revoke(first, "operator", now.Add(time.Second)); err != nil { t.Fatal(err) }
    result := m.Validate(first, "prod/db", "worker", now.Add(2*time.Second))
    if result.Reason != ErrTokenRevoked.Error() { t.Fatalf("reason=%q want revoked", result.Reason) }
}
