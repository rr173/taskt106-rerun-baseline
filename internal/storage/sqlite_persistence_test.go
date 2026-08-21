package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
)

func TestLockStateSurvivesStorageReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persistence.db")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := &model.Lock{
		Name:      "persisted-resource",
		Status:    model.LockStatusHeld,
		Holder:    "owner-a",
		Reentrant: true,
		Count:     2,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.UpsertLock(want); err != nil {
		store.Close()
		t.Fatalf("UpsertLock returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened, err := New(dbPath)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.GetLock("persisted-resource")
	if err != nil {
		t.Fatalf("GetLock returned error: %v", err)
	}
	if got == nil {
		t.Fatal("GetLock returned nil after reopen")
	}
	if got.Status != want.Status || got.Holder != want.Holder || got.Count != want.Count || !got.Reentrant {
		t.Fatalf("persisted lock mismatch: got %+v, want %+v", got, want)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database file disappeared: %v", err)
	}
}
