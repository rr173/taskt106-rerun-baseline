package fencing

import (
	"errors"
	"path/filepath"
	"task106/internal/storage"
	"testing"
	"time"
)

type failingEventStore struct {
	*storage.Storage
	failEvents bool
}

func (f *failingEventStore) RecordCoordinationEvent(eventType, resourcePath, holder, detail string) error {
	if f.failEvents {
		return errors.New("event store unavailable")
	}
	return f.Storage.RecordCoordinationEvent(eventType, resourcePath, holder, detail)
}

func TestIssueLeavesNoTokenWhenCoordinationEventFails(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	wrapped := &failingEventStore{Storage: store, failEvents: true}
	manager := NewManager(wrapped)
	now := time.Now().UTC()

	if _, err := manager.Issue("prod/db", "worker-a", 60, now); err == nil {
		t.Fatal("issue should fail when coordination event recording fails")
	}

	tokens, err := store.ListFencingTokens("prod/db", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 0 {
		t.Fatalf("no token should remain after failed issue, got %v", tokens)
	}
}


func TestFencingSequencesPersistAndRevocationIsObservable(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "fencing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	now := time.Now().UTC()
	first, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("fencing tokens must be unique")
	}
	if !manager.Validate(second, "prod/db", "worker-a", now.Add(time.Second)).Valid {
		t.Fatal("new token should validate")
	}
	if manager.Validate(first, "prod/db", "worker-a", now.Add(time.Second)).Valid {
		t.Fatal("older token should be stale after a newer token is issued")
	}
	if manager.Validate(first, "prod/cache", "worker-a", now).Valid {
		t.Fatal("cross-resource token should fail")
	}
	if err := manager.Revoke(second, "handover", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if manager.Validate(second, "prod/db", "worker-a", now.Add(3*time.Second)).Valid {
		t.Fatal("revoked token should fail")
	}
}
