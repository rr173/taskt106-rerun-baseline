package fencing

import (
	"errors"
	"path/filepath"
	"task106/internal/storage"
	"testing"
	"time"
)

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

func TestIssueRejectsDuplicateSeparators(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "fencing-invalid.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	now := time.Now().UTC()
	for _, badPath := range []string{"prod//db", "prod/", "/prod/db", "a///b"} {
		token, err := manager.Issue(badPath, "worker-a", 60, now)
		if err == nil {
			t.Fatalf("expected error for path %q, got token %q", badPath, token)
		}
		if token != "" {
			t.Fatalf("no token must be created for invalid path %q, got %q", badPath, token)
		}
		if !errors.Is(err, ErrInvalidResource) {
			t.Fatalf("expected ErrInvalidResource for path %q, got %v", badPath, err)
		}
	}
	// Sanity: valid paths still issue tokens.
	token, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil || token == "" {
		t.Fatalf("valid path should issue a token, got %q %v", token, err)
	}
}
