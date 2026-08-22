package storage

import (
	"path/filepath"
	"task106/internal/model"
	"testing"
	"time"
)

// TestFinishRecoveryCheckpointWithEventCommitsBoth asserts that the checkpoint is
// marked complete and the coordination event is recorded together.
func TestFinishRecoveryCheckpointWithEventCommitsBoth(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cp := &model.RecoveryCheckpoint{Scope: "scope", Status: "running", StartedAt: now, CreatedAt: now}
	if err := store.CreateRecoveryCheckpoint(cp); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRecoveryCheckpointWithEvent(cp.ID, "healthy", nil, now, "recovery_checkpoint", "scope", "", "healthy"); err != nil {
		t.Fatalf("FinishRecoveryCheckpointWithEvent: %v", err)
	}
	finished, err := store.GetRecoveryCheckpoint(cp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != "healthy" || finished.FinishedAt == nil {
		t.Fatalf("checkpoint not committed: %+v", finished)
	}
	events, err := store.ListCoordinationEvents("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventType != "recovery_checkpoint" {
		t.Fatalf("event not recorded: %+v", events)
	}
}

// TestFinishRecoveryCheckpointWithEventRollsBack asserts that when the event write
// fails, the checkpoint is NOT left completed — the next inspection can resume.
func TestFinishRecoveryCheckpointWithEventRollsBack(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Break the coordination_events table so the event INSERT inside the transaction fails.
	if _, err := store.db.Exec(`DROP TABLE coordination_events`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	cp := &model.RecoveryCheckpoint{Scope: "scope", Status: "running", StartedAt: now, CreatedAt: now}
	if err := store.CreateRecoveryCheckpoint(cp); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRecoveryCheckpointWithEvent(cp.ID, "healthy", nil, now, "recovery_checkpoint", "scope", "", "healthy"); err == nil {
		t.Fatal("expected the event write to fail and the transaction to surface an error")
	}
	// The checkpoint must still be running: the result was rolled back together with the event.
	left, err := store.GetRecoveryCheckpoint(cp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if left.Status != "running" || left.FinishedAt != nil {
		t.Fatalf("checkpoint was left committed despite the event failure: %+v", left)
	}
}
