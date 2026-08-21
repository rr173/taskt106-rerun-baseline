package storage

import (
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
)

// newTestStore creates a fresh SQLite storage backed by a temp file for a single test.
func newTestStore(t *testing.T) *Storage {
	t.Helper()
	dir := t.TempDir()
	store, err := New(filepath.Join(dir, "handover_cancel.db"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// seedPendingHandover writes a single handover in the pending_receive status.
func seedPendingHandover(t *testing.T, store *Storage) *model.Handover {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	h := &model.Handover{
		FromCaller:        "caller-a",
		ToCaller:          "caller-b",
		Status:            model.HandoverStatusPending,
		Initiator:         "ops",
		NeedConfirm:       true,
		ConfirmTimeoutSec: 3600,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := store.CreateHandover(h); err != nil {
		t.Fatalf("CreateHandover returned error: %v", err)
	}
	return h
}

// TestCancelHandoverWithTimelineAtomicSuccess verifies that on the happy path both
// the status flip to cancelled and the timeline entry are committed together.
func TestCancelHandoverWithTimelineAtomicSuccess(t *testing.T) {
	store := newTestStore(t)
	h := seedPendingHandover(t, store)

	cancelledAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.CancelHandoverWithTimeline(h.ID, cancelledAt, "manual cancel", "ops"); err != nil {
		t.Fatalf("CancelHandoverWithTimeline returned error: %v", err)
	}

	got, err := store.GetHandover(h.ID)
	if err != nil {
		t.Fatalf("GetHandover returned error: %v", err)
	}
	if got.Status != model.HandoverStatusCancelled {
		t.Fatalf("status = %q, want %q", got.Status, model.HandoverStatusCancelled)
	}
	if got.CancelReason != "manual cancel" {
		t.Fatalf("cancel_reason = %q, want %q", got.CancelReason, "manual cancel")
	}
	if got.CancelledAt == nil || !got.CancelledAt.Equal(cancelledAt) {
		t.Fatalf("cancelled_at = %v, want %v", got.CancelledAt, cancelledAt)
	}

	timeline, err := store.ListHandoverTimeline(h.ID)
	if err != nil {
		t.Fatalf("ListHandoverTimeline returned error: %v", err)
	}
	if len(timeline) != 1 {
		t.Fatalf("timeline entries = %d, want 1", len(timeline))
	}
	entry := timeline[0]
	if entry.Status != model.HandoverStatusCancelled {
		t.Fatalf("timeline status = %q, want %q", entry.Status, model.HandoverStatusCancelled)
	}
	if entry.Operator != "ops" {
		t.Fatalf("timeline operator = %q, want %q", entry.Operator, "ops")
	}
	if entry.Detail != "manual cancel" {
		t.Fatalf("timeline detail = %q, want %q", entry.Detail, "manual cancel")
	}
}

// TestCancelHandoverWithTimelineAtomicRollback verifies that when the timeline
// insert fails, the status update is rolled back so the handover keeps its
// original status instead of being left cancelled with a missing audit trail.
func TestCancelHandoverWithTimelineAtomicRollback(t *testing.T) {
	store := newTestStore(t)
	h := seedPendingHandover(t, store)

	// Drop the timeline table to force the timeline insert inside the
	// transaction to fail; the whole cancel must roll back.
	if _, err := store.db.Exec(`DROP TABLE handover_timeline`); err != nil {
		t.Fatalf("drop timeline table: %v", err)
	}

	cancelledAt := time.Now().UTC().Truncate(time.Microsecond)
	err := store.CancelHandoverWithTimeline(h.ID, cancelledAt, "manual cancel", "ops")
	if err == nil {
		t.Fatal("expected CancelHandoverWithTimeline to fail when timeline insert fails, got nil")
	}

	got, err := store.GetHandover(h.ID)
	if err != nil {
		t.Fatalf("GetHandover returned error: %v", err)
	}
	if got.Status != model.HandoverStatusPending {
		t.Fatalf("status = %q, want handover to remain %q (rollback failed)",
			got.Status, model.HandoverStatusPending)
	}
	if got.CancelReason != "" {
		t.Fatalf("cancel_reason = %q, want empty after rollback", got.CancelReason)
	}
	if got.CancelledAt != nil {
		t.Fatalf("cancelled_at = %v, want nil after rollback", got.CancelledAt)
	}
}
