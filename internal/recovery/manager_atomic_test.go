package recovery_test

import (
	"errors"
	"task106/internal/model"
	"task106/internal/recovery"
	"testing"
	"time"
)

// failingEventStore records every call so the test can assert that a failed event
// write leaves the checkpoint untouched.
type failingEventStore struct {
	createCalls  int
	finishCalls  int
	finishArgs   finishArgs
	eventFailErr error
}

type finishArgs struct {
	id         int64
	status     string
	issues     []string
	finished   time.Time
	eventType  string
	resource   string
	holder     string
	detail     string
}

func (f *failingEventStore) CreateRecoveryCheckpoint(c *model.RecoveryCheckpoint) error {
	f.createCalls++
	c.ID = int64(f.createCalls)
	return nil
}

func (f *failingEventStore) FinishRecoveryCheckpoint(int64, string, []string, time.Time) error {
	f.finishCalls++
	return nil
}

func (f *failingEventStore) FinishRecoveryCheckpointWithEvent(id int64, status string, issues []string, finished time.Time, eventType, resourcePath, holder, detail string) error {
	f.finishCalls++
	f.finishArgs = finishArgs{id: id, status: status, issues: issues, finished: finished, eventType: eventType, resource: resourcePath, holder: holder, detail: detail}
	return f.eventFailErr
}

func (f *failingEventStore) GetRecoveryCheckpoint(int64) (*model.RecoveryCheckpoint, error) {
	return nil, errors.New("not implemented")
}

func (f *failingEventStore) ListRecoveryCheckpoints(string, int) ([]model.RecoveryCheckpoint, error) {
	return nil, errors.New("not implemented")
}

func (f *failingEventStore) RecordCoordinationEvent(string, string, string, string) error {
	return nil
}

type emptyResourceReader struct{}

func (emptyResourceReader) Get(string) (*model.Resource, error) { return nil, nil }
func (emptyResourceReader) List(string) ([]model.Resource, error) {
	return nil, nil
}

type emptyLeaseReader struct{}

func (emptyLeaseReader) ListActiveLeases() ([]model.Lease, error) { return nil, nil }

// When the coordination-event write fails, Run must surface the error and the
// checkpoint must not be left as completed (FinishRecoveryCheckpointWithEvent is the
// single atomic commit point, so a failure means the checkpoint stays running).
func TestRunLeavesCheckpointRunningWhenEventFails(t *testing.T) {
	store := &failingEventStore{eventFailErr: errors.New("event write failed")}
	mgr := recovery.NewManager(store, emptyResourceReader{}, emptyLeaseReader{})
	if _, err := mgr.Run("scope"); err == nil {
		t.Fatal("expected Run to surface the event-write failure")
	}
	if store.finishCalls != 1 {
		t.Fatalf("expected the atomic finish+event to be attempted once, got %d", store.finishCalls)
	}
	// A healthy scan with no leases resolves to status healthy; the failure must come
	// from recording the event, and the manager must not have reported success.
	if store.finishArgs.status != "healthy" {
		t.Fatalf("unexpected checkpoint status passed to atomic commit: %q", store.finishArgs.status)
	}
}
