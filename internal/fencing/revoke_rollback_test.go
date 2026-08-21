package fencing

import (
	"errors"
	"task106/internal/model"
	"testing"
	"time"
)

// fakeStore implements fencing.Store entirely in memory so the manager can be
// exercised without a SQLite database. The interesting knob is
// RevokeFencingTokenWithEvent, which simulates the event-write failing so the
// rollback contract (token must NOT be left revoked) can be asserted.
type fakeStore struct {
	tokens map[string]*model.FencingToken
	revoked map[string]bool
	events []string
	nextSeq int64
	revokeEventErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{tokens: make(map[string]*model.FencingToken), revoked: make(map[string]bool), nextSeq: 1}
}

func (f *fakeStore) NextFencingSequence(string) (int64, error) { s := f.nextSeq; f.nextSeq++; return s, nil }
func (f *fakeStore) InsertFencingToken(t *model.FencingToken) error { f.tokens[t.Token] = t; return nil }
func (f *fakeStore) GetFencingToken(token string) (*model.FencingToken, error) {
	if t, ok := f.tokens[token]; ok {
		return t, nil
	}
	return nil, nil
}
func (f *fakeStore) CurrentFencingSequence(string) (int64, error) { return f.nextSeq - 1, nil }
func (f *fakeStore) RevokeFencingToken(token, _ string, _ time.Time) error { f.revoked[token] = true; return nil }
func (f *fakeStore) RevokeFencingTokenWithEvent(token, reason, _, _ string, now time.Time) error {
	if f.revokeEventErr != nil {
		return f.revokeEventErr
	}
	f.revoked[token] = true
	if item := f.tokens[token]; item != nil {
		ts := now
		item.RevokedAt = &ts
	}
	f.events = append(f.events, "fencing_revoked:"+token+":"+reason)
	return nil
}
func (f *fakeStore) ListFencingTokens(string, int) ([]model.FencingToken, error) { return nil, nil }
func (f *fakeStore) RecordCoordinationEvent(string, string, string, string) error { return nil }

// TestRevokeRollsBackTokenWhenEventWriteFails asserts the bug fix: when the
// coordination-event write fails, the token must NOT be left revoked. Before
// the fix the manager revoked the token in one statement and recorded the
// event in a second, non-transactional statement — a failure of the second
// left the token revoked with no audit trail.
func TestRevokeRollsBackTokenWhenEventWriteFails(t *testing.T) {
	store := newFakeStore()
	manager := NewManager(store)
	now := time.Now().UTC()
	token, err := manager.Issue("prod/db", "worker-a", 60, now)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !manager.Validate(token, "prod/db", "worker-a", now.Add(time.Second)).Valid {
		t.Fatal("issued token should validate before revocation")
	}

	eventErr := errors.New("coordination event write failed")
	store.revokeEventErr = eventErr
	if err := manager.Revoke(token, "handover", now.Add(time.Second)); err == nil {
		t.Fatal("revoke should surface the event-write error")
	} else if !errors.Is(err, eventErr) {
		t.Fatalf("revoke should propagate the event-write error, got %v", err)
	}

	// The transaction must have rolled back: the token is still usable and no
	// revocation event was appended to the audit trail.
	if store.revoked[token] {
		t.Fatal("token must not be left revoked when the event write fails")
	}
	if len(store.events) != 0 {
		t.Fatalf("no revocation event should be recorded on failure, got %v", store.events)
	}
	if !manager.Validate(token, "prod/db", "worker-a", now.Add(2*time.Second)).Valid {
		t.Fatal("token should still validate after a rolled-back revocation")
	}

	// Once the failure clears, revoking again must revoke the token AND record
	// the event atomically.
	store.revokeEventErr = nil
	if err := manager.Revoke(token, "handover", now.Add(3*time.Second)); err != nil {
		t.Fatalf("revoke on retry: %v", err)
	}
	if !store.revoked[token] {
		t.Fatal("token should be revoked on the successful retry")
	}
	if len(store.events) != 1 || store.events[0] != "fencing_revoked:"+token+":handover" {
		t.Fatalf("a single revocation event should be recorded, got %v", store.events)
	}
	if manager.Validate(token, "prod/db", "worker-a", now.Add(4*time.Second)).Valid {
		t.Fatal("token should fail validation once revoked for real")
	}
}
