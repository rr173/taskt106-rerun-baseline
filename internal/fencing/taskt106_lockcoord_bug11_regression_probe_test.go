package fencing

import (
    "errors"
    "task106/internal/model"
    "testing"
    "time"
)

type eventFailureStore struct { tokens map[string]*model.FencingToken }
func (s *eventFailureStore) NextFencingSequence(string) (int64, error) { return 1, nil }
func (s *eventFailureStore) InsertFencingToken(v *model.FencingToken) error { if s.tokens == nil { s.tokens = map[string]*model.FencingToken{} }; s.tokens[v.Token] = v; return nil }
func (s *eventFailureStore) DeleteFencingToken(token string) error { delete(s.tokens, token); return nil }
func (s *eventFailureStore) GetFencingToken(string) (*model.FencingToken, error) { return nil, nil }
func (s *eventFailureStore) CurrentFencingSequence(string) (int64, error) { return 1, nil }
func (s *eventFailureStore) RevokeFencingToken(string, string, time.Time) error { return nil }
func (s *eventFailureStore) ListFencingTokens(string, int) ([]model.FencingToken, error) { return nil, nil }
func (s *eventFailureStore) RecordCoordinationEvent(string, string, string, string) error { return errors.New("event store unavailable") }

func TestIssueRollsBackWhenEventWriteFails(t *testing.T) {
    store := &eventFailureStore{}; m := NewManager(store)
    if _, err := m.Issue("prod/db", "worker", 10, time.Now().UTC()); err == nil { t.Fatal("expected event failure") }
    if len(store.tokens) != 0 { t.Fatalf("token leaked after event failure: %#v", store.tokens) }
}
