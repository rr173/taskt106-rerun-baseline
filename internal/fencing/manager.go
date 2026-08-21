package fencing

import (
	"task106/internal/model"
	"time"
)

func NewManager(store Store) *Manager { return &Manager{store: store} }

func (m *Manager) Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error) {
	if resourcePath == "" || holder == "" || leaseSec <= 0 {
		return "", ErrTokenMismatch
	}
	sequence, err := m.store.NextFencingSequence(resourcePath)
	if err != nil {
		return "", err
	}
	token := &model.FencingToken{Token: makeToken(resourcePath, sequence), ResourcePath: resourcePath, Holder: holder, Sequence: sequence, IssuedAt: now, ExpiresAt: now.Add(time.Duration(leaseSec) * time.Second)}
	if err := m.store.InsertFencingToken(token); err != nil {
		return "", err
	}
	if err := m.store.RecordCoordinationEvent("fencing_issued", resourcePath, holder, token.Token); err != nil {
		return "", err
	}
	return token.Token, nil
}

func (m *Manager) Validate(token, resourcePath, holder string, now time.Time) model.TokenValidation {
	item, err := m.store.GetFencingToken(token)
	if err != nil || item == nil {
		return model.TokenValidation{Reason: ErrTokenNotFound.Error()}
	}
	if item.ResourcePath != resourcePath || item.Holder != holder {
		return model.TokenValidation{Reason: ErrTokenMismatch.Error(), ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
	}
	latest, err := m.store.CurrentFencingSequence(resourcePath)
	if err != nil {
		return model.TokenValidation{Reason: err.Error(), ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
	}
	if item.Sequence < latest {
		return model.TokenValidation{Reason: ErrTokenStale.Error(), ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
	}
	if item.RevokedAt != nil {
		return model.TokenValidation{Reason: ErrTokenRevoked.Error(), ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
	}
	if !now.Before(item.ExpiresAt) {
		return model.TokenValidation{Reason: ErrTokenExpired.Error(), ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
	}
	return model.TokenValidation{Valid: true, ResourcePath: item.ResourcePath, Holder: item.Holder, Sequence: item.Sequence}
}

func (m *Manager) Revoke(token, reason string, now time.Time) error {
	item, err := m.store.GetFencingToken(token)
	if err != nil {
		return err
	}
	if item == nil {
		return ErrTokenNotFound
	}
	if err := m.store.RevokeFencingToken(token, reason, now); err != nil {
		return err
	}
	return m.store.RecordCoordinationEvent("fencing_revoked", item.ResourcePath, item.Holder, reason)
}
