package fencing

import "task106/internal/model"

func (m *Manager) List(resourcePath string, limit int) ([]model.FencingToken, error) {
	return m.store.ListFencingTokens(resourcePath, limit)
}
