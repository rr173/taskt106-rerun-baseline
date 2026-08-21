package controlplane

import (
	"task106/internal/model"
	"time"
)

func (m *Manager) ValidateToken(token, resourcePath, holder string, now time.Time) model.TokenValidation {
	return m.fencing.Validate(token, resourcePath, holder, now)
}

func (m *Manager) Events(resourcePath string, limit int) ([]model.CoordinationEvent, error) {
	return m.events.ListCoordinationEvents(resourcePath, limit)
}
