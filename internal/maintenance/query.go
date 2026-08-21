package maintenance

import (
	"task106/internal/model"
	"task106/internal/namespace"
	"time"
)

func (m *Manager) IsBlocked(path string, now time.Time) (bool, string, error) {
	windows, err := m.store.ListMaintenanceWindows("")
	if err != nil {
		return false, "", err
	}
	for _, window := range windows {
		if window.Status == "cancelled" || now.Before(window.StartAt) || !now.Before(window.EndAt) {
			continue
		}
		if namespace.IsSameOrDescendant(path, window.ResourcePath) {
			return true, "maintenance window active: " + window.Reason, nil
		}
	}
	return false, "", nil
}

func (m *Manager) List(resourcePath string) ([]model.MaintenanceWindow, error) {
	return m.store.ListMaintenanceWindows(resourcePath)
}

func (m *Manager) Cancel(id int64, operator string) error {
	m.mu.RLock()
	window, ok := m.windows[id]
	m.mu.RUnlock()
	if !ok {
		return ErrWindowClosed
	}
	if window.Status == "cancelled" || window.Status == "completed" {
		return ErrWindowClosed
	}
	if err := m.store.UpdateMaintenanceStatus(id, "cancelled"); err != nil {
		return err
	}
	window.Status = "cancelled"
	m.mu.Lock()
	m.windows[id] = window
	m.mu.Unlock()
	return m.store.RecordCoordinationEvent("maintenance_cancelled", window.ResourcePath, operator, window.Reason)
}
