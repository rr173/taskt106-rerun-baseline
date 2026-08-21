package maintenance

import (
	"task106/internal/model"
	"time"
)

func (m *Manager) ActiveWindows(now time.Time) []ActiveWindow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]ActiveWindow, 0)
	for _, window := range m.windows {
		if window.Status != "cancelled" && !now.Before(window.StartAt) && now.Before(window.EndAt) {
			result = append(result, ActiveWindow{Window: window, Reason: window.Reason, Until: window.EndAt})
		}
	}
	return result
}

func (m *Manager) RefreshStatuses(now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, window := range m.windows {
		if window.Status == "cancelled" || window.Status == "completed" {
			continue
		}
		next := statusFor(window.StartAt, window.EndAt, now)
		if next != window.Status {
			if err := m.store.UpdateMaintenanceStatus(id, next); err != nil {
				return err
			}
			window.Status = next
			m.windows[id] = window
		}
	}
	return nil
}

func modeAllowsExistingLease(mode model.MaintenanceMode) bool {
	return mode == model.MaintenanceDrain
}
