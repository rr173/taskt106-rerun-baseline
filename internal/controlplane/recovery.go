package controlplane

import (
	"task106/internal/model"
	"time"
)

func (m *Manager) RunRecovery(scope string) (*model.RecoveryCheckpoint, error) {
	return m.recovery.Run(scope)
}

func (m *Manager) RefreshMaintenance(now time.Time) error {
	return m.maintenance.RefreshStatuses(now)
}
