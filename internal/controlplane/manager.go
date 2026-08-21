package controlplane

import (
	"task106/internal/fencing"
	"task106/internal/maintenance"
	"task106/internal/recovery"
	"task106/internal/resource"
	"task106/internal/storage"
	"time"
)

func NewManager(store *storage.Storage) *Manager {
	resources := resource.NewManager(store)
	maintenanceMgr := maintenance.NewManager(store)
	fencingMgr := fencing.NewManager(store)
	recoveryMgr := recovery.NewManager(store, resources, store)
	resources.SetMaintenanceChecker(maintenanceMgr)
	return &Manager{resources: resources, maintenance: maintenanceMgr, fencing: fencingMgr, recovery: recoveryMgr, events: store}
}

func (m *Manager) Start() error {
	if err := m.resources.Start(); err != nil {
		return err
	}
	if err := m.maintenance.Start(); err != nil {
		return err
	}
	return m.recovery.Start()
}

func (m *Manager) Resources() *resource.Manager      { return m.resources }
func (m *Manager) Maintenance() *maintenance.Manager { return m.maintenance }
func (m *Manager) Fencing() *fencing.Manager         { return m.fencing }
func (m *Manager) Recovery() *recovery.Manager       { return m.recovery }

func (m *Manager) BeforeAcquire(lockName, holder string, leaseSec int) error {
	if _, err := m.resources.Ensure(lockName, holder); err != nil {
		return err
	}
	return m.resources.BeforeAcquire(lockName, holder, leaseSec)
}

func (m *Manager) Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error) {
	return m.fencing.Issue(resourcePath, holder, leaseSec, now)
}
