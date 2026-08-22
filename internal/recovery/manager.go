package recovery

import (
	"task106/internal/model"
	"time"
)

func NewManager(store Store, resources ResourceReader, leases LeaseReader) *Manager {
	return &Manager{store: store, resources: resources, leases: leases}
}

func (m *Manager) Start() error {
	_, err := m.Run("startup")
	return err
}

func (m *Manager) Run(scope string) (*model.RecoveryCheckpoint, error) {
	checkpoint := &model.RecoveryCheckpoint{Scope: scope, Status: "running", StartedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}
	if err := m.store.CreateRecoveryCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	issues, err := m.scanIssues()
	if err != nil {
		_ = m.store.FinishRecoveryCheckpoint(checkpoint.ID, "failed", []string{err.Error()}, time.Now().UTC())
		return nil, err
	}
	status := "healthy"
	if len(issues) > 0 {
		status = "attention"
	}
	finished := time.Now().UTC()
	// Mark the checkpoint complete and record the coordination event together so the
	// result and its audit record commit atomically. If the event fails to land, the
	// checkpoint stays running and the next inspection resumes from the correct state.
	if err := m.store.FinishRecoveryCheckpointWithEvent(checkpoint.ID, status, issues, finished, "recovery_checkpoint", scope, "", status); err != nil {
		return nil, err
	}
	checkpoint.Status = status
	checkpoint.Issues = issues
	checkpoint.FinishedAt = &finished
	return checkpoint, nil
}

func (m *Manager) Get(id int64) (*model.RecoveryCheckpoint, error) {
	item, err := m.store.GetRecoveryCheckpoint(id)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, ErrCheckpointNotFound
	}
	return item, nil
}

func (m *Manager) List(scope string, limit int) ([]model.RecoveryCheckpoint, error) {
	return m.store.ListRecoveryCheckpoints(scope, limit)
}
