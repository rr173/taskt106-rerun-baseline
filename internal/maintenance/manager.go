package maintenance

import (
	"task106/internal/model"
	"time"
)

func NewManager(store Store) *Manager {
	return &Manager{store: store, windows: make(map[int64]model.MaintenanceWindow)}
}

func (m *Manager) Start() error {
	windows, err := m.store.ListMaintenanceWindows("")
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, window := range windows {
		m.windows[window.ID] = window
	}
	return nil
}

func (m *Manager) Create(req model.MaintenanceCreateRequest) (*model.MaintenanceWindow, error) {
	if !req.StartAt.Before(req.EndAt) || req.Reason == "" || req.Operator == "" {
		return nil, ErrInvalidWindow
	}
	if req.Mode != model.MaintenanceDrain && req.Mode != model.MaintenanceForce {
		return nil, ErrInvalidWindow
	}
	// Hold the write lock across the overlap check and the insert so that
	// concurrent requests for the same time range are serialized: the first
	// request commits its window before any other request performs the check,
	// forcing all overlapping followers to report ErrWindowOverlap instead of
	// racing past the check and creating a duplicate.
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, err := m.store.ListMaintenanceWindows(req.ResourcePath)
	if err != nil {
		return nil, err
	}
	for _, item := range existing {
		if item.Status == "cancelled" || item.EndAt.Before(req.StartAt) || item.StartAt.After(req.EndAt) {
			continue
		}
		return nil, ErrWindowOverlap
	}
	window := &model.MaintenanceWindow{ResourcePath: req.ResourcePath, Mode: req.Mode, StartAt: req.StartAt, EndAt: req.EndAt, Reason: req.Reason, Operator: req.Operator, Status: statusFor(req.StartAt, req.EndAt, time.Now().UTC()), CreatedAt: time.Now().UTC()}
	if err := m.store.CreateMaintenanceWindow(window); err != nil {
		return nil, err
	}
	m.windows[window.ID] = *window
	_ = m.store.RecordCoordinationEvent("maintenance_created", window.ResourcePath, window.Operator, window.Reason)
	return window, nil
}

func statusFor(start, end, now time.Time) string {
	if now.Before(start) {
		return "scheduled"
	}
	if now.Before(end) {
		return "active"
	}
	return "completed"
}
