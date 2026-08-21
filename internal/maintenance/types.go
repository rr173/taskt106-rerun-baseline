package maintenance

import (
	"sync"
	"task106/internal/model"
	"time"
)

type Store interface {
	CreateMaintenanceWindow(*model.MaintenanceWindow) error
	ListMaintenanceWindows(string) ([]model.MaintenanceWindow, error)
	UpdateMaintenanceStatus(int64, string) error
	RecordCoordinationEvent(string, string, string, string) error
}

type Manager struct {
	mu      sync.RWMutex
	store   Store
	windows map[int64]model.MaintenanceWindow
}

type ActiveWindow struct {
	Window model.MaintenanceWindow `json:"window"`
	Reason string                  `json:"reason"`
	Until  time.Time               `json:"until"`
}
