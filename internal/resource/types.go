package resource

import (
	"sync"
	"task106/internal/model"
	"time"
)

type MaintenanceChecker interface {
	IsBlocked(path string, now time.Time) (bool, string, error)
}

type Decision struct {
	Allowed  bool                  `json:"allowed"`
	Resource *model.Resource       `json:"resource,omitempty"`
	Policy   *model.ResourcePolicy `json:"policy,omitempty"`
	Reasons  []string              `json:"reasons,omitempty"`
}

type Manager struct {
	mu          sync.RWMutex
	store       Store
	resources   map[string]model.Resource
	policies    map[string]model.ResourcePolicy
	maintenance MaintenanceChecker
}

type Store interface {
	ListResources() ([]model.Resource, error)
	UpsertResource(*model.Resource) error
	GetResource(string) (*model.Resource, error)
	ListResourcePolicies() ([]model.ResourcePolicy, error)
	UpsertResourcePolicy(*model.ResourcePolicy) error
	GetResourcePolicy(string) (*model.ResourcePolicy, error)
	RecordCoordinationEvent(string, string, string, string) error
}
