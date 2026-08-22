package resource

import (
	"sort"
	"task106/internal/model"
	"task106/internal/namespace"
	"time"
)

func (m *Manager) SetPolicy(path string, policy model.ResourcePolicy) (*model.ResourcePolicy, error) {
	path, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	_, exists := m.resources[path]
	m.mu.RUnlock()
	if !exists {
		return nil, ErrNotFound
	}
	if policy.MaxLeaseSec <= 0 {
		policy.MaxLeaseSec = 60
	}
	policy.Path = path
	policy.UpdatedAt = time.Now().UTC()
	if err := m.store.UpsertResourcePolicy(&policy); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.policies[path] = policy
	m.mu.Unlock()
	_ = m.store.RecordCoordinationEvent("resource_policy_changed", path, policy.RequiredHolder, "policy updated")
	return &policy, nil
}

func (m *Manager) GetPolicy(path string) (*model.ResourcePolicy, error) {
	path, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	policy, ok := m.policies[path]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	copy := policy
	return &copy, nil
}

func (m *Manager) ListPolicies() []model.ResourcePolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]model.ResourcePolicy, 0, len(m.policies))
	for _, policy := range m.policies {
		result = append(result, policy)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Path < result[j].Path
	})
	return result
}
