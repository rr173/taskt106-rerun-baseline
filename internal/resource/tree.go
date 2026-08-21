package resource

import (
	"task106/internal/model"
	"task106/internal/namespace"
)

func (m *Manager) Children(path string) []model.Resource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]model.Resource, 0)
	for _, item := range m.resources {
		if item.ParentPath == path {
			result = append(result, item)
		}
	}
	return namespaceOrder(result)
}

func (m *Manager) Descendants(path string) []model.Resource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]model.Resource, 0)
	for _, item := range m.resources {
		if item.Path != path && namespace.IsSameOrDescendant(item.Path, path) {
			result = append(result, item)
		}
	}
	return namespaceOrder(result)
}

func (m *Manager) ResolveScope(path string) ([]string, error) {
	item, err := m.Get(path)
	if err != nil {
		return nil, err
	}
	paths := []string{item.Path}
	for _, child := range m.Descendants(item.Path) {
		paths = append(paths, child.Path)
	}
	return paths, nil
}

// effectivePolicy returns the policy that governs the given path. A resource's
// own policy (if any) takes precedence and overrides any ancestor policy. When
// the resource has no policy of its own, the nearest ancestor that defines a
// max lease bound is inherited so that a parent's max lease policy constrains
// child resources unless the child overrides it.
func (m *Manager) effectivePolicy(path string) *model.ResourcePolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if policy, ok := m.policies[path]; ok {
		policy := policy
		return &policy
	}
	ancestors := namespace.Ancestors(path)
	for i := len(ancestors) - 1; i >= 0; i-- {
		if policy, ok := m.policies[ancestors[i]]; ok && policy.MaxLeaseSec > 0 {
			policy := policy
			return &policy
		}
	}
	return nil
}
