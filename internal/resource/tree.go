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
