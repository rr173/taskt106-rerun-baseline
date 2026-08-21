package resource

import (
	"task106/internal/model"
	"task106/internal/namespace"
	"time"
)

func NewManager(store Store) *Manager {
	return &Manager{store: store, resources: make(map[string]model.Resource), policies: make(map[string]model.ResourcePolicy)}
}

func (m *Manager) Start() error {
	resources, err := m.store.ListResources()
	if err != nil {
		return err
	}
	m.mu.Lock()
	for _, item := range resources {
		m.resources[item.Path] = item
	}
	policies, err := m.store.ListResourcePolicies()
	if err != nil {
		return err
	}
	for _, policy := range policies {
		m.policies[policy.Path] = policy
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) SetMaintenanceChecker(checker MaintenanceChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maintenance = checker
}

func (m *Manager) Register(req model.ResourceCreateRequest) (*model.Resource, error) {
	path, err := namespace.Normalize(req.Path)
	if err != nil {
		return nil, err
	}
	parent := req.ParentPath
	if parent == "" {
		parent, _ = namespace.Parent(path)
	}
	if parent != "" {
		parent, err = namespace.Normalize(parent)
		if err != nil {
			return nil, err
		}
		if parent == path || namespace.IsAncestor(path, parent) {
			return nil, namespace.ErrPathCycle
		}
		m.mu.RLock()
		_, ok := m.resources[parent]
		m.mu.RUnlock()
		if !ok {
			return nil, ErrNotFound
		}
	}
	now := time.Now().UTC()
	item := &model.Resource{Path: path, ParentPath: parent, Owner: req.Owner, State: model.ResourceActive, Generation: 1, Labels: req.Labels, CreatedAt: now, UpdatedAt: now}
	if existing, _ := m.store.GetResource(path); existing != nil {
		return existing, nil
	}
	if err := m.store.UpsertResource(item); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.resources[path] = *item
	m.mu.Unlock()
	_ = m.store.RecordCoordinationEvent("resource_registered", path, req.Owner, "resource registered")
	return item, nil
}

func (m *Manager) Ensure(path, owner string) (*model.Resource, error) {
	normalized, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	item, ok := m.resources[normalized]
	m.mu.RUnlock()
	if ok {
		return &item, nil
	}
	return m.Register(model.ResourceCreateRequest{Path: normalized, Owner: owner})
}

func (m *Manager) Get(path string) (*model.Resource, error) {
	path, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	item, ok := m.resources[path]
	m.mu.RUnlock()
	if ok {
		copy := item
		return &copy, nil
	}
	return nil, ErrNotFound
}

func (m *Manager) List(root string) ([]model.Resource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]model.Resource, 0, len(m.resources))
	if root != "" {
		var err error
		root, err = namespace.Normalize(root)
		if err != nil {
			return nil, err
		}
	}
	for _, item := range m.resources {
		if root == "" || namespace.IsSameOrDescendant(item.Path, root) {
			items = append(items, item)
		}
	}
	return namespaceOrder(items), nil
}

func namespaceOrder(items []model.Resource) []model.Resource {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].Path < items[j-1].Path; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
	return items
}
