package resource

import (
	"fmt"
	"task106/internal/model"
	"task106/internal/namespace"
	"time"
)

func (m *Manager) SetState(path string, next model.ResourceState, reason string) (*model.Resource, error) {
	path, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	itemValue, ok := m.resources[path]
	if !ok {
		return nil, ErrNotFound
	}
	item := &itemValue
	if !validTransition(item.State, next) {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, item.State, next)
	}
	if next == model.ResourceActive {
		for _, child := range m.resources {
			if namespace.IsAncestor(item.Path, child.Path) && child.State == model.ResourceRetired {
				return nil, fmt.Errorf("cannot activate %s while retired child %s exists", item.Path, child.Path)
			}
		}
	}
	item.State = next
	item.Generation++
	item.UpdatedAt = time.Now().UTC()
	if err := m.store.UpsertResource(item); err != nil {
		return nil, err
	}
	m.resources[item.Path] = *item
	_ = m.store.RecordCoordinationEvent("resource_state_changed", item.Path, item.Owner, fmt.Sprintf("%s: %s", next, reason))
	return item, nil
}

func validTransition(from, to model.ResourceState) bool {
	if from == to {
		return true
	}
	if from == model.ResourceRetired {
		return false
	}
	return (from == model.ResourceActive && (to == model.ResourceDraining || to == model.ResourceRetired)) ||
		(from == model.ResourceDraining && (to == model.ResourceActive || to == model.ResourceRetired))
}
