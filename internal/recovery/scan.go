package recovery

import (
	"errors"
	"fmt"
	"task106/internal/model"
	resourcerepo "task106/internal/resource"
)

func (m *Manager) scanIssues() ([]string, error) {
	leases, err := m.leases.ListActiveLeases()
	if err != nil {
		return nil, err
	}
	issues := make([]string, 0)
	for _, lease := range leases {
		item, err := m.resources.Get(lease.LockName)
		if err != nil {
			if errors.Is(err, resourcerepo.ErrNotFound) {
				issues = append(issues, fmt.Sprintf("active lease %s references missing resource", lease.LockName))
				continue
			}
			return nil, err
		}
		if item == nil {
			issues = append(issues, fmt.Sprintf("active lease %s has no registered resource", lease.LockName))
			continue
		}
		if item.State == model.ResourceRetired {
			issues = append(issues, fmt.Sprintf("retired resource %s still has active lease", lease.LockName))
		}
		if lease.ExpiresAt.IsZero() {
			issues = append(issues, fmt.Sprintf("active lease %s has no expiry", lease.LockName))
		}
	}
	return issues, nil
}
