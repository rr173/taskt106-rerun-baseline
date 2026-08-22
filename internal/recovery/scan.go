package recovery

import (
	"errors"
	"fmt"
	"task106/internal/model"
	"task106/internal/resource"
)

func (m *Manager) scanIssues() ([]string, error) {
	leases, err := m.leases.ListActiveLeases()
	if err != nil {
		return nil, err
	}
	issues := make([]string, 0)
	for _, lease := range leases {
		r, err := m.resources.Get(lease.LockName)
		// A missing resource is a recovery problem, not a scan failure: the
		// lookup may wrap ErrNotFound with extra context, so unwrap to detect
		// it, record the issue, and keep scanning the remaining leases.
		if err != nil && errors.Is(err, resource.ErrNotFound) {
			issues = append(issues, fmt.Sprintf("active lease %s has no registered resource", lease.LockName))
			continue
		}
		if err != nil {
			return nil, err
		}
		if r == nil {
			issues = append(issues, fmt.Sprintf("active lease %s has no registered resource", lease.LockName))
			continue
		}
		if r.State == model.ResourceRetired {
			issues = append(issues, fmt.Sprintf("retired resource %s still has active lease", lease.LockName))
		}
		if lease.ExpiresAt.IsZero() {
			issues = append(issues, fmt.Sprintf("active lease %s has no expiry", lease.LockName))
		}
	}
	return issues, nil
}
