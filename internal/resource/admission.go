package resource

import (
	"fmt"
	"task106/internal/model"
	"task106/internal/namespace"
	"time"
)

func (m *Manager) BeforeAcquire(path, holder string, leaseSec int) error {
	decision, err := m.Decide(path, holder, leaseSec, time.Now().UTC())
	if err != nil {
		return err
	}
	if !decision.Allowed {
		if len(decision.Reasons) == 0 {
			return fmt.Errorf("resource acquisition denied")
		}
		return fmt.Errorf("resource acquisition denied: %s", decision.Reasons[0])
	}
	return nil
}

func (m *Manager) Decide(path, holder string, leaseSec int, now time.Time) (*Decision, error) {
	path, err := namespace.Normalize(path)
	if err != nil {
		return nil, err
	}
	item, err := m.Get(path)
	if err != nil {
		return nil, err
	}
	decision := &Decision{Allowed: true, Resource: item}
	if item.State == model.ResourceRetired {
		decision.Allowed = false
		decision.Reasons = append(decision.Reasons, ErrRetired.Error())
	}
	if item.State == model.ResourceDraining {
		decision.Allowed = false
		decision.Reasons = append(decision.Reasons, ErrDraining.Error())
	}
	if m.maintenance != nil {
		blocked, reason, err := m.maintenance.IsBlocked(path, now)
		if err != nil {
			return nil, err
		}
		if blocked {
			decision.Allowed = false
			decision.Reasons = append(decision.Reasons, reason)
		}
	}
	m.mu.RLock()
	policy, ok := m.policies[path]
	m.mu.RUnlock()
	if ok {
		decision.Policy = &policy
		if policy.MaxLeaseSec > 0 && leaseSec > policy.MaxLeaseSec {
			decision.Allowed = false
			decision.Reasons = append(decision.Reasons, ErrLeaseTooLong.Error())
		}
		if policy.RequiredHolder != "" && policy.RequiredHolder != holder {
			decision.Allowed = false
			decision.Reasons = append(decision.Reasons, ErrPolicyDenied.Error())
		}
		if len(policy.AllowedHolders) > 0 && !contains(policy.AllowedHolders, holder) {
			decision.Allowed = false
			decision.Reasons = append(decision.Reasons, ErrPolicyDenied.Error())
		}
	}
	return decision, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
