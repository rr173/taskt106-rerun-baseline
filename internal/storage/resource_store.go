package storage

import (
	"database/sql"
	"encoding/json"
	"task106/internal/model"
	"time"
)

func (s *Storage) ListResources() ([]model.Resource, error) {
	rows, err := s.db.Query(`SELECT path, parent_path, owner, state, generation, labels_json, created_at, updated_at FROM coord_resources ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Resource, 0)
	for rows.Next() {
		var item model.Resource
		var labels string
		if err := rows.Scan(&item.Path, &item.ParentPath, &item.Owner, &item.State, &item.Generation, &labels, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(labels), &item.Labels); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Storage) GetResource(path string) (*model.Resource, error) {
	row := s.db.QueryRow(`SELECT path, parent_path, owner, state, generation, labels_json, created_at, updated_at FROM coord_resources WHERE path = ?`, path)
	var item model.Resource
	var labels string
	if err := row.Scan(&item.Path, &item.ParentPath, &item.Owner, &item.State, &item.Generation, &labels, &item.CreatedAt, &item.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal([]byte(labels), &item.Labels); err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Storage) UpsertResource(item *model.Resource) error {
	labels, err := json.Marshal(item.Labels)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO coord_resources(path, parent_path, owner, state, generation, labels_json, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET parent_path=excluded.parent_path, owner=excluded.owner,
state=excluded.state, generation=excluded.generation, labels_json=excluded.labels_json, updated_at=excluded.updated_at
`, item.Path, item.ParentPath, item.Owner, item.State, item.Generation, string(labels), item.CreatedAt, item.UpdatedAt)
	return err
}

func (s *Storage) ListResourcePolicies() ([]model.ResourcePolicy, error) {
	rows, err := s.db.Query(`SELECT path, max_lease_sec, required_holder, priority, require_fencing, allowed_holders_json, updated_at FROM coord_resource_policies ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.ResourcePolicy, 0)
	for rows.Next() {
		policy, err := scanResourcePolicy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *policy)
	}
	return result, rows.Err()
}

func (s *Storage) GetResourcePolicy(path string) (*model.ResourcePolicy, error) {
	row := s.db.QueryRow(`SELECT path, max_lease_sec, required_holder, priority, require_fencing, allowed_holders_json, updated_at FROM coord_resource_policies WHERE path = ?`, path)
	return scanResourcePolicy(row)
}

type scanner interface{ Scan(...any) error }

func scanResourcePolicy(row scanner) (*model.ResourcePolicy, error) {
	var policy model.ResourcePolicy
	var required, holders string
	var fencing int
	if err := row.Scan(&policy.Path, &policy.MaxLeaseSec, &required, &policy.Priority, &fencing, &holders, &policy.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	policy.RequiredHolder = required
	policy.RequireFencing = fencing != 0
	if err := json.Unmarshal([]byte(holders), &policy.AllowedHolders); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (s *Storage) UpsertResourcePolicy(policy *model.ResourcePolicy) error {
	holders, err := json.Marshal(policy.AllowedHolders)
	if err != nil {
		return err
	}
	fencing := 0
	if policy.RequireFencing {
		fencing = 1
	}
	_, err = s.db.Exec(`
INSERT INTO coord_resource_policies(path, max_lease_sec, required_holder, priority, require_fencing, allowed_holders_json, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET max_lease_sec=excluded.max_lease_sec, required_holder=excluded.required_holder,
priority=excluded.priority, require_fencing=excluded.require_fencing, allowed_holders_json=excluded.allowed_holders_json, updated_at=excluded.updated_at
`, policy.Path, policy.MaxLeaseSec, policy.RequiredHolder, policy.Priority, fencing, string(holders), policy.UpdatedAt)
	return err
}

func (s *Storage) RecordCoordinationEvent(eventType, resourcePath, holder, detail string) error {
	_, err := s.db.Exec(`INSERT INTO coordination_events(event_type, resource_path, holder, detail, created_at) VALUES(?, ?, ?, ?, ?)`, eventType, resourcePath, holder, detail, time.Now().UTC())
	return err
}

func (s *Storage) ListCoordinationEvents(resourcePath string, limit int) ([]model.CoordinationEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, event_type, resource_path, holder, detail, created_at FROM coordination_events WHERE (? = '' OR resource_path = ?) ORDER BY id DESC LIMIT ?`
	rows, err := s.db.Query(query, resourcePath, resourcePath, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.CoordinationEvent, 0)
	for rows.Next() {
		var event model.CoordinationEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.ResourcePath, &event.Holder, &event.Detail, &event.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}
