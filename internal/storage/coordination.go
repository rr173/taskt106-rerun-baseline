package storage

import (
	"database/sql"
	"fmt"
)

func (s *Storage) initCoordinationSchema() error {
	schema := `
CREATE TABLE IF NOT EXISTS coord_resources (
  path TEXT PRIMARY KEY,
  parent_path TEXT NOT NULL DEFAULT '',
  owner TEXT NOT NULL,
  state TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1,
  labels_json TEXT NOT NULL DEFAULT '{}',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coord_resources_parent ON coord_resources(parent_path);
CREATE INDEX IF NOT EXISTS idx_coord_resources_state ON coord_resources(state);
CREATE TABLE IF NOT EXISTS coord_resource_policies (
  path TEXT PRIMARY KEY,
  max_lease_sec INTEGER NOT NULL,
  required_holder TEXT NOT NULL DEFAULT '',
  priority INTEGER NOT NULL DEFAULT 0,
  require_fencing INTEGER NOT NULL DEFAULT 1,
  allowed_holders_json TEXT NOT NULL DEFAULT '[]',
  updated_at DATETIME NOT NULL
);
CREATE TABLE IF NOT EXISTS coord_maintenance_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  resource_path TEXT NOT NULL,
  mode TEXT NOT NULL,
  start_at DATETIME NOT NULL,
  end_at DATETIME NOT NULL,
  reason TEXT NOT NULL,
  operator TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coord_maintenance_resource ON coord_maintenance_windows(resource_path);
CREATE INDEX IF NOT EXISTS idx_coord_maintenance_time ON coord_maintenance_windows(start_at, end_at);
CREATE TABLE IF NOT EXISTS coord_fencing_counters (
  resource_path TEXT PRIMARY KEY,
  next_sequence INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS coord_fencing_tokens (
  token TEXT PRIMARY KEY,
  resource_path TEXT NOT NULL,
  holder TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  issued_at DATETIME NOT NULL,
  expires_at DATETIME NOT NULL,
  revoked_at DATETIME,
  revoke_reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_coord_tokens_resource ON coord_fencing_tokens(resource_path, sequence);
CREATE TABLE IF NOT EXISTS coord_recovery_checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scope TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME,
  issues_json TEXT NOT NULL DEFAULT '[]',
  created_at DATETIME NOT NULL
);
CREATE TABLE IF NOT EXISTS coordination_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  resource_path TEXT NOT NULL DEFAULT '',
  holder TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coord_events_type ON coordination_events(event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_coord_events_resource ON coordination_events(resource_path, created_at);
`
	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('leases') WHERE name = 'fencing_token'`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := s.db.Exec(`ALTER TABLE leases ADD COLUMN fencing_token TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	return nil
}

func scanOptionalString(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func coordinationError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("coordination %s: %w", action, err)
}
