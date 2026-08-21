package storage

import (
	"database/sql"
	"encoding/json"
	"task106/internal/model"
	"time"
)

func (s *Storage) CreateRecoveryCheckpoint(item *model.RecoveryCheckpoint) error {
	issues, err := json.Marshal(item.Issues)
	if err != nil {
		return err
	}
	result, err := s.db.Exec(`INSERT INTO coord_recovery_checkpoints(scope, status, started_at, finished_at, issues_json, created_at) VALUES(?, ?, ?, ?, ?, ?)`, item.Scope, item.Status, item.StartedAt, item.FinishedAt, string(issues), item.CreatedAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) FinishRecoveryCheckpoint(id int64, status string, issues []string, finished time.Time) error {
	encoded, err := json.Marshal(issues)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE coord_recovery_checkpoints SET status = ?, finished_at = ?, issues_json = ? WHERE id = ?`, status, finished, string(encoded), id)
	return err
}

func (s *Storage) GetRecoveryCheckpoint(id int64) (*model.RecoveryCheckpoint, error) {
	row := s.db.QueryRow(`SELECT id, scope, status, started_at, finished_at, issues_json, created_at FROM coord_recovery_checkpoints WHERE id = ?`, id)
	return scanRecoveryCheckpoint(row)
}

func (s *Storage) ListRecoveryCheckpoints(scope string, limit int) ([]model.RecoveryCheckpoint, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.Query(`SELECT id, scope, status, started_at, finished_at, issues_json, created_at FROM coord_recovery_checkpoints WHERE (? = '' OR scope = ?) ORDER BY id DESC LIMIT ?`, scope, scope, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.RecoveryCheckpoint, 0)
	for rows.Next() {
		item, err := scanRecoveryCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		if item != nil {
			result = append(result, *item)
		}
	}
	return result, rows.Err()
}

func scanRecoveryCheckpoint(row interface{ Scan(...any) error }) (*model.RecoveryCheckpoint, error) {
	var item model.RecoveryCheckpoint
	var finished sql.NullTime
	var issues string
	if err := row.Scan(&item.ID, &item.Scope, &item.Status, &item.StartedAt, &finished, &issues, &item.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if finished.Valid {
		value := finished.Time
		item.FinishedAt = &value
	}
	if err := json.Unmarshal([]byte(issues), &item.Issues); err != nil {
		return nil, err
	}
	return &item, nil
}
