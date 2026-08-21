package storage

import (
	"database/sql"
	"task106/internal/model"
	"time"
)

func (s *Storage) NextFencingSequence(resourcePath string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var next int64
	err = tx.QueryRow(`SELECT next_sequence FROM coord_fencing_counters WHERE resource_path = ?`, resourcePath).Scan(&next)
	if err == sql.ErrNoRows {
		next = 1
		if _, err = tx.Exec(`INSERT INTO coord_fencing_counters(resource_path, next_sequence) VALUES(?, ?)`, resourcePath, 2); err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	} else if _, err = tx.Exec(`UPDATE coord_fencing_counters SET next_sequence = ? WHERE resource_path = ?`, next+1, resourcePath); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Storage) InsertFencingToken(item *model.FencingToken) error {
	_, err := s.db.Exec(`INSERT INTO coord_fencing_tokens(token, resource_path, holder, sequence, issued_at, expires_at, revoked_at, revoke_reason) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, item.Token, item.ResourcePath, item.Holder, item.Sequence, item.IssuedAt, item.ExpiresAt, item.RevokedAt, item.RevokeReason)
	return err
}

func (s *Storage) GetFencingToken(token string) (*model.FencingToken, error) {
	row := s.db.QueryRow(`SELECT token, resource_path, holder, sequence, issued_at, expires_at, revoked_at, revoke_reason FROM coord_fencing_tokens WHERE token = ?`, token)
	var item model.FencingToken
	var revoked sql.NullTime
	if err := row.Scan(&item.Token, &item.ResourcePath, &item.Holder, &item.Sequence, &item.IssuedAt, &item.ExpiresAt, &revoked, &item.RevokeReason); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if revoked.Valid {
		value := revoked.Time
		item.RevokedAt = &value
	}
	return &item, nil
}

func (s *Storage) CurrentFencingSequence(resourcePath string) (int64, error) {
	var next int64
	if err := s.db.QueryRow(`SELECT next_sequence FROM coord_fencing_counters WHERE resource_path = ?`, resourcePath).Scan(&next); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if next <= 1 {
		return 0, nil
	}
	return next - 1, nil
}

func (s *Storage) RevokeFencingToken(token, reason string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE coord_fencing_tokens SET revoked_at = ?, revoke_reason = ? WHERE token = ? AND revoked_at IS NULL`, now, reason, token)
	return err
}

func (s *Storage) ListFencingTokens(resourcePath string, limit int) ([]model.FencingToken, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT token, resource_path, holder, sequence, issued_at, expires_at, revoked_at, revoke_reason FROM coord_fencing_tokens WHERE (? = '' OR resource_path = ?) ORDER BY sequence DESC LIMIT ?`, resourcePath, resourcePath, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.FencingToken, 0)
	for rows.Next() {
		var item model.FencingToken
		var revoked sql.NullTime
		if err := rows.Scan(&item.Token, &item.ResourcePath, &item.Holder, &item.Sequence, &item.IssuedAt, &item.ExpiresAt, &revoked, &item.RevokeReason); err != nil {
			return nil, err
		}
		if revoked.Valid {
			value := revoked.Time
			item.RevokedAt = &value
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
