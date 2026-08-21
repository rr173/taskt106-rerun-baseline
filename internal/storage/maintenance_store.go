package storage

import (
	"database/sql"
	"task106/internal/model"
)

func (s *Storage) CreateMaintenanceWindow(window *model.MaintenanceWindow) error {
	result, err := s.db.Exec(`INSERT INTO coord_maintenance_windows(resource_path, mode, start_at, end_at, reason, operator, status, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, window.ResourcePath, window.Mode, window.StartAt, window.EndAt, window.Reason, window.Operator, window.Status, window.CreatedAt)
	if err != nil {
		return err
	}
	window.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListMaintenanceWindows(resourcePath string) ([]model.MaintenanceWindow, error) {
	rows, err := s.db.Query(`SELECT id, resource_path, mode, start_at, end_at, reason, operator, status, created_at FROM coord_maintenance_windows WHERE (? = '' OR resource_path = ?) ORDER BY start_at, id`, resourcePath, resourcePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.MaintenanceWindow, 0)
	for rows.Next() {
		var item model.MaintenanceWindow
		if err := rows.Scan(&item.ID, &item.ResourcePath, &item.Mode, &item.StartAt, &item.EndAt, &item.Reason, &item.Operator, &item.Status, &item.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Storage) UpdateMaintenanceStatus(id int64, status string) error {
	result, err := s.db.Exec(`UPDATE coord_maintenance_windows SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}
