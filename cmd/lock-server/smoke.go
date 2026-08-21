package main

import (
	"fmt"
	"os"
	"path/filepath"
	"task106/internal/controlplane"
	"task106/internal/lock"
	"task106/internal/storage"
	"time"
)

// runSmokeTest exercises the local persistence path without requiring a
// network listener or any external service. It is used by the Benzhi image
// checks for both supported CPU architectures.
func runSmokeTest() error {
	dir, err := os.MkdirTemp("", "task106-smoke-")
	if err != nil {
		return fmt.Errorf("create temp directory: %w", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "locks.db")
	store, err := storage.New(dbPath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}

	manager := lock.NewManager(store)
	coordination := controlplane.NewManager(store)
	if err := coordination.Start(); err != nil {
		store.Close()
		return fmt.Errorf("start coordination control plane: %w", err)
	}
	manager.SetAdmissionGuard(coordination)
	manager.SetFencingIssuer(coordination)
	if err := manager.Start(); err != nil {
		store.Close()
		return fmt.Errorf("start lock manager: %w", err)
	}
	if _, err := manager.AcquireLock("smoke-lock", "smoke-holder", 30, true); err != nil {
		manager.Stop()
		store.Close()
		return fmt.Errorf("acquire smoke lock: %w", err)
	}
	lease, err := manager.GetActiveLease("smoke-lock")
	if err != nil || lease == nil || lease.FencingToken == "" {
		manager.Stop()
		store.Close()
		return fmt.Errorf("fencing token was not persisted: lease=%+v err=%v", lease, err)
	}
	token := lease.FencingToken
	manager.Stop()
	if err := store.Close(); err != nil {
		return fmt.Errorf("close first storage: %w", err)
	}

	// Reopen the same database to prove the state survives a process restart.
	reopened, err := storage.New(dbPath)
	if err != nil {
		return fmt.Errorf("reopen storage: %w", err)
	}
	defer reopened.Close()
	restartedCoordination := controlplane.NewManager(reopened)
	if err := restartedCoordination.Start(); err != nil {
		return fmt.Errorf("restart coordination control plane: %w", err)
	}
	validation := restartedCoordination.ValidateToken(token, "smoke-lock", "smoke-holder", time.Now().UTC())
	if !validation.Valid {
		return fmt.Errorf("persisted fencing token failed validation: %+v", validation)
	}
	lockState, err := reopened.GetLock("smoke-lock")
	if err != nil {
		return fmt.Errorf("read persisted lock: %w", err)
	}
	if lockState == nil || lockState.Holder != "smoke-holder" {
		return fmt.Errorf("persisted lock missing or has wrong holder: %+v", lockState)
	}

	fmt.Println("task106 smoke test passed: SQLite state persisted across restart")
	return nil
}
