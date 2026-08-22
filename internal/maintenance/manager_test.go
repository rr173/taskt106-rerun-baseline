package maintenance

import (
	"path/filepath"
	"sync"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func TestOverlappingWindowsAreRejectedAndCancellationUnblocks(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	first, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceDrain, StartAt: now, EndAt: now.Add(time.Hour), Reason: "schema change", Operator: "ops"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(model.MaintenanceCreateRequest{ResourcePath: "prod", Mode: model.MaintenanceForce, StartAt: now.Add(10 * time.Minute), EndAt: now.Add(2 * time.Hour), Reason: "overlap", Operator: "ops"}); err != ErrWindowOverlap {
		t.Fatalf("expected overlap error, got %v", err)
	}
	blocked, _, err := manager.IsBlocked("prod/child", now.Add(2*time.Minute))
	if err != nil || !blocked {
		t.Fatalf("expected active maintenance block: %v %v", blocked, err)
	}
	if err := manager.Cancel(first.ID, "ops"); err != nil {
		t.Fatal(err)
	}
	blocked, _, err = manager.IsBlocked("prod/child", now.Add(2*time.Minute))
	if err != nil || blocked {
		t.Fatalf("expected cancellation to unblock: %v %v", blocked, err)
	}
}

// TestConcurrentSameTimeRangeOnlyOneSucceeds reproduces BUG8FIX: several
// concurrent requests creating a maintenance window for the exact same time
// range must end with exactly one success and the rest reporting overlap.
// Before the fix the overlap check ran outside the lock, so multiple goroutines
// could pass the check and insert overlapping windows.
func TestConcurrentSameTimeRangeOnlyOneSucceeds(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	req := model.MaintenanceCreateRequest{
		ResourcePath: "prod",
		Mode:         model.MaintenanceDrain,
		StartAt:      now,
		EndAt:        now.Add(time.Hour),
		Reason:       "concurrent create",
		Operator:     "ops",
	}

	const goroutines = 16
	var wg sync.WaitGroup
	results := make([]struct {
		window *model.MaintenanceWindow
		err    error
	}, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			results[i].window, results[i].err = manager.Create(req)
		}(i)
	}
	wg.Wait()

	var success, overlap int
	for _, r := range results {
		switch {
		case r.err == nil:
			success++
		case r.err == ErrWindowOverlap:
			overlap++
		default:
			t.Fatalf("unexpected error from goroutine %v: %v", r.err, r.err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly one successful create, got %d", success)
	}
	if overlap != goroutines-1 {
		t.Fatalf("expected %d overlap errors, got %d", goroutines-1, overlap)
	}

	windows, err := manager.List("prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 {
		t.Fatalf("expected exactly one persisted window, got %d", len(windows))
	}
}
