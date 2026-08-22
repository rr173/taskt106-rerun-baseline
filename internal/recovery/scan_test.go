package recovery_test

import (
	"path/filepath"
	"strings"
	"task106/internal/controlplane"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

// TestScanRecordsMissingResourceAndContinues reproduces the bug where a missing
// resource error (which may carry extra context, e.g. a wrapped ErrNotFound)
// aborted the whole recovery scan so later problems were never reported. The
// expected behavior is to record the missing resource as a recovery issue and
// keep scanning so subsequent issues (here: a lease on a retired resource) are
// surfaced in the same checkpoint.
func TestScanRecordsMissingResourceAndContinues(t *testing.T) {
	store, err := storage.New(filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	coord := controlplane.NewManager(store)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	mgr := lock.NewManager(store)
	mgr.SetAdmissionGuard(coord)
	mgr.SetFencingIssuer(coord)
	if err := mgr.Start(); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()

	// Register a resource, then retire it so the lease on it becomes an issue.
	if _, err := coord.Resources().Register(model.ResourceCreateRequest{Path: "retired-resource", Owner: "platform"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.Resources().SetState("retired-resource", model.ResourceRetired, "decommissioned"); err != nil {
		t.Fatal(err)
	}

	// Insert an active lease for a resource that was never registered. Using the
	// storage layer directly bypasses admission (which would auto-register the
	// resource) and yields a wrapped missing-resource error during the scan.
	now := time.Now().UTC()
	missingLease := &model.Lease{
		LockName:   "missing-resource",
		Holder:     "worker",
		LeaseSec:   30,
		AcquiredAt: now,
		ExpiresAt:  now.Add(30 * time.Second),
		Active:     true,
	}
	if err := store.CreateLease(missingLease); err != nil {
		t.Fatal(err)
	}
	retiredLease := &model.Lease{
		LockName:   "retired-resource",
		Holder:     "worker",
		LeaseSec:   30,
		AcquiredAt: now,
		ExpiresAt:  now.Add(30 * time.Second),
		Active:     true,
	}
	if err := store.CreateLease(retiredLease); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := coord.RunRecovery("test")
	if err != nil {
		t.Fatalf("recovery scan aborted instead of recording issues: %v", err)
	}
	if checkpoint.Status != "attention" {
		t.Fatalf("expected status attention, got %q (issues=%v)", checkpoint.Status, checkpoint.Issues)
	}

	var sawMissing, sawRetired bool
	joined := strings.Join(checkpoint.Issues, "\n")
	for _, issue := range checkpoint.Issues {
		switch {
		case strings.Contains(issue, "missing-resource") && strings.Contains(issue, "no registered resource"):
			sawMissing = true
		case strings.Contains(issue, "retired-resource") && strings.Contains(issue, "retired resource"):
			sawRetired = true
		}
	}
	if !sawMissing {
		t.Fatalf("missing-resource issue not recorded: %s", joined)
	}
	if !sawRetired {
		t.Fatalf("later retired-resource issue not surfaced (scan aborted early): %s", joined)
	}
}
