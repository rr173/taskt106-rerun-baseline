package orchestration

import (
	"path/filepath"
	"testing"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
)

// setupTestManager wires a fresh orchestration manager on top of an isolated
// SQLite database together with its lock and rate-limit dependencies, exactly
// the way the real server composes them. It returns the db path so tests can
// reopen the database after intentionally closing the live handle.
func setupTestManager(t *testing.T) (*Manager, *lock.Manager, *ratelimit.Manager, *storage.Storage, string, func()) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "orch.db")
	s, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("storage.New failed: %v", err)
	}

	lockMgr := lock.NewManager(s)
	rlMgr := ratelimit.NewManager(s)
	if err := rlMgr.Start(); err != nil {
		s.Close()
		t.Fatalf("rlMgr.Start failed: %v", err)
	}
	orch := NewManager(s, lockMgr, rlMgr)
	if err := orch.Start(); err != nil {
		rlMgr.Stop()
		s.Close()
		t.Fatalf("orch.Start failed: %v", err)
	}

	cleanup := func() {
		orch.Stop()
		rlMgr.Stop()
		s.Close()
	}
	return orch, lockMgr, rlMgr, s, dbPath, cleanup
}

// createCommittedTx builds a committed transaction holding a single lock so
// ReleaseTx has something concrete to release.
func createCommittedTx(t *testing.T, orch *Manager, rlMgr *ratelimit.Manager, holder string) *model.OrchestrationTx {
	t.Helper()

	if _, err := rlMgr.CreatePolicy("orch-test-policy", model.AlgoTokenBucket, 0, 100, 1.0, "per_second"); err != nil {
		t.Fatalf("create policy failed: %v", err)
	}
	if _, err := rlMgr.BindCaller(holder, "orch-test-policy", 100); err != nil {
		t.Fatalf("bind caller failed: %v", err)
	}

	locks := []model.TxLockSpec{{LockName: "orch-test-lock", LeaseSec: 300}}
	tokens := []model.TxTokenSpec{{CallerID: holder, Tokens: 1}}

	tx, err := orch.CreateTx(holder, 300, locks, tokens)
	if err != nil {
		t.Fatalf("CreateTx failed: %v", err)
	}
	if tx.Status != model.TxStatusCommitted {
		t.Fatalf("expected committed tx, got status=%s reason=%s", tx.Status, tx.FailReason)
	}
	return tx
}

// TestReleaseTxKeepsStatusAndAuditConsistent verifies the happy path: after a
// successful release the transaction is marked released and its committed ->
// released state-change record is persisted alongside, so the audit trail is
// never broken.
func TestReleaseTxKeepsStatusAndAuditConsistent(t *testing.T) {
	orch, _, rlMgr, s, _, cleanup := setupTestManager(t)
	defer cleanup()

	holder := "orch-holder"
	tx := createCommittedTx(t, orch, rlMgr, holder)

	released, err := orch.ReleaseTx(tx.ID, holder)
	if err != nil {
		t.Fatalf("ReleaseTx failed: %v", err)
	}
	if released.Status != model.TxStatusReleased {
		t.Fatalf("expected released status, got %s", released.Status)
	}

	persisted, err := s.GetOrchTx(tx.ID)
	if err != nil {
		t.Fatalf("GetOrchTx failed: %v", err)
	}
	if persisted.Status != model.TxStatusReleased {
		t.Fatalf("persisted status mismatch: got %s, want released", persisted.Status)
	}

	changes, err := s.ListTxStateChanges(tx.ID)
	if err != nil {
		t.Fatalf("ListTxStateChanges failed: %v", err)
	}

	var releaseChange *model.TxStateChange
	for i := range changes {
		if changes[i].ToState == model.TxStatusReleased {
			releaseChange = &changes[i]
			break
		}
	}
	if releaseChange == nil {
		t.Fatalf("missing committed->released state change record; got %d changes: %+v", len(changes), changes)
	}
	if releaseChange.FromState != model.TxStatusCommitted {
		t.Fatalf("release change from_state mismatch: got %s, want committed", releaseChange.FromState)
	}
}

// TestReleaseTxFailsOpenStateWhenAuditUnwritable is the regression guard for the
// bug: when the state-change (audit) record cannot be persisted, the
// transaction must NOT be left marked as released. Its status stays committed,
// no committed->released audit record is written, and ReleaseTx surfaces an
// error so the caller can retry. Previously the status update and the audit
// write were independent and both error-ignored: the UPDATE to orch_txs
// succeeded while the INSERT into orch_tx_state_changes failed, leaving the
// transaction released with a broken audit trail. The fix makes both writes a
// single atomic transaction, so a failed audit INSERT rolls the status update
// back too.
func TestReleaseTxFailsOpenStateWhenAuditUnwritable(t *testing.T) {
	orch, _, rlMgr, s, _, cleanup := setupTestManager(t)
	defer cleanup()

	holder := "orch-holder"
	tx := createCommittedTx(t, orch, rlMgr, holder)

	// Reproduce the partial-failure scenario: the status UPDATE must succeed
	// while the state-change INSERT fails. Dropping the audit table makes
	// the INSERT fail; the orch_txs table is untouched so an UPDATE there
	// would still succeed. With the fix both writes run in one transaction,
	// so the failed INSERT rolls the UPDATE back too.
	if _, err := s.DB().Exec(`DROP TABLE orch_tx_state_changes`); err != nil {
		t.Fatalf("drop audit table to inject partial write failure: %v", err)
	}

	if _, err := orch.ReleaseTx(tx.ID, holder); err == nil {
		t.Fatal("expected ReleaseTx to fail when the audit record cannot be persisted")
	}

	// The durable status must still be committed (never released) and no
	// released audit record may exist, proving status and audit stayed
	// consistent on failure.
	persisted, err := s.GetOrchTx(tx.ID)
	if err != nil {
		t.Fatalf("GetOrchTx failed: %v", err)
	}
	if persisted.Status != model.TxStatusCommitted {
		t.Fatalf("status should stay committed when audit write failed, got %s", persisted.Status)
	}

	// Recreate the audit table so ListTxStateChanges can be queried: there
	// must be no leaked released record.
	if _, err := s.DB().Exec(`
		CREATE TABLE IF NOT EXISTS orch_tx_state_changes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tx_id TEXT NOT NULL,
			from_state TEXT NOT NULL,
			to_state TEXT NOT NULL,
			reason TEXT DEFAULT '',
			created_at DATETIME NOT NULL
		)`); err != nil {
		t.Fatalf("recreate audit table failed: %v", err)
	}
	changes, err := s.ListTxStateChanges(tx.ID)
	if err != nil {
		t.Fatalf("ListTxStateChanges failed: %v", err)
	}
	for _, c := range changes {
		if c.ToState == model.TxStatusReleased {
			t.Fatalf("found leaked released state-change record, audit trail is inconsistent: %+v", c)
		}
	}
}

