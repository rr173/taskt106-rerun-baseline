package lockbudget

import (
	"os"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func setupTestManager(t *testing.T) (*Manager, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "lockbudget_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	s, err := storage.New(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("failed to create storage: %v", err)
	}

	m := NewManager(s)
	if err := m.Start(); err != nil {
		s.Close()
		os.Remove(tmpPath)
		t.Fatalf("failed to start manager: %v", err)
	}

	cleanup := func() {
		m.Stop()
		s.Close()
		os.Remove(tmpPath)
	}

	return m, cleanup
}

func TestSetAndGetConfig(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	cfg, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}
	if cfg.CallerID != "caller-a" {
		t.Errorf("expected caller-a, got %s", cfg.CallerID)
	}
	if cfg.BudgetLimit != 500 {
		t.Errorf("expected limit 500, got %d", cfg.BudgetLimit)
	}
	if cfg.PeriodSec != 60 {
		t.Errorf("expected period 60, got %d", cfg.PeriodSec)
	}
	if cfg.WarningPct != 80 {
		t.Errorf("expected warning 80, got %d", cfg.WarningPct)
	}

	got, err := m.GetConfig("caller-a")
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected config, got nil")
	}
	if got.BudgetLimit != 500 {
		t.Errorf("expected limit 500, got %d", got.BudgetLimit)
	}
}

func TestListConfigs(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	configs, err := m.ListConfigs()
	if err != nil {
		t.Fatalf("ListConfigs failed: %v", err)
	}
	if len(configs) != 2 {
		t.Errorf("expected 2 configs, got %d", len(configs))
	}
}

func TestDeleteConfig(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	if err := m.DeleteConfig("caller-a"); err != nil {
		t.Fatalf("DeleteConfig failed: %v", err)
	}

	got, err := m.GetConfig("caller-a")
	if err != nil {
		t.Fatalf("GetConfig after delete failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil config after delete, got %+v", got)
	}
}

func TestCheckAcquireNoBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	result, err := m.CheckAcquire("no-budget-caller", "lock-1", 60)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if !result.Allowed {
		t.Errorf("expected allowed for caller without budget, got rejected: %s", result.Reason)
	}
}

func TestCheckAcquireWithinBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	result, err := m.CheckAcquire("caller-a", "lock-1", 60)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if !result.Allowed {
		t.Errorf("expected allowed within budget, got rejected: %s", result.Reason)
	}
	if result.BudgetLimit != 500 {
		t.Errorf("expected budget limit 500, got %d", result.BudgetLimit)
	}
	if result.ConsumedUnits != 0 {
		t.Errorf("expected 0 consumed units at start, got %d", result.ConsumedUnits)
	}
}

func TestHoldingAndReleaseMetering(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	acquiredAt := now
	expiresAt := now.Add(30 * time.Second)

	if err := m.StartHolding("caller-a", "lock-1", acquiredAt, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	status, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status == nil {
		t.Fatal("expected status, got nil")
	}
	if status.ActiveLocks != 1 {
		t.Errorf("expected 1 active lock, got %d", status.ActiveLocks)
	}

	time.Sleep(2500 * time.Millisecond)

	releasedAt := time.Now()
	totalUnits, err := m.StopHolding("caller-a", "lock-1", releasedAt)
	if err != nil {
		t.Fatalf("StopHolding failed: %v", err)
	}
	if totalUnits < 2 {
		t.Errorf("expected at least 2 units after 2.5s hold, got %d", totalUnits)
	}

	status, err = m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after release failed: %v", err)
	}
	if status.ConsumedUnits < 2 {
		t.Errorf("expected at least 2 consumed units, got %d", status.ConsumedUnits)
	}
	if status.RemainingUnits > 498 {
		t.Errorf("expected remaining <= 498, got %d", status.RemainingUnits)
	}
}

func TestConcurrentHoldingMetering(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)

	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-1 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-2 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-3", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-3 failed: %v", err)
	}

	status, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.ActiveLocks != 3 {
		t.Errorf("expected 3 active locks, got %d", status.ActiveLocks)
	}

	time.Sleep(2500 * time.Millisecond)

	status, err = m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after wait failed: %v", err)
	}

	expectedMinConsumed := 3 * 2
	if status.ConsumedUnits < expectedMinConsumed {
		t.Errorf("expected at least %d consumed units (3 locks x 2+ sec), got %d",
			expectedMinConsumed, status.ConsumedUnits)
	}
}

func TestBudgetExhaustion(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 5, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(6500 * time.Millisecond)

	result, err := m.CheckAcquire("caller-a", "lock-2", 30)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if result.Allowed {
		t.Errorf("expected reject after budget exhaustion, got allowed. consumed=%d limit=%d",
			result.ConsumedUnits, result.BudgetLimit)
	}
	if !result.BudgetRejected() {
		t.Error("expected BudgetRejected true")
	}

	events, err := m.ListExhaustEvents("caller-a", 10)
	if err != nil {
		t.Fatalf("ListExhaustEvents failed: %v", err)
	}
	if len(events) < 1 {
		t.Error("expected at least 1 exhaust event, got 0")
	}
}

func TestRenewHolding(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(10 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	newExpiresAt := now.Add(60 * time.Second)
	if err := m.RenewHolding("caller-a", "lock-1", newExpiresAt); err != nil {
		t.Fatalf("RenewHolding failed: %v", err)
	}

	info, err := m.GetCallerStatus("caller-a")
	if err != nil {
		t.Fatalf("GetCallerStatus failed: %v", err)
	}
	found := false
	for _, h := range info.HeldLocks {
		if h.LockName == "lock-1" {
			found = true
			if !h.ExpiresAt.Equal(newExpiresAt) {
				t.Errorf("expected expires at %v, got %v", newExpiresAt, h.ExpiresAt)
			}
			break
		}
	}
	if !found {
		t.Error("lock-1 not found in held locks after renew")
	}
}

func TestListStatuses(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	statuses, err := m.ListStatuses()
	if err != nil {
		t.Fatalf("ListStatuses failed: %v", err)
	}
	if len(statuses) != 2 {
		t.Errorf("expected 2 statuses, got %d", len(statuses))
	}
}

func TestListHeldLocks(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-1 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-2 failed: %v", err)
	}

	holdings, err := m.ListHeldLocks("caller-a", time.Now())
	if err != nil {
		t.Fatalf("ListHeldLocks failed: %v", err)
	}
	if len(holdings) != 2 {
		t.Errorf("expected 2 held locks, got %d", len(holdings))
	}
}

func TestGlobalStats(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	stats, err := m.GetGlobalStats()
	if err != nil {
		t.Fatalf("GetGlobalStats failed: %v", err)
	}
	if stats.TotalCallers != 2 {
		t.Errorf("expected 2 total callers, got %d", stats.TotalCallers)
	}
	if stats.TotalActiveLocks != 1 {
		t.Errorf("expected 1 total active locks, got %d", stats.TotalActiveLocks)
	}
}

func TestHasBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	if m.HasBudget("caller-unknown") {
		t.Error("expected false for unknown caller")
	}

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	if !m.HasBudget("caller-a") {
		t.Error("expected true for configured caller")
	}
}

func TestPeriodReset(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 100, 2, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	status1, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus first period failed: %v", err)
	}
	if status1.ConsumedUnits < 1 {
		t.Errorf("expected >= 1 consumed units in first period, got %d", status1.ConsumedUnits)
	}

	time.Sleep(2100 * time.Millisecond)
	status2, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after reset failed: %v", err)
	}
	if !status2.PeriodStartAt.After(status1.PeriodStartAt) {
		t.Errorf("expected new period start after old, old=%v new=%v",
			status1.PeriodStartAt, status2.PeriodStartAt)
	}
}

func TestBudgetAcquireCheckResultMethods(t *testing.T) {
	result := &model.BudgetAcquireCheckResult{
		Allowed:        true,
		ConsumedUnits:  100,
		RemainingUnits: 400,
		BudgetLimit:    500,
		Reason:         "ok",
	}
	if result.BudgetRejected() {
		t.Error("expected BudgetRejected false for allowed result")
	}

	result2 := &model.BudgetAcquireCheckResult{
		Allowed:        false,
		ConsumedUnits:  500,
		RemainingUnits: 0,
		BudgetLimit:    500,
		Reason:         "exhausted",
	}
	if !result2.BudgetRejected() {
		t.Error("expected BudgetRejected true for rejected result")
	}
}

func TestSetConfigWithOverdraft(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	cfg, err := m.SetConfigWithOverdraft("caller-overdraft", 100, 60, 80, 50)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}
	if cfg.OverdraftLimit != 50 {
		t.Errorf("expected overdraft limit 50, got %d", cfg.OverdraftLimit)
	}

	got, err := m.GetConfig("caller-overdraft")
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if got.OverdraftLimit != 50 {
		t.Errorf("expected overdraft limit 50 in config, got %d", got.OverdraftLimit)
	}
}

func TestTransferBudgetBasic(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(10 * time.Second)
	if err := m.StartHolding("caller-a", "lock-a1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-a failed: %v", err)
	}

	time.Sleep(2500 * time.Millisecond)

	statusA, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus caller-a before transfer failed: %v", err)
	}
	remainingBefore := statusA.RemainingUnits
	t.Logf("caller-a before transfer: consumed=%d remaining=%d", statusA.ConsumedUnits, remainingBefore)

	transferAmount := 30
	record, err := m.TransferBudget("caller-a", "caller-b", transferAmount, "test transfer")
	if err != nil {
		t.Fatalf("TransferBudget failed: %v", err)
	}
	if record.FromCaller != "caller-a" {
		t.Errorf("expected from caller-a, got %s", record.FromCaller)
	}
	if record.ToCaller != "caller-b" {
		t.Errorf("expected to caller-b, got %s", record.ToCaller)
	}
	if record.Amount != transferAmount {
		t.Errorf("expected amount %d, got %d", transferAmount, record.Amount)
	}

	statusA, err = m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus caller-a after transfer failed: %v", err)
	}
	expectedConsumedA := 100 - remainingBefore + transferAmount
	if statusA.ConsumedUnits != expectedConsumedA {
		t.Errorf("caller-a expected consumed=%d, got %d", expectedConsumedA, statusA.ConsumedUnits)
	}
	if statusA.TransferredOut != transferAmount {
		t.Errorf("caller-a expected transferred_out=%d, got %d", transferAmount, statusA.TransferredOut)
	}

	statusB, err := m.GetStatus("caller-b")
	if err != nil {
		t.Fatalf("GetStatus caller-b after transfer failed: %v", err)
	}
	if statusB.TransferredIn != transferAmount {
		t.Errorf("caller-b expected transferred_in=%d, got %d", transferAmount, statusB.TransferredIn)
	}
	if statusB.RemainingUnits < transferAmount {
		t.Errorf("caller-b expected remaining >= %d, got %d", transferAmount, statusB.RemainingUnits)
	}
}

func TestTransferBudgetValidation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 50, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 50, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	_, err = m.TransferBudget("", "caller-b", 10, "test")
	if err == nil {
		t.Error("expected error for empty from_caller")
	}

	_, err = m.TransferBudget("caller-a", "", 10, "test")
	if err == nil {
		t.Error("expected error for empty to_caller")
	}

	_, err = m.TransferBudget("caller-a", "caller-a", 10, "test")
	if err == nil {
		t.Error("expected error for self transfer")
	}

	_, err = m.TransferBudget("caller-a", "caller-b", 0, "test")
	if err == nil {
		t.Error("expected error for zero amount")
	}

	_, err = m.TransferBudget("caller-a", "caller-b", -5, "test")
	if err == nil {
		t.Error("expected error for negative amount")
	}

	_, err = m.TransferBudget("caller-unknown", "caller-b", 10, "test")
	if err == nil {
		t.Error("expected error for unknown from caller")
	}

	_, err = m.TransferBudget("caller-a", "caller-unknown", 10, "test")
	if err == nil {
		t.Error("expected error for unknown to caller")
	}

	_, err = m.TransferBudget("caller-a", "caller-b", 200, "test")
	if err == nil {
		t.Error("expected error for insufficient remaining budget")
	}
}

func TestOverdraftWithinLimit(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-overdraft", 10, 60, 80, 20)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}

	result1, err := m.CheckAcquire("caller-overdraft", "lock-1", 60)
	if err != nil {
		t.Fatalf("CheckAcquire within budget failed: %v", err)
	}
	if !result1.Allowed {
		t.Errorf("expected allowed within budget, got rejected: %s", result1.Reason)
	}
	if result1.UsingOverdraft {
		t.Error("expected not using overdraft within budget")
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-overdraft", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(12500 * time.Millisecond)

	result2, err := m.CheckAcquire("caller-overdraft", "lock-2", 60)
	if err != nil {
		t.Fatalf("CheckAcquire in overdraft failed: %v", err)
	}
	if !result2.Allowed {
		t.Errorf("expected allowed in overdraft (within limit), got rejected: %s", result2.Reason)
	}
	if !result2.UsingOverdraft {
		t.Error("expected UsingOverdraft true")
	}
	if result2.CurrentOverdraft <= 0 {
		t.Errorf("expected current overdraft > 0, got %d", result2.CurrentOverdraft)
	}
	t.Logf("Overdraft state: consumed=%d budget=%d overdraft=%d penalty_multiplier=%.1f",
		result2.ConsumedUnits, result2.BudgetLimit, result2.CurrentOverdraft, model.OverdraftPenaltyMultiplier)
}

func TestOverdraftExhaustion(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-overdraft", 5, 60, 80, 5)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-overdraft", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(12500 * time.Millisecond)

	result, err := m.CheckAcquire("caller-overdraft", "lock-2", 60)
	if err != nil {
		t.Fatalf("CheckAcquire after overdraft exhausted failed: %v", err)
	}
	if result.Allowed {
		t.Errorf("expected rejected when budget+overdraft exhausted, got allowed. consumed=%d budget=%d overdraft_limit=%d",
			result.ConsumedUnits, result.BudgetLimit, result.OverdraftLimit)
	}
}

func TestOverdraftPenaltyCalculation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-penalty", 5, 60, 80, 20)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-penalty", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(10500 * time.Millisecond)

	status, err := m.GetStatus("caller-penalty")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status.CurrentOverdraft <= 0 {
		t.Errorf("expected current overdraft > 0, got %d", status.CurrentOverdraft)
	}
	if status.OverdraftPenaltyUnits <= 0 {
		t.Errorf("expected overdraft penalty > 0, got %d", status.OverdraftPenaltyUnits)
	}

	expectedPenaltyMin := status.CurrentOverdraft / 2
	if status.OverdraftPenaltyUnits < expectedPenaltyMin {
		t.Errorf("expected penalty >= %d (50%% of overdraft %d), got %d",
			expectedPenaltyMin, status.CurrentOverdraft, status.OverdraftPenaltyUnits)
	}

	totalCharged := status.ConsumedUnits
	normalBudget := status.BudgetLimit
	overdraftWithPenalty := totalCharged - normalBudget
	expectedOverdraftBase := overdraftWithPenalty - status.OverdraftPenaltyUnits

	t.Logf("Penalty breakdown: total_consumed=%d budget=%d overdraft_used=%d penalty=%d (multiplier=%.1f)",
		totalCharged, normalBudget, expectedOverdraftBase, status.OverdraftPenaltyUnits, model.OverdraftPenaltyMultiplier)
}

func TestPeriodResetWithOverdraft(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-reset", 10, 4, 80, 20)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(120 * time.Second)
	if err := m.StartHolding("caller-reset", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(2500 * time.Millisecond)

	status0, err := m.GetStatus("caller-reset")
	if err != nil {
		t.Fatalf("GetStatus mid-period failed: %v", err)
	}
	t.Logf("Mid first period: consumed=%d overdraft=%d penalty=%d",
		status0.ConsumedUnits, status0.CurrentOverdraft, status0.OverdraftPenaltyUnits)

	if status0.ConsumedUnits < 2 {
		t.Errorf("expected consumed >= 2 in mid-period, got %d", status0.ConsumedUnits)
	}

	time.Sleep(3000 * time.Millisecond)

	status1, err := m.GetStatus("caller-reset")
	if err != nil {
		t.Fatalf("GetStatus first period end failed: %v", err)
	}
	t.Logf("After first period reset: period_start=%v period_end=%v consumed=%d overdraft=%d penalty=%d next_period_deduction=%d carry_over=%d",
		status1.PeriodStartAt, status1.PeriodEndAt,
		status1.ConsumedUnits, status1.CurrentOverdraft, status1.OverdraftPenaltyUnits,
		status1.NextPeriodDeduction, status0.CurrentOverdraft+status0.OverdraftPenaltyUnits)

	carryOverExpected := status0.CurrentOverdraft + status0.OverdraftPenaltyUnits
	if carryOverExpected > 0 {
		if status1.ConsumedUnits < carryOverExpected {
			t.Errorf("expected consumed >= carryover %d after reset, got %d",
				carryOverExpected, status1.ConsumedUnits)
		}
		if !status1.PeriodStartAt.After(status0.PeriodStartAt) {
			t.Errorf("expected new period start after first. old=%v new=%v",
				status0.PeriodStartAt, status1.PeriodStartAt)
		}
	}

	deductionNow := status1.CurrentOverdraft + status1.OverdraftPenaltyUnits
	if deductionNow != status1.NextPeriodDeduction {
		t.Errorf("NextPeriodDeduction should be overdraft+penalty. expected %d, got %d",
			deductionNow, status1.NextPeriodDeduction)
	}

	time.Sleep(5000 * time.Millisecond)

	status2, err := m.GetStatus("caller-reset")
	if err != nil {
		t.Fatalf("GetStatus after second period failed: %v", err)
	}
	t.Logf("After second period reset: period_start=%v period_end=%v consumed=%d",
		status2.PeriodStartAt, status2.PeriodEndAt, status2.ConsumedUnits)

	if !status2.PeriodStartAt.After(status1.PeriodStartAt) {
		t.Errorf("expected second period reset to happen. old_start=%v new_start=%v",
			status1.PeriodStartAt, status2.PeriodStartAt)
	}
}

func TestListTransferRecords(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 200, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 200, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}
	_, err = m.SetConfig("caller-c", 200, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-c failed: %v", err)
	}

	_, err = m.TransferBudget("caller-a", "caller-b", 30, "first transfer")
	if err != nil {
		t.Fatalf("TransferBudget a->b failed: %v", err)
	}
	_, err = m.TransferBudget("caller-a", "caller-c", 20, "second transfer")
	if err != nil {
		t.Fatalf("TransferBudget a->c failed: %v", err)
	}
	_, err = m.TransferBudget("caller-b", "caller-c", 10, "third transfer")
	if err != nil {
		t.Fatalf("TransferBudget b->c failed: %v", err)
	}

	result, err := m.ListTransferRecords(nil)
	if err != nil {
		t.Fatalf("ListTransferRecords (all) failed: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("expected 3 total transfer records, got %d", result.Total)
	}
	if len(result.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result.Items))
	}

	resultA, err := m.ListTransferRecords(&model.BudgetTransferListQuery{FromCaller: "caller-a", Limit: 10})
	if err != nil {
		t.Fatalf("ListTransferRecords from caller-a failed: %v", err)
	}
	if resultA.Total != 2 {
		t.Errorf("expected 2 transfers from caller-a, got %d", resultA.Total)
	}

	resultToC, err := m.ListTransferRecords(&model.BudgetTransferListQuery{ToCaller: "caller-c", Limit: 10})
	if err != nil {
		t.Fatalf("ListTransferRecords to caller-c failed: %v", err)
	}
	if resultToC.Total != 2 {
		t.Errorf("expected 2 transfers to caller-c, got %d", resultToC.Total)
	}

	resultPaged, err := m.ListTransferRecords(&model.BudgetTransferListQuery{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatalf("ListTransferRecords paged failed: %v", err)
	}
	if resultPaged.Total != 3 {
		t.Errorf("expected total=3 for paged query, got %d", resultPaged.Total)
	}
	if len(resultPaged.Items) != 2 {
		t.Errorf("expected 2 items on first page, got %d", len(resultPaged.Items))
	}
}

func TestListOverdraftCallers(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-od-1", 5, 60, 80, 30)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft caller-od-1 failed: %v", err)
	}
	_, err = m.SetConfigWithOverdraft("caller-od-2", 5, 60, 80, 30)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft caller-od-2 failed: %v", err)
	}
	_, err = m.SetConfig("caller-normal", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-normal failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-od-1", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-od-1 failed: %v", err)
	}
	if err := m.StartHolding("caller-od-2", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-od-2 failed: %v", err)
	}
	if err := m.StartHolding("caller-normal", "lock-3", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-normal failed: %v", err)
	}

	time.Sleep(9500 * time.Millisecond)

	result, err := m.ListOverdraftCallers()
	if err != nil {
		t.Fatalf("ListOverdraftCallers failed: %v", err)
	}

	t.Logf("Overdraft callers: total=%d total_amount=%d", result.TotalInOverdraft, result.TotalOverdraftAmount)
	for _, item := range result.Items {
		t.Logf("  - %s: overdraft=%d limit=%d penalty=%d next_deduction=%d",
			item.CallerID, item.CurrentOverdraft, item.OverdraftLimit,
			item.OverdraftPenaltyUnits, item.NextPeriodDeduction)
	}

	if result.TotalInOverdraft < 2 {
		t.Errorf("expected at least 2 callers in overdraft, got %d", result.TotalInOverdraft)
	}
	if result.TotalOverdraftAmount <= 0 {
		t.Errorf("expected total overdraft amount > 0, got %d", result.TotalOverdraftAmount)
	}

	for _, item := range result.Items {
		if item.CallerID != "caller-od-1" && item.CallerID != "caller-od-2" {
			continue
		}
		if item.CurrentOverdraft <= 0 {
			t.Errorf("%s: expected overdraft > 0, got %d", item.CallerID, item.CurrentOverdraft)
		}
		if !item.InOverdraft {
			t.Errorf("%s: expected InOverdraft true", item.CallerID)
		}
		if item.NextPeriodDeduction <= 0 {
			t.Errorf("%s: expected next period deduction > 0, got %d", item.CallerID, item.NextPeriodDeduction)
		}
		remainingLimit := item.OverdraftLimit - item.CurrentOverdraft
		if remainingLimit != item.OverdraftRemaining {
			t.Errorf("%s: overdraft remaining mismatch: expected %d, got %d",
				item.CallerID, remainingLimit, item.OverdraftRemaining)
		}
	}
}

func TestGetNextPeriodDeduction(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-deduct", 8, 60, 80, 25)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}
	_, err = m.SetConfig("caller-clean", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-clean failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-deduct", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-deduct failed: %v", err)
	}

	time.Sleep(12500 * time.Millisecond)

	deductInfo, err := m.GetNextPeriodDeduction("caller-deduct")
	if err != nil {
		t.Fatalf("GetNextPeriodDeduction failed: %v", err)
	}

	t.Logf("caller-deduct: deduction=%d (overdraft=%d penalty=%d) projected_remaining=%d",
		deductInfo.NextPeriodDeduction, deductInfo.CurrentOverdraft,
		deductInfo.OverdraftPenaltyUnits, deductInfo.ProjectedRemaining)

	expectedDeduction := deductInfo.CurrentOverdraft + deductInfo.OverdraftPenaltyUnits
	if deductInfo.NextPeriodDeduction != expectedDeduction {
		t.Errorf("deduction mismatch: expected %d, got %d", expectedDeduction, deductInfo.NextPeriodDeduction)
	}
	if deductInfo.BudgetLimit != 8 {
		t.Errorf("expected budget limit 8, got %d", deductInfo.BudgetLimit)
	}
	if deductInfo.ProjectedRemaining < 0 {
		t.Errorf("projected remaining should not be negative, got %d", deductInfo.ProjectedRemaining)
	}

	cleanInfo, err := m.GetNextPeriodDeduction("caller-clean")
	if err != nil {
		t.Fatalf("GetNextPeriodDeduction caller-clean failed: %v", err)
	}
	if cleanInfo.NextPeriodDeduction != 0 {
		t.Errorf("caller-clean expected deduction=0, got %d", cleanInfo.NextPeriodDeduction)
	}

	_, err = m.GetNextPeriodDeduction("caller-unknown")
	if err == nil {
		t.Error("expected error for unknown caller")
	}
}

func TestListAllNextPeriodDeductions(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-od-a", 5, 60, 80, 20)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft caller-od-a failed: %v", err)
	}
	_, err = m.SetConfigWithOverdraft("caller-od-b", 5, 60, 80, 20)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft caller-od-b failed: %v", err)
	}
	_, err = m.SetConfig("caller-no-deduct", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-no-deduct failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-od-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-od-a failed: %v", err)
	}
	if err := m.StartHolding("caller-od-b", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding caller-od-b failed: %v", err)
	}

	time.Sleep(10500 * time.Millisecond)

	deductions, err := m.ListAllNextPeriodDeductions()
	if err != nil {
		t.Fatalf("ListAllNextPeriodDeductions failed: %v", err)
	}

	t.Logf("Total callers with deductions: %d", len(deductions))
	for _, d := range deductions {
		t.Logf("  - %s: deduction=%d (od=%d penalty=%d)",
			d.CallerID, d.NextPeriodDeduction, d.CurrentOverdraft, d.OverdraftPenaltyUnits)
	}

	if len(deductions) < 2 {
		t.Errorf("expected at least 2 callers with deductions, got %d", len(deductions))
	}

	for _, d := range deductions {
		if d.CallerID == "caller-no-deduct" {
			t.Error("caller-no-deduct should not appear in deduction list")
		}
		if d.NextPeriodDeduction <= 0 {
			t.Errorf("%s: expected deduction > 0, got %d", d.CallerID, d.NextPeriodDeduction)
		}
	}
}

func TestStatusIncludesNewFields(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-status", 100, 60, 80, 50)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}

	status, err := m.GetStatus("caller-status")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status.OverdraftLimit != 50 {
		t.Errorf("expected OverdraftLimit=50, got %d", status.OverdraftLimit)
	}
	if status.CurrentOverdraft != 0 {
		t.Errorf("expected CurrentOverdraft=0 initially, got %d", status.CurrentOverdraft)
	}
	if status.InOverdraft {
		t.Error("expected InOverdraft=false initially")
	}
	if status.OverdraftPenaltyUnits != 0 {
		t.Errorf("expected OverdraftPenaltyUnits=0 initially, got %d", status.OverdraftPenaltyUnits)
	}
	if status.NextPeriodDeduction != 0 {
		t.Errorf("expected NextPeriodDeduction=0 initially, got %d", status.NextPeriodDeduction)
	}
	if status.TransferredIn != 0 {
		t.Errorf("expected TransferredIn=0 initially, got %d", status.TransferredIn)
	}
	if status.TransferredOut != 0 {
		t.Errorf("expected TransferredOut=0 initially, got %d", status.TransferredOut)
	}
}

func TestGlobalStatsIncludesOverdraft(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfigWithOverdraft("caller-stats-od", 5, 60, 80, 30)
	if err != nil {
		t.Fatalf("SetConfigWithOverdraft failed: %v", err)
	}
	_, err = m.SetConfig("caller-stats-normal", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-stats-od", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(9500 * time.Millisecond)

	stats, err := m.GetGlobalStats()
	if err != nil {
		t.Fatalf("GetGlobalStats failed: %v", err)
	}

	if stats.TotalCallers != 2 {
		t.Errorf("expected TotalCallers=2, got %d", stats.TotalCallers)
	}
	if stats.CallersInOverdraft < 1 {
		t.Errorf("expected CallersInOverdraft >= 1, got %d", stats.CallersInOverdraft)
	}
	if stats.TotalOverdraftAmount <= 0 {
		t.Errorf("expected TotalOverdraftAmount > 0, got %d", stats.TotalOverdraftAmount)
	}

	t.Logf("Global stats: callers=%d in_overdraft=%d total_overdraft_amount=%d",
		stats.TotalCallers, stats.CallersInOverdraft, stats.TotalOverdraftAmount)
}

func TestOverdraftMultiplierConstant(t *testing.T) {
	if model.OverdraftPenaltyMultiplier != 1.5 {
		t.Errorf("expected OverdraftPenaltyMultiplier=1.5, got %.2f", model.OverdraftPenaltyMultiplier)
	}
}
