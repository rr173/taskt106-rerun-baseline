package lockbudget

import (
	"fmt"
	"log"
	"math"
	"task106/internal/model"
	"task106/internal/ratealert"
	"task106/internal/storage"
	"sync"
	"time"
)

type callerRuntimeState struct {
	config              *model.LockBudgetConfig
	periodStartAt       time.Time
	periodEndAt         time.Time
	consumedUnits       int
	peakConcurrent      int
	lockCount           int
	exhaustCount        int
	holdings            map[string]*holdingState

	currentOverdraft    int
	peakOverdraft       int
	hadOverdraft        bool
	overdraftPenalty    int
	carryOverDeduction  int
	transferredIn       int
	transferredOut      int
}

type holdingState struct {
	lockName        string
	acquiredAt      time.Time
	expiresAt       time.Time
	lastMeteredAt   time.Time
	unitsAccrued    int
	inOverdraft     bool
}

type ReputationChecker interface {
	GetEffectiveOverdraftLimit(callerID string, configOverdraftLimit int) int
	CanTransferBudget(callerID string) bool
}

type Manager struct {
	storage          *storage.Storage
	mu               sync.Mutex
	callers          map[string]*callerRuntimeState
	stopCh           chan struct{}
	ticker           *time.Ticker
	dirty            bool
	rateAlertMgr     *ratealert.Manager
	reputationChecker ReputationChecker
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage: s,
		callers: make(map[string]*callerRuntimeState),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) SetRateAlertManager(ram *ratealert.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rateAlertMgr = ram
}

func (m *Manager) SetReputationChecker(rc ReputationChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reputationChecker = rc
}

func (m *Manager) RateAlertManager() *ratealert.Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rateAlertMgr
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadConfigsLocked(); err != nil {
		return fmt.Errorf("load budget configs: %w", err)
	}
	if err := m.loadHoldingsLocked(); err != nil {
		return fmt.Errorf("load budget holdings: %w", err)
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.meterLoop()

	log.Println("[lockbudget-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for callerID, rt := range m.callers {
		arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
		m.finalizeCurrentPeriodLocked(callerID, rt, now, arrears != nil)
	}

	log.Println("[lockbudget-manager] stopped")
}

func (m *Manager) loadConfigsLocked() error {
	configs, err := m.storage.ListLockBudgetConfigs()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range configs {
		cfg := &configs[i]
		rt := &callerRuntimeState{
			config:        cfg,
			periodStartAt: now,
			periodEndAt:   now.Add(time.Duration(cfg.PeriodSec) * time.Second),
			holdings:      make(map[string]*holdingState),
		}
		m.callers[cfg.CallerID] = rt
	}
	return nil
}

func (m *Manager) loadHoldingsLocked() error {
	records, err := m.storage.ListAllBudgetHoldings()
	if err != nil {
		return err
	}
	now := time.Now()
	for _, r := range records {
		rt, ok := m.callers[r.CallerID]
		if !ok {
			continue
		}
		if r.ExpiresAt.Before(now) {
			continue
		}
		h := &holdingState{
			lockName:      r.LockName,
			acquiredAt:    r.AcquiredAt,
			expiresAt:     r.ExpiresAt,
			lastMeteredAt: r.LastMeteredAt,
			unitsAccrued:  r.UnitsAccrued,
		}
		rt.holdings[r.LockName] = h
		if h.lastMeteredAt.After(rt.periodStartAt) || h.lastMeteredAt.Equal(rt.periodStartAt) {
			rt.consumedUnits += h.unitsAccrued
		}
		rt.lockCount++
	}
	return nil
}

func (m *Manager) meterLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.meterTick()
		}
	}
}

func (m *Manager) meterTick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.dirty = false

	for callerID, rt := range m.callers {
		arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
		m.checkPeriodResetLocked(callerID, rt, now, arrears != nil)
		m.meterCallerLocked(callerID, rt, now, arrears != nil)
	}

	if m.dirty {
		m.persistDirtyLocked(now)
	}
}

func (m *Manager) checkPeriodResetLocked(callerID string, rt *callerRuntimeState, now time.Time, inArrears bool) {
	if now.Before(rt.periodEndAt) {
		return
	}

	m.finalizeCurrentPeriodLocked(callerID, rt, rt.periodEndAt, inArrears)

	elapsedSec := int(now.Sub(rt.periodEndAt).Seconds())
	periodsToAdvance := 1
	if elapsedSec > rt.config.PeriodSec {
		periodsToAdvance = elapsedSec / rt.config.PeriodSec
		if elapsedSec%rt.config.PeriodSec > 0 {
			periodsToAdvance++
		}
	}

	rt.periodStartAt = rt.periodEndAt
	for i := 1; i < periodsToAdvance; i++ {
		periodStart := rt.periodStartAt
		periodEnd := periodStart.Add(time.Duration(rt.config.PeriodSec) * time.Second)

		summary := &model.BudgetPeriodSummary{
			CallerID:       callerID,
			PeriodStartAt:  periodStart,
			PeriodEndAt:    periodEnd,
			BudgetLimit:    rt.config.BudgetLimit,
			OverdraftLimit: rt.config.OverdraftLimit,
			TotalConsumed:  0,
			PeakConcurrent: 0,
			LockCount:      0,
			ExhaustEvents:  0,
		}
		if i == periodsToAdvance-1 {
			summary.CarryOverDeduction = rt.currentOverdraft + rt.overdraftPenalty
		}
		_ = m.storage.UpsertBudgetPeriodSummary(summary)
		rt.periodStartAt = periodEnd
	}

	rt.periodEndAt = rt.periodStartAt.Add(time.Duration(rt.config.PeriodSec) * time.Second)

	if inArrears {
		rt.carryOverDeduction = 0
		rt.consumedUnits = 0
	} else {
		rt.carryOverDeduction = rt.currentOverdraft + rt.overdraftPenalty
		rt.consumedUnits = rt.carryOverDeduction
	}
	rt.currentOverdraft = 0
	rt.peakOverdraft = 0
	rt.hadOverdraft = false
	rt.overdraftPenalty = 0
	rt.peakConcurrent = 0
	rt.lockCount = len(rt.holdings)
	rt.exhaustCount = 0
	rt.transferredIn = 0
	rt.transferredOut = 0

	for _, h := range rt.holdings {
		h.lastMeteredAt = now
		h.unitsAccrued = 0
		h.inOverdraft = rt.consumedUnits > rt.config.BudgetLimit
	}

	log.Printf("[lockbudget] period reset: caller=%s period_start=%v period_end=%v carry_over_deduction=%d in_arrears=%v",
		callerID, rt.periodStartAt.Format(time.RFC3339), rt.periodEndAt.Format(time.RFC3339), rt.carryOverDeduction, inArrears)
}

func (m *Manager) finalizeCurrentPeriodLocked(callerID string, rt *callerRuntimeState, endTime time.Time, inArrears bool) {
	if inArrears {
		for _, h := range rt.holdings {
			if h.lastMeteredAt.Before(endTime) {
				h.lastMeteredAt = endTime
				_ = m.storage.UpdateBudgetHoldingMeter(callerID, h.lockName, h.lastMeteredAt, h.unitsAccrued)
			}
		}

		summary := &model.BudgetPeriodSummary{
			CallerID:           callerID,
			PeriodStartAt:      rt.periodStartAt,
			PeriodEndAt:        endTime,
			BudgetLimit:        rt.config.BudgetLimit,
			OverdraftLimit:     rt.config.OverdraftLimit,
			TotalConsumed:      0,
			TransferredIn:      rt.transferredIn,
			TransferredOut:     rt.transferredOut,
			PeakConcurrent:     rt.peakConcurrent,
			LockCount:          rt.lockCount,
			ExhaustEvents:      rt.exhaustCount,
		}
		_ = m.storage.UpsertBudgetPeriodSummary(summary)

		log.Printf("[lockbudget] finalize skipped (in arrears): caller=%s period=%v~%v",
			callerID, rt.periodStartAt.Format(time.RFC3339), endTime.Format(time.RFC3339))
		return
	}

	for _, h := range rt.holdings {
		if h.lastMeteredAt.Before(endTime) {
			elapsed := endTime.Sub(h.lastMeteredAt)
			units := int(elapsed.Seconds())
			if units > 0 {
				unitsToAdd, penalty := m.applyOverdraftPenaltyLocked(rt, units)
				rt.consumedUnits += unitsToAdd
				rt.overdraftPenalty += penalty
				h.unitsAccrued += unitsToAdd
				h.lastMeteredAt = endTime
				_ = m.storage.UpdateBudgetHoldingMeter(callerID, h.lockName, h.lastMeteredAt, h.unitsAccrued)
			}
		}
	}

	if rt.lockCount > rt.peakConcurrent {
		rt.peakConcurrent = rt.lockCount
	}

	if rt.currentOverdraft > rt.peakOverdraft {
		rt.peakOverdraft = rt.currentOverdraft
	}

	overdraftUsed := 0
	normalConsumption := rt.consumedUnits
	if rt.consumedUnits > rt.config.BudgetLimit {
		overdraftUsed = rt.consumedUnits - rt.config.BudgetLimit
		normalConsumption = rt.config.BudgetLimit
	}

	summary := &model.BudgetPeriodSummary{
		CallerID:           callerID,
		PeriodStartAt:      rt.periodStartAt,
		PeriodEndAt:        endTime,
		BudgetLimit:        rt.config.BudgetLimit,
		OverdraftLimit:     rt.config.OverdraftLimit,
		TotalConsumed:      rt.consumedUnits,
		OverdraftUsed:      overdraftUsed,
		OverdraftPenalty:   rt.overdraftPenalty,
		TransferredIn:      rt.transferredIn,
		TransferredOut:     rt.transferredOut,
		CarryOverDeduction: rt.currentOverdraft + rt.overdraftPenalty,
		PeakConcurrent:     rt.peakConcurrent,
		LockCount:          rt.lockCount,
		ExhaustEvents:      rt.exhaustCount,
	}
	_ = m.storage.UpsertBudgetPeriodSummary(summary)

	endingBalance := rt.config.BudgetLimit - rt.consumedUnits + rt.transferredIn - rt.transferredOut
	carryOverToNextPeriod := 0
	if endingBalance < 0 {
		carryOverToNextPeriod = -endingBalance
	}

	bill := &model.BudgetSettlementBill{
		CallerID:              callerID,
		PeriodStartAt:         rt.periodStartAt,
		PeriodEndAt:           endTime,
		BudgetLimit:           rt.config.BudgetLimit,
		OverdraftLimit:        rt.config.OverdraftLimit,
		NormalConsumption:     normalConsumption,
		OverdraftConsumption:  overdraftUsed,
		OverdraftPenalty:      rt.overdraftPenalty,
		TotalConsumption:      rt.consumedUnits + rt.overdraftPenalty,
		TransferredIn:         rt.transferredIn,
		TransferredOut:        rt.transferredOut,
		EndingBalance:         endingBalance,
		HadOverdraft:          rt.hadOverdraft,
		PeakOverdraft:         rt.peakOverdraft,
		PeakConcurrent:        rt.peakConcurrent,
		ExhaustEvents:         rt.exhaustCount,
		CarryOverToNextPeriod: carryOverToNextPeriod,
		Status:                model.BudgetBillStatusFinalized,
		CreatedAt:             time.Now(),
	}
	_ = m.storage.CreateBudgetSettlementBill(bill)

	transfers, _ := m.storage.ListBudgetTransfersInPeriod(callerID, rt.periodStartAt, endTime)
	for _, t := range transfers {
		detail := &model.BudgetBillTransferDetail{
			BillID:    bill.ID,
			CallerID:  callerID,
			Amount:    t.Amount,
			Reason:    t.Reason,
			CreatedAt: t.CreatedAt,
		}
		if t.FromCaller == callerID {
			detail.Direction = "out"
			detail.PeerCaller = t.ToCaller
		} else {
			detail.Direction = "in"
			detail.PeerCaller = t.FromCaller
		}
		_ = m.storage.AddBudgetBillTransferDetail(detail)
	}

	if carryOverToNextPeriod > 0 {
		newBudget := rt.config.BudgetLimit
		if newBudget < carryOverToNextPeriod {
			arrearsAmount := carryOverToNextPeriod - newBudget
			arrears := &model.BudgetCallerArrears{
				CallerID:              callerID,
				ArrearsAmount:         arrearsAmount,
				OriginalBillID:        bill.ID,
				OriginalPeriodStartAt: bill.PeriodStartAt,
				OriginalPeriodEndAt:   bill.PeriodEndAt,
				Status:                model.BudgetArrearsStatusActive,
				CreatedAt:             time.Now(),
				UpdatedAt:             time.Now(),
			}
			_ = m.storage.CreateBudgetCallerArrears(arrears)
			log.Printf("[lockbudget] caller in arrears: caller=%s bill_id=%d arrears_amount=%d period=%v~%v",
				callerID, bill.ID, arrearsAmount, bill.PeriodStartAt, bill.PeriodEndAt)
		}
	}

	log.Printf("[lockbudget] settlement bill generated: caller=%s bill_id=%d period=%v~%v total_consumption=%d ending_balance=%d had_overdraft=%v",
		callerID, bill.ID, bill.PeriodStartAt.Format(time.RFC3339), bill.PeriodEndAt.Format(time.RFC3339),
		bill.TotalConsumption, bill.EndingBalance, bill.HadOverdraft)
}

func (m *Manager) applyOverdraftPenaltyLocked(rt *callerRuntimeState, units int) (int, int) {
	if rt.config.OverdraftLimit <= 0 {
		return units, 0
	}

	budgetLimit := rt.config.BudgetLimit
	currentWithNew := rt.consumedUnits + units

	if currentWithNew <= budgetLimit {
		return units, 0
	}

	normalUnits := 0
	if rt.consumedUnits < budgetLimit {
		normalUnits = budgetLimit - rt.consumedUnits
	}
	overdraftUnits := units - normalUnits

	penaltyUnits := int(math.Ceil(float64(overdraftUnits) * (model.OverdraftPenaltyMultiplier - 1.0)))
	totalUnits := normalUnits + overdraftUnits + penaltyUnits

	return totalUnits, penaltyUnits
}

func (m *Manager) meterCallerLocked(callerID string, rt *callerRuntimeState, now time.Time, inArrears bool) {
	concurrentCount := 0
	var toRemove []string

	for lockName, h := range rt.holdings {
		if !h.expiresAt.After(now) {
			toRemove = append(toRemove, lockName)
			continue
		}
		concurrentCount++

		if inArrears {
			h.lastMeteredAt = now
			_ = m.storage.UpdateBudgetHoldingMeter(callerID, lockName, h.lastMeteredAt, h.unitsAccrued)
			continue
		}

		if h.lastMeteredAt.Before(now) {
			elapsed := now.Sub(h.lastMeteredAt)
			units := int(elapsed.Seconds())
			if units > 0 {
				unitsToAdd, penalty := m.applyOverdraftPenaltyLocked(rt, units)
				rt.consumedUnits += unitsToAdd
				rt.overdraftPenalty += penalty
				h.unitsAccrued += unitsToAdd
				h.lastMeteredAt = h.lastMeteredAt.Add(time.Duration(units) * time.Second)

				if rt.consumedUnits > rt.config.BudgetLimit {
					rt.currentOverdraft = rt.consumedUnits - rt.config.BudgetLimit
					h.inOverdraft = true
					rt.hadOverdraft = true
					if rt.currentOverdraft > rt.peakOverdraft {
						rt.peakOverdraft = rt.currentOverdraft
					}
				}

				if m.rateAlertMgr != nil {
					_ = m.rateAlertMgr.RecordConsumption(callerID, unitsToAdd, now)
				}

				_ = m.storage.UpdateBudgetHoldingMeter(callerID, lockName, h.lastMeteredAt, h.unitsAccrued)
				m.dirty = true
			}
		}
	}

	for _, lockName := range toRemove {
		delete(rt.holdings, lockName)
		_ = m.storage.RemoveBudgetHolding(callerID, lockName)
	}

	rt.lockCount = concurrentCount
	if concurrentCount > rt.peakConcurrent {
		rt.peakConcurrent = rt.lockCount
	}
}

func (m *Manager) persistDirtyLocked(now time.Time) {
	_ = now
}

func (m *Manager) refreshCallerLocked(callerID string, rt *callerRuntimeState, now time.Time) {
	arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
	inArrears := arrears != nil
	m.checkPeriodResetLocked(callerID, rt, now, inArrears)
	m.meterCallerLocked(callerID, rt, now, inArrears)
}

func (m *Manager) SetBudget(callerID string, budgetLimit int, periodSec int, warningPct int) (*model.LockBudgetConfig, error) {
	return m.SetBudgetWithOverdraft(callerID, budgetLimit, periodSec, warningPct, 0)
}

func (m *Manager) SetBudgetWithOverdraft(callerID string, budgetLimit int, periodSec int, warningPct int, overdraftLimit int) (*model.LockBudgetConfig, error) {
	if callerID == "" {
		return nil, fmt.Errorf("caller_id is required")
	}
	if budgetLimit <= 0 {
		return nil, fmt.Errorf("budget_limit must be positive")
	}
	if periodSec <= 0 {
		return nil, fmt.Errorf("period_sec must be positive")
	}
	if warningPct < 0 || warningPct > 100 {
		warningPct = 80
	}
	if overdraftLimit < 0 {
		overdraftLimit = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cfg := &model.LockBudgetConfig{
		CallerID:       callerID,
		BudgetLimit:    budgetLimit,
		PeriodSec:      periodSec,
		WarningPct:     warningPct,
		OverdraftLimit: overdraftLimit,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if rt, ok := m.callers[callerID]; ok {
		cfg.ID = rt.config.ID
		arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
		m.finalizeCurrentPeriodLocked(callerID, rt, now, arrears != nil)
		rt.config = cfg
		rt.periodStartAt = now
		rt.periodEndAt = now.Add(time.Duration(periodSec) * time.Second)
		rt.consumedUnits = 0
		rt.currentOverdraft = 0
		rt.overdraftPenalty = 0
		rt.carryOverDeduction = 0
		rt.peakConcurrent = 0
		rt.exhaustCount = 0
		rt.transferredIn = 0
		rt.transferredOut = 0
		for _, h := range rt.holdings {
			h.lastMeteredAt = now
			h.unitsAccrued = 0
			h.inOverdraft = false
		}
	} else {
		rt = &callerRuntimeState{
			config:        cfg,
			periodStartAt: now,
			periodEndAt:   now.Add(time.Duration(periodSec) * time.Second),
			holdings:      make(map[string]*holdingState),
		}
		m.callers[callerID] = rt
	}

	if err := m.storage.UpsertLockBudgetConfig(cfg); err != nil {
		return nil, err
	}

	log.Printf("[lockbudget] budget set: caller=%s limit=%d period=%ds warning=%d%% overdraft=%d",
		callerID, budgetLimit, periodSec, warningPct, overdraftLimit)
	return cfg, nil
}

func (m *Manager) DeleteBudget(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return fmt.Errorf("no budget configured for caller: %s", callerID)
	}

	now := time.Now()
	arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
	m.finalizeCurrentPeriodLocked(callerID, rt, now, arrears != nil)

	for lockName := range rt.holdings {
		_ = m.storage.RemoveBudgetHolding(callerID, lockName)
	}

	delete(m.callers, callerID)
	if err := m.storage.DeleteLockBudgetConfig(callerID); err != nil {
		return err
	}

	log.Printf("[lockbudget] budget deleted: caller=%s", callerID)
	return nil
}

func (m *Manager) TransferBudget(fromCaller, toCaller string, amount int, reason string) (*model.BudgetTransferRecord, error) {
	if fromCaller == "" {
		return nil, fmt.Errorf("from_caller is required")
	}
	if toCaller == "" {
		return nil, fmt.Errorf("to_caller is required")
	}
	if fromCaller == toCaller {
		return nil, fmt.Errorf("cannot transfer to self")
	}
	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reputationChecker != nil && !m.reputationChecker.CanTransferBudget(fromCaller) {
		return nil, fmt.Errorf("铜牌调用方不允许使用预算转移功能: %s", fromCaller)
	}

	now := time.Now()

	fromRT, ok := m.callers[fromCaller]
	if !ok {
		return nil, fmt.Errorf("no budget configured for source caller: %s", fromCaller)
	}
	toRT, ok := m.callers[toCaller]
	if !ok {
		return nil, fmt.Errorf("no budget configured for target caller: %s", toCaller)
	}

	m.refreshCallerLocked(fromCaller, fromRT, now)
	m.refreshCallerLocked(toCaller, toRT, now)

	fromRemaining := fromRT.config.BudgetLimit - fromRT.consumedUnits
	if fromRemaining < amount {
		return nil, fmt.Errorf("insufficient remaining budget: caller=%s remaining=%d requested=%d",
			fromCaller, fromRemaining, amount)
	}

	fromRT.consumedUnits += amount
	fromRT.transferredOut += amount
	if fromRT.consumedUnits > fromRT.config.BudgetLimit {
		fromRT.currentOverdraft = fromRT.consumedUnits - fromRT.config.BudgetLimit
	}

	toRT.consumedUnits -= amount
	if toRT.consumedUnits < 0 {
		toRT.consumedUnits = 0
	}
	toRT.currentOverdraft = 0
	if toRT.consumedUnits > toRT.config.BudgetLimit {
		toRT.currentOverdraft = toRT.consumedUnits - toRT.config.BudgetLimit
	}
	toRT.transferredIn += amount

	record := &model.BudgetTransferRecord{
		FromCaller: fromCaller,
		ToCaller:   toCaller,
		Amount:     amount,
		Reason:     reason,
		CreatedAt:  now,
	}
	if err := m.storage.AddBudgetTransfer(record); err != nil {
		return nil, err
	}

	log.Printf("[lockbudget] transfer: from=%s to=%s amount=%d reason=%q", fromCaller, toCaller, amount, reason)
	return record, nil
}

func (m *Manager) CheckAcquire(callerID string, lockName string, leaseSec int) (*model.BudgetAcquireCheckResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return &model.BudgetAcquireCheckResult{
			Allowed: true,
			Reason:  "no budget configured, unlimited",
		}, nil
	}

	arrears, _ := m.storage.GetBudgetCallerArrears(callerID)
	if arrears != nil {
		return &model.BudgetAcquireCheckResult{
			Allowed:           false,
			ArrearsRejected:   true,
			ArrearsAmount:     arrears.ArrearsAmount,
			Reason:            fmt.Sprintf("caller is in arrears: owe %d units, please recharge before acquiring new locks", arrears.ArrearsAmount),
		}, nil
	}

	if m.rateAlertMgr != nil {
		allowed, freezeReason, _ := m.rateAlertMgr.CheckAcquire(callerID)
		if !allowed {
			return &model.BudgetAcquireCheckResult{
				Allowed: false,
				Reason:  freezeReason,
			}, nil
		}
	}

	now := time.Now()
	m.refreshCallerLocked(callerID, rt, now)

	remaining := rt.config.BudgetLimit - rt.consumedUnits
	if remaining < 0 {
		remaining = 0
	}

	result := &model.BudgetAcquireCheckResult{
		ConsumedUnits:    rt.consumedUnits,
		RemainingUnits:   remaining,
		BudgetLimit:      rt.config.BudgetLimit,
		OverdraftLimit:   rt.config.OverdraftLimit,
		CurrentOverdraft: rt.currentOverdraft,
	}

	effectiveOverdraftLimit := rt.config.OverdraftLimit
	if m.reputationChecker != nil {
		effectiveOverdraftLimit = m.reputationChecker.GetEffectiveOverdraftLimit(callerID, rt.config.OverdraftLimit)
	}
	result.OverdraftLimit = effectiveOverdraftLimit

	if effectiveOverdraftLimit > 0 {
		effectiveLimit := rt.config.BudgetLimit + effectiveOverdraftLimit
		if rt.consumedUnits >= effectiveLimit {
			result.Allowed = false
			result.UsingOverdraft = true
			result.Reason = fmt.Sprintf("budget and overdraft exhausted: consumed=%d, budget=%d, overdraft_limit=%d, effective_limit=%d",
				rt.consumedUnits, rt.config.BudgetLimit, effectiveOverdraftLimit, effectiveLimit)

			event := &model.BudgetExhaustEvent{
				CallerID:       callerID,
				ConsumedUnits:  rt.consumedUnits,
				BudgetLimit:    rt.config.BudgetLimit,
				PeriodStartAt:  rt.periodStartAt,
				PeriodEndAt:    rt.periodEndAt,
				AttemptedLock:  lockName,
				UnitsRequested: leaseSec,
				Detail:         result.Reason,
				CreatedAt:      now,
			}
			_ = m.storage.AddBudgetExhaustEvent(event)
			rt.exhaustCount++

			log.Printf("[lockbudget] acquire rejected (overdraft exhausted): caller=%s lock=%s consumed=%d budget=%d overdraft=%d",
				callerID, lockName, rt.consumedUnits, rt.config.BudgetLimit, effectiveOverdraftLimit)
			return result, nil
		}

		if rt.consumedUnits >= rt.config.BudgetLimit {
			result.Allowed = true
			result.UsingOverdraft = true
			result.Reason = fmt.Sprintf("using overdraft: consumed=%d, budget=%d, current_overdraft=%d, overdraft_limit=%d (penalty x%.1f)",
				rt.consumedUnits, rt.config.BudgetLimit, rt.currentOverdraft, effectiveOverdraftLimit, model.OverdraftPenaltyMultiplier)
			return result, nil
		}
	} else {
		if rt.consumedUnits >= rt.config.BudgetLimit {
			result.Allowed = false
			result.Reason = fmt.Sprintf("budget exhausted: consumed=%d, limit=%d, remaining=%d",
				rt.consumedUnits, rt.config.BudgetLimit, remaining)

			event := &model.BudgetExhaustEvent{
				CallerID:       callerID,
				ConsumedUnits:  rt.consumedUnits,
				BudgetLimit:    rt.config.BudgetLimit,
				PeriodStartAt:  rt.periodStartAt,
				PeriodEndAt:    rt.periodEndAt,
				AttemptedLock:  lockName,
				UnitsRequested: leaseSec,
				Detail:         result.Reason,
				CreatedAt:      now,
			}
			_ = m.storage.AddBudgetExhaustEvent(event)
			rt.exhaustCount++

			log.Printf("[lockbudget] acquire rejected: caller=%s lock=%s consumed=%d limit=%d",
				callerID, lockName, rt.consumedUnits, rt.config.BudgetLimit)
			return result, nil
		}
	}

	result.Allowed = true

	warningThreshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
	if rt.consumedUnits >= warningThreshold {
		result.Reason = fmt.Sprintf("budget warning: consumed=%d, limit=%d, remaining=%d (warning threshold %d%%)",
			rt.consumedUnits, rt.config.BudgetLimit, remaining, rt.config.WarningPct)
	}

	return result, nil
}

func (m *Manager) StartHolding(callerID string, lockName string, acquiredAt time.Time, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return nil
	}

	meterStart := acquiredAt
	if meterStart.Before(rt.periodStartAt) {
		meterStart = rt.periodStartAt
	}

	inOverdraft := rt.consumedUnits >= rt.config.BudgetLimit

	h := &holdingState{
		lockName:      lockName,
		acquiredAt:    acquiredAt,
		expiresAt:     expiresAt,
		lastMeteredAt: meterStart,
		unitsAccrued:  0,
		inOverdraft:   inOverdraft,
	}

	rt.holdings[lockName] = h
	rt.lockCount = len(rt.holdings)
	if rt.lockCount > rt.peakConcurrent {
		rt.peakConcurrent = rt.lockCount
	}

	return m.storage.UpsertBudgetHolding(callerID, lockName, acquiredAt, expiresAt, meterStart, 0)
}

func (m *Manager) StopHolding(callerID string, lockName string, releasedAt time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return 0, nil
	}

	h, ok := rt.holdings[lockName]
	if !ok {
		return 0, nil
	}

	if h.lastMeteredAt.Before(releasedAt) {
		elapsedSec := releasedAt.Sub(h.lastMeteredAt).Seconds()
		rawUnits := int(math.Ceil(elapsedSec))
		if rawUnits > 0 {
			unitsToAdd, penalty := m.applyOverdraftPenaltyLocked(rt, rawUnits)
			rt.consumedUnits += unitsToAdd
			rt.overdraftPenalty += penalty
			h.unitsAccrued += unitsToAdd
			h.lastMeteredAt = releasedAt

			if rt.consumedUnits > rt.config.BudgetLimit {
				rt.currentOverdraft = rt.consumedUnits - rt.config.BudgetLimit
			}

			if m.rateAlertMgr != nil {
				_ = m.rateAlertMgr.RecordConsumption(callerID, unitsToAdd, releasedAt)
			}
		}
	}

	totalUnits := h.unitsAccrued
	delete(rt.holdings, lockName)
	rt.lockCount = len(rt.holdings)

	_ = m.storage.RemoveBudgetHolding(callerID, lockName)

	return totalUnits, nil
}

func (m *Manager) RenewHolding(callerID string, lockName string, newExpiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return nil
	}

	h, ok := rt.holdings[lockName]
	if !ok {
		return nil
	}

	h.expiresAt = newExpiresAt
	return m.storage.UpdateBudgetHoldingExpiry(callerID, lockName, newExpiresAt)
}

func (m *Manager) GetCallerStatus(callerID string) (*model.CallerBudgetStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		cfg, err := m.storage.GetLockBudgetConfig(callerID)
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return &model.CallerBudgetStatusInfo{}, nil
		}
		return &model.CallerBudgetStatusInfo{Config: cfg}, nil
	}

	now := time.Now()
	m.refreshCallerLocked(callerID, rt, now)

	remaining := rt.config.BudgetLimit - rt.consumedUnits
	if remaining < 0 {
		remaining = 0
	}
	exhausted := rt.consumedUnits >= rt.config.BudgetLimit && (rt.config.OverdraftLimit <= 0 || rt.currentOverdraft >= rt.config.OverdraftLimit)
	warningTriggered := false
	if rt.config.WarningPct > 0 {
		threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
		warningTriggered = rt.consumedUnits >= threshold
	}

	inOverdraft := rt.consumedUnits > rt.config.BudgetLimit
	if rt.currentOverdraft < 0 {
		rt.currentOverdraft = 0
	}

	status := &model.LockBudgetStatus{
		CallerID:             callerID,
		BudgetLimit:          rt.config.BudgetLimit,
		PeriodSec:            rt.config.PeriodSec,
		ConsumedUnits:        rt.consumedUnits,
		RemainingUnits:       remaining,
		WarningPct:           rt.config.WarningPct,
		WarningTriggered:     warningTriggered,
		Exhausted:            exhausted,
		OverdraftLimit:       rt.config.OverdraftLimit,
		CurrentOverdraft:     rt.currentOverdraft,
		InOverdraft:          inOverdraft,
		OverdraftPenaltyUnits: rt.overdraftPenalty,
		NextPeriodDeduction:  rt.currentOverdraft + rt.overdraftPenalty,
		TransferredIn:        rt.transferredIn,
		TransferredOut:       rt.transferredOut,
		PeriodStartAt:        rt.periodStartAt,
		PeriodEndAt:          rt.periodEndAt,
		ActiveLocks:          rt.lockCount,
		UpdatedAt:            now,
	}

	heldLocks := make([]model.HeldLockDetail, 0, len(rt.holdings))
	for lockName, h := range rt.holdings {
		heldSec := now.Sub(h.acquiredAt).Seconds()
		projectedSec := h.expiresAt.Sub(h.acquiredAt).Seconds()
		if heldSec < 0 {
			heldSec = 0
		}
		if projectedSec < heldSec {
			projectedSec = heldSec
		}
		heldLocks = append(heldLocks, model.HeldLockDetail{
			LockName:       lockName,
			AcquiredAt:     h.acquiredAt,
			ExpiresAt:      h.expiresAt,
			HeldSec:        heldSec,
			UnitsConsumed:  h.unitsAccrued,
			UnitsProjected: int(projectedSec),
		})
	}

	cfgCopy := *rt.config
	return &model.CallerBudgetStatusInfo{
		Config:    &cfgCopy,
		Status:    status,
		HeldLocks: heldLocks,
	}, nil
}

func (m *Manager) ListAllStatuses() ([]model.LockBudgetStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var result []model.LockBudgetStatus

	for callerID, rt := range m.callers {
		m.refreshCallerLocked(callerID, rt, now)

		remaining := rt.config.BudgetLimit - rt.consumedUnits
		if remaining < 0 {
			remaining = 0
		}
		exhausted := rt.consumedUnits >= rt.config.BudgetLimit && (rt.config.OverdraftLimit <= 0 || rt.currentOverdraft >= rt.config.OverdraftLimit)
		warningTriggered := false
		if rt.config.WarningPct > 0 {
			threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
			warningTriggered = rt.consumedUnits >= threshold
		}

		inOverdraft := rt.consumedUnits > rt.config.BudgetLimit
		if rt.currentOverdraft < 0 {
			rt.currentOverdraft = 0
		}

		result = append(result, model.LockBudgetStatus{
			CallerID:             callerID,
			BudgetLimit:          rt.config.BudgetLimit,
			PeriodSec:            rt.config.PeriodSec,
			ConsumedUnits:        rt.consumedUnits,
			RemainingUnits:       remaining,
			WarningPct:           rt.config.WarningPct,
			WarningTriggered:     warningTriggered,
			Exhausted:            exhausted,
			OverdraftLimit:       rt.config.OverdraftLimit,
			CurrentOverdraft:     rt.currentOverdraft,
			InOverdraft:          inOverdraft,
			OverdraftPenaltyUnits: rt.overdraftPenalty,
			NextPeriodDeduction:  rt.currentOverdraft + rt.overdraftPenalty,
			TransferredIn:        rt.transferredIn,
			TransferredOut:       rt.transferredOut,
			PeriodStartAt:        rt.periodStartAt,
			PeriodEndAt:          rt.periodEndAt,
			ActiveLocks:          rt.lockCount,
			UpdatedAt:            now,
		})
	}

	return result, nil
}

func (m *Manager) ListConfigs() ([]model.LockBudgetConfig, error) {
	return m.storage.ListLockBudgetConfigs()
}

func (m *Manager) ListExhaustEvents(callerID string, limit int) ([]model.BudgetExhaustEvent, error) {
	return m.storage.ListBudgetExhaustEvents(callerID, limit)
}

func (m *Manager) ListPeriodSummaries(callerID string, limit int) ([]model.BudgetPeriodSummary, error) {
	return m.storage.ListBudgetPeriodSummaries(callerID, limit)
}

func (m *Manager) GetGlobalStats() (*model.GlobalBudgetStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	yesterday := now.Add(-24 * time.Hour)

	totalActiveLocks := 0
	callersOverBudget := 0
	callersNearBudget := 0
	callersInOverdraft := 0
	totalOverdraftAmount := 0

	for callerID, rt := range m.callers {
		m.refreshCallerLocked(callerID, rt, now)
		totalActiveLocks += rt.lockCount

		inOverdraft := rt.consumedUnits > rt.config.BudgetLimit
		if inOverdraft && rt.config.OverdraftLimit > 0 {
			callersInOverdraft++
			if rt.currentOverdraft > 0 {
				totalOverdraftAmount += rt.currentOverdraft
			}
		}

		effectiveLimit := rt.config.BudgetLimit
		if rt.config.OverdraftLimit > 0 {
			effectiveLimit += rt.config.OverdraftLimit
		}
		if rt.consumedUnits >= effectiveLimit {
			callersOverBudget++
		} else if rt.config.WarningPct > 0 {
			threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
			if rt.consumedUnits >= threshold {
				callersNearBudget++
			}
		}
	}

	totalConsumedToday, _ := m.storage.SumTotalConsumedSince(todayStart)
	exhaustEvents24h, _ := m.storage.CountBudgetExhaustEventsSince("", yesterday)

	return &model.GlobalBudgetStats{
		TotalCallers:         len(m.callers),
		TotalActiveLocks:     totalActiveLocks,
		TotalConsumedToday:   totalConsumedToday,
		ExhaustEvents24h:     exhaustEvents24h,
		CallersOverBudget:    callersOverBudget,
		CallersNearBudget:    callersNearBudget,
		CallersInOverdraft:   callersInOverdraft,
		TotalOverdraftAmount: totalOverdraftAmount,
	}, nil
}

func (m *Manager) HasBudget(callerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.callers[callerID]
	return ok
}

func (m *Manager) GetConfig(callerID string) (*model.LockBudgetConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rt, ok := m.callers[callerID]
	if ok {
		cfg := *rt.config
		return &cfg, nil
	}
	return m.storage.GetLockBudgetConfig(callerID)
}

func (m *Manager) SetConfig(callerID string, budgetLimit int, periodSec int, warningPct int) (*model.LockBudgetConfig, error) {
	return m.SetBudget(callerID, budgetLimit, periodSec, warningPct)
}

func (m *Manager) SetConfigWithOverdraft(callerID string, budgetLimit int, periodSec int, warningPct int, overdraftLimit int) (*model.LockBudgetConfig, error) {
	return m.SetBudgetWithOverdraft(callerID, budgetLimit, periodSec, warningPct, overdraftLimit)
}

func (m *Manager) DeleteConfig(callerID string) error {
	return m.DeleteBudget(callerID)
}

func (m *Manager) ListStatuses() ([]model.LockBudgetStatus, error) {
	return m.ListAllStatuses()
}

func (m *Manager) GetStatus(callerID string) (*model.LockBudgetStatus, error) {
	info, err := m.GetCallerStatus(callerID)
	if err != nil {
		return nil, err
	}
	return info.Status, nil
}

func (m *Manager) ListHeldLocks(callerID string, now time.Time) ([]model.HeldLockDetail, error) {
	info, err := m.GetCallerStatus(callerID)
	if err != nil {
		return nil, err
	}
	_ = now
	return info.HeldLocks, nil
}

func (m *Manager) ListTransferRecords(query *model.BudgetTransferListQuery) (*model.BudgetTransferListResult, error) {
	if query == nil {
		query = &model.BudgetTransferListQuery{Limit: 50}
	}
	records, total, err := m.storage.ListBudgetTransfers(query)
	if err != nil {
		return nil, err
	}
	return &model.BudgetTransferListResult{
		Total:  total,
		Items:  records,
		Limit:  query.Limit,
		Offset: query.Offset,
	}, nil
}

func (m *Manager) ListOverdraftCallers() (*model.BudgetOverdraftListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var items []model.BudgetOverdraftInfo
	totalAmount := 0

	for callerID, rt := range m.callers {
		m.refreshCallerLocked(callerID, rt, now)

		inOverdraft := rt.consumedUnits > rt.config.BudgetLimit
		if !inOverdraft && rt.currentOverdraft <= 0 {
			continue
		}

		overdraftRemaining := 0
		if rt.config.OverdraftLimit > 0 {
			overdraftRemaining = rt.config.OverdraftLimit - rt.currentOverdraft
			if overdraftRemaining < 0 {
				overdraftRemaining = 0
			}
		}

		items = append(items, model.BudgetOverdraftInfo{
			CallerID:             callerID,
			CurrentOverdraft:     rt.currentOverdraft,
			OverdraftLimit:       rt.config.OverdraftLimit,
			OverdraftRemaining:   overdraftRemaining,
			OverdraftPenaltyUnits: rt.overdraftPenalty,
			NextPeriodDeduction:  rt.currentOverdraft + rt.overdraftPenalty,
			InOverdraft:          inOverdraft,
			ActiveLocks:          rt.lockCount,
			PeriodStartAt:        rt.periodStartAt,
			PeriodEndAt:          rt.periodEndAt,
		})
		totalAmount += rt.currentOverdraft
	}

	return &model.BudgetOverdraftListResult{
		TotalInOverdraft:    len(items),
		TotalOverdraftAmount: totalAmount,
		Items:                items,
	}, nil
}

func (m *Manager) GetNextPeriodDeduction(callerID string) (*model.BudgetNextPeriodDeductionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return nil, fmt.Errorf("no budget configured for caller: %s", callerID)
	}

	now := time.Now()
	m.refreshCallerLocked(callerID, rt, now)

	deduction := rt.currentOverdraft + rt.overdraftPenalty
	projectedRemaining := rt.config.BudgetLimit - deduction
	if projectedRemaining < 0 {
		projectedRemaining = 0
	}

	return &model.BudgetNextPeriodDeductionInfo{
		CallerID:             callerID,
		NextPeriodDeduction:  deduction,
		CurrentOverdraft:     rt.currentOverdraft,
		OverdraftPenaltyUnits: rt.overdraftPenalty,
		PeriodEndAt:          rt.periodEndAt,
		BudgetLimit:          rt.config.BudgetLimit,
		ProjectedRemaining:   projectedRemaining,
	}, nil
}

func (m *Manager) ListAllNextPeriodDeductions() ([]model.BudgetNextPeriodDeductionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var result []model.BudgetNextPeriodDeductionInfo

	for callerID, rt := range m.callers {
		m.refreshCallerLocked(callerID, rt, now)

		deduction := rt.currentOverdraft + rt.overdraftPenalty
		if deduction <= 0 {
			continue
		}

		projectedRemaining := rt.config.BudgetLimit - deduction
		if projectedRemaining < 0 {
			projectedRemaining = 0
		}

		result = append(result, model.BudgetNextPeriodDeductionInfo{
			CallerID:             callerID,
			NextPeriodDeduction:  deduction,
			CurrentOverdraft:     rt.currentOverdraft,
			OverdraftPenaltyUnits: rt.overdraftPenalty,
			PeriodEndAt:          rt.periodEndAt,
			BudgetLimit:          rt.config.BudgetLimit,
			ProjectedRemaining:   projectedRemaining,
		})
	}

	return result, nil
}

func (m *Manager) ListSettlementBills(query *model.BudgetBillListQuery) (*model.BudgetBillListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	bills, total, err := m.storage.ListBudgetSettlementBills(query)
	if err != nil {
		return nil, err
	}

	return &model.BudgetBillListResult{
		Items:  bills,
		Total:  total,
		Limit:  query.Limit,
		Offset: query.Offset,
	}, nil
}

func (m *Manager) GetSettlementBillDetail(billID int64) (*model.BudgetBillDetailResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	bill, err := m.storage.GetBudgetSettlementBill(billID)
	if err != nil {
		return nil, err
	}
	if bill == nil {
		return nil, fmt.Errorf("bill not found: %d", billID)
	}

	transferDetails, err := m.storage.ListBudgetBillTransferDetails(billID)
	if err != nil {
		return nil, err
	}

	return &model.BudgetBillDetailResult{
		Bill:            bill,
		TransferDetails: transferDetails,
	}, nil
}

func (m *Manager) ListActiveArrears() (*model.BudgetArrearsListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	arrearsList, err := m.storage.ListActiveBudgetArrears()
	if err != nil {
		return nil, err
	}

	totalArrearsAmount := 0
	var items []model.BudgetArrearsCallerInfo
	for _, a := range arrearsList {
		totalArrearsAmount += a.ArrearsAmount

		currentBudgetLimit := 0
		currentPeriodStartAt := a.OriginalPeriodEndAt
		currentPeriodEndAt := a.OriginalPeriodEndAt
		rt, ok := m.callers[a.CallerID]
		if ok {
			currentBudgetLimit = rt.config.BudgetLimit
			currentPeriodStartAt = rt.periodStartAt
			currentPeriodEndAt = rt.periodEndAt
		}

		items = append(items, model.BudgetArrearsCallerInfo{
			CallerID:              a.CallerID,
			ArrearsAmount:         a.ArrearsAmount,
			OriginalBillID:        a.OriginalBillID,
			OriginalPeriodStartAt: a.OriginalPeriodStartAt,
			OriginalPeriodEndAt:   a.OriginalPeriodEndAt,
			CurrentBudgetLimit:    currentBudgetLimit,
			CurrentPeriodStartAt:  currentPeriodStartAt,
			CurrentPeriodEndAt:    currentPeriodEndAt,
			CreatedAt:             a.CreatedAt,
		})
	}

	return &model.BudgetArrearsListResult{
		TotalInArrears:     len(items),
		TotalArrearsAmount: totalArrearsAmount,
		Items:              items,
	}, nil
}

func (m *Manager) RechargeBudget(callerID string, amount int) (*model.BudgetRechargeResult, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("recharge amount must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	result, err := m.storage.RechargeBudget(callerID, amount)
	if err != nil {
		return nil, err
	}

	rt, ok := m.callers[callerID]
	if ok {
		rt.config.BudgetLimit = result.NewBudgetLimit
	}

	log.Printf("[lockbudget] recharge: caller=%s amount=%d new_budget=%d arrears_cleared=%d remaining_arrears=%d",
		callerID, amount, result.NewBudgetLimit, result.ArrearsCleared, result.RemainingArrears)

	return result, nil
}
