package ratealert

import (
	"fmt"
	"log"
	"task106/internal/model"
	"task106/internal/storage"
	"sync"
	"time"
)

type windowRecord struct {
	Timestamp time.Time
	Units     int
}

type callerRuntime struct {
	rule              *model.LockBudgetRateAlertRule
	consumptionWindow []windowRecord
	consecutiveAlerts int
	lastAlertAt       time.Time
}

type Manager struct {
	storage *storage.Storage
	mu      sync.Mutex
	callers map[string]*callerRuntime
	stopCh  chan struct{}
	ticker  *time.Ticker
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage: s,
		callers: make(map[string]*callerRuntime),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadRulesLocked(); err != nil {
		return fmt.Errorf("load rate alert rules: %w", err)
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.cleanupLoop()

	log.Println("[ratealert-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[ratealert-manager] stopped")
}

func (m *Manager) loadRulesLocked() error {
	rules, err := m.storage.ListRateAlertRules()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range rules {
		rule := &rules[i]
		rt := &callerRuntime{
			rule:              rule,
			consumptionWindow: make([]windowRecord, 0),
			consecutiveAlerts: 0,
			lastAlertAt:       now.Add(-time.Duration(rule.WindowSec*2) * time.Second),
		}
		m.callers[rule.CallerID] = rt
	}
	return nil
}

func (m *Manager) cleanupLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.cleanupExpiredRecords()
		}
	}
}

func (m *Manager) cleanupExpiredRecords() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for callerID, rt := range m.callers {
		if rt.rule == nil {
			continue
		}
		cutoff := now.Add(-time.Duration(rt.rule.WindowSec) * time.Second)
		validIdx := 0
		for i, r := range rt.consumptionWindow {
			if r.Timestamp.After(cutoff) || r.Timestamp.Equal(cutoff) {
				validIdx = i
				break
			}
			if i == len(rt.consumptionWindow)-1 {
				validIdx = len(rt.consumptionWindow)
			}
		}
		if validIdx > 0 {
			rt.consumptionWindow = rt.consumptionWindow[validIdx:]
		}
		_ = callerID
	}
}

func (m *Manager) SetRule(callerID string, windowSec int, maxUnitsInWindow int, freezeTriggerN int, enabled bool) (*model.LockBudgetRateAlertRule, error) {
	if callerID == "" {
		return nil, fmt.Errorf("caller_id is required")
	}
	if windowSec <= 0 {
		return nil, fmt.Errorf("window_sec must be positive")
	}
	if maxUnitsInWindow <= 0 {
		return nil, fmt.Errorf("max_units_in_window must be positive")
	}
	if freezeTriggerN <= 0 {
		freezeTriggerN = 3
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := &model.LockBudgetRateAlertRule{
		CallerID:         callerID,
		WindowSec:        windowSec,
		MaxUnitsInWindow: maxUnitsInWindow,
		FreezeTriggerN:   freezeTriggerN,
		Enabled:          enabled,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if rt, ok := m.callers[callerID]; ok {
		rule.ID = rt.rule.ID
		rt.rule = rule
		rt.consumptionWindow = make([]windowRecord, 0)
		rt.consecutiveAlerts = 0
		rt.lastAlertAt = now.Add(-time.Duration(windowSec*2) * time.Second)
	} else {
		rt = &callerRuntime{
			rule:              rule,
			consumptionWindow: make([]windowRecord, 0),
			consecutiveAlerts: 0,
			lastAlertAt:       now.Add(-time.Duration(windowSec*2) * time.Second),
		}
		m.callers[callerID] = rt
	}

	if err := m.storage.UpsertRateAlertRule(rule); err != nil {
		return nil, err
	}

	log.Printf("[ratealert] rule set: caller=%s window=%ds max=%d freeze_n=%d enabled=%v",
		callerID, windowSec, maxUnitsInWindow, freezeTriggerN, enabled)
	return rule, nil
}

func (m *Manager) DeleteRule(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.callers[callerID]; !ok {
		return fmt.Errorf("no rate alert rule configured for caller: %s", callerID)
	}

	delete(m.callers, callerID)
	if err := m.storage.DeleteRateAlertRule(callerID); err != nil {
		return err
	}

	log.Printf("[ratealert] rule deleted: caller=%s", callerID)
	return nil
}

func (m *Manager) GetRule(callerID string) (*model.LockBudgetRateAlertRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rt, ok := m.callers[callerID]; ok {
		cfg := *rt.rule
		return &cfg, nil
	}
	return m.storage.GetRateAlertRule(callerID)
}

func (m *Manager) ListRules() ([]model.LockBudgetRateAlertRule, error) {
	return m.storage.ListRateAlertRules()
}

func (m *Manager) RecordConsumption(callerID string, units int, at time.Time) error {
	if units <= 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok || rt.rule == nil || !rt.rule.Enabled {
		return nil
	}

	frozen, _ := m.storage.GetActiveFreeze(callerID)
	if frozen != nil && frozen.Active {
		return nil
	}

	rt.consumptionWindow = append(rt.consumptionWindow, windowRecord{
		Timestamp: at,
		Units:     units,
	})

	cutoff := at.Add(-time.Duration(rt.rule.WindowSec) * time.Second)
	validIdx := 0
	for i, r := range rt.consumptionWindow {
		if r.Timestamp.After(cutoff) || r.Timestamp.Equal(cutoff) {
			validIdx = i
			break
		}
		if i == len(rt.consumptionWindow)-1 {
			validIdx = len(rt.consumptionWindow)
		}
	}
	if validIdx > 0 {
		rt.consumptionWindow = rt.consumptionWindow[validIdx:]
	}

	totalUnits := 0
	for _, r := range rt.consumptionWindow {
		totalUnits += r.Units
	}

	if totalUnits >= rt.rule.MaxUnitsInWindow {
		actualRate := float64(totalUnits) / float64(rt.rule.WindowSec)
		alertSuppressionWindow := time.Duration(rt.rule.WindowSec) * time.Second / 2
		if at.Sub(rt.lastAlertAt) >= alertSuppressionWindow {
			m.triggerAlertLocked(callerID, rt, actualRate, totalUnits, at)
		}
	}

	return nil
}

func (m *Manager) triggerAlertLocked(callerID string, rt *callerRuntime, actualRate float64, consumedInWindow int, at time.Time) {
	detail := fmt.Sprintf("consumption rate exceeded: %.2f units/s (window=%ds, max=%d, consumed=%d)",
		actualRate, rt.rule.WindowSec, rt.rule.MaxUnitsInWindow, consumedInWindow)

	event := &model.LockBudgetRateAlertEvent{
		CallerID:         callerID,
		WindowSec:        rt.rule.WindowSec,
		MaxUnitsInWindow: rt.rule.MaxUnitsInWindow,
		ActualRate:       actualRate,
		ConsumedInWindow: consumedInWindow,
		Detail:           detail,
		CreatedAt:        at,
	}
	if err := m.storage.AddRateAlertEvent(event); err != nil {
		log.Printf("[ratealert] failed to persist alert event: caller=%s err=%v", callerID, err)
	}

	rt.consecutiveAlerts++
	rt.lastAlertAt = at

	log.Printf("[ratealert] alert triggered: caller=%s rate=%.2f units/s consumed=%d max=%d consecutive=%d",
		callerID, actualRate, consumedInWindow, rt.rule.MaxUnitsInWindow, rt.consecutiveAlerts)

	if rt.consecutiveAlerts >= rt.rule.FreezeTriggerN {
		m.triggerFreezeLocked(callerID, rt, at)
	}
}

func (m *Manager) triggerFreezeLocked(callerID string, rt *callerRuntime, at time.Time) {
	existing, _ := m.storage.GetActiveFreeze(callerID)
	if existing != nil && existing.Active {
		return
	}

	reason := fmt.Sprintf("auto-frozen after %d consecutive rate alerts (threshold=%d)",
		rt.consecutiveAlerts, rt.rule.FreezeTriggerN)

	freeze := &model.LockBudgetCallerFreeze{
		CallerID:         callerID,
		FrozenAt:         at,
		FrozenBy:         "system",
		Reason:           reason,
		AlertCountBefore: rt.consecutiveAlerts,
		Active:           true,
		CreatedAt:        at,
		UpdatedAt:        at,
	}
	if err := m.storage.FreezeCaller(freeze); err != nil {
		log.Printf("[ratealert] failed to persist freeze: caller=%s err=%v", callerID, err)
		return
	}

	log.Printf("[ratealert] caller frozen: caller=%s reason=%s alerts=%d",
		callerID, reason, rt.consecutiveAlerts)
}

func (m *Manager) IsFrozen(callerID string) (*model.LockBudgetCallerFreeze, error) {
	return m.storage.GetActiveFreeze(callerID)
}

func (m *Manager) Unfreeze(callerID string, operator string, reason string) error {
	if callerID == "" {
		return fmt.Errorf("caller_id is required")
	}
	if operator == "" {
		return fmt.Errorf("operator is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	frozen, err := m.storage.GetActiveFreeze(callerID)
	if err != nil {
		return err
	}
	if frozen == nil || !frozen.Active {
		return fmt.Errorf("caller is not frozen: %s", callerID)
	}

	now := time.Now()
	if err := m.storage.UnfreezeCaller(callerID, now, operator, reason); err != nil {
		return err
	}

	if rt, ok := m.callers[callerID]; ok {
		rt.consecutiveAlerts = 0
	}

	log.Printf("[ratealert] caller unfrozen: caller=%s operator=%s reason=%s",
		callerID, operator, reason)
	return nil
}

func (m *Manager) ListFrozen() ([]model.LockBudgetCallerFreeze, error) {
	return m.storage.ListActiveFreezes()
}

func (m *Manager) ListAlertEvents(query *model.RateAlertEventListQuery) (*model.RateAlertEventListResult, error) {
	if query == nil {
		query = &model.RateAlertEventListQuery{Limit: 50}
	}
	events, total, err := m.storage.ListRateAlertEvents(query)
	if err != nil {
		return nil, err
	}
	return &model.RateAlertEventListResult{
		Total:  total,
		Items:  events,
		Limit:  query.Limit,
		Offset: query.Offset,
	}, nil
}

func (m *Manager) GetCallerStatus(callerID string) (*model.CallerRateStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := &model.CallerRateStatus{
		CallerID: callerID,
	}

	rt, ok := m.callers[callerID]
	if !ok || rt.rule == nil {
		rule, err := m.storage.GetRateAlertRule(callerID)
		if err != nil {
			return nil, err
		}
		if rule != nil {
			status.Rule = rule
			status.WindowSec = rule.WindowSec
			status.MaxUnitsInWindow = rule.MaxUnitsInWindow
		}
	} else {
		ruleCopy := *rt.rule
		status.Rule = &ruleCopy
		status.WindowSec = rt.rule.WindowSec
		status.MaxUnitsInWindow = rt.rule.MaxUnitsInWindow
		status.ConsecutiveAlerts = rt.consecutiveAlerts

		now := time.Now()
		cutoff := now.Add(-time.Duration(rt.rule.WindowSec) * time.Second)
		totalUnits := 0
		for _, r := range rt.consumptionWindow {
			if r.Timestamp.After(cutoff) || r.Timestamp.Equal(cutoff) {
				totalUnits += r.Units
			}
		}
		status.ConsumedInWindow = totalUnits
		status.CurrentRate = float64(totalUnits) / float64(rt.rule.WindowSec)
	}

	frozen, err := m.storage.GetActiveFreeze(callerID)
	if err != nil {
		return nil, err
	}
	if frozen != nil && frozen.Active {
		status.IsFrozen = true
		status.FreezeInfo = frozen
	}

	return status, nil
}

func (m *Manager) CheckAcquire(callerID string) (allowed bool, reason string, freezeInfo *model.LockBudgetCallerFreeze) {
	frozen, err := m.storage.GetActiveFreeze(callerID)
	if err != nil {
		log.Printf("[ratealert] check acquire error: caller=%s err=%v", callerID, err)
		return true, "", nil
	}
	if frozen != nil && frozen.Active {
		return false, fmt.Sprintf("caller is frozen: %s", frozen.Reason), frozen
	}
	return true, "", nil
}

func (m *Manager) ListEvents(callerID string, limit int) ([]model.LockBudgetRateAlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	query := &model.RateAlertEventListQuery{
		CallerID: callerID,
		Limit:    limit,
	}
	events, _, err := m.storage.ListRateAlertEvents(query)
	return events, err
}

func (m *Manager) ListFrozenCallers() ([]model.LockBudgetCallerFreeze, error) {
	return m.storage.ListActiveFreezes()
}

func (m *Manager) GetFreezeStatus(callerID string) (*model.LockBudgetCallerFreeze, error) {
	return m.storage.GetActiveFreeze(callerID)
}
