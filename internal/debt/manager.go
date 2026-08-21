package debt

import (
	"fmt"
	"log"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"sync"
	"time"
)

var ErrDebtRestricted = fmt.Errorf("debt_restricted")

type Manager struct {
	storage   *storage.Storage
	rateMgr   *ratelimit.Manager

	mu        sync.Mutex
	rules     map[string]*model.LiquidationRule
	stopCh    chan struct{}
	ticker    *time.Ticker
}

func NewManager(s *storage.Storage, rm *ratelimit.Manager) *Manager {
	return &Manager{
		storage: s,
		rateMgr: rm,
		rules:   make(map[string]*model.LiquidationRule),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rules, err := m.storage.ListLiquidationRules()
	if err != nil {
		return fmt.Errorf("load liquidation rules: %w", err)
	}
	for i := range rules {
		r := &rules[i]
		m.rules[r.CallerID] = r
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.liquidationLoop()

	log.Println("[debt-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[debt-manager] stopped")
}

func (m *Manager) RecordBorrow(debtor, creditor string, amount int, resourceType, resourceKey string, gracePeriodSec int) (*model.DebtRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := m.rules[debtor]
	grace := gracePeriodSec
	if rule != nil && rule.GracePeriodSec > 0 {
		grace = rule.GracePeriodSec
	}

	dueAt := now.Add(time.Duration(grace) * time.Second)

	record := &model.DebtRecord{
		Debtor:       debtor,
		Creditor:     creditor,
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Status:       model.DebtStatusActive,
		DueAt:        dueAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.CreateDebtRecord(record); err != nil {
		return nil, err
	}

	event := &model.DebtLedgerEvent{
		DebtID:       record.ID,
		Debtor:       debtor,
		Creditor:     creditor,
		EventType:    model.DebtEventBorrow,
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Detail:       fmt.Sprintf("borrowed %d from %s, due at %s", amount, creditor, dueAt.Format(time.RFC3339)),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(event)

	log.Printf("[debt] borrow recorded: debtor=%s creditor=%s amount=%d due=%s", debtor, creditor, amount, dueAt.Format(time.RFC3339))
	return record, nil
}

func (m *Manager) RecordReturn(debtor, creditor string, amount int, resourceType, resourceKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	activeDebts, err := m.storage.ListDebtRecords(debtor, string(model.DebtStatusActive))
	if err != nil {
		return err
	}

	remaining := amount
	for i := range activeDebts {
		if remaining <= 0 {
			break
		}
		d := &activeDebts[i]
		if d.Creditor != creditor {
			continue
		}
		if d.Amount <= remaining {
			remaining -= d.Amount
			d.Status = model.DebtStatusCollected
			d.CollectedAt = now
			d.UpdatedAt = now
			_ = m.storage.UpdateDebtRecord(d)

			evt := &model.DebtLedgerEvent{
				DebtID:       d.ID,
				Debtor:       debtor,
				Creditor:     creditor,
				EventType:    model.DebtEventReturn,
				Amount:       d.Amount,
				ResourceType: resourceType,
				ResourceKey:  resourceKey,
				Detail:       fmt.Sprintf("returned %d, debt %d settled", d.Amount, d.ID),
				CreatedAt:    now,
			}
			_ = m.storage.CreateDebtLedgerEvent(evt)
		} else {
			d.Amount -= remaining
			d.UpdatedAt = now
			_ = m.storage.UpdateDebtRecord(d)

			evt := &model.DebtLedgerEvent{
				DebtID:       d.ID,
				Debtor:       debtor,
				Creditor:     creditor,
				EventType:    model.DebtEventReturn,
				Amount:       remaining,
				ResourceType: resourceType,
				ResourceKey:  resourceKey,
				Detail:       fmt.Sprintf("returned %d partial, debt %d remaining=%d", remaining, d.ID, d.Amount),
				CreatedAt:    now,
			}
			_ = m.storage.CreateDebtLedgerEvent(evt)
			remaining = 0
		}
	}

	overdueDebts, err := m.storage.ListDebtRecords(debtor, string(model.DebtStatusOverdue))
	if err != nil {
		return err
	}
	for i := range overdueDebts {
		if remaining <= 0 {
			break
		}
		d := &overdueDebts[i]
		if d.Creditor != creditor {
			continue
		}
		if d.Amount <= remaining {
			remaining -= d.Amount
			d.Status = model.DebtStatusCollected
			d.CollectedAt = now
			d.UpdatedAt = now
			_ = m.storage.UpdateDebtRecord(d)

			evt := &model.DebtLedgerEvent{
				DebtID:       d.ID,
				Debtor:       debtor,
				Creditor:     creditor,
				EventType:    model.DebtEventReturn,
				Amount:       d.Amount,
				ResourceType: resourceType,
				ResourceKey:  resourceKey,
				Detail:       fmt.Sprintf("returned %d, overdue debt %d settled", d.Amount, d.ID),
				CreatedAt:    now,
			}
			_ = m.storage.CreateDebtLedgerEvent(evt)
		} else {
			d.Amount -= remaining
			d.UpdatedAt = now
			_ = m.storage.UpdateDebtRecord(d)

			evt := &model.DebtLedgerEvent{
				DebtID:       d.ID,
				Debtor:       debtor,
				Creditor:     creditor,
				EventType:    model.DebtEventReturn,
				Amount:       remaining,
				ResourceType: resourceType,
				ResourceKey:  resourceKey,
				Detail:       fmt.Sprintf("returned %d partial, overdue debt %d remaining=%d", remaining, d.ID, d.Amount),
				CreatedAt:    now,
			}
			_ = m.storage.CreateDebtLedgerEvent(evt)
			remaining = 0
		}
	}

	if remaining > 0 {
		evt := &model.DebtLedgerEvent{
			Debtor:       debtor,
			Creditor:     creditor,
			EventType:    model.DebtEventReturn,
			Amount:       amount - remaining,
			ResourceType: resourceType,
			ResourceKey:  resourceKey,
			Detail:       fmt.Sprintf("return of %d processed, %d applied (no matching debt)", amount, amount-remaining),
			CreatedAt:    now,
		}
		_ = m.storage.CreateDebtLedgerEvent(evt)
	}

	m.tryLiftRestrictions(debtor, now)

	log.Printf("[debt] return recorded: debtor=%s creditor=%s amount=%d", debtor, creditor, amount)
	return nil
}

func (m *Manager) RecordRollbackFail(debtor string, amount int, resourceType, resourceKey, reason string) (*model.DebtRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := m.rules[debtor]
	grace := 30
	if rule != nil && rule.GracePeriodSec > 0 {
		grace = rule.GracePeriodSec
	}
	dueAt := now.Add(time.Duration(grace) * time.Second)

	record := &model.DebtRecord{
		Debtor:       debtor,
		Creditor:     "system",
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Status:       model.DebtStatusActive,
		DueAt:        dueAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.CreateDebtRecord(record); err != nil {
		return nil, err
	}

	evt := &model.DebtLedgerEvent{
		DebtID:       record.ID,
		Debtor:       debtor,
		Creditor:     "system",
		EventType:    model.DebtEventRollbackFail,
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Detail:       fmt.Sprintf("rollback failed: %s, debt %d created", reason, record.ID),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	log.Printf("[debt] rollback fail recorded: debtor=%s amount=%d reason=%s", debtor, amount, reason)
	return record, nil
}

func (m *Manager) RecordReservationExpire(callerID string, tokens int, policyName string) (*model.DebtRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := m.rules[callerID]
	grace := 30
	if rule != nil && rule.GracePeriodSec > 0 {
		grace = rule.GracePeriodSec
	}
	dueAt := now.Add(time.Duration(grace) * time.Second)

	record := &model.DebtRecord{
		Debtor:       callerID,
		Creditor:     "system",
		Amount:       tokens,
		ResourceType: "reservation",
		ResourceKey:  policyName,
		Status:       model.DebtStatusActive,
		DueAt:        dueAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.CreateDebtRecord(record); err != nil {
		return nil, err
	}

	evt := &model.DebtLedgerEvent{
		DebtID:       record.ID,
		Debtor:       callerID,
		Creditor:     "system",
		EventType:    model.DebtEventReservExpir,
		Amount:       tokens,
		ResourceType: "reservation",
		ResourceKey:  policyName,
		Detail:       fmt.Sprintf("reservation expired: policy=%s tokens=%d", policyName, tokens),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	log.Printf("[debt] reservation expire recorded: caller=%s tokens=%d policy=%s", callerID, tokens, policyName)
	return record, nil
}

func (m *Manager) RecordForceReclaim(debtor, resourceType, resourceKey string, amount int) (*model.DebtRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := m.rules[debtor]
	grace := 60
	if rule != nil && rule.GracePeriodSec > 0 {
		grace = rule.GracePeriodSec
	}
	dueAt := now.Add(time.Duration(grace) * time.Second)

	record := &model.DebtRecord{
		Debtor:       debtor,
		Creditor:     "system",
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Status:       model.DebtStatusActive,
		DueAt:        dueAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.CreateDebtRecord(record); err != nil {
		return nil, err
	}

	evt := &model.DebtLedgerEvent{
		DebtID:       record.ID,
		Debtor:       debtor,
		Creditor:     "system",
		EventType:    model.DebtEventForceReclaim,
		Amount:       amount,
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		Detail:       fmt.Sprintf("force reclaim: %s/%s amount=%d", resourceType, resourceKey, amount),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	log.Printf("[debt] force reclaim recorded: debtor=%s type=%s key=%s amount=%d", debtor, resourceType, resourceKey, amount)
	return record, nil
}

func (m *Manager) CheckRestriction(callerID string, scope model.RestrictionScope) *model.CheckRestrictionResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	restrictions, err := m.storage.ListActiveDebtRestrictions(callerID)
	if err != nil || len(restrictions) == 0 {
		return &model.CheckRestrictionResult{Restricted: false}
	}

	for _, r := range restrictions {
		if r.Scope == model.RestrictionScopeAll || r.Scope == scope {
			return &model.CheckRestrictionResult{
				Restricted:      true,
				RestrictionType: r.RestrictionType,
				Scope:           r.Scope,
				Reason:          r.Reason,
			}
		}
	}

	return &model.CheckRestrictionResult{Restricted: false}
}

func (m *Manager) liquidationLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.runLiquidationTick()
		}
	}
}

func (m *Manager) runLiquidationTick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	overdueDebts, err := m.storage.ListOverdueDebts(now)
	if err != nil {
		log.Printf("[debt] list overdue: %v", err)
		return
	}

	for i := range overdueDebts {
		d := &overdueDebts[i]
		if d.Status == model.DebtStatusActive {
			d.Status = model.DebtStatusOverdue
			d.OverdueAt = now
			d.UpdatedAt = now
			_ = m.storage.UpdateDebtRecord(d)

			evt := &model.DebtLedgerEvent{
				DebtID:       d.ID,
				Debtor:       d.Debtor,
				Creditor:     d.Creditor,
				EventType:    model.DebtEventOverdueMark,
				Amount:       d.Amount,
				ResourceType: d.ResourceType,
				ResourceKey:  d.ResourceKey,
				Detail:       fmt.Sprintf("debt %d marked overdue, was due at %s", d.ID, d.DueAt.Format(time.RFC3339)),
				CreatedAt:    now,
			}
			_ = m.storage.CreateDebtLedgerEvent(evt)
			log.Printf("[debt] overdue: debtor=%s debt=%d amount=%d", d.Debtor, d.ID, d.Amount)
		}

		m.tryCollectDebt(d, now)
		m.applyRestrictionsIfNeeded(d.Debtor, now)
	}
}

func (m *Manager) tryCollectDebt(d *model.DebtRecord, now time.Time) {
	rule := m.rules[d.Debtor]
	maxRetries := 3
	protectionAfter := 5
	if rule != nil {
		maxRetries = rule.MaxCollectRetries
		protectionAfter = rule.ProtectionAfter
	}

	if d.CollectAttempts >= maxRetries {
		failures, _ := m.storage.CountCollectFailures(d.Debtor, 300, now)
		if failures >= protectionAfter {
			m.writeOffDebt(d, now, "protection: too many consecutive collect failures")
			return
		}
	}

	d.CollectAttempts++
	d.LastCollectAt = now
	d.UpdatedAt = now
	_ = m.storage.UpdateDebtRecord(d)

	collected := false
	if d.Creditor != "system" && m.rateMgr != nil {
		err := m.rateMgr.ReturnTokens(d.Debtor, d.Amount)
		if err == nil {
			collected = true
		}
	}

	if collected {
		d.Status = model.DebtStatusCollected
		d.CollectedAt = now
		d.UpdatedAt = now
		_ = m.storage.UpdateDebtRecord(d)

		evt := &model.DebtLedgerEvent{
			DebtID:       d.ID,
			Debtor:       d.Debtor,
			Creditor:     d.Creditor,
			EventType:    model.DebtEventCollect,
			Amount:       d.Amount,
			ResourceType: d.ResourceType,
			ResourceKey:  d.ResourceKey,
			Detail:       fmt.Sprintf("auto-collect success: debt %d, attempt %d", d.ID, d.CollectAttempts),
			CreatedAt:    now,
		}
		_ = m.storage.CreateDebtLedgerEvent(evt)

		audit := &model.LiquidationAuditEntry{
			DebtID:   d.ID,
			Debtor:   d.Debtor,
			Creditor: d.Creditor,
			Action:   "collect",
			Amount:   d.Amount,
			Success:  true,
			Detail:   fmt.Sprintf("auto-collect success on attempt %d", d.CollectAttempts),
			CreatedAt: now,
		}
		_ = m.storage.AddLiquidationAudit(audit)

		log.Printf("[debt] auto-collect success: debtor=%s debt=%d attempt=%d", d.Debtor, d.ID, d.CollectAttempts)
	} else {
		evt := &model.DebtLedgerEvent{
			DebtID:       d.ID,
			Debtor:       d.Debtor,
			Creditor:     d.Creditor,
			EventType:    model.DebtEventCollectFail,
			Amount:       d.Amount,
			ResourceType: d.ResourceType,
			ResourceKey:  d.ResourceKey,
			Detail:       fmt.Sprintf("auto-collect failed: debt %d, attempt %d", d.ID, d.CollectAttempts),
			CreatedAt:    now,
		}
		_ = m.storage.CreateDebtLedgerEvent(evt)

		audit := &model.LiquidationAuditEntry{
			DebtID:   d.ID,
			Debtor:   d.Debtor,
			Creditor: d.Creditor,
			Action:   "collect",
			Amount:   d.Amount,
			Success:  false,
			Detail:   fmt.Sprintf("auto-collect failed on attempt %d/%d", d.CollectAttempts, maxRetries),
			CreatedAt: now,
		}
		_ = m.storage.AddLiquidationAudit(audit)

		log.Printf("[debt] auto-collect failed: debtor=%s debt=%d attempt=%d/%d", d.Debtor, d.ID, d.CollectAttempts, maxRetries)
	}
}

func (m *Manager) writeOffDebt(d *model.DebtRecord, now time.Time, reason string) {
	d.Status = model.DebtStatusWrittenOff
	d.WriteOffAt = now
	d.UpdatedAt = now
	_ = m.storage.UpdateDebtRecord(d)

	evt := &model.DebtLedgerEvent{
		DebtID:       d.ID,
		Debtor:       d.Debtor,
		Creditor:     d.Creditor,
		EventType:    model.DebtEventWriteOff,
		Amount:       d.Amount,
		ResourceType: d.ResourceType,
		ResourceKey:  d.ResourceKey,
		Detail:       fmt.Sprintf("debt %d written off: %s", d.ID, reason),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	audit := &model.LiquidationAuditEntry{
		DebtID:   d.ID,
		Debtor:   d.Debtor,
		Creditor: d.Creditor,
		Action:   "write_off",
		Amount:   d.Amount,
		Success:  true,
		Detail:   reason,
		CreatedAt: now,
	}
	_ = m.storage.AddLiquidationAudit(audit)

	log.Printf("[debt] written off: debtor=%s debt=%d reason=%s", d.Debtor, d.ID, reason)
}

func (m *Manager) applyRestrictionsIfNeeded(debtor string, now time.Time) {
	rule := m.rules[debtor]
	if rule == nil {
		return
	}

	overdueCount, err := m.storage.CountOverdueDebts(debtor)
	if err != nil {
		return
	}

	if overdueCount < rule.OverdueThreshold {
		return
	}

	existing, _ := m.storage.ListActiveDebtRestrictions(debtor)
	if len(existing) > 0 {
		return
	}

	restriction := &model.DebtRestriction{
		CallerID:         debtor,
		RestrictionType:  model.RestrictionType(rule.RestrictionType),
		Scope:            model.RestrictionScope(rule.RestrictionScope),
		OverdueThreshold: rule.OverdueThreshold,
		Reason:           fmt.Sprintf("overdue debts (%d) reached threshold (%d)", overdueCount, rule.OverdueThreshold),
		Active:           true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := m.storage.CreateDebtRestriction(restriction); err != nil {
		log.Printf("[debt] failed to create restriction: %v", err)
		return
	}

	evt := &model.DebtLedgerEvent{
		Debtor:       debtor,
		EventType:    model.DebtEventRestrict,
		Detail:       fmt.Sprintf("restricted: type=%s scope=%s, overdue=%d threshold=%d", rule.RestrictionType, rule.RestrictionScope, overdueCount, rule.OverdueThreshold),
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	audit := &model.LiquidationAuditEntry{
		DebtID:   0,
		Debtor:   debtor,
		Creditor: "system",
		Action:   "restrict",
		Amount:   overdueCount,
		Success:  true,
		Detail:   fmt.Sprintf("caller restricted: type=%s scope=%s overdue=%d", rule.RestrictionType, rule.RestrictionScope, overdueCount),
		CreatedAt: now,
	}
	_ = m.storage.AddLiquidationAudit(audit)

	log.Printf("[debt] restriction applied: caller=%s type=%s scope=%s overdue=%d", debtor, rule.RestrictionType, rule.RestrictionScope, overdueCount)
}

func (m *Manager) tryLiftRestrictions(debtor string, now time.Time) {
	overdueCount, err := m.storage.CountOverdueDebts(debtor)
	if err != nil || overdueCount > 0 {
		return
	}

	restrictions, err := m.storage.ListActiveDebtRestrictions(debtor)
	if err != nil || len(restrictions) == 0 {
		return
	}

	for _, r := range restrictions {
		_ = m.storage.LiftDebtRestriction(r.ID, now)

		evt := &model.DebtLedgerEvent{
			Debtor:    debtor,
			EventType: model.DebtEventRestrictLift,
			Detail:    fmt.Sprintf("restriction lifted: type=%s scope=%s, all overdue debts resolved", r.RestrictionType, r.Scope),
			CreatedAt: now,
		}
		_ = m.storage.CreateDebtLedgerEvent(evt)

		audit := &model.LiquidationAuditEntry{
			DebtID:   0,
			Debtor:   debtor,
			Creditor: "system",
			Action:   "lift_restriction",
			Amount:   0,
			Success:  true,
			Detail:   fmt.Sprintf("restriction lifted: type=%s scope=%s", r.RestrictionType, r.Scope),
			CreatedAt: now,
		}
		_ = m.storage.AddLiquidationAudit(audit)

		log.Printf("[debt] restriction lifted: caller=%s", debtor)
	}
}

func (m *Manager) SetLiquidationRule(callerID string, gracePeriodSec, overdueThreshold int, restrictionType, restrictionScope string, maxCollectRetries, protectionAfter int) (*model.LiquidationRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := &model.LiquidationRule{
		CallerID:          callerID,
		GracePeriodSec:    gracePeriodSec,
		OverdueThreshold:  overdueThreshold,
		RestrictionType:   restrictionType,
		RestrictionScope:  restrictionScope,
		MaxCollectRetries: maxCollectRetries,
		ProtectionAfter:   protectionAfter,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := m.storage.CreateLiquidationRule(rule); err != nil {
		return nil, err
	}
	m.rules[callerID] = rule
	log.Printf("[debt] liquidation rule set: caller=%s grace=%ds threshold=%d type=%s scope=%s", callerID, gracePeriodSec, overdueThreshold, restrictionType, restrictionScope)
	return rule, nil
}

func (m *Manager) GetLiquidationRule(callerID string) (*model.LiquidationRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rule, ok := m.rules[callerID]
	if !ok {
		return nil, fmt.Errorf("liquidation rule not found for caller: %s", callerID)
	}
	return rule, nil
}

func (m *Manager) ListLiquidationRules() ([]model.LiquidationRule, error) {
	return m.storage.ListLiquidationRules()
}

func (m *Manager) DeleteLiquidationRule(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.storage.DeleteLiquidationRule(callerID); err != nil {
		return err
	}
	delete(m.rules, callerID)
	log.Printf("[debt] liquidation rule deleted: caller=%s", callerID)
	return nil
}

func (m *Manager) GetDebtRecord(id int64) (*model.DebtRecord, error) {
	return m.storage.GetDebtRecord(id)
}

func (m *Manager) ListDebtRecords(debtor, status string) ([]model.DebtRecord, error) {
	return m.storage.ListDebtRecords(debtor, status)
}

func (m *Manager) GetDebtTimeline(debtID int64) ([]model.DebtTimelineEntry, error) {
	events, err := m.storage.ListDebtLedgerEvents("", debtID, 100)
	if err != nil {
		return nil, err
	}
	var timeline []model.DebtTimelineEntry
	for _, e := range events {
		timeline = append(timeline, model.DebtTimelineEntry{
			ID:        e.ID,
			EventType: e.EventType,
			Amount:    e.Amount,
			Detail:    e.Detail,
			CreatedAt: e.CreatedAt,
		})
	}
	return timeline, nil
}

func (m *Manager) GetCallerDebtSummary(callerID string) (*model.CallerDebtSummary, error) {
	totalDebt, _ := m.storage.SumActiveDebt(callerID)
	totalCredit, _ := m.storage.SumActiveCredit(callerID)
	activeDebts, _ := m.storage.CountDebtsByStatus(callerID, string(model.DebtStatusActive))
	overdueDebts, _ := m.storage.CountDebtsByStatus(callerID, string(model.DebtStatusOverdue))
	collectedDebts, _ := m.storage.CountDebtsByStatus(callerID, string(model.DebtStatusCollected))
	restrictions, _ := m.storage.ListActiveDebtRestrictions(callerID)
	lastCollectResult, _ := m.storage.GetLastCollectResult(callerID)
	affectedResources, _ := m.storage.GetAffectedResources(callerID)

	summary := &model.CallerDebtSummary{
		CallerID:          callerID,
		TotalDebt:         totalDebt,
		TotalCredit:       totalCredit,
		ActiveDebts:       activeDebts,
		OverdueDebts:      overdueDebts,
		CollectedDebts:    collectedDebts,
		Restricted:        len(restrictions) > 0,
		Restrictions:      restrictions,
		LastCollectResult: lastCollectResult,
		AffectedResources: affectedResources,
	}
	return summary, nil
}

func (m *Manager) ListLedgerEvents(debtor string, limit int) ([]model.DebtLedgerEvent, error) {
	return m.storage.ListDebtLedgerEvents(debtor, 0, limit)
}

func (m *Manager) ListLiquidationAudit(debtor string, limit int) ([]model.LiquidationAuditEntry, error) {
	return m.storage.ListLiquidationAudit(debtor, limit)
}

func (m *Manager) ListRestrictions(callerID string) ([]model.DebtRestriction, error) {
	return m.storage.ListActiveDebtRestrictions(callerID)
}

func (m *Manager) LiftRestriction(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if err := m.storage.LiftDebtRestriction(id, now); err != nil {
		return err
	}
	log.Printf("[debt] restriction lifted: id=%d", id)
	return nil
}

func (m *Manager) ManualCollect(debtID int64) (*model.DebtRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, err := m.storage.GetDebtRecord(debtID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("debt not found: %d", debtID)
	}
	if d.Status != model.DebtStatusActive && d.Status != model.DebtStatusOverdue {
		return nil, fmt.Errorf("debt is not active or overdue: %s", d.Status)
	}

	now := time.Now()
	d.CollectAttempts++
	d.LastCollectAt = now
	d.UpdatedAt = now

	if d.Creditor != "system" && m.rateMgr != nil {
		err := m.rateMgr.ReturnTokens(d.Debtor, d.Amount)
		if err == nil {
			d.Status = model.DebtStatusCollected
			d.CollectedAt = now
		}
	} else {
		d.Status = model.DebtStatusCollected
		d.CollectedAt = now
	}
	_ = m.storage.UpdateDebtRecord(d)

	eventType := model.DebtEventCollect
	detail := fmt.Sprintf("manual collect success, attempt %d", d.CollectAttempts)
	if d.Status != model.DebtStatusCollected {
		eventType = model.DebtEventCollectFail
		detail = fmt.Sprintf("manual collect failed, attempt %d", d.CollectAttempts)
	}

	evt := &model.DebtLedgerEvent{
		DebtID:       d.ID,
		Debtor:       d.Debtor,
		Creditor:     d.Creditor,
		EventType:    eventType,
		Amount:       d.Amount,
		ResourceType: d.ResourceType,
		ResourceKey:  d.ResourceKey,
		Detail:       detail,
		CreatedAt:    now,
	}
	_ = m.storage.CreateDebtLedgerEvent(evt)

	audit := &model.LiquidationAuditEntry{
		DebtID:   d.ID,
		Debtor:   d.Debtor,
		Creditor: d.Creditor,
		Action:   "manual_collect",
		Amount:   d.Amount,
		Success:  d.Status == model.DebtStatusCollected,
		Detail:   detail,
		CreatedAt: now,
	}
	_ = m.storage.AddLiquidationAudit(audit)

	if d.Status == model.DebtStatusCollected {
		m.tryLiftRestrictions(d.Debtor, now)
	}

	return d, nil
}

func (m *Manager) GetDefaultGracePeriod() int {
	return 60
}
