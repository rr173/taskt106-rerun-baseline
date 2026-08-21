package audit

import (
	"fmt"
	"log"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"strings"
	"sync"
	"time"
)

var ErrCircuitBreakerOpen = fmt.Errorf("circuit_breaker_open")

type Manager struct {
	storage      *storage.Storage
	lockMgr      *lock.Manager
	rateLimitMgr *ratelimit.Manager

	mu           sync.Mutex
	rules        map[string]*model.CircuitBreakerRule
	defaultRule  *model.CircuitBreakerRule
	statuses     map[string]*model.CircuitBreakerStatus
	stopCh       chan struct{}
	ticker       *time.Ticker

	shadowShadowEvaluateFn func(caller, op, resource string, success bool, failReason string)
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager) *Manager {
	return &Manager{
		storage:      s,
		lockMgr:      lm,
		rateLimitMgr: rlm,
		rules:        make(map[string]*model.CircuitBreakerRule),
		statuses:     make(map[string]*model.CircuitBreakerStatus),
		stopCh:       make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rules, err := m.storage.ListCircuitBreakerRules()
	if err != nil {
		return fmt.Errorf("load circuit breaker rules: %w", err)
	}
	for i := range rules {
		r := &rules[i]
		if r.CallerID == "" {
			m.defaultRule = r
		} else {
			m.rules[r.CallerID] = r
		}
	}

	statuses, err := m.storage.ListCircuitBreakerStatuses("")
	if err != nil {
		return fmt.Errorf("load circuit breaker statuses: %w", err)
	}
	now := time.Now()
	for i := range statuses {
		st := &statuses[i]
		if st.State == model.CircuitBreakerOpen && !st.ExpiresAt.IsZero() && st.ExpiresAt.Before(now) {
			log.Printf("[audit] circuit breaker expired on startup: caller=%s", st.CallerID)
			m.recoverCircuitBreakerLocked(st, "cooldown_expired_on_startup")
		} else {
			m.statuses[st.CallerID] = st
		}
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.cooldownLoop()

	log.Println("[audit-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[audit-manager] stopped")
}

func (m *Manager) cooldownLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.checkCooldownExpiry()
		}
	}
}

func (m *Manager) checkCooldownExpiry() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, st := range m.statuses {
		if st.State == model.CircuitBreakerOpen && !st.ExpiresAt.IsZero() && st.ExpiresAt.Before(now) {
			log.Printf("[audit] circuit breaker cooldown expired: caller=%s", st.CallerID)
			m.recoverCircuitBreakerLocked(st, "cooldown_expired")
		}
	}
}

func (m *Manager) IsCircuitBreakerOpen(caller string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.statuses[caller]
	if !ok {
		return false
	}
	if st.State == model.CircuitBreakerOpen {
		if !st.ExpiresAt.IsZero() && st.ExpiresAt.Before(time.Now()) {
			m.recoverCircuitBreakerLocked(st, "cooldown_expired_on_check")
			return false
		}
		return true
	}
	return false
}

func (m *Manager) SetShadowEvaluator(fn func(caller, op, resource string, success bool, failReason string)) {
	m.shadowShadowEvaluateFn = fn
}

func (m *Manager) recordAuditLog(caller string, op model.AuditOperationType, resource string, success bool, failReason string) {
	logEntry := &model.AuditLog{
		Timestamp:  time.Now(),
		Caller:     caller,
		Operation:  op,
		Resource:   resource,
		Success:    success,
		FailReason: failReason,
	}
	if err := m.storage.AddAuditLog(logEntry); err != nil {
		log.Printf("[audit] failed to add audit log: %v", err)
	}

	if m.shadowShadowEvaluateFn != nil {
		m.shadowShadowEvaluateFn(caller, string(op), resource, success, failReason)
	}
}

func (m *Manager) checkAndTriggerCircuitBreaker(caller string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rule := m.getRuleForCallerLocked(caller)
	if rule == nil {
		return
	}

	now := time.Now()
	failures, err := m.storage.CountFailuresInWindow(caller, rule.WindowSec, now)
	if err != nil {
		log.Printf("[audit] failed to count failures: %v", err)
		return
	}

	st, ok := m.statuses[caller]
	if !ok {
		st = &model.CircuitBreakerStatus{
			CallerID:  caller,
			State:     model.CircuitBreakerClosed,
			UpdatedAt: now,
		}
		m.statuses[caller] = st
	}
	st.FailuresInWindow = failures
	st.UpdatedAt = now

	if failures >= rule.FailureThreshold && st.State != model.CircuitBreakerOpen {
		triggerReason := fmt.Sprintf("failure count %d reached threshold %d in %d seconds",
			failures, rule.FailureThreshold, rule.WindowSec)
		st.State = model.CircuitBreakerOpen
		st.TriggeredAt = now
		st.ExpiresAt = now.Add(time.Duration(rule.CooldownSec) * time.Second)
		st.TriggerReason = triggerReason

		history := &model.CircuitBreakerHistory{
			CallerID:      caller,
			State:         "open",
			TriggeredAt:   now,
			TriggerReason: triggerReason,
		}
		if err := m.storage.AddCircuitBreakerHistory(history); err != nil {
			log.Printf("[audit] failed to add cb history: %v", err)
		}

		log.Printf("[audit] circuit breaker OPENED: caller=%s reason=%s", caller, triggerReason)
	}

	_ = m.storage.UpsertCircuitBreakerStatus(st)
}

func (m *Manager) getRuleForCallerLocked(caller string) *model.CircuitBreakerRule {
	if rule, ok := m.rules[caller]; ok {
		return rule
	}
	return m.defaultRule
}

func (m *Manager) recoverCircuitBreakerLocked(st *model.CircuitBreakerStatus, reason string) {
	now := time.Now()
	st.State = model.CircuitBreakerClosed
	st.FailuresInWindow = 0
	st.TriggerReason = ""
	st.UpdatedAt = now
	st.TriggeredAt = time.Time{}
	st.ExpiresAt = time.Time{}

	history, err := m.storage.GetLatestOpenHistory(st.CallerID)
	if err == nil && history != nil {
		_ = m.storage.UpdateCircuitBreakerHistoryRecover(history.ID, now, reason)
	}

	_ = m.storage.UpsertCircuitBreakerStatus(st)
	log.Printf("[audit] circuit breaker CLOSED: caller=%s reason=%s", st.CallerID, reason)
}

func (m *Manager) AcquireLock(lockName, holder string, leaseSec int, reentrant bool) (*lock.AcquireResult, error) {
	if m.IsCircuitBreakerOpen(holder) {
		m.recordAuditLog(holder, model.AuditOpAcquireLock, lockName, false, ErrCircuitBreakerOpen.Error())
		return nil, ErrCircuitBreakerOpen
	}

	result, err := m.lockMgr.AcquireLock(lockName, holder, leaseSec, reentrant)

	success := err == nil && result != nil && (result.Acquired || result.Queued)
	failReason := ""
	if err != nil {
		failReason = err.Error()
	} else if result != nil && !result.Acquired && !result.Queued {
		failReason = "not acquired"
	}
	m.recordAuditLog(holder, model.AuditOpAcquireLock, lockName, success, failReason)

	if !success {
		m.checkAndTriggerCircuitBreaker(holder)
	}

	return result, err
}

func (m *Manager) RenewLock(lockName, holder string, addSec int) (*model.Lease, error) {
	if m.IsCircuitBreakerOpen(holder) {
		m.recordAuditLog(holder, model.AuditOpRenewLock, lockName, false, ErrCircuitBreakerOpen.Error())
		return nil, ErrCircuitBreakerOpen
	}

	lease, err := m.lockMgr.RenewLease(lockName, holder, addSec)

	success := err == nil && lease != nil
	failReason := ""
	if err != nil {
		failReason = err.Error()
	}
	m.recordAuditLog(holder, model.AuditOpRenewLock, lockName, success, failReason)

	if !success {
		m.checkAndTriggerCircuitBreaker(holder)
	}

	return lease, err
}

func (m *Manager) AcquireLocksBatch(lockNames []string, holder string, leaseSec int, reentrant bool) (*model.BatchAcquireResult, error) {
	if m.IsCircuitBreakerOpen(holder) {
		resource := "batch:" + strings.Join(lockNames, ",")
		m.recordAuditLog(holder, model.AuditOpAcquireLocksBatch, resource, false, ErrCircuitBreakerOpen.Error())
		return nil, ErrCircuitBreakerOpen
	}

	result, err := m.lockMgr.AcquireLocksBatch(lockNames, holder, leaseSec, reentrant)

	success := err == nil && result != nil && result.Acquired
	failReason := ""
	if err != nil {
		failReason = err.Error()
	} else if result != nil && !result.Acquired {
		failReason = fmt.Sprintf("batch failed on lock: %s (held by %s)", result.FailedLock, result.FailedBy)
	}
	resource := "batch:" + strings.Join(lockNames, ",")
	m.recordAuditLog(holder, model.AuditOpAcquireLocksBatch, resource, success, failReason)

	if !success {
		m.checkAndTriggerCircuitBreaker(holder)
	}

	return result, err
}

func (m *Manager) ReleaseLock(lockName, holder string) (*lock.ReleaseResult, error) {
	result, err := m.lockMgr.ReleaseLock(lockName, holder)

	success := err == nil && result != nil && result.Released
	failReason := ""
	if err != nil {
		failReason = err.Error()
	}
	m.recordAuditLog(holder, model.AuditOpReleaseLock, lockName, success, failReason)

	return result, err
}

func (m *Manager) RequestTokens(callerID string, tokens int, waitable bool, waitSec int) (*model.TokenResult, error) {
	if m.IsCircuitBreakerOpen(callerID) {
		m.recordAuditLog(callerID, model.AuditOpRequestTokens, callerID, false, ErrCircuitBreakerOpen.Error())
		return nil, ErrCircuitBreakerOpen
	}

	result, err := m.rateLimitMgr.RequestTokens(callerID, tokens, waitable, waitSec)

	success := err == nil && result != nil && result.Allowed
	failReason := ""
	if err != nil {
		failReason = err.Error()
	} else if result != nil && !result.Allowed {
		failReason = result.Reason
		if failReason == "" {
			failReason = "rate limited"
		}
	}
	m.recordAuditLog(callerID, model.AuditOpRequestTokens, callerID, success, failReason)

	if !success {
		m.checkAndTriggerCircuitBreaker(callerID)
	}

	return result, err
}

func (m *Manager) ReturnTokens(callerID string, tokens int) error {
	err := m.rateLimitMgr.ReturnTokens(callerID, tokens)

	success := err == nil
	failReason := ""
	if err != nil {
		failReason = err.Error()
	}
	m.recordAuditLog(callerID, model.AuditOpReturnTokens, callerID, success, failReason)

	return err
}

func (m *Manager) BorrowQuota(fromCaller, toCaller string, amount int) (*model.BorrowResult, error) {
	if m.IsCircuitBreakerOpen(toCaller) {
		m.recordAuditLog(toCaller, model.AuditOpBorrowQuota, fromCaller+"->"+toCaller, false, ErrCircuitBreakerOpen.Error())
		return nil, ErrCircuitBreakerOpen
	}

	result, err := m.rateLimitMgr.BorrowQuota(fromCaller, toCaller, amount)

	success := err == nil && result != nil && result.Success
	failReason := ""
	if err != nil {
		failReason = err.Error()
	} else if result != nil && !result.Success {
		failReason = result.Message
	}
	m.recordAuditLog(toCaller, model.AuditOpBorrowQuota, fromCaller+"->"+toCaller, success, failReason)

	if !success {
		m.checkAndTriggerCircuitBreaker(toCaller)
	}

	return result, err
}

func (m *Manager) ReturnQuota(fromCaller, toCaller string, amount int) (*model.BorrowResult, error) {
	result, err := m.rateLimitMgr.ReturnQuota(fromCaller, toCaller, amount)

	success := err == nil && result != nil && result.Success
	failReason := ""
	if err != nil {
		failReason = err.Error()
	} else if result != nil && !result.Success {
		failReason = result.Message
	}
	m.recordAuditLog(fromCaller, model.AuditOpReturnQuota, fromCaller+"->"+toCaller, success, failReason)

	return result, err
}

func (m *Manager) AdjustQuota(callerID string, newQuotaLimit int) error {
	if m.IsCircuitBreakerOpen(callerID) {
		m.recordAuditLog(callerID, model.AuditOpAdjustQuota, callerID, false, ErrCircuitBreakerOpen.Error())
		return ErrCircuitBreakerOpen
	}

	err := m.rateLimitMgr.AdjustQuota(callerID, newQuotaLimit)

	success := err == nil
	failReason := ""
	if err != nil {
		failReason = err.Error()
	}
	m.recordAuditLog(callerID, model.AuditOpAdjustQuota, callerID, success, failReason)

	return err
}

func (m *Manager) SetCircuitBreakerRule(callerID string, windowSec, failureThreshold, cooldownSec int) (*model.CircuitBreakerRule, error) {
	if windowSec <= 0 || failureThreshold <= 0 || cooldownSec <= 0 {
		return nil, fmt.Errorf("window_sec, failure_threshold and cooldown_sec must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	rule := &model.CircuitBreakerRule{
		CallerID:         callerID,
		WindowSec:        windowSec,
		FailureThreshold: failureThreshold,
		CooldownSec:      cooldownSec,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if err := m.storage.CreateCircuitBreakerRule(rule); err != nil {
		return nil, err
	}

	if callerID == "" {
		m.defaultRule = rule
	} else {
		m.rules[callerID] = rule
	}

	return rule, nil
}

func (m *Manager) GetCircuitBreakerRule(callerID string) (*model.CircuitBreakerRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if callerID == "" {
		return m.defaultRule, nil
	}
	rule, ok := m.rules[callerID]
	if !ok {
		return nil, fmt.Errorf("rule not found for caller: %s", callerID)
	}
	return rule, nil
}

func (m *Manager) ListCircuitBreakerRules() ([]model.CircuitBreakerRule, error) {
	return m.storage.ListCircuitBreakerRules()
}

func (m *Manager) DeleteCircuitBreakerRule(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.storage.DeleteCircuitBreakerRule(callerID); err != nil {
		return err
	}
	if callerID == "" {
		m.defaultRule = nil
	} else {
		delete(m.rules, callerID)
	}
	return nil
}

func (m *Manager) ManuallyCloseCircuitBreaker(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.statuses[callerID]
	if !ok || st.State != model.CircuitBreakerOpen {
		return fmt.Errorf("circuit breaker is not open for caller: %s", callerID)
	}

	m.recoverCircuitBreakerLocked(st, "manual_reset")
	return nil
}

func (m *Manager) GetCircuitBreakerStatus(callerID string) (*model.CircuitBreakerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.statuses[callerID]
	if !ok {
		return &model.CircuitBreakerStatus{
			CallerID:         callerID,
			State:            model.CircuitBreakerClosed,
			FailuresInWindow: 0,
		}, nil
	}
	if st.State == model.CircuitBreakerOpen && !st.ExpiresAt.IsZero() && st.ExpiresAt.Before(time.Now()) {
		m.recoverCircuitBreakerLocked(st, "cooldown_expired_on_get")
	}
	return st, nil
}

func (m *Manager) ListOpenCircuitBreakers() ([]model.CircuitBreakerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var open []model.CircuitBreakerStatus
	for _, st := range m.statuses {
		if st.State == model.CircuitBreakerOpen {
			if !st.ExpiresAt.IsZero() && st.ExpiresAt.Before(now) {
				m.recoverCircuitBreakerLocked(st, "cooldown_expired_on_list")
				continue
			}
			open = append(open, *st)
		}
	}
	return open, nil
}

func (m *Manager) ListAllCircuitBreakerStatuses() ([]model.CircuitBreakerStatus, error) {
	return m.storage.ListCircuitBreakerStatuses("")
}

func (m *Manager) GetCircuitBreakerHistory(callerID string, limit int) ([]model.CircuitBreakerHistory, error) {
	return m.storage.ListCircuitBreakerHistory(callerID, limit)
}

func (m *Manager) QueryAuditLogs(caller, resource string, success *bool, startTime, endTime time.Time, page, pageSize int) (*model.PaginatedAuditLogs, error) {
	logs, total, err := m.storage.QueryAuditLogs(caller, resource, success, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	return &model.PaginatedAuditLogs{
		Logs:     logs,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

func (m *Manager) GetCallerStats(callerID string) (*model.CallerStats, error) {
	return m.storage.GetCallerStats(callerID, time.Now())
}

func (m *Manager) GetAllCallerStats() ([]model.CallerStats, error) {
	return m.storage.GetAllCallerStats(time.Now())
}

func (m *Manager) GetGlobalStats() (*model.GlobalAuditStats, error) {
	return m.storage.GetGlobalAuditStats(time.Now())
}
