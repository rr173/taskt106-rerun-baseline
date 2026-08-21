package lock

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"task106/internal/lockbudget"
	"task106/internal/model"
	"task106/internal/storage"
	"time"
)

const (
	WaitTimeoutSec = 30
)

type Manager struct {
	storage           *storage.Storage
	mu                sync.Mutex
	timers            map[string]*time.Timer
	stopCh            chan struct{}
	stopOnce          sync.Once
	heatmap           HeatmapRecorder
	acceleratedGrants map[string]int
	heatmapMgr        HeatmapCooldownManager
	budgetMgr         *lockbudget.Manager
	reputationChecker ReputationChecker
	admissionGuard    AdmissionGuard
	fencingIssuer     FencingIssuer
}

type HeatmapCooldownManager interface {
	IncrementLeaseShortenedCount(lockName string) error
}

type HeatmapRecorder interface {
	RecordLockRequest(lockName string)
	RecordLockEnqueue(lockName, holder string)
	RecordLockGranted(lockName, holder string)
	RecordLockGrantedWithEnqueue(lockName, holder string, enqueuedAt time.Time)
	RecordLockTimeout(lockName, holder string)
	RecordLockTimeoutWithEnqueue(lockName, holder string, enqueuedAt time.Time)
	RecordLockRequestWithWait(lockName string, waitMs int64)
}

type ReputationChecker interface {
	ShouldPrioritizeInQueue(callerID string) bool
	CheckBronzeLockLimit(callerID string, currentHeldLocks int, configuredMax int) (bool, string)
	IsBronze(callerID string) bool
}

type AdmissionGuard interface {
	BeforeAcquire(lockName, holder string, leaseSec int) error
}

type FencingIssuer interface {
	Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error)
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage:           s,
		timers:            make(map[string]*time.Timer),
		stopCh:            make(chan struct{}),
		acceleratedGrants: make(map[string]int),
	}
}

func (m *Manager) SetHeatmapCooldownManager(h HeatmapCooldownManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heatmapMgr = h
}

func (m *Manager) SetHeatmap(h HeatmapRecorder) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heatmap = h
}

func (m *Manager) SetBudgetManager(b *lockbudget.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budgetMgr = b
}

func (m *Manager) SetReputationChecker(rc ReputationChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reputationChecker = rc
}

func (m *Manager) SetAdmissionGuard(guard AdmissionGuard) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.admissionGuard = guard
}

func (m *Manager) SetFencingIssuer(issuer FencingIssuer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fencingIssuer = issuer
}

func (m *Manager) BudgetManager() *lockbudget.Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.budgetMgr
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.rebuildTimersLocked(); err != nil {
		return fmt.Errorf("rebuild timers: %w", err)
	}
	go m.watchWaitQueue()
	log.Println("[lock-manager] started")
	return nil
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.timers {
		t.Stop()
	}
	log.Println("[lock-manager] stopped")
}

func (m *Manager) rebuildTimersLocked() error {
	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		return err
	}

	now := time.Now()
	for _, lease := range leases {
		l := lease
		if l.ExpiresAt.Before(now) {
			log.Printf("[lock-manager] lease expired on startup: lock=%s holder=%s", l.LockName, l.Holder)
			m.expireLockLocked(l.LockName)
		} else {
			duration := time.Until(l.ExpiresAt)
			m.setLeaseTimerLocked(l.LockName, duration)
			log.Printf("[lock-manager] rebuilt lease timer: lock=%s holder=%s remaining=%.1fs", l.LockName, l.Holder, duration.Seconds())
		}
	}
	return nil
}

func (m *Manager) setLeaseTimerLocked(lockName string, duration time.Duration) {
	if t, ok := m.timers[lockName]; ok {
		t.Stop()
	}

	m.timers[lockName] = time.AfterFunc(duration, func() {
		log.Printf("[lock-manager] lease expired: lock=%s", lockName)
		m.expireLock(lockName)
	})
}

func (m *Manager) stopLeaseTimerLocked(lockName string) {
	if t, ok := m.timers[lockName]; ok {
		t.Stop()
		delete(m.timers, lockName)
	}
}

type AcquireResult struct {
	Acquired          bool
	Queued            bool
	Lock              *model.Lock
	Lease             *model.Lease
	Position          int
	Deadlock          bool
	DeadlockCycle     *model.DeadlockCycle
	BudgetRejected    bool
	BudgetCheckResult *model.BudgetAcquireCheckResult
}

func (m *Manager) AcquireLock(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
	if leaseSec <= 0 {
		return nil, fmt.Errorf("lease_sec must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.acquireLockLocked(lockName, holder, leaseSec, reentrant)
}

func (m *Manager) acquireLockLocked(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
	if m.admissionGuard != nil {
		if err := m.admissionGuard.BeforeAcquire(lockName, holder, leaseSec); err != nil {
			return nil, err
		}
	}
	if m.heatmap != nil {
		m.heatmap.RecordLockRequest(lockName)
	}

	if m.budgetMgr != nil {
		checkResult, err := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
		if err != nil {
			log.Printf("[lock-manager] budget check error: holder=%s lock=%s err=%v", holder, lockName, err)
		}
		if checkResult != nil && !checkResult.Allowed {
			m.addHistoryLocked(lockName, holder, model.OpAcquire,
				fmt.Sprintf("rejected: budget exhausted (consumed=%d, limit=%d, remaining=%d)",
					checkResult.ConsumedUnits, checkResult.BudgetLimit, checkResult.RemainingUnits))
			return &AcquireResult{
				BudgetRejected:    true,
				BudgetCheckResult: checkResult,
			}, nil
		}
	}

	if m.reputationChecker != nil && m.budgetMgr != nil {
		heldLocks, _ := m.storage.CountActiveLeasesByHolder(holder)
		budgetCfg, _ := m.storage.GetLockBudgetConfig(holder)
		configuredMax := 0
		if budgetCfg != nil {
			configuredMax = budgetCfg.MaxConcurrentLocks
		}
		if configuredMax > 0 {
			allowed, reason := m.reputationChecker.CheckBronzeLockLimit(holder, heldLocks, configuredMax)
			if !allowed {
				m.addHistoryLocked(lockName, holder, model.OpAcquire, "rejected: "+reason)
				return &AcquireResult{
					BudgetRejected: false,
				}, fmt.Errorf("rejected: %s", reason)
			}
		}
	}

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	if lock == nil {
		lock = &model.Lock{
			Name:      lockName,
			Status:    model.LockStatusFree,
			Holder:    "",
			Reentrant: reentrant,
			Count:     0,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := m.storage.UpsertLock(lock); err != nil {
			return nil, err
		}
	}

	if lock.Status == model.LockStatusHeld {
		if lock.Holder == holder {
			if lock.Reentrant && reentrant {
				lock.Count++
				lock.UpdatedAt = now
				if err := m.storage.UpsertLock(lock); err != nil {
					return nil, err
				}
				m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("reentrant acquire, count=%d", lock.Count))
				lease, _ := m.storage.GetActiveLease(lockName)
				fillLeaseRemaining(lease)
				result := &AcquireResult{Acquired: true, Lock: lock, Lease: lease}
				if m.budgetMgr != nil {
					check, _ := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
					result.BudgetCheckResult = check
				}
				return result, nil
			}
			return nil, fmt.Errorf("already hold this lock (non-reentrant)")
		}

		deadlockCycle, err := m.checkDeadlockLocked(holder, lockName, lock.Holder)
		if err != nil {
			return nil, err
		}
		if deadlockCycle != nil {
			m.addHistoryLocked(lockName, holder, model.OpAcquire, "rejected: deadlock detected")
			return &AcquireResult{
				Deadlock:      true,
				DeadlockCycle: deadlockCycle,
				Lock:          lock,
			}, nil
		}

		queue, err := m.storage.ListWaitQueue(lockName)
		if err != nil {
			return nil, err
		}
		for _, item := range queue {
			if item.Holder == holder {
				m.addHistoryLocked(lockName, holder, model.OpAcquire, "rejected: already in queue")
				return nil, fmt.Errorf("already in wait queue for this lock")
			}
		}

		item := &model.WaitQueueItem{
			LockName:   lockName,
			Holder:     holder,
			Reentrant:  reentrant,
			LeaseSec:   leaseSec,
			EnqueuedAt: now,
			TimeoutAt:  now.Add(time.Duration(WaitTimeoutSec) * time.Second),
		}
		if err := m.storage.Enqueue(item); err != nil {
			return nil, err
		}

		if m.reputationChecker != nil && m.reputationChecker.ShouldPrioritizeInQueue(holder) {
			goldHolders := map[string]bool{holder: true}
			if err := m.storage.ReorderWaitQueueForGold(lockName, goldHolders); err != nil {
				log.Printf("[lock-manager] reorder queue for gold error: lock=%s holder=%s err=%v", lockName, holder, err)
			}
		}

		m.addHistoryLocked(lockName, holder, model.OpAcquire, "queued")
		if m.heatmap != nil {
			m.heatmap.RecordLockEnqueue(lockName, holder)
		}

		position := len(queue) + 1

		return &AcquireResult{Queued: true, Position: position, Lock: lock}, nil
	}

	effectiveLeaseSec := leaseSec
	accelerated := false
	if cooldownSec, ok := m.acceleratedGrants[lockName]; ok && cooldownSec > 0 {
		if effectiveLeaseSec > cooldownSec {
			effectiveLeaseSec = cooldownSec
			accelerated = true
		}
	}

	lock.Status = model.LockStatusHeld
	lock.Holder = holder
	lock.Reentrant = reentrant
	lock.Count = 1
	lock.UpdatedAt = now
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	lease := &model.Lease{
		LockName:   lockName,
		Holder:     holder,
		LeaseSec:   effectiveLeaseSec,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(effectiveLeaseSec) * time.Second),
		Active:     true,
	}
	if m.fencingIssuer != nil {
		token, err := m.fencingIssuer.Issue(lockName, holder, effectiveLeaseSec, now)
		if err != nil {
			return nil, fmt.Errorf("issue fencing token: %w", err)
		}
		lease.FencingToken = token
	}
	if err := m.storage.CreateLease(lease); err != nil {
		return nil, err
	}

	m.setLeaseTimerLocked(lockName, time.Duration(effectiveLeaseSec)*time.Second)

	if m.budgetMgr != nil {
		if err := m.budgetMgr.StartHolding(holder, lockName, lease.AcquiredAt, lease.ExpiresAt); err != nil {
			log.Printf("[lock-manager] start holding budget error: holder=%s lock=%s err=%v", holder, lockName, err)
		}
	}

	historyDetail := fmt.Sprintf("acquired, lease=%ds", effectiveLeaseSec)
	if accelerated {
		historyDetail += fmt.Sprintf(" (加速授予: 原租约%ds)", leaseSec)
		if m.heatmapMgr != nil {
			_ = m.heatmapMgr.IncrementLeaseShortenedCount(lockName)
		}
	}
	m.addHistoryLocked(lockName, holder, model.OpAcquire, historyDetail)

	result := &AcquireResult{Acquired: true, Lock: lock, Lease: lease}
	if m.budgetMgr != nil {
		check, _ := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
		result.BudgetCheckResult = check
	}
	fillLeaseRemaining(lease)
	return result, nil
}

type ReleaseResult struct {
	Released bool
	Count    int
	Granted  *model.Lock
}

func (m *Manager) ReleaseLock(lockName, holder string) (*ReleaseResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.releaseLockLocked(lockName, holder)
}

func (m *Manager) releaseLockLocked(lockName, holder string) (*ReleaseResult, error) {
	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("lock not found: %s", lockName)
	}

	if lock.Status != model.LockStatusHeld {
		return &ReleaseResult{Released: false}, nil
	}

	if lock.Holder != holder {
		return nil, fmt.Errorf("not the holder: current=%s", lock.Holder)
	}

	if lock.Reentrant && lock.Count > 1 {
		lock.Count--
		lock.UpdatedAt = time.Now()
		if err := m.storage.UpsertLock(lock); err != nil {
			return nil, err
		}
		m.addHistoryLocked(lockName, holder, model.OpRelease, fmt.Sprintf("reentrant release, count=%d", lock.Count))
		return &ReleaseResult{Released: true, Count: lock.Count}, nil
	}

	releaseTime := time.Now()
	if m.budgetMgr != nil {
		units, err := m.budgetMgr.StopHolding(holder, lockName, releaseTime)
		if err != nil {
			log.Printf("[lock-manager] stop holding budget error: holder=%s lock=%s err=%v", holder, lockName, err)
		}
		_ = units
	}

	m.stopLeaseTimerLocked(lockName)

	if err := m.storage.DeactivateLease(lockName); err != nil {
		return nil, err
	}

	lock.Status = model.LockStatusFree
	lock.Holder = ""
	lock.Count = 0
	lock.UpdatedAt = releaseTime
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	m.addHistoryLocked(lockName, holder, model.OpRelease, "released")

	grantedLock, err := m.tryGrantNextLocked(lockName)
	if err != nil {
		return nil, err
	}

	return &ReleaseResult{Released: true, Count: 0, Granted: grantedLock}, nil
}

func (m *Manager) tryGrantNextLocked(lockName string) (*model.Lock, error) {
	item, err := m.storage.Dequeue(lockName)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	now := time.Now()
	if item.TimeoutAt.Before(now) {
		m.addHistoryLocked(lockName, item.Holder, model.OpTimeout, "timed out before grant")
		if m.heatmap != nil {
			m.heatmap.RecordLockTimeoutWithEnqueue(lockName, item.Holder, item.EnqueuedAt)
		}
		return m.tryGrantNextLocked(lockName)
	}

	if m.budgetMgr != nil {
		checkResult, err := m.budgetMgr.CheckAcquire(item.Holder, lockName, item.LeaseSec)
		if err != nil {
			log.Printf("[lock-manager] budget check error on grant: holder=%s lock=%s err=%v", item.Holder, lockName, err)
		}
		if checkResult != nil && !checkResult.Allowed {
			m.addHistoryLocked(lockName, item.Holder, model.OpTimeout,
				fmt.Sprintf("skipped from queue: budget exhausted (consumed=%d, limit=%d, remaining=%d)",
					checkResult.ConsumedUnits, checkResult.BudgetLimit, checkResult.RemainingUnits))
			return m.tryGrantNextLocked(lockName)
		}
	}

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}

	leaseSec := item.LeaseSec
	accelerated := false
	if cooldownSec, ok := m.acceleratedGrants[lockName]; ok && cooldownSec > 0 {
		if leaseSec > cooldownSec {
			leaseSec = cooldownSec
			accelerated = true
		}
	}

	lock.Status = model.LockStatusHeld
	lock.Holder = item.Holder
	lock.Reentrant = item.Reentrant
	lock.Count = 1
	lock.UpdatedAt = now
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	lease := &model.Lease{
		LockName:   lockName,
		Holder:     item.Holder,
		LeaseSec:   leaseSec,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(leaseSec) * time.Second),
		Active:     true,
	}
	if m.fencingIssuer != nil {
		token, err := m.fencingIssuer.Issue(lockName, item.Holder, leaseSec, now)
		if err != nil {
			return nil, fmt.Errorf("issue fencing token: %w", err)
		}
		lease.FencingToken = token
	}
	if err := m.storage.CreateLease(lease); err != nil {
		return nil, err
	}

	m.setLeaseTimerLocked(lockName, time.Duration(leaseSec)*time.Second)

	if m.budgetMgr != nil {
		if err := m.budgetMgr.StartHolding(item.Holder, lockName, lease.AcquiredAt, lease.ExpiresAt); err != nil {
			log.Printf("[lock-manager] start holding budget error on grant: holder=%s lock=%s err=%v", item.Holder, lockName, err)
		}
	}

	grantDetail := fmt.Sprintf("granted from queue, lease=%ds", leaseSec)
	if accelerated {
		grantDetail += fmt.Sprintf(" (加速授予: 原租约%ds)", item.LeaseSec)
		if m.heatmapMgr != nil {
			_ = m.heatmapMgr.IncrementLeaseShortenedCount(lockName)
		}
	}
	m.addHistoryLocked(lockName, item.Holder, model.OpGrantNext, grantDetail)
	if m.heatmap != nil {
		m.heatmap.RecordLockGrantedWithEnqueue(lockName, item.Holder, item.EnqueuedAt)
	}

	return lock, nil
}

func (m *Manager) RenewLease(lockName, holder string, addSec int) (*model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, err := m.storage.GetActiveLease(lockName)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("no active lease for lock: %s", lockName)
	}

	if lease.Holder != holder {
		return nil, fmt.Errorf("not the lease holder: current=%s", lease.Holder)
	}

	now := time.Now()
	if lease.ExpiresAt.Before(now) {
		return nil, fmt.Errorf("lease already expired")
	}

	addSeconds := addSec
	if cooldownSec, ok := m.acceleratedGrants[lockName]; ok && cooldownSec > 0 {
		maxAllowedExpiry := now.Add(time.Duration(cooldownSec) * time.Second)
		newExpiresAt := lease.ExpiresAt.Add(time.Duration(addSec) * time.Second)
		if newExpiresAt.After(maxAllowedExpiry) {
			addSeconds = int(maxAllowedExpiry.Sub(now).Seconds())
			if addSeconds < 0 {
				addSeconds = 0
			}
		}
	}

	newExpiresAt := lease.ExpiresAt.Add(time.Duration(addSeconds) * time.Second)
	if err := m.storage.UpdateLeaseExpiry(lockName, newExpiresAt); err != nil {
		return nil, err
	}

	if m.budgetMgr != nil {
		if err := m.budgetMgr.RenewHolding(holder, lockName, newExpiresAt); err != nil {
			log.Printf("[lock-manager] renew holding budget error: holder=%s lock=%s err=%v", holder, lockName, err)
		}
	}

	remaining := time.Until(newExpiresAt)
	m.setLeaseTimerLocked(lockName, remaining)
	lease.ExpiresAt = newExpiresAt

	historyDetail := fmt.Sprintf("renewed +%ds", addSec)
	if addSeconds != addSec {
		historyDetail += fmt.Sprintf(" (降温状态限制续期, 实际+%ds)", addSeconds)
	}
	m.addHistoryLocked(lockName, holder, model.OpRenew, historyDetail)

	fillLeaseRemaining(lease)
	return lease, nil
}

func (m *Manager) expireLock(lockName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLockLocked(lockName)
}

func (m *Manager) expireLockLocked(lockName string) {
	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		log.Printf("[lock-manager] expireLock get lock error: %v", err)
		return
	}
	if lock == nil || lock.Status != model.LockStatusHeld {
		return
	}

	holder := lock.Holder
	expireTime := time.Now()

	if m.budgetMgr != nil {
		units, err := m.budgetMgr.StopHolding(holder, lockName, expireTime)
		if err != nil {
			log.Printf("[lock-manager] stop holding budget error on expire: holder=%s lock=%s err=%v", holder, lockName, err)
		}
		_ = units
	}

	delete(m.timers, lockName)

	if err := m.storage.DeactivateLease(lockName); err != nil {
		log.Printf("[lock-manager] deactivate lease error: %v", err)
		return
	}

	lock.Status = model.LockStatusExpired
	lock.Holder = ""
	lock.Count = 0
	lock.UpdatedAt = expireTime
	if err := m.storage.UpsertLock(lock); err != nil {
		log.Printf("[lock-manager] upsert lock error: %v", err)
		return
	}

	m.addHistoryLocked(lockName, holder, model.OpExpire, "lease expired")

	lock.Status = model.LockStatusFree
	lock.UpdatedAt = expireTime
	if err := m.storage.UpsertLock(lock); err != nil {
		log.Printf("[lock-manager] upsert lock free error: %v", err)
		return
	}

	if _, err := m.tryGrantNextLocked(lockName); err != nil {
		log.Printf("[lock-manager] tryGrantNext error: %v", err)
	}
}

func (m *Manager) watchWaitQueue() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkWaitQueueTimeouts()
		}
	}
}

func (m *Manager) checkWaitQueueTimeouts() {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, err := m.storage.ListAllWaitQueue()
	if err != nil {
		log.Printf("[lock-manager] list wait queue error: %v", err)
		return
	}

	now := time.Now()
	for _, item := range items {
		if item.TimeoutAt.Before(now) {
			if err := m.storage.RemoveFromQueueByID(item.ID); err != nil {
				log.Printf("[lock-manager] remove from queue error: %v", err)
				continue
			}
			m.addHistoryLocked(item.LockName, item.Holder, model.OpTimeout, "wait timeout")
			if m.heatmap != nil {
				m.heatmap.RecordLockTimeoutWithEnqueue(item.LockName, item.Holder, item.EnqueuedAt)
			}
			log.Printf("[lock-manager] wait timeout: lock=%s holder=%s", item.LockName, item.Holder)
		}
	}
}

func (m *Manager) WaitQueueLen(lockName string) (int, error) {
	return m.storage.WaitQueueLen(lockName)
}

func (m *Manager) ListAllWaitQueue() ([]model.WaitQueueItem, error) {
	return m.storage.ListAllWaitQueue()
}

func (m *Manager) ListAllLocks() ([]model.LockStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	locks, err := m.storage.ListLocks()
	if err != nil {
		return nil, err
	}

	result := make([]model.LockStatusInfo, 0, len(locks))
	for _, lock := range locks {
		info := model.LockStatusInfo{
			Name:      lock.Name,
			Status:    lock.Status,
			Holder:    lock.Holder,
			Reentrant: lock.Reentrant,
			Count:     lock.Count,
		}

		if lock.Status == model.LockStatusHeld {
			lease, err := m.storage.GetActiveLease(lock.Name)
			if err == nil && lease != nil {
				remaining := time.Until(lease.ExpiresAt).Seconds()
				if remaining < 0 {
					remaining = 0
				}
				info.RemainingSec = remaining
			}
		}

		queueLen, err := m.storage.WaitQueueLen(lock.Name)
		if err == nil {
			info.WaitQueueLen = queueLen
		}

		result = append(result, info)
	}
	return result, nil
}

func (m *Manager) GetLockDetail(lockName string, withHistory bool) (*model.LockDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("lock not found: %s", lockName)
	}

	detail := &model.LockDetail{
		Lock: *lock,
	}

	if lock.Status == model.LockStatusHeld {
		lease, err := m.storage.GetActiveLease(lockName)
		if err == nil && lease != nil {
			fillLeaseRemaining(lease)
			detail.Lease = lease
		}
	}

	queue, err := m.storage.ListWaitQueue(lockName)
	if err != nil {
		return nil, err
	}
	detail.WaitQueue = queue

	if withHistory {
		history, err := m.storage.ListHistory(lockName, 50)
		if err != nil {
			return nil, err
		}
		detail.History = history
	}

	return detail, nil
}

func (m *Manager) ListActiveLeases() ([]model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		return nil, err
	}
	for i := range leases {
		fillLeaseRemaining(&leases[i])
	}
	return leases, nil
}

func (m *Manager) GetLockHistory(lockName string, limit int) ([]model.OperationHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage.ListHistory(lockName, limit)
}

func (m *Manager) CancelWaitForHolder(holder string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, err := m.storage.ListAllWaitQueue()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range items {
		if item.Holder == holder {
			if err := m.storage.RemoveFromQueueByID(item.ID); err != nil {
				return count, err
			}
			m.addHistoryLocked(item.LockName, holder, model.OpRelease, "cancelled from wait queue by orchestration rollback")
			count++
		}
	}
	return count, nil
}

func (m *Manager) addHistoryLocked(lockName, holder string, op model.OperationType, detail string) {
	h := &model.OperationHistory{
		LockName:  lockName,
		Holder:    holder,
		Operation: op,
		Detail:    detail,
		CreatedAt: time.Now(),
	}
	_ = m.storage.AddHistory(h)
}

func fillLeaseRemaining(lease *model.Lease) {
	if lease == nil || !lease.Active {
		return
	}
	remaining := time.Until(lease.ExpiresAt).Seconds()
	if remaining < 0 {
		remaining = 0
	}
	lease.RemainingSec = remaining
}

func (m *Manager) buildWaitGraphLocked() ([]model.WaitGraphEdge, map[string]map[string]string, error) {
	waitItems, err := m.storage.ListAllWaitQueue()
	if err != nil {
		return nil, nil, err
	}

	locks, err := m.storage.ListLocks()
	if err != nil {
		return nil, nil, err
	}

	lockHolderMap := make(map[string]string)
	for _, l := range locks {
		if l.Status == model.LockStatusHeld {
			lockHolderMap[l.Name] = l.Holder
		}
	}

	var edges []model.WaitGraphEdge
	adj := make(map[string]map[string]string)

	for _, item := range waitItems {
		holder, ok := lockHolderMap[item.LockName]
		if !ok || holder == "" {
			continue
		}
		if holder == item.Holder {
			continue
		}
		edge := model.WaitGraphEdge{
			Waiter:   item.Holder,
			LockName: item.LockName,
			Holder:   holder,
		}
		edges = append(edges, edge)
		if adj[item.Holder] == nil {
			adj[item.Holder] = make(map[string]string)
		}
		adj[item.Holder][holder] = item.LockName
	}

	return edges, adj, nil
}

func (m *Manager) detectCycle(adj map[string]map[string]string, start string) ([]string, []string, bool) {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	path := make([]string, 0)

	var dfs func(node string) bool
	var cycleNodes []string
	var cycleStartIdx int

	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true
		path = append(path, node)

		for next := range adj[node] {
			if !visited[next] {
				if dfs(next) {
					return true
				}
			} else if recStack[next] {
				for i, p := range path {
					if p == next {
						cycleStartIdx = i
						break
					}
				}
				cycleNodes = make([]string, len(path)-cycleStartIdx)
				copy(cycleNodes, path[cycleStartIdx:])
				return true
			}
		}

		path = path[:len(path)-1]
		recStack[node] = false
		return false
	}

	if !dfs(start) {
		return nil, nil, false
	}

	cycleLocks := make([]string, 0, len(cycleNodes))
	for i := 0; i < len(cycleNodes); i++ {
		cur := cycleNodes[i]
		next := cycleNodes[(i+1)%len(cycleNodes)]
		if lockName, ok := adj[cur][next]; ok {
			cycleLocks = append(cycleLocks, lockName)
		}
	}

	return cycleNodes, cycleLocks, true
}

func (m *Manager) checkDeadlockLocked(waiter, lockName, holder string) (*model.DeadlockCycle, error) {
	_, adj, err := m.buildWaitGraphLocked()
	if err != nil {
		return nil, err
	}

	if adj[waiter] == nil {
		adj[waiter] = make(map[string]string)
	}
	adj[waiter][holder] = lockName

	cycleNodes, cycleLocks, hasCycle := m.detectCycle(adj, waiter)
	if !hasCycle {
		return nil, nil
	}

	var cycle []model.WaitGraphEdge
	for i := 0; i < len(cycleNodes)-1; i++ {
		cycle = append(cycle, model.WaitGraphEdge{
			Waiter:   cycleNodes[i],
			LockName: cycleLocks[i],
			Holder:   cycleNodes[i+1],
		})
	}
	if len(cycleNodes) > 0 && len(cycleLocks) > 0 {
		lastIdx := len(cycleLocks) - 1
		cycle = append(cycle, model.WaitGraphEdge{
			Waiter:   cycleNodes[len(cycleNodes)-1],
			LockName: cycleLocks[lastIdx],
			Holder:   cycleNodes[0],
		})
	}

	return &model.DeadlockCycle{Cycle: cycle}, nil
}

func (m *Manager) GetWaitGraph() (*model.WaitGraph, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	edges, _, err := m.buildWaitGraphLocked()
	if err != nil {
		return nil, err
	}

	nodeSet := make(map[string]bool)
	for _, e := range edges {
		nodeSet[e.Waiter] = true
		nodeSet[e.Holder] = true
	}
	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	return &model.WaitGraph{
		Edges: edges,
		Nodes: nodes,
	}, nil
}

func (m *Manager) AcquireLocksBatch(lockNames []string, holder string, leaseSec int, reentrant bool) (*model.BatchAcquireResult, error) {
	if leaseSec <= 0 {
		return nil, fmt.Errorf("lease_sec must be positive")
	}
	if len(lockNames) == 0 {
		return nil, fmt.Errorf("lock_names must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	uniqueLocks := make(map[string]bool)
	for _, name := range lockNames {
		uniqueLocks[name] = true
	}
	sortedNames := make([]string, 0, len(uniqueLocks))
	for name := range uniqueLocks {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	for _, lockName := range sortedNames {
		lock, err := m.storage.GetLock(lockName)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Status == model.LockStatusHeld {
			return &model.BatchAcquireResult{
				Acquired:   false,
				FailedLock: lockName,
				FailedBy:   lock.Holder,
			}, nil
		}
	}

	acquiredLocks := make([]*model.Lock, 0)
	acquiredLeases := make([]*model.Lease, 0)

	for _, lockName := range sortedNames {
		result, err := m.acquireLockNoQueueLocked(lockName, holder, leaseSec, reentrant)
		if err != nil {
			m.rollbackBatchLocked(acquiredLocks, holder)
			return &model.BatchAcquireResult{
				Acquired:   false,
				FailedLock: lockName,
				FailedBy:   err.Error(),
			}, nil
		}
		if result.BudgetRejected {
			m.rollbackBatchLocked(acquiredLocks, holder)
			br := result.BudgetCheckResult
			return &model.BatchAcquireResult{
				Acquired:          false,
				FailedLock:        lockName,
				FailedBy:          fmt.Sprintf("budget exhausted: consumed=%d, limit=%d, remaining=%d", br.ConsumedUnits, br.BudgetLimit, br.RemainingUnits),
				BudgetRejected:    true,
				BudgetCheckResult: br,
			}, nil
		}
		if result != nil {
			acquiredLocks = append(acquiredLocks, result.Lock)
			acquiredLeases = append(acquiredLeases, result.Lease)
		}
	}

	resultLocks := make([]model.Lock, 0, len(acquiredLocks))
	resultLeases := make([]model.Lease, 0, len(acquiredLeases))
	for _, l := range acquiredLocks {
		resultLocks = append(resultLocks, *l)
	}
	for _, l := range acquiredLeases {
		lease := *l
		fillLeaseRemaining(&lease)
		resultLeases = append(resultLeases, lease)
	}

	for _, lockName := range sortedNames {
		m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("batch acquire, lease=%ds", leaseSec))
	}

	return &model.BatchAcquireResult{
		Acquired: true,
		Locks:    resultLocks,
		Leases:   resultLeases,
	}, nil
}

func (m *Manager) acquireLockNoQueueLocked(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
	if m.admissionGuard != nil {
		if err := m.admissionGuard.BeforeAcquire(lockName, holder, leaseSec); err != nil {
			return nil, err
		}
	}
	if m.budgetMgr != nil {
		checkResult, err := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
		if err != nil {
			log.Printf("[lock-manager] budget check error (noqueue): holder=%s lock=%s err=%v", holder, lockName, err)
		}
		if checkResult != nil && !checkResult.Allowed {
			m.addHistoryLocked(lockName, holder, model.OpAcquire,
				fmt.Sprintf("rejected (batch): budget exhausted (consumed=%d, limit=%d, remaining=%d)",
					checkResult.ConsumedUnits, checkResult.BudgetLimit, checkResult.RemainingUnits))
			return &AcquireResult{
				BudgetRejected:    true,
				BudgetCheckResult: checkResult,
			}, nil
		}
	}

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	if lock == nil {
		lock = &model.Lock{
			Name:      lockName,
			Status:    model.LockStatusFree,
			Holder:    "",
			Reentrant: reentrant,
			Count:     0,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := m.storage.UpsertLock(lock); err != nil {
			return nil, err
		}
	}

	if lock.Status == model.LockStatusHeld {
		if lock.Holder == holder {
			if lock.Reentrant && reentrant {
				lock.Count++
				lock.UpdatedAt = now
				if err := m.storage.UpsertLock(lock); err != nil {
					return nil, err
				}
				m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("reentrant acquire, count=%d", lock.Count))
				lease, _ := m.storage.GetActiveLease(lockName)
				fillLeaseRemaining(lease)
				result := &AcquireResult{Acquired: true, Lock: lock, Lease: lease}
				if m.budgetMgr != nil {
					check, _ := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
					result.BudgetCheckResult = check
				}
				return result, nil
			}
			return nil, fmt.Errorf("already hold this lock (non-reentrant)")
		}
		return nil, fmt.Errorf("lock held by %s", lock.Holder)
	}

	lock.Status = model.LockStatusHeld
	lock.Holder = holder
	lock.Reentrant = reentrant
	lock.Count = 1
	lock.UpdatedAt = now
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	lease := &model.Lease{
		LockName:   lockName,
		Holder:     holder,
		LeaseSec:   leaseSec,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(leaseSec) * time.Second),
		Active:     true,
	}
	if m.fencingIssuer != nil {
		token, err := m.fencingIssuer.Issue(lockName, holder, leaseSec, now)
		if err != nil {
			return nil, fmt.Errorf("issue fencing token: %w", err)
		}
		lease.FencingToken = token
	}
	if err := m.storage.CreateLease(lease); err != nil {
		return nil, err
	}

	m.setLeaseTimerLocked(lockName, time.Duration(leaseSec)*time.Second)

	if m.budgetMgr != nil {
		if err := m.budgetMgr.StartHolding(holder, lockName, lease.AcquiredAt, lease.ExpiresAt); err != nil {
			log.Printf("[lock-manager] start holding budget error (batch): holder=%s lock=%s err=%v", holder, lockName, err)
		}
	}

	result := &AcquireResult{Acquired: true, Lock: lock, Lease: lease}
	if m.budgetMgr != nil {
		check, _ := m.budgetMgr.CheckAcquire(holder, lockName, leaseSec)
		result.BudgetCheckResult = check
	}
	fillLeaseRemaining(lease)
	return result, nil
}

func (m *Manager) rollbackBatchLocked(locks []*model.Lock, holder string) {
	for i := len(locks) - 1; i >= 0; i-- {
		lock := locks[i]
		_, _ = m.releaseLockLocked(lock.Name, holder)
		m.addHistoryLocked(lock.Name, holder, model.OpRelease, "batch rollback")
	}
}

func (m *Manager) GetActiveLease(lockName string) (*model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, err := m.storage.GetActiveLease(lockName)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		fillLeaseRemaining(lease)
	}
	return lease, nil
}

func (m *Manager) ShortenLease(lockName string, newLeaseSec int) (*model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, err := m.storage.GetActiveLease(lockName)
	if err != nil {
		return nil, err
	}
	if lease == nil || !lease.Active {
		return nil, fmt.Errorf("no active lease for lock: %s", lockName)
	}

	now := time.Now()
	currentRemaining := time.Until(lease.ExpiresAt).Seconds()
	if currentRemaining < 0 {
		currentRemaining = 0
	}

	if newLeaseSec <= 0 {
		return nil, fmt.Errorf("newLeaseSec must be positive")
	}

	newExpiresAt := now.Add(time.Duration(newLeaseSec) * time.Second)
	if newExpiresAt.After(lease.ExpiresAt) {
		newExpiresAt = lease.ExpiresAt
	}

	if err := m.storage.UpdateLeaseExpiry(lockName, newExpiresAt); err != nil {
		return nil, err
	}

	remaining := time.Until(newExpiresAt)
	m.setLeaseTimerLocked(lockName, remaining)

	lease.ExpiresAt = newExpiresAt
	lease.LeaseSec = newLeaseSec
	fillLeaseRemaining(lease)

	if m.heatmapMgr != nil {
		_ = m.heatmapMgr.IncrementLeaseShortenedCount(lockName)
	}

	m.addHistoryLocked(lockName, lease.Holder, model.OpCooldownStart,
		fmt.Sprintf("租约缩短: 原剩余%.1fs, 新租约%ds, 新到期时间%s",
			currentRemaining, newLeaseSec, newExpiresAt.Format(time.RFC3339)))

	return lease, nil
}

func (m *Manager) SetAcceleratedGrant(lockName string, enabled bool, cooldownLeaseSec int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if enabled {
		m.acceleratedGrants[lockName] = cooldownLeaseSec
	} else {
		delete(m.acceleratedGrants, lockName)
	}
	return nil
}

func (m *Manager) IsAcceleratedGrant(lockName string) (bool, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	leaseSec, ok := m.acceleratedGrants[lockName]
	return ok, leaseSec
}

func (m *Manager) AddCooldownHistory(lockName string, holder string, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addHistoryLocked(lockName, holder, model.OpCooldownStart, detail)
}
