package orchestration

import (
	"fmt"
	"log"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Manager struct {
	storage     *storage.Storage
	lockMgr     *lock.Manager
	rlMgr       *ratelimit.Manager
	mu          sync.Mutex
	timers      map[string]*time.Timer
	stopCh      chan struct{}
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager) *Manager {
	return &Manager{
		storage: s,
		lockMgr: lm,
		rlMgr:   rlm,
		timers:  make(map[string]*time.Timer),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.rebuildTimersLocked(); err != nil {
		return fmt.Errorf("rebuild timers: %w", err)
	}
	log.Println("[orchestration-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.timers {
		t.Stop()
	}
	log.Println("[orchestration-manager] stopped")
}

func (m *Manager) rebuildTimersLocked() error {
	now := time.Now()
	txs, err := m.storage.ListActiveOrchTxs(now)
	if err != nil {
		return err
	}

	for _, tx := range txs {
		t := tx
		if t.ExpiresAt.Before(now) {
			log.Printf("[orchestration-manager] tx expired on startup: tx=%s", t.ID)
			go m.timeoutTx(t.ID)
		} else {
			duration := time.Until(t.ExpiresAt)
			m.setTxTimerLocked(t.ID, duration)
			log.Printf("[orchestration-manager] rebuilt tx timer: tx=%s remaining=%.1fs", t.ID, duration.Seconds())
		}
	}
	return nil
}

func (m *Manager) setTxTimerLocked(txID string, duration time.Duration) {
	if t, ok := m.timers[txID]; ok {
		t.Stop()
	}
	m.timers[txID] = time.AfterFunc(duration, func() {
		log.Printf("[orchestration-manager] tx timeout: tx=%s", txID)
		m.timeoutTx(txID)
	})
}

func (m *Manager) stopTxTimerLocked(txID string) {
	if t, ok := m.timers[txID]; ok {
		t.Stop()
		delete(m.timers, txID)
	}
}

func (m *Manager) addStateChangeLocked(txID string, from, to model.TxStatus, reason string) {
	sc := &model.TxStateChange{
		TxID:      txID,
		FromState: from,
		ToState:   to,
		Reason:    reason,
		CreatedAt: time.Now(),
	}
	_ = m.storage.AddTxStateChange(sc)
}

func (m *Manager) PreCheck(locks []model.TxLockSpec, tokens []model.TxTokenSpec) (*model.PreCheckResult, error) {
	result := &model.PreCheckResult{
		ConflictingLocks:  make([]model.ConflictingLockInfo, 0),
		InsufficientQuota: make([]model.InsufficientQuotaInfo, 0),
		CanProceed:        true,
	}

	allLocks, err := m.lockMgr.ListAllLocks()
	if err != nil {
		return nil, err
	}

	lockStatusMap := make(map[string]model.LockStatusInfo)
	for _, l := range allLocks {
		lockStatusMap[l.Name] = l
	}

	for _, spec := range locks {
		if info, ok := lockStatusMap[spec.LockName]; ok && info.Status == model.LockStatusHeld {
			result.ConflictingLocks = append(result.ConflictingLocks, model.ConflictingLockInfo{
				LockName: spec.LockName,
				Holder:   info.Holder,
			})
			result.CanProceed = false
		}
	}

	for _, spec := range tokens {
		status, err := m.rlMgr.GetCallerStatus(spec.CallerID)
		if err != nil {
			result.InsufficientQuota = append(result.InsufficientQuota, model.InsufficientQuotaInfo{
				CallerID:  spec.CallerID,
				Requested: spec.Tokens,
				Remaining: 0,
			})
			result.CanProceed = false
			continue
		}
		if status.Remaining < spec.Tokens {
			result.InsufficientQuota = append(result.InsufficientQuota, model.InsufficientQuotaInfo{
				CallerID:  spec.CallerID,
				Requested: spec.Tokens,
				Remaining: status.Remaining,
			})
			result.CanProceed = false
		}
	}

	return result, nil
}

func (m *Manager) CreateTx(holder string, timeoutSec int, lockSpecs []model.TxLockSpec, tokenSpecs []model.TxTokenSpec) (*model.OrchestrationTx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	txID := uuid.New().String()

	tx := &model.OrchestrationTx{
		ID:         txID,
		Holder:     holder,
		Status:     model.TxStatusCreated,
		TimeoutSec: timeoutSec,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  now.Add(time.Duration(timeoutSec) * time.Second),
	}

	if err := m.storage.CreateOrchTx(tx); err != nil {
		return nil, err
	}
	m.addStateChangeLocked(txID, "", model.TxStatusCreated, "transaction created")

	acquiredLocks := make([]string, 0)
	grantedTokens := make([]model.TxToken, 0)
	failReason := ""

	sortedLocks := make([]model.TxLockSpec, len(lockSpecs))
	copy(sortedLocks, lockSpecs)
	sort.Slice(sortedLocks, func(i, j int) bool {
		return sortedLocks[i].LockName < sortedLocks[j].LockName
	})

	for _, spec := range sortedLocks {
		res, err := m.lockMgr.AcquireLock(spec.LockName, holder, spec.LeaseSec, false)
		if err != nil {
			failReason = fmt.Sprintf("lock %s acquire error: %v", spec.LockName, err)
			break
		}
		if !res.Acquired {
			if res.Queued {
				failReason = fmt.Sprintf("lock %s is held by %s, would queue", spec.LockName, res.Lock.Holder)
			} else if res.Deadlock {
				failReason = fmt.Sprintf("lock %s deadlock detected", spec.LockName)
			} else {
				failReason = fmt.Sprintf("lock %s not acquired", spec.LockName)
			}
			break
		}

		txLock := &model.TxLock{
			TxID:      txID,
			LockName:  spec.LockName,
			LeaseSec:  spec.LeaseSec,
			Holder:    holder,
			CreatedAt: time.Now(),
		}
		if err := m.storage.AddTxLock(txLock); err != nil {
			failReason = fmt.Sprintf("persist lock %s error: %v", spec.LockName, err)
			break
		}
		acquiredLocks = append(acquiredLocks, spec.LockName)
	}

	if failReason == "" {
		for _, spec := range tokenSpecs {
			res, err := m.rlMgr.RequestTokens(spec.CallerID, spec.Tokens, false, 0)
			if err != nil {
				failReason = fmt.Sprintf("token caller %s request error: %v", spec.CallerID, err)
				break
			}
			if !res.Allowed {
				failReason = fmt.Sprintf("caller %s insufficient quota: requested=%d, remaining=%d", spec.CallerID, spec.Tokens, res.Remaining)
				break
			}

			txToken := &model.TxToken{
				TxID:      txID,
				CallerID:  spec.CallerID,
				Tokens:    spec.Tokens,
				CreatedAt: time.Now(),
			}
			if err := m.storage.AddTxToken(txToken); err != nil {
				failReason = fmt.Sprintf("persist token for %s error: %v", spec.CallerID, err)
				_ = m.rlMgr.ReturnTokens(spec.CallerID, spec.Tokens)
				break
			}
			grantedTokens = append(grantedTokens, *txToken)
		}
	}

	if failReason != "" {
		for i := len(grantedTokens) - 1; i >= 0; i-- {
			_ = m.rlMgr.ReturnTokens(grantedTokens[i].CallerID, grantedTokens[i].Tokens)
		}
		for i := len(acquiredLocks) - 1; i >= 0; i-- {
			_, _ = m.lockMgr.ReleaseLock(acquiredLocks[i], holder)
		}
		_, _ = m.lockMgr.CancelWaitForHolder(holder)

		tx.Status = model.TxStatusRolledBack
		tx.FailReason = failReason
		tx.UpdatedAt = time.Now()
		_ = m.storage.UpdateOrchTxStatus(txID, model.TxStatusRolledBack, failReason, tx.UpdatedAt)
		m.addStateChangeLocked(txID, model.TxStatusCreated, model.TxStatusRolledBack, failReason)

		detailTx, _ := m.loadTxDetailLocked(txID)
		return detailTx, nil
	}

	tx.Status = model.TxStatusCommitted
	tx.UpdatedAt = time.Now()
	_ = m.storage.UpdateOrchTxStatus(txID, model.TxStatusCommitted, "", tx.UpdatedAt)
	m.addStateChangeLocked(txID, model.TxStatusCreated, model.TxStatusCommitted, "all locks and tokens acquired")

	m.setTxTimerLocked(txID, time.Duration(timeoutSec)*time.Second)

	log.Printf("[orchestration-manager] tx committed: tx=%s holder=%s locks=%d tokens=%d",
		txID, holder, len(acquiredLocks), len(grantedTokens))

	detailTx, _ := m.loadTxDetailLocked(txID)
	return detailTx, nil
}

func (m *Manager) ReleaseTx(txID string, callerHolder string) (*model.OrchestrationTx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.storage.GetOrchTx(txID)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}
	if tx.Status != model.TxStatusCommitted {
		return nil, fmt.Errorf("transaction not in committed state: current=%s", tx.Status)
	}
	if tx.Holder != callerHolder {
		return nil, fmt.Errorf("permission denied: not the transaction holder: tx_holder=%s, caller=%s", tx.Holder, callerHolder)
	}

	locks, err := m.storage.ListTxLocks(txID)
	if err != nil {
		return nil, err
	}

	for _, l := range locks {
		_, _ = m.lockMgr.ReleaseLock(l.LockName, l.Holder)
	}

	m.stopTxTimerLocked(txID)

	tx.Status = model.TxStatusReleased
	tx.UpdatedAt = time.Now()
	_ = m.storage.UpdateOrchTxStatus(txID, model.TxStatusReleased, "", tx.UpdatedAt)
	m.addStateChangeLocked(txID, model.TxStatusCommitted, model.TxStatusReleased, "manual release")

	log.Printf("[orchestration-manager] tx released: tx=%s holder=%s", txID, callerHolder)

	detailTx, _ := m.loadTxDetailLocked(txID)
	return detailTx, nil
}

func (m *Manager) timeoutTx(txID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.storage.GetOrchTx(txID)
	if err != nil {
		log.Printf("[orchestration-manager] timeoutTx get error: %v", err)
		return
	}
	if tx == nil || tx.Status != model.TxStatusCommitted {
		return
	}

	locks, err := m.storage.ListTxLocks(txID)
	if err != nil {
		log.Printf("[orchestration-manager] timeoutTx list locks error: %v", err)
		return
	}

	for _, l := range locks {
		_, _ = m.lockMgr.ReleaseLock(l.LockName, l.Holder)
	}

	tokens, err := m.storage.ListTxTokens(txID)
	if err != nil {
		log.Printf("[orchestration-manager] timeoutTx list tokens error: %v", err)
		return
	}
	for _, t := range tokens {
		_ = m.rlMgr.ReturnTokens(t.CallerID, t.Tokens)
	}

	m.stopTxTimerLocked(txID)

	tx.Status = model.TxStatusTimedOut
	tx.UpdatedAt = time.Now()
	_ = m.storage.UpdateOrchTxStatus(txID, model.TxStatusTimedOut, "", tx.UpdatedAt)
	m.addStateChangeLocked(txID, model.TxStatusCommitted, model.TxStatusTimedOut, "transaction timeout")

	log.Printf("[orchestration-manager] tx timed out: tx=%s", txID)
}

func (m *Manager) loadTxDetailLocked(txID string) (*model.OrchestrationTx, error) {
	tx, err := m.storage.GetOrchTx(txID)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	locks, err := m.storage.ListTxLocks(txID)
	if err != nil {
		return nil, err
	}
	tx.Locks = locks

	tokens, err := m.storage.ListTxTokens(txID)
	if err != nil {
		return nil, err
	}
	tx.Tokens = tokens

	changes, err := m.storage.ListTxStateChanges(txID)
	if err != nil {
		return nil, err
	}
	tx.StateChanges = changes

	return tx, nil
}

func (m *Manager) GetTx(txID string) (*model.OrchestrationTx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadTxDetailLocked(txID)
}

func (m *Manager) ListTxs(status string) ([]model.OrchestrationTx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txs, err := m.storage.ListOrchTxs(status)
	if err != nil {
		return nil, err
	}

	for i := range txs {
		locks, _ := m.storage.ListTxLocks(txs[i].ID)
		txs[i].Locks = locks

		tokens, _ := m.storage.ListTxTokens(txs[i].ID)
		txs[i].Tokens = tokens

		changes, _ := m.storage.ListTxStateChanges(txs[i].ID)
		txs[i].StateChanges = changes
	}

	return txs, nil
}

func (m *Manager) GetTxHistory(txID string) ([]model.TxStateChange, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage.ListTxStateChanges(txID)
}
