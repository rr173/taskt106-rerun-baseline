package handover

import (
	"encoding/json"
	"fmt"
	"log"
	"task106/internal/debt"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/orchestration"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"task106/internal/topology"
	"sync"
	"time"
)

type Manager struct {
	storage   *storage.Storage
	lockMgr   *lock.Manager
	rlMgr     *ratelimit.Manager
	orchMgr   *orchestration.Manager
	topoMgr   *topology.Manager
	debtMgr   *debt.Manager
	mu        sync.Mutex
	stopCh    chan struct{}
	ticker    *time.Ticker
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager,
	om *orchestration.Manager, tm *topology.Manager, dm *debt.Manager) *Manager {
	return &Manager{
		storage: s,
		lockMgr: lm,
		rlMgr:   rlm,
		orchMgr: om,
		topoMgr: tm,
		debtMgr: dm,
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.ticker = time.NewTicker(2 * time.Second)
	go m.watchTimeoutLoop()
	log.Println("[handover-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[handover-manager] stopped")
}

func (m *Manager) watchTimeoutLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.checkAndCancelExpired()
		}
	}
}

func (m *Manager) checkAndCancelExpired() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	expired, err := m.storage.ListExpiredPendingHandovers(now)
	if err != nil {
		log.Printf("[handover-manager] list expired error: %v", err)
		return
	}

	for _, h := range expired {
		log.Printf("[handover-manager] auto-cancelling expired handover: id=%d", h.ID)
		if err := m.cancelHandoverInternal(&h, "system", "confirm_timeout"); err != nil {
			log.Printf("[handover-manager] cancel expired error id=%d: %v", h.ID, err)
		} else {
			m.writeAudit(&h, "system", "timeout_cancel", true, "confirm_timeout")
		}
	}
}

func (m *Manager) addTimelineLocked(handoverID int64, status model.HandoverStatus, operator, detail string) {
	entry := &model.HandoverTimelineEntry{
		HandoverID: handoverID,
		Status:     status,
		Operator:   operator,
		Detail:     detail,
		CreatedAt:  time.Now(),
	}
	_ = m.storage.AddHandoverTimeline(entry)
}

func (m *Manager) CreateHandover(req *model.CreateHandoverRequest) (*model.Handover, error) {
	if req.FromCaller == req.ToCaller {
		return nil, fmt.Errorf("from_caller and to_caller cannot be the same")
	}
	if req.ConfirmTimeoutSec <= 0 {
		req.ConfirmTimeoutSec = 3600
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	h := &model.Handover{
		FromCaller:       req.FromCaller,
		ToCaller:         req.ToCaller,
		Status:           model.HandoverStatusCreated,
		Initiator:        req.Initiator,
		Description:      req.Description,
		NeedConfirm:      req.NeedConfirm,
		ConfirmTimeoutSec: req.ConfirmTimeoutSec,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	deadline := now.Add(time.Duration(req.ConfirmTimeoutSec) * time.Second)
	h.ConfirmDeadline = &deadline

	if err := m.storage.CreateHandover(h); err != nil {
		return nil, fmt.Errorf("create handover: %w", err)
	}

	topoLockNames, err := m.expandTopologySubtrees(req.TopologyRoots, req.FromCaller)
	if err != nil {
		return nil, fmt.Errorf("expand topology: %w", err)
	}

	allLockNames := make([]string, 0, len(req.LockNames)+len(topoLockNames))
	seen := make(map[string]bool)
	for _, n := range req.LockNames {
		if !seen[n] {
			seen[n] = true
			allLockNames = append(allLockNames, n)
		}
	}
	for _, n := range topoLockNames {
		if !seen[n] {
			seen[n] = true
			allLockNames = append(allLockNames, n)
		}
	}

	for _, lockName := range allLockNames {
		item := &model.HandoverResourceItem{
			HandoverID:    h.ID,
			ResourceType:  model.HandoverResourceLock,
			ResourceKey:   lockName,
			ResourceName:  lockName,
			PreCheckStatus: model.PreCheckOK,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := m.storage.AddHandoverResource(item); err != nil {
			return nil, fmt.Errorf("add lock resource: %w", err)
		}
	}

	for _, callerID := range req.QuotaCallers {
		item := &model.HandoverResourceItem{
			HandoverID:    h.ID,
			ResourceType:  model.HandoverResourceQuota,
			ResourceKey:   callerID,
			ResourceName:  "quota:" + callerID,
			PreCheckStatus: model.PreCheckOK,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := m.storage.AddHandoverResource(item); err != nil {
			return nil, fmt.Errorf("add quota resource: %w", err)
		}
	}

	for _, txID := range req.OrchTxIDs {
		item := &model.HandoverResourceItem{
			HandoverID:    h.ID,
			ResourceType:  model.HandoverResourceOrchTx,
			ResourceKey:   txID,
			ResourceName:  "tx:" + txID,
			PreCheckStatus: model.PreCheckOK,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := m.storage.AddHandoverResource(item); err != nil {
			return nil, fmt.Errorf("add orch tx resource: %w", err)
		}
	}

	for _, rootName := range req.TopologyRoots {
		item := &model.HandoverResourceItem{
			HandoverID:    h.ID,
			ResourceType:  model.HandoverResourceTopology,
			ResourceKey:   rootName,
			ResourceName:  "topology:" + rootName,
			PreCheckStatus: model.PreCheckOK,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := m.storage.AddHandoverResource(item); err != nil {
			return nil, fmt.Errorf("add topology resource: %w", err)
		}
	}

	for _, rid := range req.ReservationIDs {
		item := &model.HandoverResourceItem{
			HandoverID:    h.ID,
			ResourceType:  model.HandoverResourceReservation,
			ResourceKey:   fmt.Sprintf("%d", rid),
			ResourceName:  fmt.Sprintf("reservation:%d", rid),
			PreCheckStatus: model.PreCheckOK,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := m.storage.AddHandoverResource(item); err != nil {
			return nil, fmt.Errorf("add reservation resource: %w", err)
		}
	}

	m.addTimelineLocked(h.ID, model.HandoverStatusCreated, req.Initiator,
		fmt.Sprintf("handover created with %d locks, %d quotas, %d txs, %d topo roots, %d reservations",
			len(allLockNames), len(req.QuotaCallers), len(req.OrchTxIDs), len(req.TopologyRoots), len(req.ReservationIDs)))
	m.writeAudit(h, req.Initiator, "create", true,
		fmt.Sprintf("%s -> %s, resources: %d locks, %d quotas, %d txs",
			h.FromCaller, h.ToCaller, len(allLockNames), len(req.QuotaCallers), len(req.OrchTxIDs)))

	return m.loadHandoverFull(h.ID)
}

func (m *Manager) expandTopologySubtrees(rootNames []string, holder string) ([]string, error) {
	lockNames := make([]string, 0)
	for _, root := range rootNames {
		desc, err := m.topoMgr.GetDescendants(root)
		if err != nil {
			continue
		}
		allNodes := append([]string{root}, desc.Descendants...)
		for _, nodeName := range allNodes {
			node, err := m.topoMgr.GetNode(nodeName)
			if err != nil || node == nil {
				continue
			}
			lock, err := m.storage.GetLock(node.LockName)
			if err != nil || lock == nil {
				continue
			}
			if lock.Status == model.LockStatusHeld && lock.Holder == holder {
				lockNames = append(lockNames, node.LockName)
			}
		}
	}
	return lockNames, nil
}

func (m *Manager) PreCheck(handoverID int64) (*model.PreCheckHandoverResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h, err := m.storage.GetHandover(handoverID)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("handover not found: %d", handoverID)
	}
	if h.Status != model.HandoverStatusCreated && h.Status != model.HandoverStatusPreChecked {
		return nil, fmt.Errorf("handover status %s not eligible for precheck", h.Status)
	}

	resources, err := m.storage.ListHandoverResources(handoverID)
	if err != nil {
		return nil, err
	}

	result := &model.PreCheckHandoverResult{
		HandoverID: handoverID,
		CanProceed: true,
		TotalCount: len(resources),
	}

	now := time.Now()
	for i := range resources {
		item := &resources[i]
		m.preCheckResource(item, h)
		item.UpdatedAt = now
		if err := m.storage.UpdateHandoverResource(item); err != nil {
			return nil, fmt.Errorf("update resource: %w", err)
		}

		switch item.PreCheckStatus {
		case model.PreCheckOK:
			result.OKCount++
		case model.PreCheckConflict:
			result.ConflictCount++
			result.CanProceed = false
		case model.PreCheckBlocked:
			result.BlockedCount++
			result.CanProceed = false
		case model.PreCheckNotFound:
			result.NotFoundCount++
		}
	}

	result.Resources = resources

	if err := m.storage.UpdateHandoverStatus(handoverID, model.HandoverStatusPreChecked, now); err != nil {
		return nil, err
	}
	m.addTimelineLocked(handoverID, model.HandoverStatusPreChecked, "system",
		fmt.Sprintf("precheck done: ok=%d conflict=%d blocked=%d not_found=%d",
			result.OKCount, result.ConflictCount, result.BlockedCount, result.NotFoundCount))
	m.writeAudit(h, "system", "precheck", true,
		fmt.Sprintf("ok=%d conflict=%d blocked=%d not_found=%d can_proceed=%v",
			result.OKCount, result.ConflictCount, result.BlockedCount, result.NotFoundCount, result.CanProceed))

	return result, nil
}

func (m *Manager) preCheckResource(item *model.HandoverResourceItem, h *model.Handover) {
	switch item.ResourceType {
	case model.HandoverResourceLock:
		m.preCheckLock(item, h)
	case model.HandoverResourceQuota:
		m.preCheckQuota(item, h)
	case model.HandoverResourceOrchTx:
		m.preCheckOrchTx(item, h)
	case model.HandoverResourceTopology:
		m.preCheckTopology(item, h)
	case model.HandoverResourceReservation:
		m.preCheckReservation(item, h)
	}
}

func (m *Manager) preCheckLock(item *model.HandoverResourceItem, h *model.Handover) {
	l, err := m.storage.GetLock(item.ResourceKey)
	if err != nil || l == nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "lock not found"
		return
	}
	if l.Status != model.LockStatusHeld {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = fmt.Sprintf("lock status=%s, not held", l.Status)
		return
	}
	if l.Holder != h.FromCaller {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("lock held by %s, not %s", l.Holder, h.FromCaller)
		item.CurrentHolder = l.Holder
		return
	}

	otherLock, _ := m.storage.GetLock(item.ResourceKey)
	if otherLock != nil && otherLock.Status == model.LockStatusHeld && otherLock.Holder == h.ToCaller {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("receiver %s already holds this lock", h.ToCaller)
		return
	}

	restriction := m.debtMgr.CheckRestriction(h.ToCaller, model.RestrictionScopeLock)
	if restriction.Restricted {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("receiver restricted: %s - %s", restriction.RestrictionType, restriction.Reason)
		return
	}

	breaker := m.getCircuitBreaker(h.ToCaller)
	if breaker != nil && breaker.State == model.CircuitBreakerOpen {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("receiver circuit breaker open: %s", breaker.TriggerReason)
		return
	}

	snap := map[string]interface{}{
		"holder":    l.Holder,
		"reentrant": l.Reentrant,
		"count":     l.Count,
	}
	lease, _ := m.storage.GetActiveLease(item.ResourceKey)
	if lease != nil {
		snap["lease_sec"] = lease.LeaseSec
		snap["expires_at"] = lease.ExpiresAt
		snap["lease_id"] = lease.ID
	}
	item.Snapshot = toJSON(snap)
	item.CurrentHolder = l.Holder
	item.PreCheckStatus = model.PreCheckOK
	item.PreCheckDetail = ""
}

func (m *Manager) preCheckQuota(item *model.HandoverResourceItem, h *model.Handover) {
	b, err := m.storage.GetCallerBinding(item.ResourceKey)
	if err != nil || b == nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "caller binding not found"
		return
	}
	if b.CallerID != h.FromCaller {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("quota belongs to %s, not %s", b.CallerID, h.FromCaller)
		item.CurrentHolder = b.CallerID
		return
	}

	existing, _ := m.storage.GetCallerBinding(h.ToCaller)
	if existing != nil && existing.PolicyName != b.PolicyName {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("receiver already bound to policy %s (source: %s)", existing.PolicyName, b.PolicyName)
		return
	}

	restriction := m.debtMgr.CheckRestriction(h.ToCaller, model.RestrictionScopeToken)
	if restriction.Restricted {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("receiver restricted: %s - %s", restriction.RestrictionType, restriction.Reason)
		return
	}

	if b.BorrowedTokens > 0 {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("source has %d borrowed tokens (debt), must return first", b.BorrowedTokens)
		return
	}

	snap := map[string]interface{}{
		"policy_name":     b.PolicyName,
		"quota_limit":     b.QuotaLimit,
		"used_tokens":     b.UsedTokens,
		"borrowed_tokens": b.BorrowedTokens,
		"lent_tokens":     b.LentTokens,
		"reserved_tokens": b.ReservedTokens,
	}
	item.Snapshot = toJSON(snap)
	item.CurrentHolder = b.CallerID
	item.PreCheckStatus = model.PreCheckOK
}

func (m *Manager) preCheckOrchTx(item *model.HandoverResourceItem, h *model.Handover) {
	tx, err := m.storage.GetOrchTx(item.ResourceKey)
	if err != nil || tx == nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "orchestration tx not found"
		return
	}
	if tx.Status != model.TxStatusCommitted {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("tx status=%s, only committed can transfer", tx.Status)
		return
	}
	if tx.Holder != h.FromCaller {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("tx held by %s, not %s", tx.Holder, h.FromCaller)
		item.CurrentHolder = tx.Holder
		return
	}

	restriction := m.debtMgr.CheckRestriction(h.ToCaller, model.RestrictionScopeOrchestration)
	if restriction.Restricted {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("receiver restricted: %s - %s", restriction.RestrictionType, restriction.Reason)
		return
	}

	snap := map[string]interface{}{
		"holder":      tx.Holder,
		"status":      tx.Status,
		"timeout_sec": tx.TimeoutSec,
		"expires_at":  tx.ExpiresAt,
	}
	item.Snapshot = toJSON(snap)
	item.CurrentHolder = tx.Holder
	item.PreCheckStatus = model.PreCheckOK
}

func (m *Manager) preCheckTopology(item *model.HandoverResourceItem, h *model.Handover) {
	node, err := m.topoMgr.GetNode(item.ResourceKey)
	if err != nil || node == nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "topology node not found"
		return
	}

	desc, err := m.topoMgr.GetDescendants(item.ResourceKey)
	if err != nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "failed to get descendants"
		return
	}
	allNodes := append([]string{item.ResourceKey}, desc.Descendants...)

	var failedLocks []string
	for _, nodeName := range allNodes {
		n, _ := m.topoMgr.GetNode(nodeName)
		if n == nil {
			continue
		}
		lock, _ := m.storage.GetLock(n.LockName)
		if lock == nil || lock.Status != model.LockStatusHeld || lock.Holder != h.FromCaller {
			failedLocks = append(failedLocks, n.LockName)
		}
	}
	if len(failedLocks) > 0 {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("subtree locks not held by source: %v", failedLocks)
		return
	}

	snap := map[string]interface{}{
		"root":       item.ResourceKey,
		"nodes":      allNodes,
		"from_caller": h.FromCaller,
	}
	item.Snapshot = toJSON(snap)
	item.CurrentHolder = h.FromCaller
	item.PreCheckStatus = model.PreCheckOK
}

func (m *Manager) preCheckReservation(item *model.HandoverResourceItem, h *model.Handover) {
	var rid int64
	fmt.Sscanf(item.ResourceKey, "%d", &rid)
	r, err := m.storage.GetReservation(rid)
	if err != nil || r == nil {
		item.PreCheckStatus = model.PreCheckNotFound
		item.PreCheckDetail = "reservation not found"
		return
	}
	if r.CallerID != h.FromCaller {
		item.PreCheckStatus = model.PreCheckConflict
		item.PreCheckDetail = fmt.Sprintf("reservation belongs to %s, not %s", r.CallerID, h.FromCaller)
		item.CurrentHolder = r.CallerID
		return
	}
	if r.Status != model.ReservationStatusPending && r.Status != model.ReservationStatusActive {
		item.PreCheckStatus = model.PreCheckBlocked
		item.PreCheckDetail = fmt.Sprintf("reservation status=%s", r.Status)
		return
	}

	snap := map[string]interface{}{
		"caller_id":   r.CallerID,
		"policy_name": r.PolicyName,
		"tokens":      r.Tokens,
		"start_at":    r.StartAt,
		"end_at":      r.EndAt,
		"status":      r.Status,
	}
	item.Snapshot = toJSON(snap)
	item.CurrentHolder = r.CallerID
	item.PreCheckStatus = model.PreCheckOK
}

func (m *Manager) getCircuitBreaker(callerID string) *model.CircuitBreakerStatus {
	st, _ := m.storage.GetCircuitBreakerStatus(callerID)
	return st
}

func (m *Manager) Initiate(handoverID int64, operator string) (*model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h, err := m.storage.GetHandover(handoverID)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("handover not found: %d", handoverID)
	}
	if h.Status != model.HandoverStatusPreChecked {
		return nil, fmt.Errorf("handover status %s must be prechecked first", h.Status)
	}

	resources, err := m.storage.ListHandoverResources(handoverID)
	if err != nil {
		return nil, err
	}

	hasOK := false
	for _, r := range resources {
		if r.PreCheckStatus == model.PreCheckConflict || r.PreCheckStatus == model.PreCheckBlocked {
			return nil, fmt.Errorf("resource %s:%s has precheck status %s: %s",
				r.ResourceType, r.ResourceKey, r.PreCheckStatus, r.PreCheckDetail)
		}
		if r.PreCheckStatus == model.PreCheckNotFound {
			return nil, fmt.Errorf("resource %s:%s not found, cannot proceed with handover",
				r.ResourceType, r.ResourceKey)
		}
		if r.PreCheckStatus == model.PreCheckOK {
			hasOK = true
		}
	}

	if len(resources) > 0 && !hasOK {
		return nil, fmt.Errorf("no resources with ok precheck status, nothing to transfer")
	}

	now := time.Now()
	if h.NeedConfirm {
		if err := m.storage.UpdateHandoverStatus(handoverID, model.HandoverStatusPending, now); err != nil {
			return nil, err
		}
		m.addTimelineLocked(handoverID, model.HandoverStatusPending, operator,
			fmt.Sprintf("initiated, waiting for receiver confirmation, deadline=%s", h.ConfirmDeadline.Format(time.RFC3339)))
		m.writeAudit(h, operator, "initiate", true, "")
		return m.loadHandoverFull(handoverID)
	}

	h2, err := m.executeInternalLocked(h, operator)
	if err != nil {
		m.writeAudit(h, operator, "initiate_execute", false, err.Error())
		return nil, err
	}
	m.writeAudit(h, operator, "initiate_execute", true, "")
	return h2, nil
}

func (m *Manager) Confirm(handoverID int64, req *model.ConfirmHandoverRequest) (*model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h, err := m.storage.GetHandover(handoverID)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("handover not found: %d", handoverID)
	}
	if h.Status != model.HandoverStatusPending {
		return nil, fmt.Errorf("handover status %s not pending", h.Status)
	}

	if !req.Accept {
		reason := req.Reason
		if reason == "" {
			reason = "rejected by receiver"
		}
		now := time.Now()
		if err := m.storage.UpdateHandoverStatus(handoverID, model.HandoverStatusRejected, now,
			"cancelled_at", now, "cancel_reason", reason); err != nil {
			return nil, err
		}
		m.addTimelineLocked(handoverID, model.HandoverStatusRejected, req.Operator, reason)
		m.writeAudit(h, req.Operator, "reject", true, reason)
		return m.loadHandoverFull(handoverID)
	}

	h2, err := m.executeInternalLocked(h, req.Operator)
	if err != nil {
		m.writeAudit(h, req.Operator, "confirm_execute", false, err.Error())
		return nil, err
	}

	now := time.Now()
	if err := m.storage.UpdateHandoverStatus(h.ID, model.HandoverStatusCompleted, now,
		"confirmed_at", now); err != nil {
		m.writeAudit(h, req.Operator, "confirm_execute", false, err.Error())
		return nil, err
	}
	m.writeAudit(h, req.Operator, "confirm_execute", true, "")
	return h2, nil
}

func (m *Manager) Cancel(handoverID int64, req *model.CancelHandoverRequest) (*model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, err := m.storage.GetHandover(handoverID)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("handover not found: %d", handoverID)
	}
	if err := m.cancelHandoverInternal(h, req.Operator, req.Reason); err != nil {
		return nil, err
	}
	m.writeAudit(h, req.Operator, "cancel", true, req.Reason)
	return m.loadHandoverFull(handoverID)
}

func (m *Manager) cancelHandoverInternal(h *model.Handover, operator, reason string) error {
	if h.Status == model.HandoverStatusCompleted ||
		h.Status == model.HandoverStatusCancelled ||
		h.Status == model.HandoverStatusRejected {
		return fmt.Errorf("handover already in final status: %s", h.Status)
	}

	now := time.Now()
	r := reason
	if r == "" {
		r = "cancelled"
	}
	if err := m.storage.UpdateHandoverStatus(h.ID, model.HandoverStatusCancelled, now,
		"cancelled_at", now, "cancel_reason", r); err != nil {
		return err
	}
	m.addTimelineLocked(h.ID, model.HandoverStatusCancelled, operator, r)
	return nil
}

type execContext struct {
	appliedLocks  []string
	appliedQuotas []string
	appliedTxs    []string
	appliedRes    []int64
}

func (m *Manager) executeInternalLocked(h *model.Handover, operator string) (*model.Handover, error) {
	ctx := &execContext{}

	resources, err := m.storage.ListHandoverResources(h.ID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	for i := range resources {
		item := &resources[i]
		if item.PreCheckStatus != model.PreCheckOK {
			continue
		}
		if err := m.executeResourceLocked(ctx, item, h, now); err != nil {
			log.Printf("[handover] execute resource fail: %s:%s err=%v", item.ResourceType, item.ResourceKey, err)
			m.rollbackAllLocked(ctx, h, now)
			return nil, fmt.Errorf("execute %s:%s: %w", item.ResourceType, item.ResourceKey, err)
		}
		item.Executed = true
		item.UpdatedAt = now
		if err := m.storage.UpdateHandoverResource(item); err != nil {
			m.rollbackAllLocked(ctx, h, now)
			return nil, fmt.Errorf("update resource: %w", err)
		}
	}

	m.rebuildManagerTimers()

	completedAt := now
	if err := m.storage.UpdateHandoverStatus(h.ID, model.HandoverStatusCompleted, now,
		"completed_at", completedAt); err != nil {
		return nil, err
	}
	m.addTimelineLocked(h.ID, model.HandoverStatusCompleted, operator,
		fmt.Sprintf("executed: %d locks, %d quotas, %d txs, %d reservations",
			len(ctx.appliedLocks), len(ctx.appliedQuotas), len(ctx.appliedTxs), len(ctx.appliedRes)))

	log.Printf("[handover] completed id=%d from=%s to=%s operator=%s", h.ID, h.FromCaller, h.ToCaller, operator)
	return m.loadHandoverFull(h.ID)
}

func (m *Manager) executeResourceLocked(ctx *execContext, item *model.HandoverResourceItem, h *model.Handover, now time.Time) error {
	switch item.ResourceType {
	case model.HandoverResourceLock:
		return m.executeLockTransfer(ctx, item, h, now)
	case model.HandoverResourceQuota:
		return m.executeQuotaTransfer(ctx, item, h, now)
	case model.HandoverResourceOrchTx:
		return m.executeOrchTxTransfer(ctx, item, h, now)
	case model.HandoverResourceTopology:
		return nil
	case model.HandoverResourceReservation:
		return m.executeReservationTransfer(ctx, item, h, now)
	}
	return nil
}

func (m *Manager) executeLockTransfer(ctx *execContext, item *model.HandoverResourceItem, h *model.Handover, now time.Time) error {
	l, err := m.storage.GetLock(item.ResourceKey)
	if err != nil {
		return err
	}
	if l == nil || l.Holder != h.FromCaller {
		return fmt.Errorf("lock no longer held by source")
	}

	if err := m.storage.TransferLockHolder(item.ResourceKey, h.ToCaller, now); err != nil {
		return err
	}

	lease, _ := m.storage.GetActiveLease(item.ResourceKey)
	if lease != nil {
		remaining := time.Until(lease.ExpiresAt)
		if remaining < 0 {
			remaining = 0
		}
		newExpires := now.Add(remaining)
		if err := m.storage.TransferLeaseHolder(item.ResourceKey, h.ToCaller, newExpires, now); err != nil {
			return err
		}
	}
	ctx.appliedLocks = append(ctx.appliedLocks, item.ResourceKey)
	return nil
}

func (m *Manager) executeQuotaTransfer(ctx *execContext, item *model.HandoverResourceItem, h *model.Handover, now time.Time) error {
	fromBinding, err := m.storage.GetCallerBinding(h.FromCaller)
	if err != nil {
		return err
	}
	if fromBinding == nil {
		return fmt.Errorf("source caller binding not found")
	}

	toBinding, _ := m.storage.GetCallerBinding(h.ToCaller)
	if toBinding == nil {
		toBinding = &model.CallerBinding{
			CallerID:   h.ToCaller,
			PolicyName: fromBinding.PolicyName,
			QuotaLimit: fromBinding.QuotaLimit,
			UsedTokens: fromBinding.UsedTokens,
			BorrowedTokens: fromBinding.BorrowedTokens,
			LentTokens: fromBinding.LentTokens,
			ReservedTokens: fromBinding.ReservedTokens,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := m.storage.UpsertCallerBinding(toBinding); err != nil {
			return err
		}
	} else {
		toBinding.UsedTokens += fromBinding.UsedTokens
		toBinding.BorrowedTokens += fromBinding.BorrowedTokens
		toBinding.LentTokens += fromBinding.LentTokens
		toBinding.ReservedTokens += fromBinding.ReservedTokens
		toBinding.UpdatedAt = now
		if err := m.storage.UpdateCallerBinding(toBinding); err != nil {
			return err
		}
	}

	fromBinding.UsedTokens = 0
	fromBinding.BorrowedTokens = 0
	fromBinding.LentTokens = 0
	fromBinding.ReservedTokens = 0
	fromBinding.UpdatedAt = now
	if err := m.storage.UpdateCallerBinding(fromBinding); err != nil {
		return err
	}

	ctx.appliedQuotas = append(ctx.appliedQuotas, item.ResourceKey)
	return nil
}

func (m *Manager) executeOrchTxTransfer(ctx *execContext, item *model.HandoverResourceItem, h *model.Handover, now time.Time) error {
	tx, err := m.storage.GetOrchTx(item.ResourceKey)
	if err != nil {
		return err
	}
	if tx == nil || tx.Holder != h.FromCaller {
		return fmt.Errorf("tx no longer held by source")
	}
	if err := m.storage.TransferOrchTxHolder(item.ResourceKey, h.ToCaller, now); err != nil {
		return err
	}
	if err := m.storage.TransferOrchTxLockHolder(item.ResourceKey, h.ToCaller); err != nil {
		return err
	}
	ctx.appliedTxs = append(ctx.appliedTxs, item.ResourceKey)
	return nil
}

func (m *Manager) executeReservationTransfer(ctx *execContext, item *model.HandoverResourceItem, h *model.Handover, now time.Time) error {
	var rid int64
	fmt.Sscanf(item.ResourceKey, "%d", &rid)
	r, err := m.storage.GetReservation(rid)
	if err != nil {
		return err
	}
	if r == nil || r.CallerID != h.FromCaller {
		return fmt.Errorf("reservation no longer held by source")
	}
	if err := m.storage.TransferReservationCaller(rid, h.ToCaller, now); err != nil {
		return err
	}
	ctx.appliedRes = append(ctx.appliedRes, rid)
	return nil
}

func (m *Manager) rollbackAllLocked(ctx *execContext, h *model.Handover, now time.Time) {
	for i := len(ctx.appliedRes) - 1; i >= 0; i-- {
		_ = m.storage.TransferReservationCaller(ctx.appliedRes[i], h.FromCaller, now)
	}
	for i := len(ctx.appliedTxs) - 1; i >= 0; i-- {
		_ = m.storage.TransferOrchTxHolder(ctx.appliedTxs[i], h.FromCaller, now)
		_ = m.storage.TransferOrchTxLockHolder(ctx.appliedTxs[i], h.FromCaller)
	}
	for i := len(ctx.appliedLocks) - 1; i >= 0; i-- {
		lease, _ := m.storage.GetActiveLease(ctx.appliedLocks[i])
		if lease != nil {
			_ = m.storage.TransferLeaseHolder(ctx.appliedLocks[i], h.FromCaller, lease.ExpiresAt, now)
		}
		_ = m.storage.TransferLockHolder(ctx.appliedLocks[i], h.FromCaller, now)
	}
}

func (m *Manager) rebuildManagerTimers() {
	_ = m.lockMgr
}

func (m *Manager) GetHandover(id int64) (*model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadHandoverFull(id)
}

func (m *Manager) loadHandoverFull(id int64) (*model.Handover, error) {
	h, err := m.storage.GetHandover(id)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, nil
	}
	resources, err := m.storage.ListHandoverResources(id)
	if err != nil {
		return nil, err
	}
	h.Resources = resources
	timeline, err := m.storage.ListHandoverTimeline(id)
	if err != nil {
		return nil, err
	}
	h.Timeline = timeline
	return h, nil
}

func (m *Manager) ListHandovers(fromCaller, toCaller, status string) ([]model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	list, err := m.storage.ListHandovers(fromCaller, toCaller, status)
	if err != nil {
		return nil, err
	}
	for i := range list {
		res, _ := m.storage.ListHandoverResources(list[i].ID)
		list[i].Resources = res
	}
	return list, nil
}

func (m *Manager) GetCallerSummary(callerID string) (*model.CallerHandoverSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	summary := &model.CallerHandoverSummary{CallerID: callerID}
	activeStatuses := []model.HandoverStatus{model.HandoverStatusCreated, model.HandoverStatusPreChecked, model.HandoverStatusPending}
	for _, s := range activeStatuses {
		c, _ := m.storage.CountHandoversByCallerAndStatus(callerID, true, s)
		summary.TransferringOut += c
		c2, _ := m.storage.CountHandoversByCallerAndStatus(callerID, false, s)
		summary.TransferringIn += c2
	}
	c, _ := m.storage.CountHandoversByCallerAndStatus(callerID, true, model.HandoverStatusCompleted)
	c2, _ := m.storage.CountHandoversByCallerAndStatus(callerID, false, model.HandoverStatusCompleted)
	summary.Completed = c + c2

	c3, _ := m.storage.CountHandoversByCallerAndStatus(callerID, true, model.HandoverStatusCancelled)
	c4, _ := m.storage.CountHandoversByCallerAndStatus(callerID, false, model.HandoverStatusCancelled)
	c5, _ := m.storage.CountHandoversByCallerAndStatus(callerID, true, model.HandoverStatusRejected)
	c6, _ := m.storage.CountHandoversByCallerAndStatus(callerID, false, model.HandoverStatusRejected)
	summary.Cancelled = c3 + c4 + c5 + c6

	return summary, nil
}

func (m *Manager) ListHandoversForCaller(callerID string) ([]model.Handover, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fromList, err := m.storage.ListHandovers(callerID, "", "")
	if err != nil {
		return nil, err
	}
	toList, err := m.storage.ListHandovers("", callerID, "")
	if err != nil {
		return nil, err
	}

	seen := make(map[int64]bool)
	combined := make([]model.Handover, 0, len(fromList)+len(toList))
	for _, h := range fromList {
		if !seen[h.ID] {
			seen[h.ID] = true
			res, _ := m.storage.ListHandoverResources(h.ID)
			h.Resources = res
			combined = append(combined, h)
		}
	}
	for _, h := range toList {
		if !seen[h.ID] {
			seen[h.ID] = true
			res, _ := m.storage.ListHandoverResources(h.ID)
			h.Resources = res
			combined = append(combined, h)
		}
	}
	return combined, nil
}

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (m *Manager) writeAudit(h *model.Handover, operator, action string, success bool, detail string) {
	if m.storage == nil || h == nil {
		return
	}
	resourceDesc := fmt.Sprintf("handover:#%d[%s] %s", h.ID, action, h.FromCaller+"->"+h.ToCaller)
	if detail != "" {
		resourceDesc = resourceDesc + " | " + detail
	}
	failReason := ""
	if !success {
		failReason = detail
	}
	callers := []string{h.FromCaller, h.ToCaller}
	if operator != "" && operator != h.FromCaller && operator != h.ToCaller {
		callers = append(callers, operator)
	}
	for _, c := range callers {
		entry := &model.AuditLog{
			Timestamp:  time.Now(),
			Caller:     c,
			Operation:  model.AuditOpHandover,
			Resource:   resourceDesc,
			Success:    success,
			FailReason: failReason,
		}
		_ = m.storage.AddAuditLog(entry)
	}
}
