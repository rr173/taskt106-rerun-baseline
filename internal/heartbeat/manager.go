package heartbeat

import (
	"fmt"
	"log"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/orchestration"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"sync"
	"time"
)

type Manager struct {
	storage  *storage.Storage
	lockMgr  *lock.Manager
	rlMgr    *ratelimit.Manager
	orchMgr  *orchestration.Manager
	mu       sync.Mutex
	stopCh   chan struct{}
	ticker   *time.Ticker
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager, om *orchestration.Manager) *Manager {
	return &Manager{
		storage: s,
		lockMgr: lm,
		rlMgr:   rlm,
		orchMgr: om,
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	registrations, err := m.storage.ListHeartbeatRegistrations()
	if err != nil {
		return fmt.Errorf("load registrations: %w", err)
	}

	now := time.Now()
	for _, reg := range registrations {
		if reg.Status == model.HeartbeatStatusLost || reg.Status == model.HeartbeatStatusFrozen {
			log.Printf("[heartbeat-manager] restored %s caller: %s (status=%s)", reg.Status, reg.CallerID, reg.Status)
		} else if reg.NextExpectedAt.Before(now) {
			missedDuration := now.Sub(reg.LastHeartbeatAt).Seconds()
			allowedWindow := float64(reg.IntervalSec * reg.MaxMissed)
			if missedDuration > allowedWindow {
				log.Printf("[heartbeat-manager] caller %s already missed heartbeat window on startup, checking status", reg.CallerID)
				go m.checkAndHandleCaller(reg.CallerID)
			}
		}
	}

	m.ticker = time.NewTicker(500 * time.Millisecond)
	go m.heartbeatCheckLoop()

	log.Printf("[heartbeat-manager] started with %d registered callers", len(registrations))
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[heartbeat-manager] stopped")
}

func (m *Manager) heartbeatCheckLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.checkAllCallers()
		}
	}
}

func (m *Manager) checkAllCallers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	registrations, err := m.storage.ListHeartbeatRegistrations()
	if err != nil {
		log.Printf("[heartbeat-manager] list registrations error: %v", err)
		return
	}

	now := time.Now()
	updatedGroups := make(map[string]bool)
	groupsToRefresh := make(map[string]bool)
	for _, reg := range registrations {
		if reg.Status == model.HeartbeatStatusFrozen {
			continue
		}

		oldStatus := reg.Status
		m.checkCallerLocked(&reg, now)
		if reg.GroupName != "" {
			if reg.Status != oldStatus {
				updatedGroups[reg.GroupName] = true
			} else if reg.Status == model.HeartbeatStatusLost || reg.Status == model.HeartbeatStatusRecovered {
				groupsToRefresh[reg.GroupName] = true
			}
		}
	}

	for groupName := range updatedGroups {
		m.updateGroupHealthLocked(groupName)
		delete(groupsToRefresh, groupName)
	}
	for groupName := range groupsToRefresh {
		m.updateGroupHealthLocked(groupName)
	}
}

func (m *Manager) checkAndHandleCaller(callerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg, err := m.storage.GetHeartbeatRegistration(callerID)
	if err != nil {
		log.Printf("[heartbeat-manager] get registration error: %v", err)
		return
	}
	if reg == nil {
		return
	}

	m.checkCallerLocked(reg, time.Now())
}

func (m *Manager) checkCallerLocked(reg *model.HeartbeatRegistration, now time.Time) {
	if reg.Status == model.HeartbeatStatusFrozen {
		return
	}

	allowedWindow := float64(reg.IntervalSec * reg.MaxMissed)
	timeSinceLast := now.Sub(reg.LastHeartbeatAt).Seconds()

	if timeSinceLast <= float64(reg.IntervalSec) {
		if reg.Status == model.HeartbeatStatusSuspect {
			reg.Status = model.HeartbeatStatusHealthy
			reg.MissedCount = 0
			reg.UpdatedAt = now
			_ = m.storage.UpdateHeartbeatRegistration(reg)
			m.addEventLocked(reg.CallerID, "status_change",
				model.HeartbeatStatusSuspect, model.HeartbeatStatusHealthy,
				fmt.Sprintf("heartbeat restored after %.1fs", timeSinceLast))
		} else if reg.Status == model.HeartbeatStatusRecovered {
			reg.Status = model.HeartbeatStatusHealthy
			reg.MissedCount = 0
			reg.UpdatedAt = now
			_ = m.storage.UpdateHeartbeatRegistration(reg)
			m.addEventLocked(reg.CallerID, "status_change",
				model.HeartbeatStatusRecovered, model.HeartbeatStatusHealthy,
				"recovered caller heartbeat confirmed healthy")
		}
		return
	}

	missedCount := int(timeSinceLast / float64(reg.IntervalSec))
	reg.MissedCount = missedCount

	if timeSinceLast > allowedWindow {
		if reg.Status == model.HeartbeatStatusHealthy || reg.Status == model.HeartbeatStatusSuspect || reg.Status == model.HeartbeatStatusRecovered {
			m.handleConnectionLostLocked(reg, now, timeSinceLast)
		}
	} else if timeSinceLast > float64(reg.IntervalSec) && reg.Status == model.HeartbeatStatusHealthy {
		reg.Status = model.HeartbeatStatusSuspect
		reg.UpdatedAt = now
		_ = m.storage.UpdateHeartbeatRegistration(reg)
		m.addEventLocked(reg.CallerID, "status_change",
			model.HeartbeatStatusHealthy, model.HeartbeatStatusSuspect,
			fmt.Sprintf("missed %.1fs of %ds interval, %.0f%% of allowed window",
				timeSinceLast, reg.IntervalSec, (timeSinceLast/allowedWindow)*100))
		log.Printf("[heartbeat-manager] caller %s suspect: missed %.1fs (allowed %.1fs)",
			reg.CallerID, timeSinceLast, allowedWindow)
	}
}

func (m *Manager) handleConnectionLostLocked(reg *model.HeartbeatRegistration, now time.Time, timeSinceLast float64) {
	oldStatus := reg.Status
	reg.Status = model.HeartbeatStatusLost
	lostAt := now
	reg.LostAt = &lostAt
	reg.UpdatedAt = now
	_ = m.storage.UpdateHeartbeatRegistration(reg)

	m.addEventLocked(reg.CallerID, "connection_lost",
		oldStatus, model.HeartbeatStatusLost,
		fmt.Sprintf("no heartbeat for %.1fs (allowed %.1fs = %ds * %d)",
			timeSinceLast, float64(reg.IntervalSec*reg.MaxMissed), reg.IntervalSec, reg.MaxMissed))

	log.Printf("[heartbeat-manager] caller %s LOST: no heartbeat for %.1fs, strategy=%s",
		reg.CallerID, timeSinceLast, reg.Strategy)

	switch reg.Strategy {
	case model.StrategyReleaseAll:
		m.releaseAllResourcesLocked(reg.CallerID, now)
	case model.StrategyReleaseLock:
		m.releaseLocksOnlyLocked(reg.CallerID, now)
	case model.StrategyFreeze:
		m.freezeResourcesLocked(reg.CallerID, now)
	}

	if reg.GroupName != "" {
		m.updateGroupHealthLocked(reg.GroupName)
	}
}

func (m *Manager) releaseAllResourcesLocked(callerID string, now time.Time) {
	log.Printf("[heartbeat-manager] releasing ALL resources for lost caller: %s", callerID)

	m.releaseLocksOnlyLocked(callerID, now)
	m.returnTokensLocked(callerID, now)
	m.cancelOrchestrationTxsLocked(callerID, now)

	m.addEventLocked(callerID, "disposal_executed",
		model.HeartbeatStatusLost, model.HeartbeatStatusLost,
		"strategy=release_all: released locks, returned tokens, cancelled transactions")
}

func (m *Manager) releaseLocksOnlyLocked(callerID string, now time.Time) {
	log.Printf("[heartbeat-manager] releasing locks for lost caller: %s", callerID)

	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		log.Printf("[heartbeat-manager] list leases error: %v", err)
		return
	}

	releasedCount := 0
	for _, lease := range leases {
		if lease.Holder == callerID {
			if _, err := m.lockMgr.ReleaseLock(lease.LockName, callerID); err != nil {
				log.Printf("[heartbeat-manager] release lock %s error: %v", lease.LockName, err)
			} else {
				releasedCount++
				log.Printf("[heartbeat-manager] released lock: %s (holder=%s)", lease.LockName, callerID)
			}
		}
	}

	_, _ = m.lockMgr.CancelWaitForHolder(callerID)

	if releasedCount > 0 {
		m.addEventLocked(callerID, "locks_released",
			model.HeartbeatStatusLost, model.HeartbeatStatusLost,
			fmt.Sprintf("released %d active locks", releasedCount))
	}
}

func (m *Manager) returnTokensLocked(callerID string, now time.Time) {
	log.Printf("[heartbeat-manager] returning tokens for lost caller: %s", callerID)

	binding, err := m.storage.GetCallerBinding(callerID)
	if err != nil {
		log.Printf("[heartbeat-manager] get caller binding error: %v", err)
		return
	}
	if binding == nil {
		log.Printf("[heartbeat-manager] no caller binding for: %s", callerID)
		return
	}

	if binding.UsedTokens > 0 {
		if err := m.rlMgr.ReturnTokens(callerID, binding.UsedTokens); err != nil {
			log.Printf("[heartbeat-manager] return tokens error: %v", err)
		} else {
			log.Printf("[heartbeat-manager] returned %d tokens for caller: %s", binding.UsedTokens, callerID)
			m.addEventLocked(callerID, "tokens_returned",
				model.HeartbeatStatusLost, model.HeartbeatStatusLost,
				fmt.Sprintf("returned %d used tokens to quota pool", binding.UsedTokens))
		}
	}

	waitItems, err := m.storage.ListWaitItemsByCaller(callerID)
	if err != nil {
		log.Printf("[heartbeat-manager] list wait items error: %v", err)
	} else {
		for _, item := range waitItems {
			_ = m.storage.RemoveWaitItem(item.ID)
		}
	}
}

func (m *Manager) cancelOrchestrationTxsLocked(callerID string, now time.Time) {
	log.Printf("[heartbeat-manager] cancelling orchestration transactions for lost caller: %s", callerID)

	txs, err := m.storage.ListOrchTxs(string(model.TxStatusCommitted))
	if err != nil {
		log.Printf("[heartbeat-manager] list txs error: %v", err)
		return
	}

	cancelledCount := 0
	for _, tx := range txs {
		if tx.Holder == callerID {
			if _, err := m.orchMgr.ReleaseTx(tx.ID, callerID); err != nil {
				log.Printf("[heartbeat-manager] cancel tx %s error: %v", tx.ID, err)
			} else {
				cancelledCount++
				log.Printf("[heartbeat-manager] cancelled orchestration tx: %s (holder=%s)", tx.ID, callerID)
			}
		}
	}

	if cancelledCount > 0 {
		m.addEventLocked(callerID, "transactions_cancelled",
			model.HeartbeatStatusLost, model.HeartbeatStatusLost,
			fmt.Sprintf("cancelled %d active orchestration transactions", cancelledCount))
	}
}

func (m *Manager) freezeResourcesLocked(callerID string, now time.Time) {
	log.Printf("[heartbeat-manager] FREEZING resources for lost caller: %s", callerID)

	reg, _ := m.storage.GetHeartbeatRegistration(callerID)
	if reg != nil {
		reg.Status = model.HeartbeatStatusFrozen
		frozenAt := now
		reg.FrozenAt = &frozenAt
		reg.UpdatedAt = now
		_ = m.storage.UpdateHeartbeatRegistration(reg)
	}

	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		log.Printf("[heartbeat-manager] list leases error: %v", err)
		return
	}

	frozenCount := 0
	for _, lease := range leases {
		if lease.Holder == callerID {
			fr := &model.FrozenResource{
				CallerID:     callerID,
				ResourceType: "lock",
				ResourceKey:  lease.LockName,
				FrozenAt:     now,
			}
			if err := m.storage.CreateFrozenResource(fr); err != nil {
				log.Printf("[heartbeat-manager] freeze lock %s error: %v", lease.LockName, err)
			} else {
				frozenCount++
				log.Printf("[heartbeat-manager] frozen lock: %s", lease.LockName)
			}
		}
	}

	binding, _ := m.storage.GetCallerBinding(callerID)
	if binding != nil && binding.UsedTokens > 0 {
		fr := &model.FrozenResource{
			CallerID:     callerID,
			ResourceType: "tokens",
			ResourceKey:  binding.PolicyName,
			FrozenAt:     now,
		}
		if err := m.storage.CreateFrozenResource(fr); err != nil {
			log.Printf("[heartbeat-manager] freeze tokens error: %v", err)
		} else {
			frozenCount++
			log.Printf("[heartbeat-manager] frozen %d tokens for policy: %s", binding.UsedTokens, binding.PolicyName)
		}
	}

	txs, _ := m.storage.ListOrchTxs(string(model.TxStatusCommitted))
	for _, tx := range txs {
		if tx.Holder == callerID {
			fr := &model.FrozenResource{
				CallerID:     callerID,
				ResourceType: "orchestration_tx",
				ResourceKey:  tx.ID,
				FrozenAt:     now,
			}
			if err := m.storage.CreateFrozenResource(fr); err != nil {
				log.Printf("[heartbeat-manager] freeze tx %s error: %v", tx.ID, err)
			} else {
				frozenCount++
				log.Printf("[heartbeat-manager] frozen orchestration tx: %s", tx.ID)
			}
		}
	}

	m.addEventLocked(callerID, "resources_frozen",
		model.HeartbeatStatusLost, model.HeartbeatStatusFrozen,
		fmt.Sprintf("frozen %d resources awaiting manual confirmation", frozenCount))
}

func (m *Manager) Register(callerID string, groupName string, intervalSec int, maxMissed int, strategy model.DisposalStrategy) (*model.HeartbeatRegistration, error) {
	if intervalSec <= 0 {
		return nil, fmt.Errorf("interval_sec must be positive")
	}
	if maxMissed <= 0 {
		return nil, fmt.Errorf("max_missed must be positive")
	}
	if strategy != model.StrategyReleaseAll &&
		strategy != model.StrategyReleaseLock &&
		strategy != model.StrategyFreeze {
		return nil, fmt.Errorf("invalid strategy: %s", strategy)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if groupName != "" {
		group, err := m.storage.GetHeartbeatGroup(groupName)
		if err != nil {
			return nil, fmt.Errorf("check group: %w", err)
		}
		if group == nil {
			return nil, fmt.Errorf("group not found: %s", groupName)
		}
	}

	existing, err := m.storage.GetHeartbeatRegistration(callerID)
	if err != nil {
		return nil, fmt.Errorf("check existing: %w", err)
	}

	var oldGroupName string
	if existing != nil {
		if existing.Status != model.HeartbeatStatusLost && existing.Status != model.HeartbeatStatusRecovered {
			return nil, fmt.Errorf("caller already registered with status: %s", existing.Status)
		}
		oldGroupName = existing.GroupName
	}

	now := time.Now()
	reg := &model.HeartbeatRegistration{
		CallerID:        callerID,
		GroupName:       groupName,
		IntervalSec:     intervalSec,
		MaxMissed:       maxMissed,
		Strategy:        strategy,
		LastHeartbeatAt: now,
		NextExpectedAt:  now.Add(time.Duration(intervalSec) * time.Second),
		MissedCount:     0,
		Status:          model.HeartbeatStatusHealthy,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := m.storage.CreateHeartbeatRegistration(reg); err != nil {
		return nil, fmt.Errorf("create registration: %w", err)
	}

	if oldGroupName != "" && oldGroupName != groupName {
		m.updateGroupHealthLocked(oldGroupName)
	}
	if groupName != "" {
		m.updateGroupHealthLocked(groupName)
	}

	m.addEventLocked(callerID, "registered", "", model.HeartbeatStatusHealthy,
		fmt.Sprintf("registered with group=%s, interval=%ds, max_missed=%d, strategy=%s", groupName, intervalSec, maxMissed, strategy))

	log.Printf("[heartbeat-manager] registered caller: %s (group=%s, interval=%ds, max_missed=%d, strategy=%s)",
		callerID, groupName, intervalSec, maxMissed, strategy)

	return reg, nil
}

func (m *Manager) CreateGroup(name string, survivalThreshold int) (*model.HeartbeatGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("group name cannot be empty")
	}
	if survivalThreshold <= 0 {
		return nil, fmt.Errorf("survival_threshold must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, err := m.storage.GetHeartbeatGroup(name)
	if err != nil {
		return nil, fmt.Errorf("check existing group: %w", err)
	}
	if existing != nil {
		return nil, fmt.Errorf("group already exists: %s", name)
	}

	now := time.Now()
	group := &model.HeartbeatGroup{
		Name:              name,
		SurvivalThreshold: survivalThreshold,
		Status:            model.HeartbeatGroupHealthy,
		AliveCount:        0,
		TotalCount:        0,
		Degraded:          false,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := m.storage.CreateHeartbeatGroup(group); err != nil {
		return nil, fmt.Errorf("create group: %w", err)
	}

	log.Printf("[heartbeat-manager] created group: %s (survival_threshold=%d)", name, survivalThreshold)
	return group, nil
}

func (m *Manager) DeleteGroup(name string) error {
	if name == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.storage.GetHeartbeatGroup(name)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf("group not found: %s", name)
	}

	members, err := m.storage.ListHeartbeatRegistrationsByGroup(name)
	if err != nil {
		return fmt.Errorf("list group members: %w", err)
	}
	if len(members) > 0 {
		return fmt.Errorf("cannot delete group with %d members", len(members))
	}

	deps, err := m.storage.ListGroupDependenciesByGroup(name)
	if err != nil {
		return fmt.Errorf("list group dependencies: %w", err)
	}
	for _, dep := range deps {
		if err := m.storage.RemoveGroupDependency(dep.GroupName, dep.DependsOn); err != nil {
			log.Printf("[heartbeat-manager] remove dependency error: %v", err)
		}
	}

	reverseDeps, err := m.storage.ListGroupsThatDependOn(name)
	if err != nil {
		return fmt.Errorf("list reverse dependencies: %w", err)
	}
	for _, dep := range reverseDeps {
		if err := m.storage.RemoveGroupDependency(dep.GroupName, dep.DependsOn); err != nil {
			log.Printf("[heartbeat-manager] remove reverse dependency error: %v", err)
		}
	}

	if err := m.storage.DeleteHeartbeatGroup(name); err != nil {
		return fmt.Errorf("delete group: %w", err)
	}

	log.Printf("[heartbeat-manager] deleted group: %s", name)
	return nil
}

func (m *Manager) updateGroupHealthLocked(groupName string) {
	group, err := m.storage.GetHeartbeatGroup(groupName)
	if err != nil {
		log.Printf("[heartbeat-manager] get group error: %v", err)
		return
	}
	if group == nil {
		return
	}

	members, err := m.storage.ListHeartbeatRegistrationsByGroup(groupName)
	if err != nil {
		log.Printf("[heartbeat-manager] list group members error: %v", err)
		return
	}

	aliveCount := 0
	for _, member := range members {
		if member.Status == model.HeartbeatStatusHealthy ||
			member.Status == model.HeartbeatStatusSuspect ||
			member.Status == model.HeartbeatStatusRecovered {
			aliveCount++
		}
	}

	oldStatus := group.Status
	group.AliveCount = aliveCount
	group.TotalCount = len(members)

	if aliveCount >= group.SurvivalThreshold {
		group.Status = model.HeartbeatGroupHealthy
	} else {
		group.Status = model.HeartbeatGroupUnhealthy
	}
	group.UpdatedAt = time.Now()

	if err := m.storage.UpdateHeartbeatGroup(group); err != nil {
		log.Printf("[heartbeat-manager] update group error: %v", err)
		return
	}

	if oldStatus != group.Status {
		log.Printf("[heartbeat-manager] group %s status changed: %s -> %s (alive=%d, threshold=%d)",
			groupName, oldStatus, group.Status, aliveCount, group.SurvivalThreshold)
		m.addGroupEventLocked(groupName, "status_change",
			string(oldStatus), string(group.Status),
			fmt.Sprintf("alive=%d, threshold=%d", aliveCount, group.SurvivalThreshold))
	}

	m.checkCascadeDegradeLocked(group)
}

func (m *Manager) checkCascadeDegradeLocked(group *model.HeartbeatGroup) {
	if group.Status != model.HeartbeatGroupUnhealthy {
		dependees, err := m.storage.ListGroupsThatDependOn(group.Name)
		if err != nil {
			log.Printf("[heartbeat-manager] list dependees error: %v", err)
			return
		}
		for _, dep := range dependees {
			depGroup, err := m.storage.GetHeartbeatGroup(dep.GroupName)
			if err != nil {
				log.Printf("[heartbeat-manager] get dependant group error: %v", err)
				continue
			}
			if depGroup != nil && depGroup.Degraded {
				m.tryRestoreGroupLocked(depGroup)
			}
		}
		return
	}

	dependees, err := m.storage.ListGroupsThatDependOn(group.Name)
	if err != nil {
		log.Printf("[heartbeat-manager] list dependees error: %v", err)
		return
	}

	for _, dep := range dependees {
		depGroup, err := m.storage.GetHeartbeatGroup(dep.GroupName)
		if err != nil {
			log.Printf("[heartbeat-manager] get dependant group error: %v", err)
			continue
		}
		if depGroup == nil || depGroup.Degraded {
			continue
		}

		now := time.Now()
		depGroup.Degraded = true
		depGroup.DegradedReason = fmt.Sprintf("dependency group %s is unhealthy", group.Name)
		depGroup.DegradedAt = &now
		depGroup.Status = model.HeartbeatGroupDegraded
		depGroup.UpdatedAt = now

		if err := m.storage.UpdateHeartbeatGroup(depGroup); err != nil {
			log.Printf("[heartbeat-manager] degrade group error: %v", err)
			continue
		}

		log.Printf("[heartbeat-manager] group %s DEGRADED: dependency %s is unhealthy", depGroup.Name, group.Name)
		m.addGroupEventLocked(depGroup.Name, "degraded",
			string(depGroup.Status), string(model.HeartbeatGroupDegraded),
			fmt.Sprintf("dependency group %s is unhealthy", group.Name))

		m.checkCascadeDegradeLocked(depGroup)
	}
}

func (m *Manager) tryRestoreGroupLocked(group *model.HeartbeatGroup) {
	deps, err := m.storage.ListGroupDependenciesByGroup(group.Name)
	if err != nil {
		log.Printf("[heartbeat-manager] list dependencies error: %v", err)
		return
	}

	allHealthy := true
	for _, dep := range deps {
		depGroup, err := m.storage.GetHeartbeatGroup(dep.DependsOn)
		if err != nil {
			log.Printf("[heartbeat-manager] get dependency group error: %v", err)
			allHealthy = false
			break
		}
		if depGroup == nil || depGroup.Status == model.HeartbeatGroupUnhealthy || depGroup.Degraded {
			allHealthy = false
			break
		}
	}

	if !allHealthy {
		return
	}

	now := time.Now()
	oldStatus := group.Status
	group.Degraded = false
	group.DegradedReason = ""
	group.DegradedAt = nil
	if group.AliveCount >= group.SurvivalThreshold {
		group.Status = model.HeartbeatGroupHealthy
	} else {
		group.Status = model.HeartbeatGroupUnhealthy
	}
	group.UpdatedAt = now

	if err := m.storage.UpdateHeartbeatGroup(group); err != nil {
		log.Printf("[heartbeat-manager] restore group error: %v", err)
		return
	}

	log.Printf("[heartbeat-manager] group %s RESTORED from degraded: %s -> %s",
		group.Name, oldStatus, group.Status)
	m.addGroupEventLocked(group.Name, "restored",
		string(model.HeartbeatGroupDegraded), string(group.Status),
		"all dependencies are healthy again")

	dependees, err := m.storage.ListGroupsThatDependOn(group.Name)
	if err != nil {
		log.Printf("[heartbeat-manager] list dependees error: %v", err)
		return
	}
	for _, dep := range dependees {
		depGroup, err := m.storage.GetHeartbeatGroup(dep.GroupName)
		if err != nil {
			log.Printf("[heartbeat-manager] get dependant group error: %v", err)
			continue
		}
		if depGroup != nil && depGroup.Degraded {
			m.tryRestoreGroupLocked(depGroup)
		}
	}
}

func (m *Manager) AddGroupDependency(groupName string, dependsOn string) error {
	if groupName == "" || dependsOn == "" {
		return fmt.Errorf("group names cannot be empty")
	}
	if groupName == dependsOn {
		return fmt.Errorf("cannot add self-dependency")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.storage.GetHeartbeatGroup(groupName)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf("group not found: %s", groupName)
	}

	depGroup, err := m.storage.GetHeartbeatGroup(dependsOn)
	if err != nil {
		return fmt.Errorf("get dependency group: %w", err)
	}
	if depGroup == nil {
		return fmt.Errorf("dependency group not found: %s", dependsOn)
	}

	if m.wouldCreateCycleLocked(groupName, dependsOn) {
		return fmt.Errorf("adding dependency would create a cycle")
	}

	now := time.Now()
	dep := &model.HeartbeatGroupDependency{
		GroupName: groupName,
		DependsOn: dependsOn,
		CreatedAt: now,
	}

	if err := m.storage.CreateGroupDependency(dep); err != nil {
		return fmt.Errorf("create dependency: %w", err)
	}

	log.Printf("[heartbeat-manager] added dependency: %s -> %s", groupName, dependsOn)

	if depGroup.Status == model.HeartbeatGroupUnhealthy || depGroup.Degraded {
		m.checkCascadeDegradeLocked(depGroup)
	}

	return nil
}

func (m *Manager) wouldCreateCycleLocked(groupName, dependsOn string) bool {
	visited := make(map[string]bool)
	var dfs func(string) bool
	dfs = func(current string) bool {
		if current == groupName {
			return true
		}
		if visited[current] {
			return false
		}
		visited[current] = true
		deps, _ := m.storage.ListGroupDependenciesByGroup(current)
		for _, dep := range deps {
			if dfs(dep.DependsOn) {
				return true
			}
		}
		return false
	}
	return dfs(dependsOn)
}

func (m *Manager) RemoveGroupDependency(groupName string, dependsOn string) error {
	if groupName == "" || dependsOn == "" {
		return fmt.Errorf("group names cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.storage.RemoveGroupDependency(groupName, dependsOn); err != nil {
		return fmt.Errorf("remove dependency: %w", err)
	}

	log.Printf("[heartbeat-manager] removed dependency: %s -> %s", groupName, dependsOn)

	group, err := m.storage.GetHeartbeatGroup(groupName)
	if err != nil {
		log.Printf("[heartbeat-manager] get group error: %v", err)
	} else if group != nil && group.Degraded {
		m.tryRestoreGroupLocked(group)
	}

	return nil
}

func (m *Manager) IsGroupDegraded(callerID string) (bool, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg, err := m.storage.GetHeartbeatRegistration(callerID)
	if err != nil {
		return false, "", fmt.Errorf("get registration: %w", err)
	}
	if reg == nil || reg.GroupName == "" {
		return false, "", nil
	}

	group, err := m.storage.GetHeartbeatGroup(reg.GroupName)
	if err != nil {
		return false, "", fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return false, "", nil
	}

	return group.Degraded, group.DegradedReason, nil
}

func (m *Manager) ReportHeartbeat(callerID string) (*model.HeartbeatRegistration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg, err := m.storage.GetHeartbeatRegistration(callerID)
	if err != nil {
		return nil, fmt.Errorf("get registration: %w", err)
	}
	if reg == nil {
		return nil, fmt.Errorf("caller not registered: %s", callerID)
	}

	now := time.Now()

	if reg.Status == model.HeartbeatStatusFrozen {
		return nil, fmt.Errorf("caller resources are frozen, cannot renew heartbeat")
	}

	reg.LastHeartbeatAt = now
	reg.NextExpectedAt = now.Add(time.Duration(reg.IntervalSec) * time.Second)
	reg.MissedCount = 0

	if reg.Status == model.HeartbeatStatusLost {
		reg.Status = model.HeartbeatStatusRecovered
		recoveredAt := now
		reg.RecoveredAt = &recoveredAt
		m.addEventLocked(callerID, "recovered",
			model.HeartbeatStatusLost, model.HeartbeatStatusRecovered,
			fmt.Sprintf("caller recovered after %.1fs outage. NOTE: previously released resources are NOT restored automatically.",
				now.Sub(*reg.LostAt).Seconds()))
		log.Printf("[heartbeat-manager] caller %s RECOVERED after %.1fs outage",
			callerID, now.Sub(*reg.LostAt).Seconds())
	} else if reg.Status == model.HeartbeatStatusSuspect {
		reg.Status = model.HeartbeatStatusHealthy
		m.addEventLocked(callerID, "recovered",
			model.HeartbeatStatusSuspect, model.HeartbeatStatusHealthy,
			"heartbeat resumed after temporary suspension")
	} else {
		reg.Status = model.HeartbeatStatusHealthy
	}

	reg.UpdatedAt = now
	if err := m.storage.UpdateHeartbeatRegistration(reg); err != nil {
		return nil, fmt.Errorf("update registration: %w", err)
	}

	if reg.GroupName != "" {
		m.updateGroupHealthLocked(reg.GroupName)
	}

	return reg, nil
}

func (m *Manager) GetStatus(callerID string) (*model.HeartbeatStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg, err := m.storage.GetHeartbeatRegistration(callerID)
	if err != nil {
		return nil, fmt.Errorf("get registration: %w", err)
	}
	if reg == nil {
		return nil, fmt.Errorf("caller not registered: %s", callerID)
	}

	now := time.Now()
	info := &model.HeartbeatStatusInfo{
		CallerID:         reg.CallerID,
		Status:           reg.Status,
		IntervalSec:      reg.IntervalSec,
		MaxMissed:        reg.MaxMissed,
		LastHeartbeatAt:  reg.LastHeartbeatAt,
		NextExpectedAt:   reg.NextExpectedAt,
		MissedCount:      reg.MissedCount,
		Strategy:         reg.Strategy,
		SecondsSinceLast: now.Sub(reg.LastHeartbeatAt).Seconds(),
	}

	return info, nil
}

func (m *Manager) ListAllStatuses() ([]model.HeartbeatStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	registrations, err := m.storage.ListHeartbeatRegistrations()
	if err != nil {
		return nil, fmt.Errorf("list registrations: %w", err)
	}

	now := time.Now()
	result := make([]model.HeartbeatStatusInfo, 0, len(registrations))
	for _, reg := range registrations {
		info := model.HeartbeatStatusInfo{
			CallerID:         reg.CallerID,
			Status:           reg.Status,
			IntervalSec:      reg.IntervalSec,
			MaxMissed:        reg.MaxMissed,
			LastHeartbeatAt:  reg.LastHeartbeatAt,
			NextExpectedAt:   reg.NextExpectedAt,
			MissedCount:      reg.MissedCount,
			Strategy:         reg.Strategy,
			SecondsSinceLast: now.Sub(reg.LastHeartbeatAt).Seconds(),
		}
		result = append(result, info)
	}

	return result, nil
}

func (m *Manager) GetReport() (*model.HeartbeatReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	registrations, err := m.storage.ListHeartbeatRegistrations()
	if err != nil {
		return nil, fmt.Errorf("list registrations: %w", err)
	}

	report := &model.HeartbeatReport{
		RegisteredCount: len(registrations),
	}

	for _, reg := range registrations {
		switch reg.Status {
		case model.HeartbeatStatusHealthy:
			report.HealthyCount++
		case model.HeartbeatStatusSuspect:
			report.SuspectCount++
		case model.HeartbeatStatusLost:
			report.LostCount++
		case model.HeartbeatStatusFrozen:
			report.FrozenCount++
		case model.HeartbeatStatusRecovered:
			report.RecoveredCount++
		}
	}

	return report, nil
}

func (m *Manager) ListEvents(callerID string, limit int) ([]model.HeartbeatEvent, error) {
	return m.storage.ListHeartbeatEvents(callerID, limit)
}

func (m *Manager) ListFrozenResources(callerID string) ([]model.FrozenResource, error) {
	return m.storage.ListFrozenResources(callerID)
}

func (m *Manager) ReleaseFrozenResource(id int64, callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	frozen, err := m.storage.ListFrozenResources(callerID)
	if err != nil {
		return fmt.Errorf("list frozen resources: %w", err)
	}

	var target *model.FrozenResource
	for _, f := range frozen {
		if f.ID == id {
			target = &f
			break
		}
	}
	if target == nil {
		return fmt.Errorf("frozen resource not found or not owned by caller: %d", id)
	}

	now := time.Now()
	if err := m.storage.ReleaseFrozenResource(id, now); err != nil {
		return fmt.Errorf("release frozen resource: %w", err)
	}

	switch target.ResourceType {
	case "lock":
		if _, err := m.lockMgr.ReleaseLock(target.ResourceKey, callerID); err != nil {
			log.Printf("[heartbeat-manager] release frozen lock %s error: %v", target.ResourceKey, err)
		} else {
			log.Printf("[heartbeat-manager] released frozen lock: %s for caller: %s", target.ResourceKey, callerID)
		}
	case "tokens":
		binding, _ := m.storage.GetCallerBinding(callerID)
		if binding != nil && binding.UsedTokens > 0 {
			if err := m.rlMgr.ReturnTokens(callerID, binding.UsedTokens); err != nil {
				log.Printf("[heartbeat-manager] return frozen tokens error: %v", err)
			} else {
				log.Printf("[heartbeat-manager] returned frozen tokens for caller: %s", callerID)
			}
		}
	case "orchestration_tx":
		if _, err := m.orchMgr.ReleaseTx(target.ResourceKey, callerID); err != nil {
			log.Printf("[heartbeat-manager] cancel frozen tx %s error: %v", target.ResourceKey, err)
		} else {
			log.Printf("[heartbeat-manager] cancelled frozen tx: %s for caller: %s", target.ResourceKey, callerID)
		}
	}

	reg, _ := m.storage.GetHeartbeatRegistration(callerID)
	if reg != nil {
		remaining, _ := m.storage.ListFrozenResources(callerID)
		if len(remaining) == 0 {
			reg.Status = model.HeartbeatStatusRecovered
			reg.UpdatedAt = now
			_ = m.storage.UpdateHeartbeatRegistration(reg)
			m.addEventLocked(callerID, "frozen_resources_released",
				model.HeartbeatStatusFrozen, model.HeartbeatStatusRecovered,
				fmt.Sprintf("released frozen resource id=%d (%s: %s), all resources now released",
					id, target.ResourceType, target.ResourceKey))
		} else {
			m.addEventLocked(callerID, "frozen_resource_released",
				model.HeartbeatStatusFrozen, model.HeartbeatStatusFrozen,
				fmt.Sprintf("released frozen resource id=%d (%s: %s), %d remaining",
					id, target.ResourceType, target.ResourceKey, len(remaining)))
		}
	}

	return nil
}

func (m *Manager) ReleaseAllFrozenResources(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	frozen, err := m.storage.ListFrozenResources(callerID)
	if err != nil {
		return fmt.Errorf("list frozen resources: %w", err)
	}

	now := time.Now()
	for _, f := range frozen {
		if err := m.storage.ReleaseFrozenResource(f.ID, now); err != nil {
			log.Printf("[heartbeat-manager] release frozen resource %d error: %v", f.ID, err)
			continue
		}

		switch f.ResourceType {
		case "lock":
			_, _ = m.lockMgr.ReleaseLock(f.ResourceKey, callerID)
		case "tokens":
			binding, _ := m.storage.GetCallerBinding(callerID)
			if binding != nil && binding.UsedTokens > 0 {
				_ = m.rlMgr.ReturnTokens(callerID, binding.UsedTokens)
			}
		case "orchestration_tx":
			_, _ = m.orchMgr.ReleaseTx(f.ResourceKey, callerID)
		}
	}

	if err := m.storage.ReleaseAllFrozenResources(callerID, now); err != nil {
		log.Printf("[heartbeat-manager] release all frozen resources error: %v", err)
	}

	reg, _ := m.storage.GetHeartbeatRegistration(callerID)
	if reg != nil {
		reg.Status = model.HeartbeatStatusRecovered
		reg.UpdatedAt = now
		_ = m.storage.UpdateHeartbeatRegistration(reg)
		m.addEventLocked(callerID, "all_frozen_released",
			model.HeartbeatStatusFrozen, model.HeartbeatStatusRecovered,
			fmt.Sprintf("manually released all %d frozen resources", len(frozen)))
	}

	log.Printf("[heartbeat-manager] manually released all %d frozen resources for caller: %s", len(frozen), callerID)
	return nil
}

func (m *Manager) addEventLocked(callerID string, eventType string, from, to model.HeartbeatStatus, detail string) {
	event := &model.HeartbeatEvent{
		CallerID:  callerID,
		EventType: eventType,
		FromStatus: from,
		ToStatus:   to,
		Detail:     detail,
		CreatedAt:  time.Now(),
	}
	_ = m.storage.AddHeartbeatEvent(event)
}

func (m *Manager) addGroupEventLocked(groupName string, eventType string, from, to string, detail string) {
	event := &model.HeartbeatEvent{
		CallerID:  "group:" + groupName,
		EventType: eventType,
		FromStatus: model.HeartbeatStatus(from),
		ToStatus:   model.HeartbeatStatus(to),
		Detail:     detail,
		CreatedAt:  time.Now(),
	}
	_ = m.storage.AddHeartbeatEvent(event)
}

func (m *Manager) GetGroup(name string) (*model.HeartbeatGroupInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.storage.GetHeartbeatGroup(name)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return nil, fmt.Errorf("group not found: %s", name)
	}

	members, err := m.storage.ListHeartbeatRegistrationsByGroup(name)
	if err != nil {
		return nil, fmt.Errorf("list group members: %w", err)
	}

	groupMembers := make([]model.HeartbeatGroupMember, 0, len(members))
	for _, member := range members {
		groupMembers = append(groupMembers, model.HeartbeatGroupMember{
			CallerID:        member.CallerID,
			Status:          member.Status,
			LastHeartbeatAt: member.LastHeartbeatAt,
		})
	}

	deps, err := m.storage.ListGroupDependenciesByGroup(name)
	if err != nil {
		return nil, fmt.Errorf("list dependencies: %w", err)
	}
	dependsOn := make([]string, 0, len(deps))
	for _, dep := range deps {
		dependsOn = append(dependsOn, dep.DependsOn)
	}

	reverseDeps, err := m.storage.ListGroupsThatDependOn(name)
	if err != nil {
		return nil, fmt.Errorf("list reverse dependencies: %w", err)
	}
	dependedBy := make([]string, 0, len(reverseDeps))
	for _, dep := range reverseDeps {
		dependedBy = append(dependedBy, dep.GroupName)
	}

	info := &model.HeartbeatGroupInfo{
		ID:                group.ID,
		Name:              group.Name,
		SurvivalThreshold: group.SurvivalThreshold,
		Status:            group.Status,
		AliveCount:        group.AliveCount,
		TotalCount:        group.TotalCount,
		Degraded:          group.Degraded,
		DegradedReason:    group.DegradedReason,
		DegradedAt:        group.DegradedAt,
		Members:           groupMembers,
		DependsOn:         dependsOn,
		DependedBy:        dependedBy,
		CreatedAt:         group.CreatedAt,
	}

	return info, nil
}

func (m *Manager) ListGroups() ([]model.HeartbeatGroup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.storage.ListHeartbeatGroups()
}

func (m *Manager) ListGroupStatuses() ([]model.GroupStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	groups, err := m.storage.ListHeartbeatGroups()
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	result := make([]model.GroupStatusInfo, 0, len(groups))
	for _, group := range groups {
		result = append(result, model.GroupStatusInfo{
			Name:              group.Name,
			Status:            group.Status,
			SurvivalThreshold: group.SurvivalThreshold,
			AliveCount:        group.AliveCount,
			TotalCount:        group.TotalCount,
			Degraded:          group.Degraded,
			DegradedReason:    group.DegradedReason,
		})
	}

	return result, nil
}

func (m *Manager) ListGroupMembers(groupName string) ([]model.HeartbeatGroupMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.storage.GetHeartbeatGroup(groupName)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return nil, fmt.Errorf("group not found: %s", groupName)
	}

	members, err := m.storage.ListHeartbeatRegistrationsByGroup(groupName)
	if err != nil {
		return nil, fmt.Errorf("list group members: %w", err)
	}

	groupMembers := make([]model.HeartbeatGroupMember, 0, len(members))
	for _, member := range members {
		groupMembers = append(groupMembers, model.HeartbeatGroupMember{
			CallerID:        member.CallerID,
			Status:          member.Status,
			LastHeartbeatAt: member.LastHeartbeatAt,
		})
	}

	return groupMembers, nil
}

func (m *Manager) ListGroupDependencies() ([]model.HeartbeatGroupDependency, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.storage.ListGroupDependencies()
}

func (m *Manager) ListDegradedGroups() ([]model.HeartbeatGroup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.storage.ListDegradedGroups()
}

func (m *Manager) updateAllGroupsHealthLocked() {
	groups, err := m.storage.ListHeartbeatGroups()
	if err != nil {
		log.Printf("[heartbeat-manager] list groups error: %v", err)
		return
	}

	for _, group := range groups {
		m.updateGroupHealthLocked(group.Name)
	}
}
