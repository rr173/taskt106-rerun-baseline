package heatmap

import (
	"fmt"
	"log"
	"task106/internal/model"
	"task106/internal/storage"
	"sort"
	"sync"
	"time"
)

type Manager struct {
	storage *storage.Storage
	lockMgr LockManager
	config  model.HeatmapConfig
	mu      sync.RWMutex
	stopCh  chan struct{}

	activeWaits      map[string]map[string]*waitRecord
	recentAlertLocks map[string]time.Time

	consecutiveHotCycles map[string]int
	consecutiveCoolCycles map[string]int
	acceleratedGrants    map[string]int
}

type LockManager interface {
	ListAllLocks() ([]model.LockStatusInfo, error)
	WaitQueueLen(lockName string) (int, error)
	ListAllWaitQueue() ([]model.WaitQueueItem, error)
	GetActiveLease(lockName string) (*model.Lease, error)
	ShortenLease(lockName string, newLeaseSec int) (*model.Lease, error)
	SetAcceleratedGrant(lockName string, enabled bool, cooldownLeaseSec int) error
	IsAcceleratedGrant(lockName string) (bool, int)
	AddCooldownHistory(lockName string, holder string, detail string)
}

type waitRecord struct {
	LockName    string
	Holder      string
	EnqueuedAt  time.Time
	WaitStartAt time.Time
}

func NewManager(s *storage.Storage, lockMgr LockManager) *Manager {
	return &Manager{
		storage:               s,
		lockMgr:               lockMgr,
		stopCh:                make(chan struct{}),
		activeWaits:           make(map[string]map[string]*waitRecord),
		recentAlertLocks:      make(map[string]time.Time),
		consecutiveHotCycles:  make(map[string]int),
		consecutiveCoolCycles: make(map[string]int),
		acceleratedGrants:     make(map[string]int),
		config: model.HeatmapConfig{
			WindowMinutes:       5,
			AlertThresholdMs:    5000,
			AlertSuppressMin:    10,
			TopN:                10,
			HistoryRetentionMin: 1440,
			Cooldown: model.CooldownConfig{
				Enabled:                true,
				ConsecutiveHotCycles:   3,
				CooldownLeaseSec:       30,
				CooldownLeaseMinPct:    10,
				ResolveThresholdMs:     1000,
				ResolveConsecutiveCycles: 2,
				MaxCooldownSec:         3600,
				AcceleratedGrant:       true,
			},
		},
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	cfg, err := m.storage.GetHeatmapConfig()
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("get heatmap config: %w", err)
	}
	m.config = *cfg

	if m.lockMgr != nil {
		items, err := m.lockMgr.ListAllWaitQueue()
		if err != nil {
			log.Printf("[heatmap] restore active waits warning: %v", err)
		} else {
			restored := 0
			for _, it := range items {
				if it.EnqueuedAt.IsZero() {
					continue
				}
				if m.activeWaits[it.LockName] == nil {
					m.activeWaits[it.LockName] = make(map[string]*waitRecord)
				}
				m.activeWaits[it.LockName][it.Holder] = &waitRecord{
					LockName:    it.LockName,
					Holder:      it.Holder,
					EnqueuedAt:  it.EnqueuedAt,
					WaitStartAt: it.EnqueuedAt,
				}
				restored++
			}
			if restored > 0 {
				log.Printf("[heatmap] restored %d active wait records from storage", restored)
			}
		}

		activeCooldowns, err := m.storage.ListActiveCooldowns()
		if err != nil {
			log.Printf("[heatmap] restore active cooldowns warning: %v", err)
		} else {
			for _, cd := range activeCooldowns {
				if cd.Status != model.CooldownStatusActive {
					continue
				}
				m.acceleratedGrants[cd.LockName] = cd.CooldownLeaseSec
				if err := m.lockMgr.SetAcceleratedGrant(cd.LockName, true, cd.CooldownLeaseSec); err != nil {
					log.Printf("[heatmap] restore accelerated grant for %s error: %v", cd.LockName, err)
				}

				lease, err := m.lockMgr.GetActiveLease(cd.LockName)
				if err == nil && lease != nil && lease.Active {
					remainingSec := lease.RemainingSec
					if remainingSec > float64(cd.CooldownLeaseSec) {
						if _, err := m.lockMgr.ShortenLease(cd.LockName, cd.CooldownLeaseSec); err != nil {
							log.Printf("[heatmap] restore shorten lease for %s error: %v", cd.LockName, err)
						} else {
							cd.LeasesShortened++
							_ = m.storage.UpsertCooldownState(&cd)
							log.Printf("[heatmap] restored shorten lease: lock=%s remaining=%.1fs->%ds", cd.LockName, remainingSec, cd.CooldownLeaseSec)
						}
					}
				}

				log.Printf("[heatmap] restored cooldown: lock=%s lease_sec=%d started_at=%s",
					cd.LockName, cd.CooldownLeaseSec, cd.StartedAt.Format(time.RFC3339))
			}
			if len(activeCooldowns) > 0 {
				log.Printf("[heatmap] restored %d active cooldown states from storage", len(activeCooldowns))
			}
		}
	}
	m.mu.Unlock()

	go m.backgroundLoop()
	log.Println("[heatmap] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	log.Println("[heatmap] stopped")
}

func (m *Manager) backgroundLoop() {
	alertTicker := time.NewTicker(30 * time.Second)
	purgeTicker := time.NewTicker(1 * time.Hour)
	cooldownTicker := time.NewTicker(30 * time.Second)
	defer alertTicker.Stop()
	defer purgeTicker.Stop()
	defer cooldownTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-alertTicker.C:
			m.checkHotspotsAndAlert()
		case <-purgeTicker.C:
			m.purgeOldData()
		case <-cooldownTicker.C:
			m.checkCooldown()
		}
	}
}

func (m *Manager) RecordLockRequest(lockName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordRequestLocked(lockName, 1, 0, 0, 0)
}

func (m *Manager) RecordLockEnqueue(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wr := &waitRecord{
		LockName:    lockName,
		Holder:      holder,
		EnqueuedAt:  time.Now(),
		WaitStartAt: time.Now(),
	}
	if m.activeWaits[lockName] == nil {
		m.activeWaits[lockName] = make(map[string]*waitRecord)
	}
	m.activeWaits[lockName][holder] = wr

	m.recordRequestLocked(lockName, 0, 1, 0, 0)
}

func (m *Manager) RecordLockGranted(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	waitMap, ok := m.activeWaits[lockName]
	if !ok {
		return
	}
	wr, ok := waitMap[holder]
	if !ok {
		return
	}
	waitMs := int64(time.Since(wr.WaitStartAt).Milliseconds())
	delete(waitMap, holder)
	if len(waitMap) == 0 {
		delete(m.activeWaits, lockName)
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockGrantedWithEnqueue(lockName, holder string, enqueuedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var waitStart time.Time
	waitMap, ok := m.activeWaits[lockName]
	if ok {
		if wr, ok := waitMap[holder]; ok {
			waitStart = wr.WaitStartAt
			delete(waitMap, holder)
			if len(waitMap) == 0 {
				delete(m.activeWaits, lockName)
			}
		}
	}
	if waitStart.IsZero() && !enqueuedAt.IsZero() {
		waitStart = enqueuedAt
	}
	if waitStart.IsZero() {
		return
	}
	waitMs := int64(time.Since(waitStart).Milliseconds())
	if waitMs < 0 {
		waitMs = 0
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockTimeout(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	waitMap, ok := m.activeWaits[lockName]
	if !ok {
		return
	}
	wr, ok := waitMap[holder]
	if !ok {
		return
	}
	waitMs := int64(time.Since(wr.WaitStartAt).Milliseconds())
	delete(waitMap, holder)
	if len(waitMap) == 0 {
		delete(m.activeWaits, lockName)
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockTimeoutWithEnqueue(lockName, holder string, enqueuedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var waitStart time.Time
	waitMap, ok := m.activeWaits[lockName]
	if ok {
		if wr, ok := waitMap[holder]; ok {
			waitStart = wr.WaitStartAt
			delete(waitMap, holder)
			if len(waitMap) == 0 {
				delete(m.activeWaits, lockName)
			}
		}
	}
	if waitStart.IsZero() && !enqueuedAt.IsZero() {
		waitStart = enqueuedAt
	}
	if waitStart.IsZero() {
		return
	}
	waitMs := int64(time.Since(waitStart).Milliseconds())
	if waitMs < 0 {
		waitMs = 0
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockRequestWithWait(lockName string, waitMs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if waitMs > 0 {
		m.recordRequestLocked(lockName, 1, 1, waitMs, waitMs)
	} else {
		m.recordRequestLocked(lockName, 1, 0, 0, 0)
	}
}

func (m *Manager) recordRequestLocked(lockName string, reqCnt, waitCnt, totalWaitMs, maxWaitMs int64) {
	bucket := time.Now().Truncate(time.Minute)
	now := time.Now()
	stat := &model.LockContentionMinuteStat{
		LockName:     lockName,
		MinuteBucket: bucket,
		RequestCount: reqCnt,
		WaitCount:    waitCnt,
		TotalWaitMs:  totalWaitMs,
		MaxWaitMs:    maxWaitMs,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.UpsertLockContentionStat(stat); err != nil {
		log.Printf("[heatmap] record stat error: %v", err)
	}
}

func (m *Manager) checkHotspotsAndAlert() {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	threshold := m.config.AlertThresholdMs
	suppressMin := m.config.AlertSuppressMin
	if suppressMin <= 0 {
		suppressMin = 10
	}
	topN := m.config.TopN
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		log.Printf("[heatmap] check hotspots get heat error: %v", err)
		return
	}

	m.mu.Lock()
	now := time.Now()
	for i, h := range heats {
		if i >= topN {
			break
		}
		if h.AvgWaitMs <= threshold {
			continue
		}

		lastAlertAt, exists := m.recentAlertLocks[h.LockName]
		suppressDur := time.Duration(suppressMin) * time.Minute
		if exists && now.Sub(lastAlertAt) < suppressDur {
			continue
		}

		qLen, _ := m.lockMgr.WaitQueueLen(h.LockName)
		h.CurrentQueueLen = qLen

		alert := &model.HotspotAlertEvent{
			LockName:        h.LockName,
			AvgWaitMs:       h.AvgWaitMs,
			ThresholdMs:     threshold,
			RequestCount:    h.RequestCount,
			WaitCount:       h.WaitCount,
			MaxWaitMs:       h.MaxWaitMs,
			CurrentQueueLen: qLen,
			WindowMinutes:   windowMin,
			AlertType:       "avg_wait_exceeded",
			Detail:          fmt.Sprintf("锁 %s 在最近 %d 分钟内平均等待 %.2fms 超过阈值 %.2fms", h.LockName, windowMin, h.AvgWaitMs, threshold),
			Acknowledged:    false,
			CreatedAt:       now,
		}
		if err := m.storage.CreateHotspotAlert(alert); err != nil {
			log.Printf("[heatmap] create alert error: %v", err)
			continue
		}
		m.recentAlertLocks[h.LockName] = now
		log.Printf("[heatmap] HOTSPOT ALERT: lock=%s avg_wait=%.2fms threshold=%.2fms reqs=%d queue=%d",
			h.LockName, h.AvgWaitMs, threshold, h.RequestCount, qLen)
	}
	m.mu.Unlock()
}

func (m *Manager) purgeOldData() {
	m.mu.RLock()
	retention := m.config.HistoryRetentionMin
	m.mu.RUnlock()

	n, err := m.storage.PurgeOldLockContentionStats(retention)
	if err != nil {
		log.Printf("[heatmap] purge error: %v", err)
		return
	}
	if n > 0 {
		log.Printf("[heatmap] purged %d old contention stats", n)
	}
}

func (m *Manager) GetTopHeatLocks(limit int) ([]model.LockHeatInfo, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	if limit <= 0 {
		limit = m.config.TopN
	}
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	for i := range heats {
		qLen, _ := m.lockMgr.WaitQueueLen(heats[i].LockName)
		heats[i].CurrentQueueLen = qLen
	}

	sort.Slice(heats, func(i, j int) bool {
		return heats[i].HeatScore > heats[j].HeatScore
	})

	if limit < len(heats) {
		heats = heats[:limit]
	}
	return heats, nil
}

func (m *Manager) GetLockTrend(lockName string, minutes int) ([]model.LockTrendPoint, error) {
	if minutes <= 0 {
		minutes = 60
	}
	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(minutes) * time.Minute)

	stats, err := m.storage.GetLockContentionStatsInWindow(lockName, startTime, endTime)
	if err != nil {
		return nil, err
	}

	bucketMap := make(map[time.Time]*model.LockContentionMinuteStat)
	for i := range stats {
		bucketMap[stats[i].MinuteBucket] = &stats[i]
	}

	points := make([]model.LockTrendPoint, 0, minutes)
	for t := startTime; t.Before(endTime); t = t.Add(time.Minute) {
		p := model.LockTrendPoint{
			MinuteBucket: t,
		}
		if s, ok := bucketMap[t]; ok {
			p.RequestCount = s.RequestCount
			p.WaitCount = s.WaitCount
			p.MaxWaitMs = s.MaxWaitMs
			if s.WaitCount > 0 {
				p.AvgWaitMs = float64(s.TotalWaitMs) / float64(s.WaitCount)
			}
		}
		points = append(points, p)
	}
	return points, nil
}

func (m *Manager) ListAlerts(lockName string, acknowledged *bool, limit int) ([]model.HotspotAlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	return m.storage.ListHotspotAlerts(lockName, acknowledged, limit)
}

func (m *Manager) AcknowledgeAlert(id int64, acknowledgedBy string) error {
	return m.storage.AcknowledgeHotspotAlert(id, acknowledgedBy)
}

func (m *Manager) GetConfig() model.HeatmapConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func (m *Manager) GetGlobalStats() (*model.HeatmapGlobalStats, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	cfg := m.config
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	stats := &model.HeatmapGlobalStats{
		Config: cfg,
	}

	totalReqs := int64(0)
	totalWaits := int64(0)
	totalWaitMs := int64(0)
	hotCount := 0

	for _, h := range heats {
		totalReqs += h.RequestCount
		totalWaits += h.WaitCount
		totalWaitMs += int64(h.AvgWaitMs * float64(h.WaitCount))
		if h.AvgWaitMs > cfg.AlertThresholdMs {
			hotCount++
		}
	}
	stats.TotalLocks = len(heats)
	stats.TotalRequests = totalReqs
	stats.TotalWaits = totalWaits
	stats.HotLocks = hotCount
	if totalWaits > 0 {
		stats.OverallAvgWaitMs = float64(totalWaitMs) / float64(totalWaits)
	}

	falseVal := false
	alerts, err := m.storage.ListHotspotAlerts("", &falseVal, 1000)
	if err == nil {
		stats.ActiveAlerts = len(alerts)
	}

	cooldowns, err := m.storage.ListActiveCooldowns()
	if err == nil {
		stats.ActiveCooldowns = len(cooldowns)
	}

	todayStart := time.Now().Truncate(24 * time.Hour)
	history, err := m.storage.ListCooldownHistory("", 1000)
	if err == nil {
		count := int64(0)
		for _, h := range history {
			if h.CreatedAt.After(todayStart) {
				count++
			}
		}
		stats.TotalCooldownToday = count
	}

	return stats, nil
}

func (m *Manager) checkCooldown() {
	m.mu.RLock()
	enabled := m.config.Cooldown.Enabled
	windowMin := m.config.WindowMinutes
	alertThreshold := m.config.AlertThresholdMs
	consecutiveHot := m.config.Cooldown.ConsecutiveHotCycles
	resolveThreshold := m.config.Cooldown.ResolveThresholdMs
	resolveCycles := m.config.Cooldown.ResolveConsecutiveCycles
	maxCooldownSec := m.config.Cooldown.MaxCooldownSec
	cooldownLeaseSec := m.config.Cooldown.CooldownLeaseSec
	acceleratedGrant := m.config.Cooldown.AcceleratedGrant
	m.mu.RUnlock()

	if !enabled {
		return
	}

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		log.Printf("[heatmap-cooldown] get heat error: %v", err)
		return
	}

	activeCooldowns, err := m.storage.ListActiveCooldowns()
	if err != nil {
		log.Printf("[heatmap-cooldown] list active cooldowns error: %v", err)
		return
	}

	activeCooldownMap := make(map[string]*model.LockCooldownState)
	for i := range activeCooldowns {
		activeCooldownMap[activeCooldowns[i].LockName] = &activeCooldowns[i]
	}

	now := time.Now()

	for _, h := range heats {
		lockName := h.LockName
		currentState := activeCooldownMap[lockName]

		if currentState != nil && currentState.Status == model.CooldownStatusActive {
			if maxCooldownSec > 0 && now.Sub(currentState.StartedAt).Seconds() > float64(maxCooldownSec) {
				m.stopCooldown(lockName, currentState, h.AvgWaitMs, "max_cooldown_duration_exceeded")
				continue
			}

			if h.AvgWaitMs <= resolveThreshold {
				m.consecutiveCoolCycles[lockName]++
				if m.consecutiveCoolCycles[lockName] >= resolveCycles {
					m.stopCooldown(lockName, currentState, h.AvgWaitMs, "avg_wait_below_resolve_threshold")
					continue
				}
			} else {
				m.consecutiveCoolCycles[lockName] = 0
			}
			continue
		}

		if h.AvgWaitMs > alertThreshold {
			m.consecutiveHotCycles[lockName]++
			m.consecutiveCoolCycles[lockName] = 0

			if m.consecutiveHotCycles[lockName] >= consecutiveHot {
				m.startCooldown(lockName, h, alertThreshold, cooldownLeaseSec, acceleratedGrant)
			}
		} else {
			m.consecutiveHotCycles[lockName] = 0
		}
	}

	for lockName, state := range activeCooldownMap {
		if state.Status != model.CooldownStatusActive {
			continue
		}

		found := false
		for _, h := range heats {
			if h.LockName == lockName {
				found = true
				break
			}
		}
		if !found {
			m.stopCooldown(lockName, state, 0, "no_heat_data")
		}
	}
}

func (m *Manager) startCooldown(lockName string, heat model.LockHeatInfo, thresholdMs float64, cooldownLeaseSec int, acceleratedGrant bool) {
	m.mu.Lock()
	now := time.Now()
	consecutiveHot := m.consecutiveHotCycles[lockName]
	m.mu.Unlock()

	lease, err := m.lockMgr.GetActiveLease(lockName)
	if err != nil {
		log.Printf("[heatmap-cooldown] get active lease error for %s: %v", lockName, err)
	}

	originalLeaseSec := 0
	if lease != nil {
		originalLeaseSec = lease.LeaseSec
	}

	state := &model.LockCooldownState{
		LockName:            lockName,
		Status:              model.CooldownStatusActive,
		TriggerType:         model.CooldownTriggerAuto,
		OriginalLeaseSec:    originalLeaseSec,
		CooldownLeaseSec:    cooldownLeaseSec,
		LeasesShortened:     0,
		ConsecutiveHotCycles: consecutiveHot,
		AvgWaitMsAtStart:    heat.AvgWaitMs,
		ThresholdMsAtStart:  thresholdMs,
		StartedAt:           now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := m.storage.UpsertCooldownState(state); err != nil {
		log.Printf("[heatmap-cooldown] upsert state error for %s: %v", lockName, err)
		return
	}

	m.triggerCooldownActions(lockName, state, heat, lease)

	m.lockMgr.AddCooldownHistory(lockName, "system",
		fmt.Sprintf("自动降温启动: 平均等待%.2fms超过阈值%.2fms, 连续%d个周期, 租约缩短为%d秒",
			heat.AvgWaitMs, thresholdMs, consecutiveHot, cooldownLeaseSec))

	log.Printf("[heatmap-cooldown] START: lock=%s avg_wait=%.2fms threshold=%.2fms consecutive=%d lease=%ds->%ds",
		lockName, heat.AvgWaitMs, thresholdMs, consecutiveHot, originalLeaseSec, cooldownLeaseSec)
}

func (m *Manager) triggerCooldownActions(lockName string, state *model.LockCooldownState, heat model.LockHeatInfo, currentLease *model.Lease) {
	if currentLease != nil && currentLease.Active {
		shortenedLease, err := m.lockMgr.ShortenLease(lockName, state.CooldownLeaseSec)
		if err != nil {
			log.Printf("[heatmap-cooldown] shorten lease error for %s: %v", lockName, err)
		} else {
			state.LeasesShortened++
			m.mu.Lock()
			if err := m.storage.UpsertCooldownState(state); err != nil {
				log.Printf("[heatmap-cooldown] update state after shorten error for %s: %v", lockName, err)
			}
			m.mu.Unlock()

			if shortenedLease != nil {
				m.lockMgr.AddCooldownHistory(lockName, shortenedLease.Holder,
					fmt.Sprintf("租约已缩短: 从%d秒到%d秒, 剩余%.1f秒",
						currentLease.LeaseSec, state.CooldownLeaseSec, shortenedLease.RemainingSec))
			}
		}
	}

	shouldAccelerate := m.config.Cooldown.AcceleratedGrant
	if state.TriggerType == model.CooldownTriggerManual {
		shouldAccelerate = true
	}
	if shouldAccelerate {
		if err := m.lockMgr.SetAcceleratedGrant(lockName, true, state.CooldownLeaseSec); err != nil {
			log.Printf("[heatmap-cooldown] set accelerated grant error for %s: %v", lockName, err)
		} else {
			m.mu.Lock()
			m.acceleratedGrants[lockName] = state.CooldownLeaseSec
			m.mu.Unlock()
			m.lockMgr.AddCooldownHistory(lockName, "system",
				fmt.Sprintf("加速授予已启用: 后续租约限制为%d秒", state.CooldownLeaseSec))
		}
	}
}

func (m *Manager) stopCooldown(lockName string, state *model.LockCooldownState, avgWaitMs float64, reason string) {
	m.mu.Lock()
	now := time.Now()
	state.Status = model.CooldownStatusResolved
	state.ResolvedAt = &now
	state.ResolveReason = reason
	state.UpdatedAt = now

	if err := m.storage.UpsertCooldownState(state); err != nil {
		log.Printf("[heatmap-cooldown] update resolved state error for %s: %v", lockName, err)
	}

	history := &model.CooldownHistoryRecord{
		LockName:         lockName,
		TriggerType:      state.TriggerType,
		OriginalLeaseSec: state.OriginalLeaseSec,
		CooldownLeaseSec: state.CooldownLeaseSec,
		LeasesShortened:  state.LeasesShortened,
		AvgWaitMsAtStart: state.AvgWaitMsAtStart,
		AvgWaitMsAtEnd:   avgWaitMs,
		ThresholdMs:      state.ThresholdMsAtStart,
		DurationSec:      now.Sub(state.StartedAt).Seconds(),
		StartedAt:        state.StartedAt,
		EndedAt:          now,
		ResolveReason:    reason,
		CreatedAt:        now,
	}
	if err := m.storage.CreateCooldownHistory(history); err != nil {
		log.Printf("[heatmap-cooldown] create history error for %s: %v", lockName, err)
	}

	if err := m.lockMgr.SetAcceleratedGrant(lockName, false, 0); err != nil {
		log.Printf("[heatmap-cooldown] disable accelerated grant error for %s: %v", lockName, err)
	}

	delete(m.acceleratedGrants, lockName)
	delete(m.consecutiveHotCycles, lockName)
	delete(m.consecutiveCoolCycles, lockName)
	m.mu.Unlock()

	m.lockMgr.AddCooldownHistory(lockName, "system",
		fmt.Sprintf("自动降温解除: 原因=%s, 持续%.1f秒, 共缩短%d个租约, 结束时平均等待%.2fms",
			reason, now.Sub(state.StartedAt).Seconds(), state.LeasesShortened, avgWaitMs))

	log.Printf("[heatmap-cooldown] STOP: lock=%s reason=%s duration=%.1fs leases_shortened=%d avg_wait_end=%.2fms",
		lockName, reason, now.Sub(state.StartedAt).Seconds(), state.LeasesShortened, avgWaitMs)
}

func (m *Manager) ManualStartCooldown(lockName string, cooldownLeaseSec int, reason string) (*model.LockCooldownState, error) {
	activeState, err := m.storage.GetActiveCooldown(lockName)
	if err != nil {
		return nil, err
	}
	if activeState != nil && activeState.Status == model.CooldownStatusActive {
		return nil, fmt.Errorf("lock %s is already in cooldown", lockName)
	}

	heat, err := m.getLatestHeat(lockName)
	if err != nil {
		return nil, err
	}

	lease, err := m.lockMgr.GetActiveLease(lockName)
	if err != nil {
		return nil, err
	}

	originalLeaseSec := 0
	avgWaitMs := 0.0
	if lease != nil {
		originalLeaseSec = lease.LeaseSec
	}
	if heat != nil {
		avgWaitMs = heat.AvgWaitMs
	}

	now := time.Now()
	state := &model.LockCooldownState{
		LockName:            lockName,
		Status:              model.CooldownStatusActive,
		TriggerType:         model.CooldownTriggerManual,
		OriginalLeaseSec:    originalLeaseSec,
		CooldownLeaseSec:    cooldownLeaseSec,
		LeasesShortened:     0,
		ConsecutiveHotCycles: 0,
		AvgWaitMsAtStart:    avgWaitMs,
		ThresholdMsAtStart:  0,
		StartedAt:           now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := m.storage.UpsertCooldownState(state); err != nil {
		return nil, err
	}

	m.triggerCooldownActions(lockName, state, *heat, lease)

	detail := fmt.Sprintf("手动降温启动: 租约缩短为%d秒", cooldownLeaseSec)
	if reason != "" {
		detail += ", 原因: " + reason
	}
	m.lockMgr.AddCooldownHistory(lockName, "manual", detail)

	return state, nil
}

func (m *Manager) ManualStopCooldown(lockName string, reason string) (*model.LockCooldownState, error) {
	state, err := m.storage.GetActiveCooldown(lockName)
	if err != nil {
		return nil, err
	}
	if state == nil || state.Status != model.CooldownStatusActive {
		return nil, fmt.Errorf("lock %s is not in active cooldown", lockName)
	}

	heat, _ := m.getLatestHeat(lockName)
	avgWaitMs := 0.0
	if heat != nil {
		avgWaitMs = heat.AvgWaitMs
	}

	stopReason := "manual_stop"
	if reason != "" {
		stopReason = reason
	}

	m.stopCooldown(lockName, state, avgWaitMs, stopReason)
	return state, nil
}

func (m *Manager) getLatestHeat(lockName string) (*model.LockHeatInfo, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	for _, h := range heats {
		if h.LockName == lockName {
			qLen, _ := m.lockMgr.WaitQueueLen(lockName)
			h.CurrentQueueLen = qLen
			return &h, nil
		}
	}
	return &model.LockHeatInfo{LockName: lockName}, nil
}

func (m *Manager) ListActiveCooldowns() ([]model.CooldownStatusInfo, error) {
	activeStates, err := m.storage.ListActiveCooldowns()
	if err != nil {
		return nil, err
	}

	result := make([]model.CooldownStatusInfo, 0, len(activeStates))
	now := time.Now()

	for _, state := range activeStates {
		heat, _ := m.getLatestHeat(state.LockName)
		lockInfo, _ := m.lockMgr.ListAllLocks()

		info := model.CooldownStatusInfo{
			LockName:            state.LockName,
			Status:              state.Status,
			TriggerType:         state.TriggerType,
			OriginalLeaseSec:    state.OriginalLeaseSec,
			CooldownLeaseSec:    state.CooldownLeaseSec,
			LeasesShortened:     state.LeasesShortened,
			ConsecutiveHotCycles: state.ConsecutiveHotCycles,
			AvgWaitMsAtStart:    state.AvgWaitMsAtStart,
			CurrentAvgWaitMs:    0,
			ThresholdMs:         state.ThresholdMsAtStart,
			StartedAt:           state.StartedAt,
			DurationSec:         now.Sub(state.StartedAt).Seconds(),
		}

		if heat != nil {
			info.CurrentAvgWaitMs = heat.AvgWaitMs
			info.WaitQueueLen = heat.CurrentQueueLen
		}

		for _, l := range lockInfo {
			if l.Name == state.LockName {
				info.CurrentHolder = l.Holder
				info.RemainingSec = l.RemainingSec
				break
			}
		}

		result = append(result, info)
	}

	return result, nil
}

func (m *Manager) ListCooldownHistory(lockName string, limit int) ([]model.CooldownHistoryRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	return m.storage.ListCooldownHistory(lockName, limit)
}

func (m *Manager) GetCooldownSuggestions() ([]model.CooldownSuggestion, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	alertThreshold := m.config.AlertThresholdMs
	cooldownLeaseSec := m.config.Cooldown.CooldownLeaseSec
	consecutiveHot := m.config.Cooldown.ConsecutiveHotCycles
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	activeCooldowns, err := m.storage.ListActiveCooldowns()
	if err != nil {
		return nil, err
	}

	activeMap := make(map[string]bool)
	for _, s := range activeCooldowns {
		if s.Status == model.CooldownStatusActive {
			activeMap[s.LockName] = true
		}
	}

	suggestions := make([]model.CooldownSuggestion, 0)

	for _, h := range heats {
		if activeMap[h.LockName] {
			continue
		}
		if h.AvgWaitMs <= alertThreshold {
			continue
		}

		m.mu.RLock()
		consec := m.consecutiveHotCycles[h.LockName]
		m.mu.RUnlock()

		lease, _ := m.lockMgr.GetActiveLease(h.LockName)
		currentLeaseSec := 0
		if lease != nil {
			currentLeaseSec = lease.LeaseSec
		}

		suggestedLeaseSec := cooldownLeaseSec
		if currentLeaseSec > 0 && cooldownLeaseSec > currentLeaseSec {
			suggestedLeaseSec = currentLeaseSec
		}

		reason := fmt.Sprintf("平均等待%.2fms超过阈值%.2fms", h.AvgWaitMs, alertThreshold)
		if consec > 0 {
			reason += fmt.Sprintf(", 已连续%d个检测周期", consec)
		}
		if consec >= consecutiveHot {
			reason += ", 即将触发自动降温"
		}

		qLen, _ := m.lockMgr.WaitQueueLen(h.LockName)

		suggestions = append(suggestions, model.CooldownSuggestion{
			LockName:         h.LockName,
			AvgWaitMs:        h.AvgWaitMs,
			ThresholdMs:      alertThreshold,
			ConsecutiveHot:   consec,
			SuggestedLeaseSec: suggestedLeaseSec,
			CurrentLeaseSec:  currentLeaseSec,
			QueueLen:         qLen,
			Reason:           reason,
		})
	}

	sort.Slice(suggestions, func(i, j int) bool {
		return suggestions[i].AvgWaitMs > suggestions[j].AvgWaitMs
	})

	return suggestions, nil
}

func (m *Manager) UpdateConfig(req model.UpdateHeatmapConfigRequest) (*model.HeatmapConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.WindowMinutes != nil && *req.WindowMinutes > 0 {
		m.config.WindowMinutes = *req.WindowMinutes
	}
	if req.AlertThresholdMs != nil && *req.AlertThresholdMs > 0 {
		m.config.AlertThresholdMs = *req.AlertThresholdMs
	}
	if req.AlertSuppressMin != nil && *req.AlertSuppressMin > 0 {
		m.config.AlertSuppressMin = *req.AlertSuppressMin
	}
	if req.TopN != nil && *req.TopN > 0 {
		m.config.TopN = *req.TopN
	}
	if req.HistoryRetentionMin != nil && *req.HistoryRetentionMin > 0 {
		m.config.HistoryRetentionMin = *req.HistoryRetentionMin
	}
	if req.Cooldown != nil {
		c := req.Cooldown
		if c.Enabled != nil {
			m.config.Cooldown.Enabled = *c.Enabled
		}
		if c.ConsecutiveHotCycles != nil && *c.ConsecutiveHotCycles > 0 {
			m.config.Cooldown.ConsecutiveHotCycles = *c.ConsecutiveHotCycles
		}
		if c.CooldownLeaseSec != nil && *c.CooldownLeaseSec > 0 {
			m.config.Cooldown.CooldownLeaseSec = *c.CooldownLeaseSec
		}
		if c.CooldownLeaseMinPct != nil && *c.CooldownLeaseMinPct > 0 {
			m.config.Cooldown.CooldownLeaseMinPct = *c.CooldownLeaseMinPct
		}
		if c.ResolveThresholdMs != nil && *c.ResolveThresholdMs > 0 {
			m.config.Cooldown.ResolveThresholdMs = *c.ResolveThresholdMs
		}
		if c.ResolveConsecutiveCycles != nil && *c.ResolveConsecutiveCycles > 0 {
			m.config.Cooldown.ResolveConsecutiveCycles = *c.ResolveConsecutiveCycles
		}
		if c.MaxCooldownSec != nil && *c.MaxCooldownSec > 0 {
			m.config.Cooldown.MaxCooldownSec = *c.MaxCooldownSec
		}
		if c.AcceleratedGrant != nil {
			m.config.Cooldown.AcceleratedGrant = *c.AcceleratedGrant
		}
	}

	if err := m.storage.UpdateHeatmapConfig(&m.config); err != nil {
		return nil, err
	}

	cfgCopy := m.config
	return &cfgCopy, nil
}

func (m *Manager) IncrementLeaseShortenedCount(lockName string) error {
	state, err := m.storage.GetActiveCooldown(lockName)
	if err != nil {
		return err
	}
	if state == nil || state.Status != model.CooldownStatusActive {
		return nil
	}

	m.mu.Lock()
	state.LeasesShortened++
	state.UpdatedAt = time.Now()
	err = m.storage.UpsertCooldownState(state)
	m.mu.Unlock()
	return err
}
