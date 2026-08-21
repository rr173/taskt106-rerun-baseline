package reputation

import (
	"fmt"
	"log"
	"math"
	"task106/internal/model"
	"task106/internal/storage"
	"sort"
	"sync"
	"time"
)

type Manager struct {
	storage *storage.Storage
	mu      sync.Mutex
	stopCh  chan struct{}
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage: s,
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.recalculateAllLocked(); err != nil {
		log.Printf("[reputation] initial calculation error: %v", err)
	}

	go m.recalcLoop()
	log.Println("[reputation-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	log.Println("[reputation-manager] stopped")
}

func (m *Manager) recalcLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.RecalculateAll()
		}
	}
}

func (m *Manager) RecalculateAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.recalculateAllLocked(); err != nil {
		log.Printf("[reputation] recalculate error: %v", err)
	}
}

func (m *Manager) recalculateAllLocked() error {
	configs, err := m.storage.ListLockBudgetConfigs()
	if err != nil {
		return fmt.Errorf("list budget configs: %w", err)
	}

	now := time.Now()
	for _, cfg := range configs {
		if err := m.recalculateCallerLocked(cfg.CallerID, now); err != nil {
			log.Printf("[reputation] recalculate caller %s error: %v", cfg.CallerID, err)
		}
	}
	return nil
}

func (m *Manager) recalculateCallerLocked(callerID string, now time.Time) error {
	day7 := now.Add(-7 * 24 * time.Hour)
	day30 := now.Add(-30 * 24 * time.Hour)

	total, onTime, err := m.storage.CountOnTimeReleases(callerID, day7)
	if err != nil {
		return fmt.Errorf("count on-time releases: %w", err)
	}
	onTimeScore := 100.0
	if total > 0 {
		onTimeScore = float64(onTime) / float64(total) * 100.0
	}

	totalPeriods, overdraftPeriods, err := m.storage.CountOverdraftPeriods(callerID, day7)
	if err != nil {
		return fmt.Errorf("count overdraft periods: %w", err)
	}
	overdraftReverseScore := 100.0
	if totalPeriods > 0 {
		overdraftReverseScore = (1.0 - float64(overdraftPeriods)/float64(totalPeriods)) * 100.0
	}

	cbCount, err := m.storage.CountCircuitBreakerTriggers(callerID, day30)
	if err != nil {
		return fmt.Errorf("count circuit breaker triggers: %w", err)
	}
	cbScore := inverseScore(cbCount, 10)

	arrearCount, err := m.storage.CountArrearEvents(callerID, day30)
	if err != nil {
		return fmt.Errorf("count arrear events: %w", err)
	}
	arrearScore := inverseScore(arrearCount, 5)

	alertCount, err := m.storage.CountRateAlertEventsSince(callerID, day7)
	if err != nil {
		return fmt.Errorf("count rate alert events: %w", err)
	}
	alertScore := inverseScore(alertCount, 10)

	score := onTimeScore*0.4 +
		overdraftReverseScore*0.2 +
		cbScore*0.2 +
		arrearScore*0.1 +
		alertScore*0.1

	score = math.Round(score*100) / 100
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	tier := tierFromScore(score)

	existing, _ := m.storage.GetCallerReputation(callerID)
	oldTier := model.ServiceTierSilver
	if existing != nil {
		oldTier = existing.Tier
	}

	rep := &model.CallerReputation{
		CallerID:              callerID,
		Score:                 score,
		Tier:                  tier,
		OnTimeReleaseScore:    math.Round(onTimeScore*100) / 100,
		OverdraftReverseScore: math.Round(overdraftReverseScore*100) / 100,
		CircuitBreakerScore:   math.Round(cbScore*100) / 100,
		ArrearScore:           math.Round(arrearScore*100) / 100,
		RateAlertScore:        math.Round(alertScore*100) / 100,
		CalculatedAt:          now,
		UpdatedAt:             now,
	}
	if existing != nil {
		rep.ID = existing.ID
		rep.CreatedAt = existing.CreatedAt
	} else {
		rep.CreatedAt = now
	}

	if err := m.storage.UpsertCallerReputation(rep); err != nil {
		return fmt.Errorf("upsert reputation: %w", err)
	}

	if oldTier != tier {
		evt := &model.TierChangeEvent{
			CallerID:  callerID,
			OldTier:   oldTier,
			NewTier:   tier,
			Score:     score,
			ChangedAt: now,
			CreatedAt: now,
		}
		if err := m.storage.AddTierChangeEvent(evt); err != nil {
			log.Printf("[reputation] add tier change event error: caller=%s err=%v", callerID, err)
		}
		log.Printf("[reputation] tier changed: caller=%s %s->%s score=%.2f", callerID, oldTier, tier, score)
	}

	return nil
}

func inverseScore(count int, scale int) float64 {
	if count <= 0 {
		return 100.0
	}
	score := 100.0 - float64(count)*100.0/float64(scale)
	if score < 0 {
		score = 0
	}
	return score
}

func tierFromScore(score float64) model.ServiceTier {
	if score >= 80 {
		return model.ServiceTierGold
	}
	if score >= 50 {
		return model.ServiceTierSilver
	}
	return model.ServiceTierBronze
}

func (m *Manager) GetTier(callerID string) model.ServiceTier {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getTierLocked(callerID)
}

func (m *Manager) getTierLocked(callerID string) model.ServiceTier {
	rep, err := m.storage.GetCallerReputation(callerID)
	if err != nil || rep == nil {
		return model.ServiceTierSilver
	}
	return rep.Tier
}

func (m *Manager) GetScore(callerID string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	rep, err := m.storage.GetCallerReputation(callerID)
	if err != nil || rep == nil {
		return 50.0
	}
	return rep.Score
}

func (m *Manager) GetCallerDetail(callerID string) (*model.CallerReputationDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rep, err := m.storage.GetCallerReputation(callerID)
	if err != nil {
		return nil, err
	}
	if rep == nil {
		return &model.CallerReputationDetail{
			CallerID:              callerID,
			Score:                 50.0,
			Tier:                  model.ServiceTierSilver,
			OnTimeReleaseScore:    100.0,
			OverdraftReverseScore: 100.0,
			CircuitBreakerScore:   100.0,
			ArrearScore:           100.0,
			RateAlertScore:        100.0,
		}, nil
	}
	return &model.CallerReputationDetail{
		CallerID:              rep.CallerID,
		Score:                 rep.Score,
		Tier:                  rep.Tier,
		OnTimeReleaseScore:    rep.OnTimeReleaseScore,
		OverdraftReverseScore: rep.OverdraftReverseScore,
		CircuitBreakerScore:   rep.CircuitBreakerScore,
		ArrearScore:           rep.ArrearScore,
		RateAlertScore:        rep.RateAlertScore,
		CalculatedAt:          rep.CalculatedAt,
	}, nil
}

func (m *Manager) GetRanking() (*model.ReputationRankingResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reps, err := m.storage.ListAllCallerReputations()
	if err != nil {
		return nil, err
	}

	sort.Slice(reps, func(i, j int) bool {
		return reps[i].Score > reps[j].Score
	})

	rankings := make([]model.CallerReputationRanking, 0, len(reps))
	for i, rep := range reps {
		rankings = append(rankings, model.CallerReputationRanking{
			CallerID: rep.CallerID,
			Score:    rep.Score,
			Tier:     rep.Tier,
			Rank:     i + 1,
		})
	}

	return &model.ReputationRankingResult{
		Total:    len(rankings),
		Rankings: rankings,
	}, nil
}

func (m *Manager) ListTierChangeEvents(callerID string, limit int, offset int) (*model.TierChangeEventListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, total, err := m.storage.ListTierChangeEvents(callerID, limit, offset)
	if err != nil {
		return nil, err
	}
	return &model.TierChangeEventListResult{
		Total:  total,
		Items:  items,
		Limit:  limit,
		Offset: offset,
	}, nil
}

func (m *Manager) IsGold(callerID string) bool {
	return m.GetTier(callerID) == model.ServiceTierGold
}

func (m *Manager) IsBronze(callerID string) bool {
	return m.GetTier(callerID) == model.ServiceTierBronze
}

func (m *Manager) GetEffectiveOverdraftLimit(callerID string, configOverdraftLimit int) int {
	if m.IsGold(callerID) && configOverdraftLimit > 0 {
		return int(math.Floor(float64(configOverdraftLimit) * 1.5))
	}
	return configOverdraftLimit
}

func (m *Manager) GetMaxConcurrentLocks(callerID string, configuredMax int) int {
	if m.IsBronze(callerID) && configuredMax > 0 {
		return int(math.Ceil(float64(configuredMax) / 2.0))
	}
	return configuredMax
}

func (m *Manager) CanTransferBudget(callerID string) bool {
	return !m.IsBronze(callerID)
}

func (m *Manager) CheckBronzeLockLimit(callerID string, currentHeldLocks int, configuredMax int) (bool, string) {
	if !m.IsBronze(callerID) {
		return true, ""
	}
	maxAllowed := int(math.Ceil(float64(configuredMax) / 2.0))
	if currentHeldLocks >= maxAllowed {
		return false, fmt.Sprintf("铜牌调用方最大同时持锁数为%d(配置值%d的一半向上取整)，当前已持有%d", maxAllowed, configuredMax, currentHeldLocks)
	}
	return true, ""
}

func (m *Manager) ShouldPrioritizeInQueue(callerID string) bool {
	return m.IsGold(callerID)
}
