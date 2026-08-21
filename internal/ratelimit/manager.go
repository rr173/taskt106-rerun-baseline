package ratelimit

import (
	"fmt"
	"log"
	"math"
	"task106/internal/model"
	"task106/internal/storage"
	"sync"
	"time"
)

type Manager struct {
	storage      *storage.Storage
	mu           sync.Mutex
	policies     map[string]*model.RateLimitPolicy
	bindings     map[string]*model.CallerBinding
	waitQueue    []*model.RateLimitWaitItem
	reservations map[int64]*model.QuotaReservation
	stopCh       chan struct{}
	ticker       *time.Ticker
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage:      s,
		policies:     make(map[string]*model.RateLimitPolicy),
		bindings:     make(map[string]*model.CallerBinding),
		waitQueue:    make([]*model.RateLimitWaitItem, 0),
		reservations: make(map[int64]*model.QuotaReservation),
		stopCh:       make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadPoliciesLocked(); err != nil {
		return fmt.Errorf("load policies: %w", err)
	}
	if err := m.loadBindingsLocked(); err != nil {
		return fmt.Errorf("load bindings: %w", err)
	}
	if err := m.loadWaitQueueLocked(); err != nil {
		return fmt.Errorf("load wait queue: %w", err)
	}
	if err := m.loadReservationsLocked(); err != nil {
		return fmt.Errorf("load reservations: %w", err)
	}

	m.ticker = time.NewTicker(500 * time.Millisecond)
	go m.refillLoop()

	log.Println("[ratelimit-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.persistAllLocked(); err != nil {
		log.Printf("[ratelimit-manager] persist error on stop: %v", err)
	}
	log.Println("[ratelimit-manager] stopped")
}

func (m *Manager) loadPoliciesLocked() error {
	policies, err := m.storage.ListPolicies()
	if err != nil {
		return err
	}
	for i := range policies {
		m.policies[policies[i].Name] = &policies[i]
	}
	return nil
}

func (m *Manager) loadBindingsLocked() error {
	bindings, err := m.storage.ListCallerBindings()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range bindings {
		b := &bindings[i]
		policy, ok := m.policies[b.PolicyName]
		if ok {
			m.reconcileState(b, policy, now)
		}
		m.bindings[b.CallerID] = b
	}
	return nil
}

func (m *Manager) loadWaitQueueLocked() error {
	items, err := m.storage.ListAllWaitItems()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range items {
		if items[i].TimeoutAt.After(now) {
			m.waitQueue = append(m.waitQueue, &items[i])
		}
	}
	return nil
}

func (m *Manager) loadReservationsLocked() error {
	reservations, err := m.storage.ListAllReservations("")
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range reservations {
		r := &reservations[i]
		if r.Status == model.ReservationStatusPending || r.Status == model.ReservationStatusActive {
			if r.Status == model.ReservationStatusActive && r.EndAt.Before(now) {
				r.Status = model.ReservationStatusCompleted
			}
			m.reservations[r.ID] = r
		}
	}
	return nil
}

func (m *Manager) expDecayFactor(elapsed float64, windowSec float64) float64 {
	if windowSec <= 0 {
		return 0
	}
	tau := windowSec / 3.0
	return math.Exp(-elapsed / tau)
}

func (m *Manager) effectiveLimit(b *model.CallerBinding) int {
	policy, ok := m.policies[b.PolicyName]
	if !ok {
		return b.QuotaLimit
	}
	limit := b.QuotaLimit
	if limit > policy.MaxTokens {
		limit = policy.MaxTokens
	}
	limit += b.BorrowedTokens
	limit -= b.LentTokens
	limit += b.ReservedTokens
	if limit < 0 {
		limit = 0
	}
	return limit
}

func (m *Manager) reconcileState(b *model.CallerBinding, policy *model.RateLimitPolicy, now time.Time) {
	switch policy.Algorithm {
	case model.AlgoFixedWindow:
		if b.WindowStartAt.IsZero() {
			b.WindowStartAt = now
			b.UsedTokens = 0
		} else {
			elapsed := now.Sub(b.WindowStartAt).Seconds()
			if elapsed >= float64(policy.WindowSec) {
				windowsPassed := int(elapsed) / policy.WindowSec
				b.WindowStartAt = b.WindowStartAt.Add(time.Duration(windowsPassed*policy.WindowSec) * time.Second)
				b.UsedTokens = 0
			}
		}
	case model.AlgoTokenBucket:
		if b.LastRefillAt.IsZero() {
			b.LastRefillAt = now
			b.UsedTokens = 0
		} else {
			elapsed := now.Sub(b.LastRefillAt).Seconds()
			refillAmount := int(elapsed * policy.RefillRate)
			if refillAmount > 0 {
				effectiveLim := m.effectiveLimit(b)
				currentTokens := effectiveLim - b.UsedTokens
				newTokens := currentTokens + refillAmount
				if newTokens > effectiveLim {
					newTokens = effectiveLim
				}
				b.UsedTokens = effectiveLim - newTokens
				if b.UsedTokens < 0 {
					b.UsedTokens = 0
				}
				b.LastRefillAt = now
			}
		}
	case model.AlgoSlidingWindow:
		if b.WindowStartAt.IsZero() {
			b.WindowStartAt = now
			b.UsedTokens = 0
		} else {
			elapsed := now.Sub(b.WindowStartAt).Seconds()
			windowSec := float64(policy.WindowSec)
			if windowSec > 0 && elapsed > 0 {
				decayFactor := m.expDecayFactor(elapsed, windowSec)
				b.UsedTokens = int(float64(b.UsedTokens) * decayFactor)
				if b.UsedTokens < 0 {
					b.UsedTokens = 0
				}
			}
			b.WindowStartAt = now
		}
	}
}

func (m *Manager) refillLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.refillTick()
		}
	}
}

func (m *Manager) refillTick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	dirty := false

	m.cleanupExpiredWaitItemsLocked(now)

	reservationDirty := m.processReservationTickLocked(now)
	if reservationDirty {
		dirty = true
	}

	for _, b := range m.bindings {
		policy, ok := m.policies[b.PolicyName]
		if !ok {
			continue
		}
		changed := m.applyAlgorithmTick(b, policy, now)
		if changed {
			dirty = true
		}
	}

	if len(m.waitQueue) > 0 {
		granted := m.tryGrantFromQueueLocked(now)
		if granted > 0 {
			dirty = true
		}
	}

	if dirty {
		if err := m.persistDirtyLocked(); err != nil {
			log.Printf("[ratelimit-manager] persist error: %v", err)
		}
	}
}

func (m *Manager) applyAlgorithmTick(b *model.CallerBinding, policy *model.RateLimitPolicy, now time.Time) bool {
	switch policy.Algorithm {
	case model.AlgoFixedWindow:
		elapsed := now.Sub(b.WindowStartAt).Seconds()
		if elapsed >= float64(policy.WindowSec) {
			windowsPassed := int(elapsed) / policy.WindowSec
			b.WindowStartAt = b.WindowStartAt.Add(time.Duration(windowsPassed*policy.WindowSec) * time.Second)
			b.UsedTokens = 0
			b.UpdatedAt = now
			return true
		}
	case model.AlgoTokenBucket:
		elapsed := now.Sub(b.LastRefillAt).Seconds()
		if elapsed < 0.001 {
			return false
		}
		refillAmount := elapsed * policy.RefillRate
		if refillAmount < 0.1 {
			b.LastRefillAt = now
			return false
		}
		effectiveLim := m.effectiveLimit(b)
		currentTokens := float64(effectiveLim - b.UsedTokens)
		newTokens := currentTokens + refillAmount
		if newTokens > float64(effectiveLim) {
			newTokens = float64(effectiveLim)
		}
		newUsed := int(float64(effectiveLim) - newTokens)
		if newUsed < 0 {
			newUsed = 0
		}
		changed := newUsed != b.UsedTokens
		b.UsedTokens = newUsed
		b.LastRefillAt = now
		b.UpdatedAt = now
		return changed
	case model.AlgoSlidingWindow:
		elapsed := now.Sub(b.WindowStartAt).Seconds()
		windowSec := float64(policy.WindowSec)
		if windowSec <= 0 {
			windowSec = 60
		}

		if elapsed > 0 {
			decayFactor := m.expDecayFactor(elapsed, windowSec)
			b.UsedTokens = int(float64(b.UsedTokens) * decayFactor)
			if b.UsedTokens < 0 {
				b.UsedTokens = 0
			}
			b.WindowStartAt = now
			b.UpdatedAt = now
			return true
		}
		return false
	}
	return false
}

func (m *Manager) cleanupExpiredWaitItemsLocked(now time.Time) {
	alive := make([]*model.RateLimitWaitItem, 0, len(m.waitQueue))
	expired := make([]int64, 0)
	for _, item := range m.waitQueue {
		if item.TimeoutAt.Before(now) || item.TimeoutAt.Equal(now) {
			expired = append(expired, item.ID)
		} else {
			alive = append(alive, item)
		}
	}
	if len(expired) > 0 {
		m.waitQueue = alive
		for _, id := range expired {
			_ = m.storage.RemoveWaitItem(id)
		}
	}
}

func (m *Manager) processReservationTickLocked(now time.Time) bool {
	dirty := false

	for _, r := range m.reservations {
		if r.Status == model.ReservationStatusPending && !r.StartAt.After(now) {
			if err := m.activateReservationLocked(r, now); err == nil {
				dirty = true
			}
		}
		if r.Status == model.ReservationStatusActive && !r.EndAt.After(now) {
			if err := m.completeReservationLocked(r, now); err == nil {
				dirty = true
			}
		}
	}

	return dirty
}

func (m *Manager) activateReservationLocked(r *model.QuotaReservation, now time.Time) error {
	b, ok := m.bindings[r.CallerID]
	if !ok {
		log.Printf("[ratelimit-reservation] caller not found for activation: reservation=%d caller=%s", r.ID, r.CallerID)
		r.Status = model.ReservationStatusCancelled
		r.UpdatedAt = now
		_ = m.storage.UpdateReservationStatus(r.ID, r.Status, now)
		return fmt.Errorf("caller not found: %s", r.CallerID)
	}

	b.ReservedTokens += r.Tokens
	b.UpdatedAt = now
	r.Status = model.ReservationStatusActive
	r.UpdatedAt = now

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		b.ReservedTokens -= r.Tokens
		b.UpdatedAt = now
		return err
	}
	if err := m.storage.UpdateReservationStatus(r.ID, r.Status, now); err != nil {
		b.ReservedTokens -= r.Tokens
		b.UpdatedAt = now
		_ = m.storage.UpdateCallerBinding(b)
		r.Status = model.ReservationStatusPending
		r.UpdatedAt = now
		return err
	}

	log.Printf("[ratelimit-reservation] activated: id=%d policy=%s caller=%s tokens=%d", r.ID, r.PolicyName, r.CallerID, r.Tokens)
	return nil
}

func (m *Manager) completeReservationLocked(r *model.QuotaReservation, now time.Time) error {
	b, ok := m.bindings[r.CallerID]
	if ok {
		if b.ReservedTokens >= r.Tokens {
			b.ReservedTokens -= r.Tokens
		} else {
			b.ReservedTokens = 0
		}
		b.UpdatedAt = now

		effectiveLim := m.effectiveLimit(b)
		if b.UsedTokens > effectiveLim {
			b.UsedTokens = effectiveLim
		}

		if err := m.storage.UpdateCallerBinding(b); err != nil {
			return err
		}
	}

	r.Status = model.ReservationStatusCompleted
	r.UpdatedAt = now
	if err := m.storage.UpdateReservationStatus(r.ID, r.Status, now); err != nil {
		return err
	}

	log.Printf("[ratelimit-reservation] completed: id=%d policy=%s caller=%s tokens=%d", r.ID, r.PolicyName, r.CallerID, r.Tokens)
	return nil
}

func (m *Manager) tryGrantFromQueueLocked(now time.Time) int {
	granted := 0
	remaining := make([]*model.RateLimitWaitItem, 0, len(m.waitQueue))

	for _, item := range m.waitQueue {
		if item.TimeoutAt.Before(now) || item.TimeoutAt.Equal(now) {
			_ = m.storage.RemoveWaitItem(item.ID)
			continue
		}

		b, ok := m.bindings[item.CallerID]
		if !ok {
			_ = m.storage.RemoveWaitItem(item.ID)
			continue
		}

		policy, ok := m.policies[b.PolicyName]
		if !ok {
			_ = m.storage.RemoveWaitItem(item.ID)
			continue
		}

		m.applyAlgorithmTick(b, policy, now)

		effectiveLim := m.effectiveLimit(b)
		remainingTokens := effectiveLim - b.UsedTokens
		if remainingTokens < 0 {
			remainingTokens = 0
		}

		if remainingTokens >= item.Tokens {
			b.UsedTokens += item.Tokens
			b.UpdatedAt = now

			event := &model.RateLimitEvent{
				CallerID:   item.CallerID,
				PolicyName: b.PolicyName,
				Requested:  item.Tokens,
				Granted:    item.Tokens,
				Allowed:    true,
				Reason:     "granted from wait queue",
				CreatedAt:  now,
			}
			_ = m.storage.AddRateLimitEvent(event)
			_ = m.storage.UpdateCallerBinding(b)
			_ = m.storage.RemoveWaitItem(item.ID)

			granted++
			log.Printf("[ratelimit] granted from queue: caller=%s tokens=%d", item.CallerID, item.Tokens)
		} else {
			remaining = append(remaining, item)
		}
	}

	m.waitQueue = remaining
	return granted
}

func (m *Manager) persistAllLocked() error {
	for _, b := range m.bindings {
		if err := m.storage.UpdateCallerBinding(b); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) persistDirtyLocked() error {
	return m.persistAllLocked()
}

func (m *Manager) CreatePolicy(name string, algorithm model.AlgorithmType, windowSec int, maxTokens int, refillRate float64, refillUnit string) (*model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.policies[name]; ok {
		return nil, fmt.Errorf("policy already exists: %s", name)
	}

	now := time.Now()
	p := &model.RateLimitPolicy{
		Name:       name,
		Algorithm:  algorithm,
		WindowSec:  windowSec,
		MaxTokens:  maxTokens,
		RefillRate: refillRate,
		RefillUnit: refillUnit,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := m.storage.CreatePolicy(p); err != nil {
		return nil, err
	}

	m.policies[name] = p
	log.Printf("[ratelimit] policy created: name=%s algo=%s max=%d", name, algorithm, maxTokens)
	return p, nil
}

func (m *Manager) GetPolicy(name string) (*model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.policies[name]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", name)
	}
	return p, nil
}

func (m *Manager) ListPolicies() ([]model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	policies := make([]model.RateLimitPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, *p)
	}
	return policies, nil
}

func (m *Manager) BindCaller(callerID string, policyName string, quotaLimit int) (*model.CallerBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	policy, ok := m.policies[policyName]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", policyName)
	}

	effectiveQuota := quotaLimit
	if effectiveQuota > policy.MaxTokens {
		effectiveQuota = policy.MaxTokens
	}

	now := time.Now()
	b := &model.CallerBinding{
		CallerID:   callerID,
		PolicyName: policyName,
		QuotaLimit: effectiveQuota,
		UsedTokens: 0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	switch policy.Algorithm {
	case model.AlgoFixedWindow:
		b.WindowStartAt = now
	case model.AlgoSlidingWindow:
		b.WindowStartAt = now
	case model.AlgoTokenBucket:
		b.LastRefillAt = now
	}

	if err := m.storage.UpsertCallerBinding(b); err != nil {
		return nil, err
	}

	m.bindings[callerID] = b
	log.Printf("[ratelimit] caller bound: caller=%s policy=%s quota=%d (effective=%d)", callerID, policyName, quotaLimit, effectiveQuota)
	return b, nil
}

func (m *Manager) RequestTokens(callerID string, tokens int, waitable bool, waitSec int) (*model.TokenResult, error) {
	if tokens <= 0 {
		return nil, fmt.Errorf("tokens must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return nil, fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", b.PolicyName)
	}

	now := time.Now()
	m.applyAlgorithmTick(b, policy, now)

	effectiveLim := m.effectiveLimit(b)
	remaining := effectiveLim - b.UsedTokens
	if remaining < 0 {
		remaining = 0
	}

	result := &model.TokenResult{
		Requested:  tokens,
		QuotaLimit: b.QuotaLimit,
		UsedTokens: b.UsedTokens,
		Remaining:  remaining,
	}

	if remaining >= tokens {
		b.UsedTokens += tokens
		b.UpdatedAt = now
		result.Allowed = true
		result.Granted = tokens
		result.UsedTokens = b.UsedTokens
		result.Remaining = effectiveLim - b.UsedTokens
	} else {
		if waitable {
			waitTimeout := waitSec
			if waitTimeout <= 0 {
				waitTimeout = 30
			}
			timeoutAt := now.Add(time.Duration(waitTimeout) * time.Second)

			item := &model.RateLimitWaitItem{
				CallerID:   callerID,
				Tokens:     tokens,
				EnqueuedAt: now,
				TimeoutAt:  timeoutAt,
			}
			if err := m.storage.AddWaitItem(item); err != nil {
				return nil, err
			}
			m.waitQueue = append(m.waitQueue, item)

			position := 0
			for i, q := range m.waitQueue {
				if q.ID == item.ID {
					position = i + 1
					break
				}
			}

			result.Queued = true
			result.Position = position
			result.Allowed = false
			result.Granted = 0
			result.Reason = fmt.Sprintf("queued for tokens: requested=%d, remaining=%d, position=%d", tokens, remaining, position)

			event := &model.RateLimitEvent{
				CallerID:   callerID,
				PolicyName: b.PolicyName,
				Requested:  tokens,
				Granted:    0,
				Allowed:    false,
				Reason:     "queued",
				CreatedAt:  now,
			}
			_ = m.storage.AddRateLimitEvent(event)

			if err := m.storage.UpdateCallerBinding(b); err != nil {
				return nil, err
			}

			log.Printf("[ratelimit] queued: caller=%s tokens=%d position=%d", callerID, tokens, position)
			return result, nil
		}

		result.Allowed = false
		result.Granted = 0
		result.Reason = fmt.Sprintf("insufficient quota: requested=%d, remaining=%d", tokens, remaining)
	}

	event := &model.RateLimitEvent{
		CallerID:   callerID,
		PolicyName: b.PolicyName,
		Requested:  tokens,
		Granted:    result.Granted,
		Allowed:    result.Allowed,
		Reason:     result.Reason,
		CreatedAt:  now,
	}
	_ = m.storage.AddRateLimitEvent(event)

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Manager) GetCallerStatus(callerID string) (*model.CallerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return nil, fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", b.PolicyName)
	}

	now := time.Now()
	m.processReservationTickLocked(now)
	m.applyAlgorithmTick(b, policy, now)

	effectiveLim := m.effectiveLimit(b)
	remaining := effectiveLim - b.UsedTokens
	if remaining < 0 {
		remaining = 0
	}

	rateLimited, _ := m.storage.CountRateLimited(callerID)
	waitCount, _ := m.storage.CountWaitItems(callerID)

	status := &model.CallerStatus{
		CallerID:       b.CallerID,
		PolicyName:     b.PolicyName,
		Algorithm:      string(policy.Algorithm),
		QuotaLimit:     b.QuotaLimit,
		PolicyMax:      policy.MaxTokens,
		UsedTokens:     b.UsedTokens,
		Remaining:      remaining,
		BorrowedTokens: b.BorrowedTokens,
		LentTokens:     b.LentTokens,
		ReservedTokens: b.ReservedTokens,
		RateLimited:    rateLimited,
		WaitQueueLen:   waitCount,
	}

	return status, nil
}

func (m *Manager) ListCallerStatuses() ([]model.CallerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var result []model.CallerStatus

	m.processReservationTickLocked(now)

	for _, b := range m.bindings {
		policy, ok := m.policies[b.PolicyName]
		if !ok {
			continue
		}
		m.applyAlgorithmTick(b, policy, now)

		effectiveLim := m.effectiveLimit(b)
		remaining := effectiveLim - b.UsedTokens
		if remaining < 0 {
			remaining = 0
		}

		rateLimited, _ := m.storage.CountRateLimited(b.CallerID)
		waitCount, _ := m.storage.CountWaitItems(b.CallerID)

		status := model.CallerStatus{
			CallerID:       b.CallerID,
			PolicyName:     b.PolicyName,
			Algorithm:      string(policy.Algorithm),
			QuotaLimit:     b.QuotaLimit,
			PolicyMax:      policy.MaxTokens,
			UsedTokens:     b.UsedTokens,
			Remaining:      remaining,
			BorrowedTokens: b.BorrowedTokens,
			LentTokens:     b.LentTokens,
			ReservedTokens: b.ReservedTokens,
			RateLimited:    rateLimited,
			WaitQueueLen:   waitCount,
		}
		result = append(result, status)
	}

	return result, nil
}

func (m *Manager) BorrowQuota(fromCaller string, toCaller string, amount int) (*model.BorrowResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fromB, ok := m.bindings[fromCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "from caller not found"}, nil
	}

	toB, ok := m.bindings[toCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "to caller not found"}, nil
	}

	fromPolicy, ok := m.policies[fromB.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(fromB, fromPolicy, now)
	}

	toPolicy, ok := m.policies[toB.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(toB, toPolicy, now)
	}

	fromEffective := m.effectiveLimit(fromB)
	fromRemaining := fromEffective - fromB.UsedTokens
	if fromRemaining < 0 {
		fromRemaining = 0
	}

	if fromRemaining < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("insufficient free quota: has %d, need %d", fromRemaining, amount)}, nil
	}

	now := time.Now()
	oldFromLent := fromB.LentTokens
	oldToBorrowed := toB.BorrowedTokens
	oldFromUpdated := fromB.UpdatedAt
	oldToUpdated := toB.UpdatedAt

	fromB.LentTokens += amount
	toB.BorrowedTokens += amount
	fromB.UpdatedAt = now
	toB.UpdatedAt = now

	fromEffectiveNew := m.effectiveLimit(fromB)
	if fromB.UsedTokens > fromEffectiveNew {
		fromB.UsedTokens = fromEffectiveNew
	}

	record := &model.QuotaBorrowRecord{
		FromCaller: fromCaller,
		ToCaller:   toCaller,
		Amount:     amount,
		Status:     "active",
		CreatedAt:  now,
	}
	if err := m.storage.CreateBorrowRecord(record); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}

	if err := m.storage.UpdateCallerBinding(fromB); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}
	if err := m.storage.UpdateCallerBinding(toB); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}

	log.Printf("[ratelimit] quota borrowed: from=%s to=%s amount=%d", fromCaller, toCaller, amount)
	return &model.BorrowResult{Success: true, Message: fmt.Sprintf("borrowed %d tokens from %s to %s", amount, fromCaller, toCaller)}, nil
}

func (m *Manager) ReturnQuota(fromCaller string, toCaller string, amount int) (*model.BorrowResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fromB, ok := m.bindings[fromCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "from caller not found"}, nil
	}

	toB, ok := m.bindings[toCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "to caller not found"}, nil
	}

	if fromB.BorrowedTokens < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("borrowed quota less than amount: borrowed=%d, return=%d", fromB.BorrowedTokens, amount)}, nil
	}

	if toB.LentTokens < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("lent quota less than amount: lent=%d, return=%d", toB.LentTokens, amount)}, nil
	}

	now := time.Now()
	oldFromBorrowed := fromB.BorrowedTokens
	oldToLent := toB.LentTokens
	oldFromUsed := fromB.UsedTokens
	oldFromUpdated := fromB.UpdatedAt
	oldToUpdated := toB.UpdatedAt

	fromB.BorrowedTokens -= amount
	toB.LentTokens -= amount
	fromB.UpdatedAt = now
	toB.UpdatedAt = now

	if _, ok := m.policies[fromB.PolicyName]; ok {
		effectiveLimit := m.effectiveLimit(fromB)
		if fromB.UsedTokens > effectiveLimit {
			fromB.UsedTokens = effectiveLimit
		}
	}

	rollback := func() {
		fromB.BorrowedTokens = oldFromBorrowed
		toB.LentTokens = oldToLent
		fromB.UsedTokens = oldFromUsed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
	}

	if err := m.storage.ReturnBorrow(fromCaller, toCaller, amount, now); err != nil {
		rollback()
		return nil, err
	}

	if err := m.storage.UpdateCallerBinding(fromB); err != nil {
		rollback()
		return nil, err
	}
	if err := m.storage.UpdateCallerBinding(toB); err != nil {
		rollback()
		return nil, err
	}

	log.Printf("[ratelimit] quota returned: from=%s to=%s amount=%d", fromCaller, toCaller, amount)
	return &model.BorrowResult{Success: true, Message: fmt.Sprintf("returned %d tokens from %s to %s", amount, fromCaller, toCaller)}, nil
}

func (m *Manager) AdjustQuota(callerID string, newQuotaLimit int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(b, policy, now)
	}

	b.QuotaLimit = newQuotaLimit
	b.UpdatedAt = time.Now()

	effectiveLimit := m.effectiveLimit(b)
	if b.UsedTokens > effectiveLimit {
		b.UsedTokens = effectiveLimit
	}

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		return err
	}

	log.Printf("[ratelimit] quota adjusted: caller=%s new_quota=%d (policy_max=%d)", callerID, newQuotaLimit, policy.MaxTokens)
	return nil
}

func (m *Manager) GetGlobalStats() (*model.GlobalStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	total, allowed, err := m.storage.CountAllEvents()
	if err != nil {
		return nil, err
	}

	borrows, err := m.storage.ListActiveBorrows()
	if err != nil {
		return nil, err
	}

	borrowedAmount := 0
	for _, b := range borrows {
		borrowedAmount += b.Amount
	}

	waitCount := len(m.waitQueue)

	stats := &model.GlobalStats{
		TotalCallers:     len(m.bindings),
		TotalPolicies:    len(m.policies),
		TotalRequests:    total,
		TotalAllowed:     allowed,
		TotalRateLimited: total - allowed,
		ActiveBorrows:    len(borrows),
		BorrowedAmount:   borrowedAmount,
	}

	_ = waitCount

	return stats, nil
}

func (m *Manager) GetCallerHistory(callerID string, limit int) ([]model.RateLimitEvent, error) {
	return m.storage.ListRateLimitEvents(callerID, limit)
}

func (m *Manager) ListBorrowRecords() ([]model.QuotaBorrowRecord, error) {
	return m.storage.ListActiveBorrows()
}

func (m *Manager) ListWaitItems(callerID string) ([]model.RateLimitWaitItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if callerID == "" {
		items := make([]model.RateLimitWaitItem, 0, len(m.waitQueue))
		for _, item := range m.waitQueue {
			items = append(items, *item)
		}
		return items, nil
	}

	var items []model.RateLimitWaitItem
	for _, item := range m.waitQueue {
		if item.CallerID == callerID {
			items = append(items, *item)
		}
	}
	return items, nil
}

func (m *Manager) CreateReservation(policyName string, callerID string, tokens int, startAt time.Time, endAt time.Time) (*model.ReservationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	if !endAt.After(startAt) {
		return &model.ReservationResult{
			Success: false,
			Message: "end time must be after start time",
		}, nil
	}

	if endAt.Before(now) {
		return &model.ReservationResult{
			Success: false,
			Message: "cannot reserve past time",
		}, nil
	}

	if tokens <= 0 {
		return &model.ReservationResult{
			Success: false,
			Message: "tokens must be positive",
		}, nil
	}

	policy, ok := m.policies[policyName]
	if !ok {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("policy not found: %s", policyName),
		}, nil
	}

	if tokens > policy.MaxTokens {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("tokens exceeds policy max tokens: max=%d, requested=%d", policy.MaxTokens, tokens),
		}, nil
	}

	if _, ok := m.bindings[callerID]; !ok {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("caller not found: %s", callerID),
		}, nil
	}

	conflict, maxConcurrent := m.checkConflictLocked(policyName, startAt, endAt, tokens)
	if conflict {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("reservation conflicts with existing reservations: max allowed concurrent tokens=%d", maxConcurrent),
		}, nil
	}

	status := model.ReservationStatusPending
	if !startAt.After(now) {
		status = model.ReservationStatusActive
	}

	r := &model.QuotaReservation{
		PolicyName: policyName,
		CallerID:   callerID,
		Tokens:     tokens,
		StartAt:    startAt,
		EndAt:      endAt,
		Status:     status,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := m.storage.CreateReservation(r); err != nil {
		return nil, err
	}

	m.reservations[r.ID] = r

	if status == model.ReservationStatusActive {
		b := m.bindings[callerID]
		b.ReservedTokens += tokens
		b.UpdatedAt = now
		if err := m.storage.UpdateCallerBinding(b); err != nil {
			delete(m.reservations, r.ID)
			return nil, err
		}
		log.Printf("[ratelimit-reservation] created and activated: id=%d policy=%s caller=%s tokens=%d", r.ID, policyName, callerID, tokens)
	} else {
		log.Printf("[ratelimit-reservation] created: id=%d policy=%s caller=%s tokens=%d start=%v end=%v", r.ID, policyName, callerID, tokens, startAt, endAt)
	}

	copyR := *r
	return &model.ReservationResult{
		Success:     true,
		Message:     "reservation created",
		Reservation: &copyR,
	}, nil
}

func (m *Manager) checkConflictLocked(policyName string, startAt time.Time, endAt time.Time, tokens int) (bool, int) {
	policy, ok := m.policies[policyName]
	if !ok {
		return true, 0
	}

	maxTokens := policy.MaxTokens

	type interval struct {
		time   time.Time
		delta  int
	}

	var events []interval
	events = append(events, interval{time: startAt, delta: tokens})
	events = append(events, interval{time: endAt, delta: -tokens})

	for _, r := range m.reservations {
		if r.PolicyName != policyName {
			continue
		}
		if r.Status != model.ReservationStatusPending && r.Status != model.ReservationStatusActive {
			continue
		}
		events = append(events, interval{time: r.StartAt, delta: r.Tokens})
		events = append(events, interval{time: r.EndAt, delta: -r.Tokens})
	}

	for i := 0; i < len(events); i++ {
		for j := i + 1; j < len(events); j++ {
			if events[i].time.After(events[j].time) {
				events[i], events[j] = events[j], events[i]
			}
		}
	}

	currentTokens := 0
	for _, e := range events {
		currentTokens += e.delta
		if currentTokens > maxTokens {
			return true, maxTokens
		}
	}

	return false, maxTokens
}

func (m *Manager) CancelReservation(reservationID int64) (*model.ReservationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	r, ok := m.reservations[reservationID]
	if !ok {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("reservation not found: %d", reservationID),
		}, nil
	}

	if r.Status == model.ReservationStatusCompleted || r.Status == model.ReservationStatusCancelled {
		return &model.ReservationResult{
			Success: false,
			Message: fmt.Sprintf("reservation already %s", r.Status),
		}, nil
	}

	if r.Status == model.ReservationStatusActive {
		b, ok := m.bindings[r.CallerID]
		if ok {
			if b.ReservedTokens >= r.Tokens {
				b.ReservedTokens -= r.Tokens
			} else {
				b.ReservedTokens = 0
			}
			b.UpdatedAt = now

			effectiveLim := m.effectiveLimit(b)
			if b.UsedTokens > effectiveLim {
				b.UsedTokens = effectiveLim
			}

			if err := m.storage.UpdateCallerBinding(b); err != nil {
				return nil, err
			}
		}
	}

	r.Status = model.ReservationStatusCancelled
	r.UpdatedAt = now

	if err := m.storage.CancelReservation(reservationID, now); err != nil {
		return nil, err
	}

	log.Printf("[ratelimit-reservation] cancelled: id=%d policy=%s caller=%s tokens=%d", r.ID, r.PolicyName, r.CallerID, r.Tokens)

	copyR := *r
	return &model.ReservationResult{
		Success:     true,
		Message:     "reservation cancelled",
		Reservation: &copyR,
	}, nil
}

func (m *Manager) GetReservation(reservationID int64) (*model.QuotaReservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.reservations[reservationID]
	if !ok {
		return nil, fmt.Errorf("reservation not found: %d", reservationID)
	}
	copyR := *r
	return &copyR, nil
}

func (m *Manager) ListReservations(policyName string, callerID string, status string) ([]model.QuotaReservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []model.QuotaReservation
	for _, r := range m.reservations {
		if policyName != "" && r.PolicyName != policyName {
			continue
		}
		if callerID != "" && r.CallerID != callerID {
			continue
		}
		if status != "" && string(r.Status) != status {
			continue
		}
		result = append(result, *r)
	}

	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].StartAt.After(result[j].StartAt) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result, nil
}

func (m *Manager) ReturnTokens(callerID string, tokens int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(b, policy, now)
	}

	if tokens > b.UsedTokens {
		b.UsedTokens = 0
	} else {
		b.UsedTokens -= tokens
	}
	b.UpdatedAt = time.Now()

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		return err
	}

	log.Printf("[ratelimit] tokens returned: caller=%s tokens=%d remaining=%d", callerID, tokens, m.effectiveLimit(b)-b.UsedTokens)
	return nil
}
