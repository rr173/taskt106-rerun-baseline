package shadow

import (
	"fmt"
	"log"
	"task106/internal/audit"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"strconv"
	"sync"
	"time"
)

type Manager struct {
	storage      *storage.Storage
	lockMgr      *lock.Manager
	rateLimitMgr *ratelimit.Manager
	auditMgr     *audit.Manager

	mu    sync.Mutex
	plans map[int64]*model.ShadowPlan

	applyMu sync.Mutex
	stopCh  chan struct{}
	ticker  *time.Ticker
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager, am *audit.Manager) *Manager {
	return &Manager{
		storage:      s,
		lockMgr:      lm,
		rateLimitMgr: rlm,
		auditMgr:     am,
		plans:        make(map[int64]*model.ShadowPlan),
		stopCh:       make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	plans, err := m.storage.ListShadowPlans()
	if err != nil {
		return fmt.Errorf("load shadow plans: %w", err)
	}
	for i := range plans {
		p := &plans[i]
		m.plans[p.ID] = p
		if p.Status == model.ShadowPlanStatusRunning {
			if p.Mode == "replay" {
				log.Printf("[shadow] resume replay plan: id=%d name=%s", p.ID, p.Name)
				go m.runReplay(p.ID)
			} else {
				log.Printf("[shadow] resume mirror plan: id=%d name=%s", p.ID, p.Name)
			}
		}
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.mirrorLoop()

	log.Println("[shadow-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[shadow-manager] stopped")
}

func (m *Manager) CreatePlan(name, description, mode string, mirrorSec int) (*model.ShadowPlan, error) {
	if mode != "replay" && mode != "mirror" {
		return nil, fmt.Errorf("mode must be 'replay' or 'mirror'")
	}

	now := time.Now()
	plan := &model.ShadowPlan{
		Name:        name,
		Description: description,
		Status:      model.ShadowPlanStatusDraft,
		Mode:        mode,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if mode == "mirror" && mirrorSec > 0 {
		plan.MirrorUntil = now.Add(time.Duration(mirrorSec) * time.Second)
	}

	if err := m.storage.CreateShadowPlan(plan); err != nil {
		return nil, err
	}

	if mode == "mirror" && mirrorSec > 0 {
		if err := m.storage.UpdateShadowPlanMirror(plan.ID, plan.MirrorUntil, now); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	m.plans[plan.ID] = plan
	m.mu.Unlock()

	log.Printf("[shadow] created plan: id=%d name=%s mode=%s", plan.ID, plan.Name, plan.Mode)
	return plan, nil
}

func (m *Manager) GetPlan(id int64) (*model.ShadowPlan, error) {
	return m.storage.GetShadowPlan(id)
}

func (m *Manager) ListPlans() ([]model.ShadowPlan, error) {
	return m.storage.ListShadowPlans()
}

func (m *Manager) AddOverride(planID int64, category model.ShadowRuleCategory, targetKey, field, newValue string) (*model.ShadowConfigOverride, error) {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusDraft {
		return nil, fmt.Errorf("can only add overrides to draft plans")
	}

	origValue := m.getCurrentValue(category, targetKey, field)

	ov := &model.ShadowConfigOverride{
		PlanID:    planID,
		Category:  category,
		TargetKey: targetKey,
		Field:     field,
		OrigValue: origValue,
		NewValue:  newValue,
		CreatedAt: time.Now(),
	}
	if err := m.storage.CreateShadowConfigOverride(ov); err != nil {
		return nil, err
	}
	log.Printf("[shadow] added override: plan=%d category=%s target=%s field=%s %s->%s",
		planID, category, targetKey, field, origValue, newValue)
	return ov, nil
}

func (m *Manager) ListOverrides(planID int64) ([]model.ShadowConfigOverride, error) {
	return m.storage.ListShadowConfigOverrides(planID)
}

func (m *Manager) RemoveOverride(id int64) error {
	return m.storage.DeleteShadowConfigOverride(id)
}

func (m *Manager) StartPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusDraft {
		return fmt.Errorf("plan must be in draft status to start")
	}

	now := time.Now()

	if plan.Mode == "replay" {
		minID, maxID, err := m.storage.GetAuditLogRange()
		if err != nil {
			return fmt.Errorf("get audit log range: %w", err)
		}
		if maxID == 0 {
			return fmt.Errorf("no audit logs available for replay")
		}
		plan.AuditLogStartID = minID
		plan.AuditLogEndID = maxID
	}

	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusRunning, now); err != nil {
		return err
	}
	plan.Status = model.ShadowPlanStatusRunning
	plan.UpdatedAt = now

	m.mu.Lock()
	m.plans[planID] = plan
	m.mu.Unlock()

	if plan.Mode == "replay" {
		go m.runReplay(planID)
	}

	log.Printf("[shadow] started plan: id=%d name=%s mode=%s (logs %d-%d)",
		planID, plan.Name, plan.Mode, plan.AuditLogStartID, plan.AuditLogEndID)
	return nil
}

func (m *Manager) CancelPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusRunning && plan.Status != model.ShadowPlanStatusDraft {
		return fmt.Errorf("plan cannot be cancelled in current status: %s", plan.Status)
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusCancelled, now); err != nil {
		return err
	}

	m.mu.Lock()
	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusCancelled
		p.UpdatedAt = now
	}
	m.mu.Unlock()

	log.Printf("[shadow] cancelled plan: id=%d", planID)
	return nil
}

func (m *Manager) ApplyPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusCompleted && plan.Status != model.ShadowPlanStatusRunning {
		return fmt.Errorf("plan must be completed or running to apply")
	}

	overrides, err := m.storage.ListShadowConfigOverrides(planID)
	if err != nil {
		return err
	}
	if len(overrides) == 0 {
		return fmt.Errorf("no overrides to apply")
	}

	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	type appliedAction struct {
		override model.ShadowConfigOverride
		undo     func() error
	}
	var applied []appliedAction

	rollback := func(finalErr error) error {
		for i := len(applied) - 1; i >= 0; i-- {
			act := applied[i]
			log.Printf("[shadow] rolling back override: %s/%s.%s (%s->%s)",
				act.override.Category, act.override.TargetKey, act.override.Field,
				act.override.NewValue, act.override.OrigValue)
			if undoErr := act.undo(); undoErr != nil {
				log.Printf("[shadow] rollback FAILED for override %d: %v", act.override.ID, undoErr)
			}
		}
		return finalErr
	}

	for i := range overrides {
		ov := overrides[i]
		undoFn, applyErr := m.applyOverrideWithUndo(ov)
		if applyErr != nil {
			return rollback(fmt.Errorf("apply override %d (category=%s target=%s field=%s): %w",
				ov.ID, ov.Category, ov.TargetKey, ov.Field, applyErr))
		}
		applied = append(applied, appliedAction{override: ov, undo: undoFn})
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanApplied(planID, now, now); err != nil {
		return rollback(fmt.Errorf("update plan applied status: %w", err))
	}

	m.mu.Lock()
	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusApplied
		p.AppliedAt = now
		p.UpdatedAt = now
	}
	m.mu.Unlock()

	log.Printf("[shadow] applied plan: id=%d name=%s - all %d overrides applied atomically",
		planID, plan.Name, len(overrides))
	return nil
}

func (m *Manager) GetDiffRecords(planID int64, limit int) ([]model.ShadowDiffRecord, error) {
	return m.storage.ListShadowDiffRecords(planID, limit)
}

func (m *Manager) GetDiffStats(planID int64) (*model.ShadowDiffStats, error) {
	return m.storage.GetShadowDiffStats(planID)
}

func (m *Manager) getCurrentValue(category model.ShadowRuleCategory, targetKey, field string) string {
	switch category {
	case model.ShadowRuleCircuitBreaker:
		rule, err := m.storage.GetCircuitBreakerRule(targetKey)
		if err != nil || rule == nil {
			return ""
		}
		switch field {
		case "failure_threshold":
			return strconv.Itoa(rule.FailureThreshold)
		case "window_sec":
			return strconv.Itoa(rule.WindowSec)
		case "cooldown_sec":
			return strconv.Itoa(rule.CooldownSec)
		}
	case model.ShadowRuleRateLimit:
		binding, err := m.storage.GetCallerBinding(targetKey)
		if err != nil || binding == nil {
			return ""
		}
		switch field {
		case "quota_limit":
			return strconv.Itoa(binding.QuotaLimit)
		case "policy_name":
			return binding.PolicyName
		}
		policy, err := m.storage.GetPolicy(binding.PolicyName)
		if err != nil || policy == nil {
			return ""
		}
		switch field {
		case "max_tokens":
			return strconv.Itoa(policy.MaxTokens)
		case "window_sec":
			return strconv.Itoa(policy.WindowSec)
		case "refill_rate":
			return fmt.Sprintf("%f", policy.RefillRate)
		}
	case model.ShadowRuleReservation:
		policy, err := m.storage.GetPolicy(targetKey)
		if err == nil && policy != nil {
			switch field {
			case "max_tokens":
				return strconv.Itoa(policy.MaxTokens)
			}
		}
	case model.ShadowRuleLockDependency:
		node, err := m.storage.GetTopoNode(targetKey)
		if err == nil && node != nil {
			switch field {
			case "token_cost":
				return strconv.Itoa(node.TokenCost)
			case "lock_name":
				return node.LockName
			case "rate_policy":
				return node.RatePolicy
			}
		}
		lock, err := m.storage.GetLock(targetKey)
		if err == nil && lock != nil {
			switch field {
			case "reentrant":
				if lock.Reentrant {
					return "true"
				}
				return "false"
			}
		}
	}
	return ""
}

func (m *Manager) runReplay(planID int64) {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil || plan == nil {
		log.Printf("[shadow] replay failed to load plan %d: %v", planID, err)
		return
	}

	overrides, err := m.storage.ListShadowConfigOverrides(planID)
	if err != nil {
		log.Printf("[shadow] replay failed to load overrides for plan %d: %v", planID, err)
		return
	}

	logs, err := m.storage.ListAuditLogsInRange(plan.AuditLogStartID, plan.AuditLogEndID)
	if err != nil {
		log.Printf("[shadow] replay failed to load audit logs for plan %d: %v", planID, err)
		return
	}

	log.Printf("[shadow] starting replay for plan %d: %d audit logs to evaluate (%d-%d)",
		planID, len(logs), plan.AuditLogStartID, plan.AuditLogEndID)

	perCallerFailures := make(map[string]int)
	perCallerAdmits := make(map[string]int)
	perCallerRateLimited := make(map[string]int)
	for _, al := range logs {
		if al.Caller == "" {
			continue
		}
		liveDecision := m.classifyLiveDecision(al)
		switch liveDecision {
		case model.ShadowDecisionRateLimit:
			perCallerRateLimited[al.Caller]++
		case model.ShadowDecisionCircuitBreak, model.ShadowDecisionReject, model.ShadowDecisionDeadlockReject:
			perCallerFailures[al.Caller]++
		case model.ShadowDecisionAdmit:
			perCallerAdmits[al.Caller]++
		}
	}

	for _, auditLog := range logs {
		liveDecision := m.classifyLiveDecision(auditLog)

		accumulated := &evalContext{
			failures:    perCallerFailures,
			admits:      perCallerAdmits,
			rateLimited: perCallerRateLimited,
		}
		shadowDecision := m.classifyShadowDecisionWithContext(auditLog, overrides, accumulated)

		if liveDecision != shadowDecision {
			cat := m.classifyRuleCategory(auditLog, overrides)
			detail := m.buildDiffDetail(auditLog, liveDecision, shadowDecision, overrides)

			diff := &model.ShadowDiffRecord{
				PlanID:          planID,
				AuditLogID:      auditLog.ID,
				RequestCaller:   auditLog.Caller,
				RequestOp:       string(auditLog.Operation),
				RequestResource: auditLog.Resource,
				LiveDecision:    liveDecision,
				ShadowDecision:  shadowDecision,
				RuleCategory:    cat,
				Detail:          detail,
				CreatedAt:       time.Now(),
			}
			if err := m.storage.CreateShadowDiffRecord(diff); err != nil {
				log.Printf("[shadow] failed to record diff: %v", err)
			}
		}
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusCompleted, now); err != nil {
		log.Printf("[shadow] failed to update plan status: %v", err)
	}

	m.mu.Lock()
	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusCompleted
		p.UpdatedAt = now
	}
	m.mu.Unlock()

	cnt, _ := m.storage.CountShadowDiffs(planID)
	log.Printf("[shadow] replay completed for plan %d: %d differences found (out of %d logs)",
		planID, cnt, len(logs))
}

type evalContext struct {
	failures    map[string]int
	admits      map[string]int
	rateLimited map[string]int
}

func (m *Manager) classifyLiveDecision(al model.AuditLog) model.ShadowDecision {
	if al.Success {
		return model.ShadowDecisionAdmit
	}
	fr := al.FailReason
	switch {
	case fr == "circuit_breaker_open" || fr == audit.ErrCircuitBreakerOpen.Error():
		return model.ShadowDecisionCircuitBreak
	case fr == "rate limited" || fr == "quota_exceeded" ||
		(len(fr) > 9 && fr[:10] == "rate limit"):
		return model.ShadowDecisionRateLimit
	case fr == "not acquired" || fr == "wait_queue":
		return model.ShadowDecisionWait
	case fr == "deadlock" || fr == "deadlock_detected" ||
		(len(fr) > 7 && fr[:8] == "deadlock"):
		return model.ShadowDecisionDeadlockReject
	case fr == "tx_rollback" || fr == "tx timed out" ||
		(len(fr) > 7 && fr[:8] == "rollback"):
		return model.ShadowDecisionTxRollback
	default:
		return model.ShadowDecisionReject
	}
}

func (m *Manager) classifyShadowDecision(al model.AuditLog, overrides []model.ShadowConfigOverride) model.ShadowDecision {
	ctx := &evalContext{
		failures:    make(map[string]int),
		admits:      make(map[string]int),
		rateLimited: make(map[string]int),
	}
	return m.classifyShadowDecisionWithContext(al, overrides, ctx)
}

func (m *Manager) classifyShadowDecisionWithContext(al model.AuditLog, overrides []model.ShadowConfigOverride, ctx *evalContext) model.ShadowDecision {
	liveDecision := m.classifyLiveDecision(al)
	caller := al.Caller

	for _, ov := range overrides {
		if ov.TargetKey != caller && ov.TargetKey != al.Resource {
			continue
		}

		switch ov.Category {
		case model.ShadowRuleCircuitBreaker:
			if ov.TargetKey != caller {
				continue
			}
			rule, err := m.storage.GetCircuitBreakerRule(caller)
			if err != nil || rule == nil {
				continue
			}

			origThreshold := rule.FailureThreshold
			origWindow := rule.WindowSec

			shadowThreshold := origThreshold
			shadowWindow := origWindow
			shadowCooldown := rule.CooldownSec

			switch ov.Field {
			case "failure_threshold":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					shadowThreshold = v
				}
			case "window_sec":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					shadowWindow = v
				}
			case "cooldown_sec":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					shadowCooldown = v
				}
			}

			totalFailures := ctx.failures[caller]
			if !al.Success && liveDecision != model.ShadowDecisionAdmit &&
				liveDecision != model.ShadowDecisionRateLimit &&
				liveDecision != model.ShadowDecisionWait {
				totalFailures++
			}

			if liveDecision == model.ShadowDecisionCircuitBreak {
				if shadowThreshold > origThreshold {
					return model.ShadowDecisionAdmit
				}
				if shadowWindow > origWindow && totalFailures < shadowThreshold {
					return model.ShadowDecisionAdmit
				}
				return liveDecision
			}

			if liveDecision == model.ShadowDecisionAdmit {
				if shadowThreshold < origThreshold && totalFailures >= shadowThreshold {
					return model.ShadowDecisionCircuitBreak
				}
				if shadowWindow < origWindow {
					ratio := float64(origWindow) / float64(shadowWindow)
					scaledFailures := int(float64(totalFailures) / ratio)
					if scaledFailures >= shadowThreshold {
						return model.ShadowDecisionCircuitBreak
					}
				}
			}

			_ = shadowCooldown

		case model.ShadowRuleRateLimit:
			if ov.TargetKey != caller {
				continue
			}
			binding, err := m.storage.GetCallerBinding(caller)
			if err != nil || binding == nil {
				continue
			}

			origQuota := binding.QuotaLimit
			shadowQuota := origQuota

			switch ov.Field {
			case "quota_limit":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					shadowQuota = v
				}
			case "max_tokens":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					shadowQuota = v
				}
			}

			totalRequests := ctx.admits[caller] + ctx.rateLimited[caller]
			if al.Success {
				totalRequests++
			}

			if liveDecision == model.ShadowDecisionRateLimit {
				if shadowQuota > origQuota {
					return model.ShadowDecisionAdmit
				}
			}

			if liveDecision == model.ShadowDecisionAdmit {
				if shadowQuota < origQuota && totalRequests > shadowQuota {
					return model.ShadowDecisionRateLimit
				}
				if shadowQuota < origQuota && binding.UsedTokens >= shadowQuota {
					return model.ShadowDecisionRateLimit
				}
				if shadowQuota <= totalRequests && totalRequests > 0 {
					if totalRequests > shadowQuota {
						return model.ShadowDecisionRateLimit
					}
				}
			}

		case model.ShadowRuleReservation:
			switch ov.Field {
			case "max_tokens":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					origLimit := 0
					if ov.OrigValue != "" {
						origLimit, _ = strconv.Atoi(ov.OrigValue)
					}
					if v < origLimit && liveDecision == model.ShadowDecisionAdmit {
						return model.ShadowDecisionRateLimit
					}
					if v > origLimit && liveDecision == model.ShadowDecisionRateLimit {
						return model.ShadowDecisionAdmit
					}
				}
			}

		case model.ShadowRuleLockDependency:
			switch ov.Field {
			case "token_cost":
				if v, e := strconv.Atoi(ov.NewValue); e == nil {
					origCost := 0
					if ov.OrigValue != "" {
						origCost, _ = strconv.Atoi(ov.OrigValue)
					}
					if v > origCost && liveDecision == model.ShadowDecisionAdmit {
						return model.ShadowDecisionRateLimit
					}
					if v < origCost && liveDecision == model.ShadowDecisionRateLimit {
						return model.ShadowDecisionAdmit
					}
				}
			case "reentrant":
				if ov.NewValue == "false" && ov.OrigValue == "true" && liveDecision == model.ShadowDecisionAdmit {
					return model.ShadowDecisionReject
				}
				if ov.NewValue == "true" && ov.OrigValue == "false" &&
					(liveDecision == model.ShadowDecisionWait || liveDecision == model.ShadowDecisionReject) {
					return model.ShadowDecisionAdmit
				}
			}
		}
	}

	return liveDecision
}

func (m *Manager) classifyRuleCategory(al model.AuditLog, overrides []model.ShadowConfigOverride) model.ShadowRuleCategory {
	for _, ov := range overrides {
		if ov.TargetKey == al.Caller || ov.TargetKey == al.Resource {
			return ov.Category
		}
	}
	fr := al.FailReason
	if fr == "circuit_breaker_open" || fr == audit.ErrCircuitBreakerOpen.Error() {
		return model.ShadowRuleCircuitBreaker
	}
	if fr == "rate limited" || fr == "quota_exceeded" {
		return model.ShadowRuleRateLimit
	}
	if fr == "deadlock" || fr == "deadlock_detected" {
		return model.ShadowRuleLockDependency
	}
	return model.ShadowRuleLockDependency
}

func (m *Manager) buildDiffDetail(al model.AuditLog, live, shadow model.ShadowDecision, overrides []model.ShadowConfigOverride) string {
	for _, ov := range overrides {
		if ov.TargetKey == al.Caller || ov.TargetKey == al.Resource {
			return fmt.Sprintf("rule %s/%s.%s changed from %s to %s causes live=%s->shadow=%s",
				ov.Category, ov.TargetKey, ov.Field, ov.OrigValue, ov.NewValue, live, shadow)
		}
	}
	return fmt.Sprintf("live=%s shadow=%s for caller=%s op=%s resource=%s",
		live, shadow, al.Caller, al.Operation, al.Resource)
}

func (m *Manager) applyOverrideWithUndo(ov model.ShadowConfigOverride) (undo func() error, err error) {
	undo = func() error { return nil }

	switch ov.Category {
	case model.ShadowRuleCircuitBreaker:
		rule, e := m.storage.GetCircuitBreakerRule(ov.TargetKey)
		if e != nil || rule == nil {
			err = fmt.Errorf("circuit breaker rule not found: %s", ov.TargetKey)
			return
		}
		savedThreshold := rule.FailureThreshold
		savedWindow := rule.WindowSec
		savedCooldown := rule.CooldownSec

		newVal, e := strconv.Atoi(ov.NewValue)
		if e != nil {
			err = fmt.Errorf("invalid new value: %s", ov.NewValue)
			return
		}

		switch ov.Field {
		case "failure_threshold":
			rule.FailureThreshold = newVal
		case "window_sec":
			rule.WindowSec = newVal
		case "cooldown_sec":
			rule.CooldownSec = newVal
		default:
			err = fmt.Errorf("unsupported circuit_breaker field: %s", ov.Field)
			return
		}

		_, e = m.auditMgr.SetCircuitBreakerRule(ov.TargetKey, rule.WindowSec, rule.FailureThreshold, rule.CooldownSec)
		if e != nil {
			err = fmt.Errorf("set circuit breaker rule: %w", e)
			return
		}

		undo = func() error {
			_, ue := m.auditMgr.SetCircuitBreakerRule(ov.TargetKey, savedWindow, savedThreshold, savedCooldown)
			return ue
		}

	case model.ShadowRuleRateLimit:
		binding, e := m.storage.GetCallerBinding(ov.TargetKey)
		if e != nil || binding == nil {
			err = fmt.Errorf("caller binding not found: %s", ov.TargetKey)
			return
		}

		switch ov.Field {
		case "quota_limit":
			newVal, e := strconv.Atoi(ov.NewValue)
			if e != nil {
				err = fmt.Errorf("invalid new value: %s", ov.NewValue)
				return
			}
			savedQuota := binding.QuotaLimit

			e = m.auditMgr.AdjustQuota(ov.TargetKey, newVal)
			if e != nil {
				err = fmt.Errorf("adjust quota: %w", e)
				return
			}
			undo = func() error {
				return m.auditMgr.AdjustQuota(ov.TargetKey, savedQuota)
			}

		case "max_tokens":
			newVal, e := strconv.Atoi(ov.NewValue)
			if e != nil {
				err = fmt.Errorf("invalid new value: %s", ov.NewValue)
				return
			}
			policy, e := m.storage.GetPolicy(binding.PolicyName)
			if e != nil || policy == nil {
				err = fmt.Errorf("policy not found: %s", binding.PolicyName)
				return
			}
			savedMax := policy.MaxTokens

			policy.MaxTokens = newVal
			_, e = m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
			if e != nil {
				err = fmt.Errorf("update policy max_tokens: %w", e)
				return
			}
			undo = func() error {
				policy.MaxTokens = savedMax
				_, ue := m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
				return ue
			}

		case "window_sec":
			newVal, e := strconv.Atoi(ov.NewValue)
			if e != nil {
				err = fmt.Errorf("invalid new value: %s", ov.NewValue)
				return
			}
			policy, e := m.storage.GetPolicy(binding.PolicyName)
			if e != nil || policy == nil {
				err = fmt.Errorf("policy not found: %s", binding.PolicyName)
				return
			}
			savedWindow := policy.WindowSec

			policy.WindowSec = newVal
			_, e = m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
			if e != nil {
				err = fmt.Errorf("update policy window_sec: %w", e)
				return
			}
			undo = func() error {
				policy.WindowSec = savedWindow
				_, ue := m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
				return ue
			}

		default:
			err = fmt.Errorf("unsupported rate_limit field: %s", ov.Field)
		}

	case model.ShadowRuleReservation:
		policy, e := m.storage.GetPolicy(ov.TargetKey)
		if e != nil || policy == nil {
			err = fmt.Errorf("policy not found: %s (for reservation)", ov.TargetKey)
			return
		}

		switch ov.Field {
		case "max_tokens":
			newVal, e := strconv.Atoi(ov.NewValue)
			if e != nil {
				err = fmt.Errorf("invalid new value: %s", ov.NewValue)
				return
			}
			savedMax := policy.MaxTokens
			policy.MaxTokens = newVal

			_, e = m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
			if e != nil {
				err = fmt.Errorf("update reservation policy: %w", e)
				return
			}
			undo = func() error {
				policy.MaxTokens = savedMax
				_, ue := m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
				return ue
			}

		default:
			err = fmt.Errorf("unsupported reservation field: %s", ov.Field)
		}

	case model.ShadowRuleLockDependency:
		switch ov.Field {
		case "token_cost", "lock_name", "rate_policy":
			node, e := m.storage.GetTopoNode(ov.TargetKey)
			if e != nil || node == nil {
				err = fmt.Errorf("topology node not found: %s", ov.TargetKey)
				return
			}
			savedTokenCost := node.TokenCost
			savedLockName := node.LockName
			savedRatePolicy := node.RatePolicy

			switch ov.Field {
			case "token_cost":
				newVal, e := strconv.Atoi(ov.NewValue)
				if e != nil {
					err = fmt.Errorf("invalid new value: %s", ov.NewValue)
					return
				}
				node.TokenCost = newVal
			case "lock_name":
				node.LockName = ov.NewValue
			case "rate_policy":
				node.RatePolicy = ov.NewValue
			}

			if e = m.storage.UpdateTopoNode(node); e != nil {
				err = fmt.Errorf("update topology node: %w", e)
				return
			}
			undo = func() error {
				node.TokenCost = savedTokenCost
				node.LockName = savedLockName
				node.RatePolicy = savedRatePolicy
				return m.storage.UpdateTopoNode(node)
			}

		case "reentrant":
			lockObj, e := m.storage.GetLock(ov.TargetKey)
			if e != nil || lockObj == nil {
				err = fmt.Errorf("lock not found: %s", ov.TargetKey)
				return
			}
			savedReentrant := lockObj.Reentrant

			lockObj.Reentrant = (ov.NewValue == "true")
			lockObj.UpdatedAt = time.Now()
			if e = m.storage.UpsertLock(lockObj); e != nil {
				err = fmt.Errorf("update lock reentrant: %w", e)
				return
			}
			undo = func() error {
				lockObj.Reentrant = savedReentrant
				lockObj.UpdatedAt = time.Now()
				return m.storage.UpsertLock(lockObj)
			}

		default:
			err = fmt.Errorf("unsupported lock_dependency field: %s", ov.Field)
		}

	default:
		err = fmt.Errorf("unsupported category: %s", ov.Category)
	}

	return
}

func (m *Manager) RecordMirrorDiff(planID int64, caller, op, resource string, liveDecision, shadowDecision model.ShadowDecision, category model.ShadowRuleCategory, detail string) error {
	diff := &model.ShadowDiffRecord{
		PlanID:          planID,
		RequestCaller:   caller,
		RequestOp:       op,
		RequestResource: resource,
		LiveDecision:    liveDecision,
		ShadowDecision:  shadowDecision,
		RuleCategory:    category,
		Detail:          detail,
		CreatedAt:       time.Now(),
	}
	return m.storage.CreateShadowDiffRecord(diff)
}

func (m *Manager) GetRunningMirrorPlans() []*model.ShadowPlan {
	m.mu.Lock()
	defer m.mu.Unlock()
	var running []*model.ShadowPlan
	for _, p := range m.plans {
		if p.Status == model.ShadowPlanStatusRunning && p.Mode == "mirror" {
			running = append(running, p)
		}
	}
	return running
}

func (m *Manager) mirrorLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.checkMirrorExpiry()
		}
	}
}

func (m *Manager) checkMirrorExpiry() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, p := range m.plans {
		if p.Status == model.ShadowPlanStatusRunning && p.Mode == "mirror" {
			if !p.MirrorUntil.IsZero() && now.After(p.MirrorUntil) {
				log.Printf("[shadow] mirror plan expired: id=%d name=%s", p.ID, p.Name)
				if err := m.storage.UpdateShadowPlanStatus(p.ID, model.ShadowPlanStatusCompleted, now); err != nil {
					log.Printf("[shadow] failed to update plan status: %v", err)
				}
				p.Status = model.ShadowPlanStatusCompleted
				p.UpdatedAt = now
			}
		}
	}
}

func (m *Manager) EvaluateShadow(caller, op, resource string, liveSuccess bool, liveFailReason string) {
	runningMirror := m.GetRunningMirrorPlans()
	if len(runningMirror) == 0 {
		return
	}

	al := model.AuditLog{
		Caller:     caller,
		Operation:  model.AuditOperationType(op),
		Resource:   resource,
		Success:    liveSuccess,
		FailReason: liveFailReason,
	}
	liveDecision := m.classifyLiveDecision(al)

	for _, plan := range runningMirror {
		overrides, err := m.storage.ListShadowConfigOverrides(plan.ID)
		if err != nil {
			continue
		}

		ctx := &evalContext{
			failures:    make(map[string]int),
			admits:      make(map[string]int),
			rateLimited: make(map[string]int),
		}
		ctx.admits[caller] = 1
		shadowDecision := m.classifyShadowDecisionWithContext(al, overrides, ctx)

		if liveDecision != shadowDecision {
			cat := m.classifyRuleCategory(al, overrides)
			detail := m.buildDiffDetail(al, liveDecision, shadowDecision, overrides)
			if err := m.RecordMirrorDiff(plan.ID, caller, op, resource, liveDecision, shadowDecision, cat, detail); err != nil {
				log.Printf("[shadow] failed to record mirror diff: %v", err)
			}
		}
	}
}
