package main

import (
	"fmt"
	"log"
	"os"
	"task106/internal/api"
	"task106/internal/audit"
	"task106/internal/controlplane"
	"task106/internal/debt"
	"task106/internal/handover"
	"task106/internal/heartbeat"
	"task106/internal/heatmap"
	"task106/internal/lock"
	"task106/internal/lockbudget"
	"task106/internal/model"
	"task106/internal/orchestration"
	"task106/internal/ratealert"
	"task106/internal/ratelimit"
	"task106/internal/reputation"
	"task106/internal/shadow"
	"task106/internal/storage"
	"task106/internal/topology"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--smoke-test" {
		if err := runSmokeTest(); err != nil {
			log.Fatalf("smoke test failed: %v", err)
		}
		return
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/locks.db"
	}

	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	s, err := storage.New(dbPath)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer s.Close()

	mgr := lock.NewManager(s)
	coordMgr := controlplane.NewManager(s)
	if err := coordMgr.Start(); err != nil {
		log.Fatalf("start coordination control plane: %v", err)
	}
	mgr.SetAdmissionGuard(coordMgr)
	mgr.SetFencingIssuer(coordMgr)

	budgetMgr := lockbudget.NewManager(s)
	if err := budgetMgr.Start(); err != nil {
		log.Fatalf("start budget manager: %v", err)
	}
	defer budgetMgr.Stop()
	mgr.SetBudgetManager(budgetMgr)

	rateAlertMgr := ratealert.NewManager(s)
	if err := rateAlertMgr.Start(); err != nil {
		log.Fatalf("start rate alert manager: %v", err)
	}
	defer rateAlertMgr.Stop()
	budgetMgr.SetRateAlertManager(rateAlertMgr)

	reputationMgr := reputation.NewManager(s)
	if err := reputationMgr.Start(); err != nil {
		log.Fatalf("start reputation manager: %v", err)
	}
	defer reputationMgr.Stop()
	mgr.SetReputationChecker(reputationMgr)
	budgetMgr.SetReputationChecker(reputationMgr)

	heatmapMgr := heatmap.NewManager(s, mgr)
	mgr.SetHeatmap(heatmapMgr)
	mgr.SetHeatmapCooldownManager(heatmapMgr)

	rlMgr := ratelimit.NewManager(s)
	if err := rlMgr.Start(); err != nil {
		log.Fatalf("start rate limit manager: %v", err)
	}
	defer rlMgr.Stop()

	orchMgr := orchestration.NewManager(s, mgr, rlMgr)
	if err := orchMgr.Start(); err != nil {
		log.Fatalf("start orchestration manager: %v", err)
	}
	defer orchMgr.Stop()

	auditMgr := audit.NewManager(s, mgr, rlMgr)
	if err := auditMgr.Start(); err != nil {
		log.Fatalf("start audit manager: %v", err)
	}
	defer auditMgr.Stop()

	topoMgr := topology.NewManager(s, mgr, rlMgr)
	if err := topoMgr.Start(); err != nil {
		log.Fatalf("start topology manager: %v", err)
	}
	defer topoMgr.Stop()

	shadowMgr := shadow.NewManager(s, mgr, rlMgr, auditMgr)
	if err := shadowMgr.Start(); err != nil {
		log.Fatalf("start shadow manager: %v", err)
	}
	defer shadowMgr.Stop()

	auditMgr.SetShadowEvaluator(shadowMgr.EvaluateShadow)

	debtMgr := debt.NewManager(s, rlMgr)
	if err := debtMgr.Start(); err != nil {
		log.Fatalf("start debt manager: %v", err)
	}
	defer debtMgr.Stop()

	handoverMgr := handover.NewManager(s, mgr, rlMgr, orchMgr, topoMgr, debtMgr)
	if err := handoverMgr.Start(); err != nil {
		log.Fatalf("start handover manager: %v", err)
	}
	defer handoverMgr.Stop()

	if err := heatmapMgr.Start(); err != nil {
		log.Fatalf("start heatmap manager: %v", err)
	}
	defer heatmapMgr.Stop()

	if err := mgr.Start(); err != nil {
		log.Fatalf("start lock manager: %v", err)
	}
	defer mgr.Stop()

	heartbeatMgr := heartbeat.NewManager(s, mgr, rlMgr, orchMgr)
	if err := heartbeatMgr.Start(); err != nil {
		log.Fatalf("start heartbeat manager: %v", err)
	}
	defer heartbeatMgr.Stop()

	if err := seedDemoData(mgr, rlMgr); err != nil {
		log.Printf("seed demo data: %v", err)
	}

	if err := seedOrchDemoData(mgr, rlMgr, orchMgr); err != nil {
		log.Printf("seed orchestration demo data: %v", err)
	}

	if err := seedAuditDemoData(auditMgr, s); err != nil {
		log.Printf("seed audit demo data: %v", err)
	}

	if err := seedTopologyDemoData(topoMgr, rlMgr, mgr); err != nil {
		log.Printf("seed topology demo data: %v", err)
	}

	if err := seedShadowDemoData(shadowMgr, s); err != nil {
		log.Printf("seed shadow demo data: %v", err)
	}

	if err := seedDebtDemoData(debtMgr, s); err != nil {
		log.Printf("seed debt demo data: %v", err)
	}

	if err := seedHandoverDemoData(handoverMgr, mgr, rlMgr, orchMgr, s); err != nil {
		log.Printf("seed handover demo data: %v", err)
	}

	if err := seedHeartbeatDemoData(heartbeatMgr, s, mgr, rlMgr); err != nil {
		log.Printf("seed heartbeat demo data: %v", err)
	}

	if err := seedHeatmapDemoData(heatmapMgr, s, mgr, rlMgr); err != nil {
		log.Printf("seed heatmap demo data: %v", err)
	}

	if err := seedRateAlertDemoData(rateAlertMgr, s, budgetMgr); err != nil {
		log.Printf("seed rate alert demo data: %v", err)
	}

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	handler := api.NewHandler(mgr, rlMgr, orchMgr, auditMgr, topoMgr, shadowMgr, debtMgr, handoverMgr, heartbeatMgr, heatmapMgr, budgetMgr, rateAlertMgr, reputationMgr)
	handler.SetControlPlane(coordMgr)
	handler.RegisterRoutes(r)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	log.Printf("server starting on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func seedDemoData(lockMgr *lock.Manager, rlMgr *ratelimit.Manager) error {
	locks, err := lockMgr.ListAllLocks()
	if err != nil {
		return err
	}
	if len(locks) > 0 {
		log.Println("[demo] lock data already exists, skipping seed")
	} else {
		log.Println("[demo] seeding lock demo data...")

		if _, err := lockMgr.AcquireLock("resource-a", "alice", 120, true); err != nil {
			return err
		}
		log.Println("[demo] acquired lock resource-a by alice (reentrant, 120s)")

		if _, err := lockMgr.AcquireLock("resource-b", "bob", 60, false); err != nil {
			return err
		}
		log.Println("[demo] acquired lock resource-b by bob (non-reentrant, 60s)")

		if result, err := lockMgr.AcquireLock("resource-a", "charlie", 30, false); err != nil {
			return err
		} else if result.Queued {
			log.Println("[demo] charlie queued for resource-a")
		}
	}

	policies, err := rlMgr.ListPolicies()
	if err != nil {
		return err
	}
	if len(policies) > 0 {
		log.Println("[demo] rate limit data already exists, skipping seed")
		return nil
	}

	log.Println("[demo] seeding rate limit demo data...")

	if _, err := rlMgr.CreatePolicy("token-bucket-policy", model.AlgoTokenBucket, 0, 100, 10.0, "per_second"); err != nil {
		return err
	}
	log.Println("[demo] created token-bucket-policy: max=100, refill=10/s")

	if _, err := rlMgr.CreatePolicy("sliding-window-policy", model.AlgoSlidingWindow, 60, 50, 0, ""); err != nil {
		return err
	}
	log.Println("[demo] created sliding-window-policy: window=60s, max=50")

	if _, err := rlMgr.CreatePolicy("fixed-window-policy", model.AlgoFixedWindow, 30, 30, 0, ""); err != nil {
		return err
	}
	log.Println("[demo] created fixed-window-policy: window=30s, max=30")

	if _, err := rlMgr.BindCaller("service-alpha", "token-bucket-policy", 100); err != nil {
		return err
	}
	log.Println("[demo] bound service-alpha to token-bucket-policy (quota=100)")

	if _, err := rlMgr.BindCaller("service-beta", "sliding-window-policy", 50); err != nil {
		return err
	}
	log.Println("[demo] bound service-beta to sliding-window-policy (quota=50)")

	if _, err := rlMgr.BindCaller("service-gamma", "fixed-window-policy", 30); err != nil {
		return err
	}
	log.Println("[demo] bound service-gamma to fixed-window-policy (quota=30)")

	if result, err := rlMgr.RequestTokens("service-alpha", 5, false, 0); err == nil && result.Allowed {
		log.Printf("[demo] service-alpha requested 5 tokens, granted, remaining=%d", result.Remaining)
	}

	if result, err := rlMgr.RequestTokens("service-beta", 3, false, 0); err == nil && result.Allowed {
		log.Printf("[demo] service-beta requested 3 tokens, granted, remaining=%d", result.Remaining)
	}

	log.Println("[demo] rate limit demo data seeded successfully")
	log.Println("[demo] tip: watch service-alpha's tokens refilling via GET /api/v1/ratelimit/callers/service-alpha")
	return nil
}

func seedOrchDemoData(lockMgr *lock.Manager, rlMgr *ratelimit.Manager, orchMgr *orchestration.Manager) error {
	existingTxs, err := orchMgr.ListTxs("")
	if err != nil {
		return err
	}
	if len(existingTxs) > 0 {
		log.Println("[demo-orch] orchestration data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-orch] seeding orchestration demo data...")

	_, _ = lockMgr.ReleaseLock("resource-a", "alice")
	_, _ = lockMgr.ReleaseLock("resource-a", "charlie")
	_, _ = lockMgr.ReleaseLock("resource-a", "bob")
	log.Println("[demo-orch] ensured resource-a is free for orchestration demo")

	locks := []model.TxLockSpec{
		{LockName: "resource-a", LeaseSec: 300},
	}
	tokens := []model.TxTokenSpec{
		{CallerID: "service-alpha", Tokens: 10},
	}

	tx, err := orchMgr.CreateTx("demo-orch-holder", 300, locks, tokens)
	if err != nil {
		return err
	}

	if tx.Status == model.TxStatusCommitted {
		log.Printf("[demo-orch] created demo transaction: tx=%s status=%s", tx.ID, tx.Status)
		log.Printf("[demo-orch]   - holds lock: resource-a (lease 300s)")
		log.Printf("[demo-orch]   - consumed tokens: service-alpha x 10")
		log.Printf("[demo-orch]   - timeout: 300s")
		log.Println("[demo-orch] tip: query via GET /api/v1/orchestration/tx/" + tx.ID)
	} else {
		log.Printf("[demo-orch] demo tx not committed: status=%s reason=%s", tx.Status, tx.FailReason)
	}

	return nil
}

func seedAuditDemoData(auditMgr *audit.Manager, s *storage.Storage) error {
	existingRules, err := s.ListCircuitBreakerRules()
	if err != nil {
		return err
	}

	hasGammaRule := false
	for _, r := range existingRules {
		if r.CallerID == "service-gamma" {
			hasGammaRule = true
			break
		}
	}

	if !hasGammaRule {
		log.Println("[demo-audit] seeding audit & circuit breaker demo data...")

		if _, err := auditMgr.SetCircuitBreakerRule("service-gamma", 10, 3, 60); err != nil {
			return err
		}
		log.Println("[demo-audit] created circuit breaker rule for service-gamma: 10s window, 3 failures threshold, 60s cooldown")

		now := time.Now()
		twoHoursAgo := now.Add(-2 * time.Hour)
		oneHourAgo := now.Add(-1 * time.Hour)

		history := &model.CircuitBreakerHistory{
			CallerID:      "service-gamma",
			State:         "open",
			TriggeredAt:   twoHoursAgo,
			RecoveredAt:   oneHourAgo,
			TriggerReason: "failure count 3 reached threshold 3 in 10 seconds",
			RecoverReason: "cooldown_expired",
		}
		if err := s.AddCircuitBreakerHistory(history); err != nil {
			return err
		}
		log.Println("[demo-audit] added historical circuit breaker record for service-gamma (triggered 2h ago, recovered 1h ago)")

		for i := 0; i < 2; i++ {
			logEntry := &model.AuditLog{
				Timestamp:  twoHoursAgo.Add(time.Duration(i) * time.Second),
				Caller:     "service-gamma",
				Operation:  model.AuditOpRequestTokens,
				Resource:   "service-gamma",
				Success:    false,
				FailReason: "rate limited",
			}
			_ = s.AddAuditLog(logEntry)
		}
		log.Println("[demo-audit] added sample audit failure logs for service-gamma")
		log.Println("[demo-audit] tip: check rules via GET /api/v1/audit/circuit-breaker/rules")
		log.Println("[demo-audit] tip: check history via GET /api/v1/audit/circuit-breaker/history/service-gamma")
	} else {
		log.Println("[demo-audit] audit demo data already exists, skipping seed")
	}

	return nil
}

func seedTopologyDemoData(topoMgr *topology.Manager, rlMgr *ratelimit.Manager, lockMgr *lock.Manager) error {
	existingNodes, err := topoMgr.ListNodes()
	if err != nil {
		return err
	}

	hasCluster := false
	for _, n := range existingNodes {
		if n.Name == "cluster" {
			hasCluster = true
			break
		}
	}

	if hasCluster {
		log.Println("[demo-topology] topology data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-topology] seeding topology demo data...")

	_, err = topoMgr.RegisterNode("cluster", "lock-cluster", "token-bucket-policy", 1)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: cluster (lock=lock-cluster, policy=token-bucket-policy, cost=1)")

	_, err = topoMgr.RegisterNode("namespace", "lock-namespace", "", 0)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: namespace (lock=lock-namespace, no policy)")

	_, err = topoMgr.RegisterNode("pod", "lock-pod", "", 0)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: pod (lock=lock-pod, no policy)")

	_, err = topoMgr.DeclareEdge("namespace", "pod")
	if err != nil {
		return err
	}
	log.Println("[demo-topology] declared edge: namespace -> pod")

	_, err = topoMgr.DeclareEdge("cluster", "namespace")
	if err != nil {
		return err
	}
	log.Println("[demo-topology] declared edge: cluster -> namespace")

	log.Println("[demo-topology] dependency chain: cluster -> namespace -> pod")
	log.Println("[demo-topology] acquiring pod to demonstrate cascade acquire...")

	result, err := topoMgr.CascadeAcquire("pod", "demo-holder", 300, false)
	if err != nil {
		log.Printf("[demo-topology] cascade acquire error: %v", err)
	} else if result.Success {
		log.Printf("[demo-topology] cascade acquire success! acquired nodes: %v", result.Acquired)
		log.Printf("[demo-topology]  - automatically acquired cluster (with 1 token consumed)")
		log.Printf("[demo-topology]  - automatically acquired namespace")
		log.Printf("[demo-topology]  - acquired target: pod")
		log.Printf("[demo-topology]  - total steps: %d, duration: %dms", len(result.Steps), result.DurationMs)

		_, _ = topoMgr.CascadeRelease("cluster", "demo-holder", true)
		log.Println("[demo-topology] force released all nodes via cluster (demo cleanup)")
	} else {
		log.Printf("[demo-topology] cascade acquire failed: %s (rolled_back=%v)", result.Message, result.RolledBack)
	}

	log.Println("[demo-topology] tip: view graph via GET /api/v1/topology/graph")
	log.Println("[demo-topology] tip: acquire pod via POST /api/v1/topology/acquire")
	log.Println("[demo-topology]   body: {\"target_node\":\"pod\",\"holder\":\"user1\",\"lease_sec\":60,\"reentrant\":false}")
	log.Println("[demo-topology] tip: check ancestors via GET /api/v1/topology/nodes/pod/ancestors")
	log.Println("[demo-topology] tip: view holder tree via GET /api/v1/topology/holders/user1/tree")

	return nil
}

func seedShadowDemoData(shadowMgr *shadow.Manager, s *storage.Storage) error {
	existingPlans, err := shadowMgr.ListPlans()
	if err != nil {
		return err
	}
	if len(existingPlans) > 0 {
		log.Println("[demo-shadow] shadow data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-shadow] seeding shadow evaluation demo data...")

	plan, err := shadowMgr.CreatePlan(
		"demo-stricter-gamma",
		"Stricter circuit breaker for service-gamma and lower quota for resource-a related callers",
		"replay",
		0,
	)
	if err != nil {
		return fmt.Errorf("create shadow plan: %w", err)
	}
	log.Printf("[demo-shadow] created shadow plan: id=%d name=%s", plan.ID, plan.Name)

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleCircuitBreaker, "service-gamma", "failure_threshold", "2")
	if err != nil {
		return fmt.Errorf("add cb override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma circuit breaker failure_threshold 3->2")

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleCircuitBreaker, "service-gamma", "window_sec", "5")
	if err != nil {
		return fmt.Errorf("add cb window override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma circuit breaker window_sec 10->5")

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleRateLimit, "service-gamma", "quota_limit", "15")
	if err != nil {
		return fmt.Errorf("add rl override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma rate limit quota_limit 30->15")

	now := time.Now()
	for i := 0; i < 4; i++ {
		logEntry := &model.AuditLog{
			Timestamp:  now.Add(-time.Duration(10-i) * time.Second),
			Caller:     "service-gamma",
			Operation:  model.AuditOpRequestTokens,
			Resource:   "service-gamma",
			Success:    false,
			FailReason: "rate limited",
		}
		if err := s.AddAuditLog(logEntry); err != nil {
			return err
		}
	}

	logEntry := &model.AuditLog{
		Timestamp:  now.Add(-3 * time.Second),
		Caller:     "service-gamma",
		Operation:  model.AuditOpRequestTokens,
		Resource:   "service-gamma",
		Success:    true,
		FailReason: "",
	}
	if err := s.AddAuditLog(logEntry); err != nil {
		return err
	}

	logEntry2 := &model.AuditLog{
		Timestamp:  now.Add(-2 * time.Second),
		Caller:     "service-alpha",
		Operation:  model.AuditOpRequestTokens,
		Resource:   "service-alpha",
		Success:    true,
		FailReason: "",
	}
	if err := s.AddAuditLog(logEntry2); err != nil {
		return err
	}

	if err := shadowMgr.StartPlan(plan.ID); err != nil {
		return fmt.Errorf("start shadow plan: %w", err)
	}
	log.Printf("[demo-shadow] started shadow plan: id=%d - replay running", plan.ID)

	log.Println("[demo-shadow] tip: view plans via GET /api/v1/shadow/plans")
	log.Println("[demo-shadow] tip: view diffs via GET /api/v1/shadow/plans/1/diffs")
	log.Println("[demo-shadow] tip: view stats via GET /api/v1/shadow/plans/1/stats")
	log.Println("[demo-shadow] tip: apply to production via POST /api/v1/shadow/plans/1/apply")

	return nil
}

func seedDebtDemoData(debtMgr *debt.Manager, s *storage.Storage) error {
	existingRules, err := s.ListLiquidationRules()
	if err != nil {
		return err
	}
	if len(existingRules) > 0 {
		log.Println("[demo-debt] debt data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-debt] seeding debt ledger & liquidation demo data...")

	_, err = debtMgr.SetLiquidationRule("service-alpha", 5, 2, "reject", "all", 3, 5)
	if err != nil {
		return err
	}
	log.Println("[demo-debt] created liquidation rule for service-alpha: grace=5s, threshold=2, reject/all")

	_, err = debtMgr.SetLiquidationRule("service-beta", 10, 3, "degrade", "token", 3, 5)
	if err != nil {
		return err
	}
	log.Println("[demo-debt] created liquidation rule for service-beta: grace=10s, threshold=3, degrade/token")

	now := time.Now()

	r1, err := debtMgr.RecordBorrow("service-alpha", "service-beta", 20, "quota", "token-bucket-policy", 5)
	if err != nil {
		return err
	}
	log.Printf("[demo-debt] borrow: service-alpha borrowed 20 from service-beta (grace=5s, debt_id=%d)", r1.ID)

	r2, err := debtMgr.RecordBorrow("service-beta", "service-alpha", 10, "quota", "sliding-window-policy", 10)
	if err != nil {
		return err
	}
	log.Printf("[demo-debt] borrow: service-beta borrowed 10 from service-alpha (grace=10s, debt_id=%d)", r2.ID)

	r3, err := debtMgr.RecordBorrow("service-alpha", "system", 5, "lock", "resource-a", 5)
	if err != nil {
		return err
	}
	log.Printf("[demo-debt] borrow: service-alpha borrowed 5 lock tokens from system (grace=5s, debt_id=%d)", r3.ID)

	pastDue := now.Add(-10 * time.Second)
	r4 := &model.DebtRecord{
		Debtor:          "service-alpha",
		Creditor:        "service-beta",
		Amount:          15,
		ResourceType:    "quota",
		ResourceKey:     "token-bucket-policy",
		Status:          model.DebtStatusActive,
		DueAt:           pastDue,
		CollectAttempts: 1,
		LastCollectAt:   now.Add(-2 * time.Second),
		CreatedAt:       now.Add(-20 * time.Second),
		UpdatedAt:       now,
	}
	if err := s.CreateDebtRecord(r4); err != nil {
		return err
	}
	evt1 := &model.DebtLedgerEvent{
		DebtID:       r4.ID,
		Debtor:       "service-alpha",
		Creditor:     "service-beta",
		EventType:    model.DebtEventBorrow,
		Amount:       15,
		ResourceType: "quota",
		ResourceKey:  "token-bucket-policy",
		Detail:       fmt.Sprintf("borrowed 15 from service-beta, due at %s (seeded overdue)", pastDue.Format(time.RFC3339)),
		CreatedAt:    now.Add(-20 * time.Second),
	}
	_ = s.CreateDebtLedgerEvent(evt1)
	log.Printf("[demo-debt] seeded overdue debt: service-alpha owes service-beta 15, debt_id=%d (due in past)", r4.ID)

	r5 := &model.DebtRecord{
		Debtor:          "service-alpha",
		Creditor:        "system",
		Amount:          8,
		ResourceType:    "reservation",
		ResourceKey:     "token-bucket-policy",
		Status:          model.DebtStatusActive,
		DueAt:           now.Add(-5 * time.Second),
		CollectAttempts: 1,
		LastCollectAt:   now.Add(-1 * time.Second),
		CreatedAt:       now.Add(-15 * time.Second),
		UpdatedAt:       now,
	}
	if err := s.CreateDebtRecord(r5); err != nil {
		return err
	}
	evt2 := &model.DebtLedgerEvent{
		DebtID:       r5.ID,
		Debtor:       "service-alpha",
		Creditor:     "system",
		EventType:    model.DebtEventReservExpir,
		Amount:       8,
		ResourceType: "reservation",
		ResourceKey:  "token-bucket-policy",
		Detail:       "reservation expired: policy=token-bucket-policy tokens=8 (seeded overdue)",
		CreatedAt:    now.Add(-15 * time.Second),
	}
	_ = s.CreateDebtLedgerEvent(evt2)
	log.Printf("[demo-debt] seeded overdue reservation debt: service-alpha owes system 8, debt_id=%d", r5.ID)

	r6 := &model.DebtRecord{
		Debtor:       "service-beta",
		Creditor:     "service-alpha",
		Amount:       10,
		ResourceType: "quota",
		ResourceKey:  "sliding-window-policy",
		Status:       model.DebtStatusCollected,
		DueAt:        now.Add(-30 * time.Second),
		CollectedAt:  now.Add(-25 * time.Second),
		CreatedAt:    now.Add(-40 * time.Second),
		UpdatedAt:    now.Add(-25 * time.Second),
	}
	if err := s.CreateDebtRecord(r6); err != nil {
		return err
	}
	evt3 := &model.DebtLedgerEvent{
		DebtID:       r6.ID,
		Debtor:       "service-beta",
		Creditor:     "service-alpha",
		EventType:    model.DebtEventBorrow,
		Amount:       10,
		ResourceType: "quota",
		ResourceKey:  "sliding-window-policy",
		Detail:       "borrowed 10 from service-alpha (seeded collected)",
		CreatedAt:    now.Add(-40 * time.Second),
	}
	_ = s.CreateDebtLedgerEvent(evt3)
	evt4 := &model.DebtLedgerEvent{
		DebtID:       r6.ID,
		Debtor:       "service-beta",
		Creditor:     "service-alpha",
		EventType:    model.DebtEventCollect,
		Amount:       10,
		ResourceType: "quota",
		ResourceKey:  "sliding-window-policy",
		Detail:       "auto-collect success on attempt 1 (seeded)",
		CreatedAt:    now.Add(-25 * time.Second),
	}
	_ = s.CreateDebtLedgerEvent(evt4)
	audit1 := &model.LiquidationAuditEntry{
		DebtID:    r6.ID,
		Debtor:    "service-beta",
		Creditor:  "service-alpha",
		Action:    "collect",
		Amount:    10,
		Success:   true,
		Detail:    "auto-collect success on attempt 1 (seeded)",
		CreatedAt: now.Add(-25 * time.Second),
	}
	_ = s.AddLiquidationAudit(audit1)
	log.Printf("[demo-debt] seeded collected debt: service-beta paid back service-alpha 10 (auto-liquidation success), debt_id=%d", r6.ID)

	log.Println("[demo-debt] demo data seeded successfully")
	log.Println("[demo-debt] tip: view all debt records via GET /api/v1/debt/records")
	log.Println("[demo-debt] tip: view service-alpha debt summary via GET /api/v1/debt/callers/service-alpha/summary")
	log.Println("[demo-debt] tip: check restriction via GET /api/v1/debt/callers/service-alpha/check")
	log.Println("[demo-debt] tip: view liquidation rules via GET /api/v1/debt/liquidation-rules")
	log.Println("[demo-debt] tip: view ledger events via GET /api/v1/debt/ledger-events")
	log.Println("[demo-debt] tip: view audit log via GET /api/v1/debt/audit")
	log.Println("[demo-debt] note: overdue debts will be auto-processed within seconds by the liquidation loop")

	return nil
}

func seedHandoverDemoData(hm *handover.Manager, lockMgr *lock.Manager, rlMgr *ratelimit.Manager, orchMgr *orchestration.Manager, s *storage.Storage) error {
	existing, err := hm.ListHandovers("", "", "")
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		log.Println("[demo-handover] handover data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-handover] seeding handover demo data...")

	fromCaller := "alice"
	toCaller := "bob"
	altCaller := "charlie"

	if _, err := lockMgr.AcquireLock("handover-lock-demo-1", fromCaller, 3600, false); err != nil {
		return fmt.Errorf("acquire demo lock 1: %w", err)
	}
	log.Println("[demo-handover] alice acquired lock: handover-lock-demo-1")

	if _, err := lockMgr.AcquireLock("handover-lock-demo-2", fromCaller, 3600, true); err != nil {
		return fmt.Errorf("acquire demo lock 2: %w", err)
	}
	log.Println("[demo-handover] alice acquired lock: handover-lock-demo-2 (reentrant)")

	if _, err := lockMgr.AcquireLock("handover-lock-demo-3", toCaller, 3600, false); err != nil {
		return fmt.Errorf("acquire demo lock 3: %w", err)
	}
	log.Println("[demo-handover] bob acquired lock: handover-lock-demo-3")

	if _, err := lockMgr.AcquireLock("handover-lock-demo-4", fromCaller, 3600, false); err != nil {
		return fmt.Errorf("acquire demo lock 4: %w", err)
	}
	log.Println("[demo-handover] alice acquired lock: handover-lock-demo-4 (to be transferred to charlie)")

	if _, err := lockMgr.AcquireLock("handover-lock-demo-5", toCaller, 3600, false); err != nil {
		return fmt.Errorf("acquire demo lock 5: %w", err)
	}
	log.Println("[demo-handover] bob acquired lock: handover-lock-demo-5 (for timeout cancel demo)")

	{
		req := &model.CreateHandoverRequest{
			FromCaller:        fromCaller,
			ToCaller:          toCaller,
			Initiator:         "ops-admin",
			Description:       "机房切流交接：alice -> bob（待接收）",
			NeedConfirm:       true,
			ConfirmTimeoutSec: 3600,
			LockNames:         []string{"handover-lock-demo-1", "handover-lock-demo-2"},
		}
		created, err := hm.CreateHandover(req)
		if err != nil {
			return fmt.Errorf("create demo handover 1: %w", err)
		}
		log.Printf("[demo-handover] created demo handover #%d: %s -> %s, %d resources", created.ID, fromCaller, toCaller, len(created.Resources))

		if _, err := hm.PreCheck(created.ID); err != nil {
			log.Printf("[demo-handover] precheck warning: %v", err)
		}
		log.Printf("[demo-handover] handover #%d prechecked", created.ID)

		if _, err := hm.Initiate(created.ID, "ops-admin"); err != nil {
			log.Printf("[demo-handover] initiate warning: %v", err)
		}
		log.Printf("[demo-handover] handover #%d initiated (pending_receive, timeout 3600s)", created.ID)
	}

	{
		req := &model.CreateHandoverRequest{
			FromCaller:        fromCaller,
			ToCaller:          altCaller,
			Initiator:         "ops-admin",
			Description:       "已完成的成功交接演示：alice -> charlie",
			NeedConfirm:       false,
			ConfirmTimeoutSec: 3600,
			LockNames:         []string{"handover-lock-demo-4"},
		}
		created, err := hm.CreateHandover(req)
		if err != nil {
			return fmt.Errorf("create demo handover 2: %w", err)
		}
		log.Printf("[demo-handover] created demo handover #%d (completed): %s -> %s", created.ID, fromCaller, altCaller)

		if _, err := hm.PreCheck(created.ID); err != nil {
			return fmt.Errorf("precheck handover 2: %w", err)
		}
		log.Printf("[demo-handover] handover #%d prechecked", created.ID)

		if _, err := hm.Initiate(created.ID, "ops-admin"); err != nil {
			return fmt.Errorf("initiate handover 2: %w", err)
		}

		afterLock, err := s.GetLock("handover-lock-demo-4")
		if err != nil {
			return fmt.Errorf("get lock after handover: %w", err)
		}
		if afterLock != nil && afterLock.Holder == altCaller && afterLock.Status == model.LockStatusHeld {
			log.Printf("[demo-handover] handover #%d completed successfully! lock handover-lock-demo-4 holder changed from %s to %s",
				created.ID, fromCaller, altCaller)
		} else {
			holder := "nil"
			status := "nil"
			if afterLock != nil {
				holder = afterLock.Holder
				status = string(afterLock.Status)
			}
			log.Printf("[demo-handover] WARNING: handover #%d completed but lock holder mismatch (holder=%s status=%s)",
				created.ID, holder, status)
		}
	}

	{
		req := &model.CreateHandoverRequest{
			FromCaller:        toCaller,
			ToCaller:          altCaller,
			Initiator:         "ops-admin",
			Description:       "超时撤销演示（deadline已过期，会被定时任务自动撤销）",
			NeedConfirm:       true,
			ConfirmTimeoutSec: 1,
			LockNames:         []string{"handover-lock-demo-5"},
		}
		created, err := hm.CreateHandover(req)
		if err != nil {
			return fmt.Errorf("create demo handover 3: %w", err)
		}
		log.Printf("[demo-handover] created demo handover #%d (for timeout cancel): %s -> %s, timeout=1s", created.ID, toCaller, altCaller)

		if _, err := hm.PreCheck(created.ID); err != nil {
			log.Printf("[demo-handover] precheck warning for #%d: %v", created.ID, err)
		}

		if _, err := hm.Initiate(created.ID, "ops-admin"); err != nil {
			log.Printf("[demo-handover] initiate warning for #%d: %v", created.ID, err)
		}

		time.Sleep(1200 * time.Millisecond)
		log.Printf("[demo-handover] slept 1.2s past deadline for handover #%d - will be auto-cancelled by timeout loop soon", created.ID)
	}

	log.Println("[demo-handover] demo data seeded successfully")
	log.Println("[demo-handover] tip: view all via GET /api/v1/handovers")
	log.Println("[demo-handover] tip: view pending (pending_receive) handover via GET /api/v1/handovers?status=pending_receive")
	log.Println("[demo-handover] tip: view completed handover via GET /api/v1/handovers?status=completed")
	log.Println("[demo-handover] tip: view cancelled handover via GET /api/v1/handovers?status=cancelled")
	log.Println("[demo-handover] tip: view alice's outgoing/incoming via GET /api/v1/handovers/callers/alice/summary")
	log.Println("[demo-handover] tip: view handover timeline via GET /api/v1/handovers/<id>/timeline")

	return nil
}

func seedHeartbeatDemoData(hbm *heartbeat.Manager, s *storage.Storage, lockMgr *lock.Manager, rlMgr *ratelimit.Manager) error {
	existing, err := s.ListHeartbeatRegistrations()
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		log.Println("[demo-heartbeat] heartbeat data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-heartbeat] seeding heartbeat demo data...")

	healthyCaller := "service-healthy"
	lostCaller := "service-lost"

	if _, err := lockMgr.AcquireLock("heartbeat-demo-lock-1", healthyCaller, 3600, false); err != nil {
		log.Printf("[demo-heartbeat] acquire lock for healthy caller warning: %v", err)
	} else {
		log.Printf("[demo-heartbeat] %s acquired lock: heartbeat-demo-lock-1", healthyCaller)
	}

	if _, err := lockMgr.AcquireLock("heartbeat-demo-lock-2", lostCaller, 3600, false); err != nil {
		log.Printf("[demo-heartbeat] acquire lock for lost caller warning: %v", err)
	} else {
		log.Printf("[demo-heartbeat] %s acquired lock: heartbeat-demo-lock-2", lostCaller)
	}

	if _, err := rlMgr.BindCaller(healthyCaller, "token-bucket-policy", 50); err != nil {
		log.Printf("[demo-heartbeat] bind healthy caller warning: %v", err)
	} else {
		log.Printf("[demo-heartbeat] bound %s to token-bucket-policy (quota=50)", healthyCaller)
	}

	if _, err := rlMgr.BindCaller(lostCaller, "sliding-window-policy", 30); err != nil {
		log.Printf("[demo-heartbeat] bind lost caller warning: %v", err)
	} else {
		log.Printf("[demo-heartbeat] bound %s to sliding-window-policy (quota=30)", lostCaller)
	}

	if result, err := rlMgr.RequestTokens(healthyCaller, 5, false, 0); err == nil && result.Allowed {
		log.Printf("[demo-heartbeat] %s requested 5 tokens, remaining=%d", healthyCaller, result.Remaining)
	}

	if result, err := rlMgr.RequestTokens(lostCaller, 3, false, 0); err == nil && result.Allowed {
		log.Printf("[demo-heartbeat] %s requested 3 tokens, remaining=%d", lostCaller, result.Remaining)
	}

	healthyReg, err := hbm.Register(healthyCaller, "", 5, 3, model.StrategyReleaseAll)
	if err != nil {
		return fmt.Errorf("register healthy caller: %w", err)
	}
	log.Printf("[demo-heartbeat] registered %s: interval=%ds, max_missed=%d, strategy=%s",
		healthyCaller, healthyReg.IntervalSec, healthyReg.MaxMissed, healthyReg.Strategy)

	lostReg, err := hbm.Register(lostCaller, "", 5, 3, model.StrategyReleaseAll)
	if err != nil {
		return fmt.Errorf("register lost caller: %w", err)
	}
	log.Printf("[demo-heartbeat] registered %s: interval=%ds, max_missed=%d, strategy=%s",
		lostCaller, lostReg.IntervalSec, lostReg.MaxMissed, lostReg.Strategy)

	now := time.Now()
	lostLastHeartbeat := now.Add(-30 * time.Second)
	lostNextExpected := now.Add(-25 * time.Second)

	lostReg.LastHeartbeatAt = lostLastHeartbeat
	lostReg.NextExpectedAt = lostNextExpected
	lostReg.MissedCount = 6
	lostReg.Status = model.HeartbeatStatusLost
	lostReg.LostAt = &lostNextExpected
	lostReg.UpdatedAt = now

	if err := s.UpdateHeartbeatRegistration(lostReg); err != nil {
		return fmt.Errorf("update lost caller status: %w", err)
	}

	lostEvent := &model.HeartbeatEvent{
		CallerID:   lostCaller,
		EventType:  "connection_lost",
		FromStatus: model.HeartbeatStatusHealthy,
		ToStatus:   model.HeartbeatStatusLost,
		Detail:     fmt.Sprintf("seeded demo: no heartbeat for 30s (allowed 15s = 5s * 3)"),
		CreatedAt:  lostNextExpected,
	}
	if err := s.AddHeartbeatEvent(lostEvent); err != nil {
		return fmt.Errorf("add lost event: %w", err)
	}

	{
		leases, err := s.ListActiveLeases()
		if err == nil {
			for _, lease := range leases {
				if lease.Holder == lostCaller {
					if _, err := lockMgr.ReleaseLock(lease.LockName, lostCaller); err == nil {
						log.Printf("[demo-heartbeat] actually released lock: %s (holder=%s)", lease.LockName, lostCaller)
					}
				}
			}
		}
		_, _ = lockMgr.CancelWaitForHolder(lostCaller)

		binding, err := s.GetCallerBinding(lostCaller)
		if err == nil && binding != nil && binding.UsedTokens > 0 {
			if err := rlMgr.ReturnTokens(lostCaller, binding.UsedTokens); err == nil {
				log.Printf("[demo-heartbeat] actually returned %d tokens for caller: %s", binding.UsedTokens, lostCaller)
			}
		}

		waitItems, err := s.ListWaitItemsByCaller(lostCaller)
		if err == nil {
			for _, item := range waitItems {
				_ = s.RemoveWaitItem(item.ID)
			}
		}
	}

	disposalEvent := &model.HeartbeatEvent{
		CallerID:   lostCaller,
		EventType:  "disposal_executed",
		FromStatus: model.HeartbeatStatusLost,
		ToStatus:   model.HeartbeatStatusLost,
		Detail:     "strategy=release_all: released locks, returned tokens, cancelled transactions",
		CreatedAt:  lostNextExpected.Add(1 * time.Second),
	}
	if err := s.AddHeartbeatEvent(disposalEvent); err != nil {
		return fmt.Errorf("add disposal event: %w", err)
	}

	log.Printf("[demo-heartbeat] set %s status to LOST (simulated 30s outage)", lostCaller)
	log.Printf("[demo-heartbeat] lost event recorded at %s", lostNextExpected.Format(time.RFC3339))

	log.Println("[demo-heartbeat] demo data seeded successfully:")
	log.Printf("[demo-heartbeat]   - %s: HEALTHY (interval=5s, max_missed=3)", healthyCaller)
	log.Printf("[demo-heartbeat]   - %s: LOST (30s outage, resources auto-released)", lostCaller)
	log.Println("[demo-heartbeat] tips:")
	log.Println("[demo-heartbeat]   GET /api/v1/heartbeat/statuses - view all caller statuses")
	log.Println("[demo-heartbeat]   GET /api/v1/heartbeat/report - view summary report")
	log.Println("[demo-heartbeat]   GET /api/v1/heartbeat/events - view all events")
	log.Println("[demo-heartbeat]   GET /api/v1/heartbeat/events/service-lost - view lost caller's events")
	log.Println("[demo-heartbeat]   POST /api/v1/heartbeat/report/service-healthy - report heartbeat")
	log.Println("[demo-heartbeat]   GET /api/v1/heartbeat/frozen - view frozen resources")

	return nil
}

func seedHeatmapDemoData(hm *heatmap.Manager, s *storage.Storage, lockMgr *lock.Manager, rlMgr *ratelimit.Manager) error {
	now := time.Now()
	bucketNow := now.Truncate(time.Minute)

	targetLocks := []string{"hot-spot-db", "warm-cache-redis", "cold-config-map"}
	existing, err := s.GetAggregatedLockHeatInWindow(bucketNow.Add(-30*time.Minute), bucketNow.Add(time.Minute))
	if err != nil {
		return err
	}
	existsCount := 0
	for _, e := range existing {
		for _, n := range targetLocks {
			if e.LockName == n {
				existsCount++
			}
		}
	}
	if existsCount == len(targetLocks) {
		log.Println("[demo-heatmap] heatmap target locks already exist, skipping seed")
		return nil
	}

	log.Println("[demo-heatmap] seeding heatmap demo data...")

	hotLocks := []struct {
		name       string
		levels     int
		reqPerMin  int64
		waitPerMin int64
		avgWaitMs  int64
		holder     string
	}{
		{"hot-spot-db", 15, 120, 80, 8500, "db-pool-1"},
		{"warm-cache-redis", 15, 50, 20, 2200, "cache-svc-2"},
		{"cold-config-map", 15, 5, 1, 300, "config-loader"},
	}

	for _, hl := range hotLocks {
		if _, err := lockMgr.AcquireLock(hl.name, hl.holder, 3600, true); err != nil {
			log.Printf("[demo-heatmap] acquire %s warning: %v", hl.name, err)
		} else {
			log.Printf("[demo-heatmap] acquired base lock: %s by %s", hl.name, hl.holder)
		}
	}

	for offset := 14; offset >= 0; offset-- {
		bucket := bucketNow.Add(-time.Duration(offset) * time.Minute)

		for _, hl := range hotLocks {
			reqCnt := hl.reqPerMin
			waitCnt := hl.waitPerMin
			avgWait := hl.avgWaitMs

			if offset >= 10 && hl.name == "hot-spot-db" {
				reqCnt = int64(float64(reqCnt) * 0.5)
				waitCnt = int64(float64(waitCnt) * 0.4)
			}
			if offset < 5 && hl.name == "hot-spot-db" {
				reqCnt = int64(float64(reqCnt) * 1.3)
				waitCnt = int64(float64(waitCnt) * 1.5)
				avgWait = int64(float64(avgWait) * 1.2)
			}

			totalWaitMs := waitCnt * avgWait
			maxWaitMs := int64(float64(avgWait) * 1.8)

			stat := &model.LockContentionMinuteStat{
				LockName:     hl.name,
				MinuteBucket: bucket,
				RequestCount: reqCnt,
				WaitCount:    waitCnt,
				TotalWaitMs:  totalWaitMs,
				MaxWaitMs:    maxWaitMs,
				CreatedAt:    bucket,
				UpdatedAt:    bucket.Add(59 * time.Second),
			}
			if err := s.UpsertLockContentionStat(stat); err != nil {
				return fmt.Errorf("upsert stat for %s at %v: %w", hl.name, bucket, err)
			}
		}
	}

	if _, err := lockMgr.AcquireLock("hot-spot-db", "db-pool-2", 60, false); err != nil {
		log.Printf("[demo-heatmap] queued db-pool-2 warning: %v", err)
	}
	if _, err := lockMgr.AcquireLock("hot-spot-db", "db-pool-3", 60, false); err != nil {
		log.Printf("[demo-heatmap] queued db-pool-3 warning: %v", err)
	}
	if _, err := lockMgr.AcquireLock("hot-spot-db", "db-pool-4", 60, false); err != nil {
		log.Printf("[demo-heatmap] queued db-pool-4 warning: %v", err)
	}
	if _, err := lockMgr.AcquireLock("hot-spot-db", "db-pool-5", 60, false); err != nil {
		log.Printf("[demo-heatmap] queued db-pool-5 warning: %v", err)
	}
	log.Println("[demo-heatmap] enqueued 4 waiters for hot-spot-db (current queue depth = 4)")

	histAlert := &model.HotspotAlertEvent{
		LockName:        "hot-spot-db",
		AvgWaitMs:       9230.5,
		ThresholdMs:     5000.0,
		RequestCount:    1380,
		WaitCount:       920,
		MaxWaitMs:       15800,
		CurrentQueueLen: 6,
		WindowMinutes:   5,
		AlertType:       "avg_wait_exceeded",
		Detail:          "锁 hot-spot-db 在最近 5 分钟内平均等待 9230.50ms 超过阈值 5000.00ms (历史高峰期告警)",
		Acknowledged:    true,
		CreatedAt:       now.Add(-45 * time.Minute),
	}
	ackTime := now.Add(-30 * time.Minute)
	histAlert.AcknowledgedAt = &ackTime
	histAlert.AcknowledgedBy = "sre-oncall-zhang"
	if err := s.CreateHotspotAlert(histAlert); err != nil {
		return fmt.Errorf("create hist alert: %w", err)
	}
	log.Printf("[demo-heatmap] seeded historical alert: hot-spot-db avg_wait=%.1fms (at %s, ack by %s at %s)",
		histAlert.AvgWaitMs, histAlert.CreatedAt.Format(time.RFC3339),
		histAlert.AcknowledgedBy, histAlert.AcknowledgedAt.Format(time.RFC3339))

	log.Println("[demo-heatmap] demo data seeded successfully:")
	log.Println("[demo-heatmap]   🔥 hot-spot-db: 高热度 - 120 req/min, 80 wait/min, avg 8.5s wait (最近5分钟加剧)")
	log.Println("[demo-heatmap]   🌡  warm-cache-redis: 中热度 - 50 req/min, 20 wait/min, avg 2.2s wait")
	log.Println("[demo-heatmap]   ❄️ cold-config-map: 低热度 - 5 req/min, 1 wait/min, avg 0.3s wait")
	log.Println("[demo-heatmap]   🚨 历史告警: hot-spot-db (45分钟前触发, 30分钟前已确认)")
	log.Println("[demo-heatmap] tips:")
	log.Println("[demo-heatmap]   GET /api/v1/heatmap/top - 热力排行榜（Top N）")
	log.Println("[demo-heatmap]   GET /api/v1/heatmap/locks/hot-spot-db/trend?minutes=15 - 竞争趋势")
	log.Println("[demo-heatmap]   GET /api/v1/heatmap/alerts - 所有告警事件")
	log.Println("[demo-heatmap]   GET /api/v1/heatmap/alerts/active - 未确认告警")
	log.Println("[demo-heatmap]   GET /api/v1/heatmap/stats - 全局热力概览")
	log.Println("[demo-heatmap]   PUT /api/v1/heatmap/config - 调整告警阈值等配置")
	log.Println("[demo-heatmap]   POST /api/v1/locks/hot-spot-db/acquire - 继续请求制造新的竞争记录")

	return nil
}

func seedRateAlertDemoData(ram *ratealert.Manager, s *storage.Storage, bm *lockbudget.Manager) error {
	existingRules, err := ram.ListRules()
	if err != nil {
		return err
	}
	if len(existingRules) > 0 {
		log.Println("[demo-ratealert] rate alert data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-ratealert] seeding rate alert & freeze demo data...")

	if _, err := bm.SetBudget("service-alpha", 10000, 3600, 80); err != nil {
		return fmt.Errorf("set budget for service-alpha: %w", err)
	}
	log.Println("[demo-ratealert] set budget for service-alpha: 10000 units/hour")

	rule1, err := ram.SetRule("service-alpha", 60, 500, 3, true)
	if err != nil {
		return fmt.Errorf("set rate alert rule for service-alpha: %w", err)
	}
	log.Printf("[demo-ratealert] created rule for service-alpha: window=%ds, max=%d units, freeze_after=%d warnings",
		rule1.WindowSec, rule1.MaxUnitsInWindow, rule1.FreezeTriggerN)

	if _, err := bm.SetBudget("service-beta", 5000, 3600, 70); err != nil {
		return fmt.Errorf("set budget for service-beta: %w", err)
	}
	log.Println("[demo-ratealert] set budget for service-beta: 5000 units/hour")

	rule2, err := ram.SetRule("service-beta", 30, 200, 2, true)
	if err != nil {
		return fmt.Errorf("set rate alert rule for service-beta: %w", err)
	}
	log.Printf("[demo-ratealert] created rule for service-beta: window=%ds, max=%d units, freeze_after=%d warnings",
		rule2.WindowSec, rule2.MaxUnitsInWindow, rule2.FreezeTriggerN)

	now := time.Now()
	historyEvent1 := &model.LockBudgetRateAlertEvent{
		CallerID:         "service-alpha",
		CreatedAt:        now.Add(-2 * time.Hour),
		WindowSec:        60,
		MaxUnitsInWindow: 500,
		ConsumedInWindow: 620,
		ActualRate:       620.0 / 60.0,
	}
	if err := s.AddRateAlertEvent(historyEvent1); err != nil {
		return fmt.Errorf("add history alert event 1: %w", err)
	}
	log.Println("[demo-ratealert] seeded historical alert event for service-alpha (2h ago)")

	historyEvent2 := &model.LockBudgetRateAlertEvent{
		CallerID:         "service-alpha",
		CreatedAt:        now.Add(-90 * time.Minute),
		WindowSec:        60,
		MaxUnitsInWindow: 500,
		ConsumedInWindow: 580,
		ActualRate:       580.0 / 60.0,
	}
	if err := s.AddRateAlertEvent(historyEvent2); err != nil {
		return fmt.Errorf("add history alert event 2: %w", err)
	}
	log.Println("[demo-ratealert] seeded historical alert event for service-alpha (90m ago)")

	log.Println("[demo-ratealert] demo data seeded successfully:")
	log.Println("[demo-ratealert]   - service-alpha: budget=10000/hour, alert rule=60s/500 units, freeze after 3 consecutive warnings")
	log.Println("[demo-ratealert]   - service-beta: budget=5000/hour, alert rule=30s/200 units, freeze after 2 consecutive warnings")
	log.Println("[demo-ratealert]   - seeded 2 historical alert events for service-alpha")
	log.Println("[demo-ratealert] tips:")
	log.Println("[demo-ratealert]   GET  /api/v1/ratealert/rules - 查看所有预警规则")
	log.Println("[demo-ratealert]   GET  /api/v1/ratealert/rules/service-alpha - 查看特定调用方规则")
	log.Println("[demo-ratealert]   POST /api/v1/ratealert/rules - 设置预警规则 (body: {caller_id, window_sec, max_units_in_window, freeze_trigger_n})")
	log.Println("[demo-ratealert]   GET  /api/v1/ratealert/events - 查看所有预警事件")
	log.Println("[demo-ratealert]   GET  /api/v1/ratealert/events/service-alpha - 查看特定调用方预警历史")
	log.Println("[demo-ratealert]   GET  /api/v1/ratealert/freezes - 查看当前冻结中的调用方")
	log.Println("[demo-ratealert]   POST /api/v1/ratealert/freezes/<caller>/unfreeze - 手动解冻调用方")

	return nil
}
