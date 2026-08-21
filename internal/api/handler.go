package api

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
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
	"task106/internal/topology"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	manager       *lock.Manager
	rateLimiter   *ratelimit.Manager
	orchMgr       *orchestration.Manager
	auditMgr      *audit.Manager
	topoMgr       *topology.Manager
	shadowMgr     *shadow.Manager
	debtMgr       *debt.Manager
	handoverMgr   *handover.Manager
	heartbeatMgr  *heartbeat.Manager
	heatmapMgr    *heatmap.Manager
	budgetMgr     *lockbudget.Manager
	rateAlertMgr  *ratealert.Manager
	reputationMgr *reputation.Manager
	coordMgr      *controlplane.Manager
}

func (h *Handler) SetControlPlane(manager *controlplane.Manager) {
	h.coordMgr = manager
}

func NewHandler(m *lock.Manager, rl *ratelimit.Manager, om *orchestration.Manager, am *audit.Manager, tm *topology.Manager, sm *shadow.Manager, dm *debt.Manager, hm *handover.Manager, hbm *heartbeat.Manager, hmm *heatmap.Manager, bm *lockbudget.Manager, ram *ratealert.Manager, repMgr *reputation.Manager) *Handler {
	if bm == nil {
		bm = m.BudgetManager()
	}
	if ram == nil && bm != nil {
		ram = bm.RateAlertManager()
	}
	return &Handler{manager: m, rateLimiter: rl, orchMgr: om, auditMgr: am, topoMgr: tm, shadowMgr: sm, debtMgr: dm, handoverMgr: hm, heartbeatMgr: hbm, heatmapMgr: hmm, budgetMgr: bm, rateAlertMgr: ram, reputationMgr: repMgr}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", h.Health)

	api := r.Group("/api/v1")
	{
		locks := api.Group("/locks")
		{
			locks.GET("", h.ListLocks)
			locks.GET("/:name", h.GetLock)
			locks.POST("/:name/acquire", h.AcquireLock)
			locks.POST("/:name/release", h.ReleaseLock)
			locks.POST("/:name/renew", h.RenewLock)
			locks.GET("/:name/history", h.GetLockHistory)
			locks.POST("/batch/acquire", h.AcquireLocksBatch)
		}
		api.GET("/leases", h.ListLeases)
		api.GET("/wait-graph", h.GetWaitGraph)

		coordination := api.Group("/coordination")
		{
			resources := coordination.Group("/resources")
			{
				resources.GET("", h.ListCoordinationResources)
				resources.POST("", h.CreateCoordinationResource)
				resources.POST("/state", h.SetCoordinationResourceState)
				resources.POST("/policy", h.SetCoordinationResourcePolicy)
				resources.GET("/policies", h.ListCoordinationPolicies)
			}
			maintenance := coordination.Group("/maintenance")
			{
				maintenance.POST("/windows", h.CreateMaintenanceWindow)
				maintenance.GET("/windows", h.ListMaintenanceWindows)
				maintenance.POST("/windows/:id/cancel", h.CancelMaintenanceWindow)
			}
			fencing := coordination.Group("/fencing")
			{
				fencing.POST("/issue", h.IssueFencingToken)
				fencing.POST("/validate", h.ValidateFencingToken)
				fencing.GET("/tokens", h.ListFencingTokens)
			}
			recovery := coordination.Group("/recovery")
			{
				recovery.POST("/run", h.RunRecovery)
				recovery.GET("/checkpoints", h.ListRecoveryCheckpoints)
			}
			coordination.GET("/events", h.ListCoordinationEvents)
		}

		rateLimit := api.Group("/ratelimit")
		{
			policies := rateLimit.Group("/policies")
			{
				policies.GET("", h.ListPolicies)
				policies.GET("/:name", h.GetPolicy)
				policies.POST("", h.CreatePolicy)
			}
			callers := rateLimit.Group("/callers")
			{
				callers.GET("", h.ListCallers)
				callers.GET("/:id", h.GetCallerStatus)
				callers.POST("/bind", h.BindCaller)
				callers.POST("/:id/request", h.RequestTokens)
				callers.POST("/:id/adjust", h.AdjustQuota)
				callers.GET("/:id/history", h.GetCallerHistory)
			}
			rateLimit.POST("/borrow", h.BorrowQuota)
			rateLimit.POST("/return", h.ReturnQuota)
			rateLimit.GET("/borrows", h.ListBorrows)
			rateLimit.GET("/stats", h.GetGlobalStats)
			rateLimit.GET("/wait-queue", h.ListWaitQueue)
			rateLimit.GET("/callers/:id/wait-queue", h.GetCallerWaitQueue)

			reservations := rateLimit.Group("/reservations")
			{
				reservations.POST("", h.CreateReservation)
				reservations.GET("", h.ListReservations)
				reservations.GET("/:id", h.GetReservation)
				reservations.POST("/:id/cancel", h.CancelReservation)
			}
		}

		orch := api.Group("/orchestration")
		{
			orch.POST("/precheck", h.PreCheckTx)
			orch.POST("/tx", h.CreateTx)
			orch.GET("/tx", h.ListTxs)
			orch.GET("/tx/:id", h.GetTx)
			orch.POST("/tx/:id/release", h.ReleaseTx)
			orch.GET("/tx/:id/history", h.GetTxHistory)
		}

		auditGroup := api.Group("/audit")
		{
			auditGroup.GET("/logs", h.QueryAuditLogs)

			cb := auditGroup.Group("/circuit-breaker")
			{
				cb.POST("/rules", h.CreateCircuitBreakerRule)
				cb.GET("/rules", h.ListCircuitBreakerRules)
				cb.GET("/rules/:caller", h.GetCircuitBreakerRule)
				cb.DELETE("/rules/:caller", h.DeleteCircuitBreakerRule)

				cb.GET("/status", h.ListAllCircuitBreakerStatuses)
				cb.GET("/status/open", h.ListOpenCircuitBreakers)
				cb.GET("/status/:caller", h.GetCircuitBreakerStatus)
				cb.POST("/status/:caller/reset", h.ResetCircuitBreaker)

				cb.GET("/history", h.ListAllCircuitBreakerHistory)
				cb.GET("/history/:caller", h.GetCircuitBreakerHistory)
			}

			stats := auditGroup.Group("/stats")
			{
				stats.GET("", h.GetAuditGlobalStats)
				stats.GET("/callers", h.GetAllCallerStats)
				stats.GET("/callers/:id", h.GetCallerStats)
			}
		}

		topo := api.Group("/topology")
		{
			nodes := topo.Group("/nodes")
			{
				nodes.GET("", h.ListTopoNodes)
				nodes.GET("/:name", h.GetTopoNode)
				nodes.POST("", h.RegisterTopoNode)
			}

			edges := topo.Group("/edges")
			{
				edges.GET("", h.ListTopoEdges)
				edges.POST("", h.DeclareTopoEdge)
				edges.DELETE("/:from/:to", h.RemoveTopoEdge)
			}

			topo.GET("/graph", h.GetTopoGraph)
			topo.GET("/nodes/:name/ancestors", h.GetNodeAncestors)
			topo.GET("/nodes/:name/descendants", h.GetNodeDescendants)
			topo.GET("/holders/:holder/tree", h.GetHolderResourceTree)

			topo.POST("/acquire", h.CascadeAcquire)
			topo.POST("/release", h.CascadeRelease)

			topo.GET("/history", h.ListTopoHistory)
			topo.GET("/stats", h.GetTopoStats)
		}

		shadowGroup := api.Group("/shadow")
		{
			shadowGroup.POST("/plans", h.CreateShadowPlan)
			shadowGroup.GET("/plans", h.ListShadowPlans)
			shadowGroup.GET("/plans/:id", h.GetShadowPlan)
			shadowGroup.POST("/plans/:id/start", h.StartShadowPlan)
			shadowGroup.POST("/plans/:id/cancel", h.CancelShadowPlan)
			shadowGroup.POST("/plans/:id/apply", h.ApplyShadowPlan)

			shadowGroup.POST("/plans/:id/overrides", h.AddShadowOverride)
			shadowGroup.GET("/plans/:id/overrides", h.ListShadowOverrides)
			shadowGroup.DELETE("/overrides/:ovId", h.RemoveShadowOverride)

			shadowGroup.GET("/plans/:id/diffs", h.GetShadowDiffs)
			shadowGroup.GET("/plans/:id/stats", h.GetShadowDiffStats)
		}

		debtGroup := api.Group("/debt")
		{
			debtGroup.POST("/borrow", h.RecordDebtBorrow)
			debtGroup.POST("/return", h.RecordDebtReturn)
			debtGroup.POST("/rollback-fail", h.RecordDebtRollbackFail)
			debtGroup.POST("/reservation-expire", h.RecordDebtReservationExpire)
			debtGroup.POST("/force-reclaim", h.RecordDebtForceReclaim)
			debtGroup.GET("/records", h.ListDebtRecords)
			debtGroup.GET("/records/:id", h.GetDebtRecord)
			debtGroup.GET("/records/:id/timeline", h.GetDebtTimeline)
			debtGroup.POST("/records/:id/collect", h.ManualCollectDebt)
			debtGroup.GET("/callers/:id/summary", h.GetCallerDebtSummary)
			debtGroup.GET("/callers/:id/check", h.CheckDebtRestriction)
			debtGroup.GET("/ledger-events", h.ListDebtLedgerEvents)
			debtGroup.GET("/restrictions", h.ListDebtRestrictions)
			debtGroup.POST("/restrictions/:id/lift", h.LiftDebtRestriction)

			liqRules := debtGroup.Group("/liquidation-rules")
			{
				liqRules.POST("", h.CreateLiquidationRule)
				liqRules.GET("", h.ListLiquidationRules)
				liqRules.GET("/:caller", h.GetLiquidationRule)
				liqRules.DELETE("/:caller", h.DeleteLiquidationRule)
			}

			debtGroup.GET("/audit", h.ListLiquidationAudit)
		}

		handoverGroup := api.Group("/handovers")
		{
			handoverGroup.POST("", h.CreateHandover)
			handoverGroup.GET("", h.ListHandovers)
			handoverGroup.GET("/:id", h.GetHandover)
			handoverGroup.POST("/:id/precheck", h.PreCheckHandover)
			handoverGroup.POST("/:id/initiate", h.InitiateHandover)
			handoverGroup.POST("/:id/confirm", h.ConfirmHandover)
			handoverGroup.POST("/:id/cancel", h.CancelHandover)
			handoverGroup.GET("/callers/:caller", h.ListCallerHandovers)
			handoverGroup.GET("/callers/:caller/summary", h.GetCallerHandoverSummary)
			handoverGroup.GET("/:id/timeline", h.GetHandoverTimeline)
		}

		heartbeatGroup := api.Group("/heartbeat")
		{
			heartbeatGroup.POST("/register", h.RegisterHeartbeat)
			heartbeatGroup.POST("/report/:caller_id", h.ReportHeartbeat)
			heartbeatGroup.GET("/status/:caller_id", h.GetHeartbeatStatus)
			heartbeatGroup.GET("/statuses", h.ListAllHeartbeatStatuses)
			heartbeatGroup.GET("/report", h.GetHeartbeatReport)
			heartbeatGroup.GET("/events", h.ListHeartbeatEvents)
			heartbeatGroup.GET("/events/:caller_id", h.GetCallerHeartbeatEvents)
			heartbeatGroup.GET("/frozen", h.ListAllFrozenResources)
			heartbeatGroup.GET("/frozen/:caller_id", h.GetCallerFrozenResources)
			heartbeatGroup.POST("/frozen/:caller_id/release/:id", h.ReleaseFrozenResource)
			heartbeatGroup.POST("/frozen/:caller_id/release-all", h.ReleaseAllFrozenResources)

			heartbeatGroup.POST("/groups", h.CreateHeartbeatGroup)
			heartbeatGroup.DELETE("/groups/:name", h.DeleteHeartbeatGroup)
			heartbeatGroup.GET("/groups", h.ListHeartbeatGroups)
			heartbeatGroup.GET("/groups/:name", h.GetHeartbeatGroup)
			heartbeatGroup.GET("/groups/:name/members", h.ListHeartbeatGroupMembers)

			heartbeatGroup.POST("/dependencies", h.AddGroupDependency)
			heartbeatGroup.DELETE("/dependencies/:group_name/:depends_on", h.RemoveGroupDependency)
			heartbeatGroup.GET("/dependencies", h.ListGroupDependencies)

			heartbeatGroup.GET("/degraded", h.ListDegradedGroups)
		}

		budgetGroup := api.Group("/budget")
		{
			budgetGroup.GET("/configs", h.ListBudgetConfigs)
			budgetGroup.POST("/configs", h.SetBudgetConfig)
			budgetGroup.GET("/configs/:caller", h.GetBudgetConfig)
			budgetGroup.DELETE("/configs/:caller", h.DeleteBudgetConfig)

			budgetGroup.GET("/statuses", h.ListBudgetStatuses)
			budgetGroup.GET("/statuses/:caller", h.GetBudgetStatus)

			budgetGroup.GET("/holdings/:caller", h.ListHeldLocks)

			budgetGroup.GET("/events", h.ListBudgetExhaustEvents)
			budgetGroup.GET("/events/:caller", h.GetCallerBudgetExhaustEvents)

			budgetGroup.POST("/transfer", h.TransferBudget)
			budgetGroup.GET("/transfers", h.ListBudgetTransferRecords)

			budgetGroup.GET("/overdrafts", h.ListOverdraftCallers)

			budgetGroup.GET("/next-period-deductions", h.ListAllNextPeriodDeductions)
			budgetGroup.GET("/next-period-deductions/:caller", h.GetNextPeriodDeduction)

			budgetGroup.GET("/bills", h.ListBudgetSettlementBills)
			budgetGroup.GET("/bills/:id", h.GetBudgetSettlementBillDetail)
			budgetGroup.GET("/arrears", h.ListBudgetArrears)
			budgetGroup.POST("/recharge", h.RechargeBudget)
		}

		rateAlertGroup := api.Group("/ratealert")
		{
			rateAlertGroup.POST("/rules", h.SetRateAlertRule)
			rateAlertGroup.GET("/rules", h.ListRateAlertRules)
			rateAlertGroup.GET("/rules/:caller", h.GetRateAlertRule)
			rateAlertGroup.DELETE("/rules/:caller", h.DeleteRateAlertRule)

			rateAlertGroup.GET("/events", h.ListRateAlertEvents)
			rateAlertGroup.GET("/events/:caller", h.GetCallerRateAlertEvents)

			rateAlertGroup.GET("/freezes", h.ListFrozenCallers)
			rateAlertGroup.GET("/freezes/:caller", h.GetCallerFreezeStatus)
			rateAlertGroup.POST("/freezes/:caller/unfreeze", h.UnfreezeCaller)
		}

		heatmapGroup := api.Group("/heatmap")
		{
			heatmapGroup.GET("/stats", h.GetHeatmapGlobalStats)
			heatmapGroup.GET("/config", h.GetHeatmapConfig)
			heatmapGroup.PUT("/config", h.UpdateHeatmapConfig)

			heatmapGroup.GET("/top", h.GetTopHeatLocks)
			heatmapGroup.GET("/locks/:name/trend", h.GetLockTrend)

			heatmapGroup.GET("/alerts", h.ListHotspotAlerts)
			heatmapGroup.POST("/alerts/:id/ack", h.AcknowledgeHotspotAlert)
			heatmapGroup.GET("/alerts/active", h.ListActiveHotspotAlerts)

			heatmapGroup.GET("/cooldowns/active", h.ListActiveCooldowns)
			heatmapGroup.GET("/cooldowns/history", h.ListCooldownHistory)
			heatmapGroup.GET("/cooldowns/suggestions", h.GetCooldownSuggestions)
			heatmapGroup.POST("/cooldowns/:name/start", h.ManualStartCooldown)
			heatmapGroup.POST("/cooldowns/:name/stop", h.ManualStopCooldown)
		}

		reputationGroup := api.Group("/reputation")
		{
			reputationGroup.GET("/ranking", h.GetReputationRanking)
			reputationGroup.GET("/callers/:caller", h.GetCallerReputationDetail)
			reputationGroup.GET("/callers/:caller/events", h.ListCallerTierChangeEvents)
		}
	}
}

type AcquireRequest struct {
	Holder    string `json:"holder" binding:"required"`
	LeaseSec  int    `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool   `json:"reentrant"`
}

type ReleaseRequest struct {
	Holder string `json:"holder" binding:"required"`
}

type RenewRequest struct {
	Holder string `json:"holder" binding:"required"`
	AddSec int    `json:"add_sec" binding:"required,min=1"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) ListLocks(c *gin.Context) {
	locks, err := h.manager.ListAllLocks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"locks": locks})
}

func (h *Handler) GetLock(c *gin.Context) {
	name := c.Param("name")
	withHistory := c.Query("history") == "true"

	detail, err := h.manager.GetLockDetail(name, withHistory)
	if err != nil {
		if strings.HasPrefix(err.Error(), "lock not found:") {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"lock": detail})
}

func (h *Handler) AcquireLock(c *gin.Context) {
	name := c.Param("name")

	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if h.heartbeatMgr != nil {
		degraded, reason, err := h.heartbeatMgr.IsGroupDegraded(req.Holder)
		if err != nil {
			log.Printf("[handler] check group degraded error: %v", err)
		} else if degraded {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":           model.ErrGroupDegraded.Error(),
				"group_degraded":  true,
				"degraded_reason": reason,
				"can_renew":       true,
			})
			return
		}
	}

	result, err := h.auditMgr.AcquireLock(name, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if result.Deadlock {
		c.JSON(http.StatusConflict, gin.H{
			"acquired":       false,
			"deadlock":       true,
			"deadlock_cycle": result.DeadlockCycle.Cycle,
			"lock":           result.Lock,
		})
		return
	}

	if result.BudgetRejected && result.BudgetCheckResult != nil {
		br := result.BudgetCheckResult
		if br.ArrearsRejected {
			c.JSON(http.StatusForbidden, gin.H{
				"acquired":         false,
				"arrears_rejected": true,
				"arrears_amount":   br.ArrearsAmount,
				"reason":           br.Reason,
			})
			return
		}
		c.JSON(http.StatusTooManyRequests, gin.H{
			"acquired":        false,
			"budget_rejected": true,
			"reason":          br.Reason,
			"consumed_units":  br.ConsumedUnits,
			"remaining_units": br.RemainingUnits,
			"budget_limit":    br.BudgetLimit,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"acquired": result.Acquired,
		"queued":   result.Queued,
		"position": result.Position,
		"lock":     result.Lock,
		"lease":    result.Lease,
	})
}

func (h *Handler) ReleaseLock(c *gin.Context) {
	name := c.Param("name")

	var req ReleaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.auditMgr.ReleaseLock(name, req.Holder)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"released": result.Released,
		"count":    result.Count,
		"granted":  result.Granted,
	})
}

func (h *Handler) RenewLock(c *gin.Context) {
	name := c.Param("name")

	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if h.heartbeatMgr != nil {
		reg, _ := h.heartbeatMgr.GetStatus(req.Holder)
		if reg != nil && reg.Status == model.HeartbeatStatusFrozen {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":         "caller resources are frozen, cannot renew lock",
				"frozen":        true,
				"caller_status": model.HeartbeatStatusFrozen,
			})
			return
		}
	}

	lease, err := h.auditMgr.RenewLock(name, req.Holder, req.AddSec)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"lease": lease})
}

func (h *Handler) GetLockHistory(c *gin.Context) {
	name := c.Param("name")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.manager.GetLockHistory(name, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) ListLeases(c *gin.Context) {
	leases, err := h.manager.ListActiveLeases()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"leases": leases})
}

type BatchAcquireRequest struct {
	LockNames []string `json:"lock_names" binding:"required,min=1"`
	Holder    string   `json:"holder" binding:"required"`
	LeaseSec  int      `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool     `json:"reentrant"`
}

func (h *Handler) AcquireLocksBatch(c *gin.Context) {
	var req BatchAcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if h.heartbeatMgr != nil {
		degraded, reason, err := h.heartbeatMgr.IsGroupDegraded(req.Holder)
		if err != nil {
			log.Printf("[handler] check group degraded error: %v", err)
		} else if degraded {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":           model.ErrGroupDegraded.Error(),
				"group_degraded":  true,
				"degraded_reason": reason,
				"can_renew":       true,
			})
			return
		}
	}

	result, err := h.auditMgr.AcquireLocksBatch(req.LockNames, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !result.Acquired {
		if result.BudgetRejected && result.BudgetCheckResult != nil {
			br := result.BudgetCheckResult
			if br.ArrearsRejected {
				c.JSON(http.StatusForbidden, gin.H{
					"acquired":         false,
					"arrears_rejected": true,
					"arrears_amount":   br.ArrearsAmount,
					"failed_lock":      result.FailedLock,
					"reason":           br.Reason,
				})
				return
			}
			c.JSON(http.StatusTooManyRequests, gin.H{
				"acquired":        false,
				"budget_rejected": true,
				"failed_lock":     result.FailedLock,
				"reason":          br.Reason,
				"consumed_units":  br.ConsumedUnits,
				"remaining_units": br.RemainingUnits,
				"budget_limit":    br.BudgetLimit,
			})
			return
		}
		c.JSON(http.StatusConflict, gin.H{
			"acquired":    false,
			"failed_lock": result.FailedLock,
			"failed_by":   result.FailedBy,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"acquired": true,
		"locks":    result.Locks,
		"leases":   result.Leases,
	})
}

func (h *Handler) GetWaitGraph(c *gin.Context) {
	graph, err := h.manager.GetWaitGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"nodes": graph.Nodes,
		"edges": graph.Edges,
	})
}

func (h *Handler) CreatePolicy(c *gin.Context) {
	var req model.PolicyCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	policy, err := h.rateLimiter.CreatePolicy(req.Name, req.Algorithm, req.WindowSec, req.MaxTokens, req.RefillRate, req.RefillUnit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policy": policy})
}

func (h *Handler) GetPolicy(c *gin.Context) {
	name := c.Param("name")

	policy, err := h.rateLimiter.GetPolicy(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policy": policy})
}

func (h *Handler) ListPolicies(c *gin.Context) {
	policies, err := h.rateLimiter.ListPolicies()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

func (h *Handler) BindCaller(c *gin.Context) {
	var req model.BindCallerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	binding, err := h.rateLimiter.BindCaller(req.CallerID, req.PolicyName, req.QuotaLimit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"binding": binding})
}

func (h *Handler) RequestTokens(c *gin.Context) {
	callerID := c.Param("id")

	var req model.TokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.auditMgr.RequestTokens(callerID, req.Tokens, req.Waitable, req.WaitSec)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetCallerStatus(c *gin.Context) {
	callerID := c.Param("id")

	status, err := h.rateLimiter.GetCallerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller": status})
}

func (h *Handler) ListCallers(c *gin.Context) {
	statuses, err := h.rateLimiter.ListCallerStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"callers": statuses})
}

func (h *Handler) AdjustQuota(c *gin.Context) {
	callerID := c.Param("id")

	var req model.AdjustQuotaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.auditMgr.AdjustQuota(callerID, req.NewQuotaLimit); err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	status, err := h.rateLimiter.GetCallerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "caller": status})
}

func (h *Handler) BorrowQuota(c *gin.Context) {
	var req model.BorrowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.auditMgr.BorrowQuota(req.FromCaller, req.ToCaller, req.Amount)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) ReturnQuota(c *gin.Context) {
	var req model.ReturnRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.auditMgr.ReturnQuota(req.FromCaller, req.ToCaller, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetGlobalStats(c *gin.Context) {
	stats, err := h.rateLimiter.GetGlobalStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) GetCallerHistory(c *gin.Context) {
	callerID := c.Param("id")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.rateLimiter.GetCallerHistory(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) ListBorrows(c *gin.Context) {
	records, err := h.rateLimiter.ListBorrowRecords()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"borrows": records})
}

func (h *Handler) ListWaitQueue(c *gin.Context) {
	items, err := h.rateLimiter.ListWaitItems("")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"wait_queue": items})
}

func (h *Handler) GetCallerWaitQueue(c *gin.Context) {
	callerID := c.Param("id")

	items, err := h.rateLimiter.ListWaitItems(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"wait_queue": items})
}

func (h *Handler) CreateReservation(c *gin.Context) {
	var req model.CreateReservationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.rateLimiter.CreateReservation(req.PolicyName, req.CallerID, req.Tokens, req.StartAt, req.EndAt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetReservation(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reservation id"})
		return
	}

	reservation, err := h.rateLimiter.GetReservation(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reservation": reservation})
}

func (h *Handler) ListReservations(c *gin.Context) {
	policyName := c.Query("policy")
	callerID := c.Query("caller")
	status := c.Query("status")

	reservations, err := h.rateLimiter.ListReservations(policyName, callerID, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reservations": reservations})
}

func (h *Handler) CancelReservation(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reservation id"})
		return
	}

	result, err := h.rateLimiter.CancelReservation(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) PreCheckTx(c *gin.Context) {
	var req model.CreateTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.orchMgr.PreCheck(req.Locks, req.Tokens)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) CreateTx(c *gin.Context) {
	var req model.CreateTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tx, err := h.orchMgr.CreateTx(req.Holder, req.TimeoutSec, req.Locks, req.Tokens)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if tx.Status == model.TxStatusRolledBack {
		c.JSON(http.StatusConflict, gin.H{
			"tx":          tx,
			"committed":   false,
			"fail_reason": tx.FailReason,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"tx":        tx,
		"committed": true,
	})
}

func (h *Handler) ListTxs(c *gin.Context) {
	status := c.Query("status")

	txs, err := h.orchMgr.ListTxs(status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"txs": txs})
}

func (h *Handler) GetTx(c *gin.Context) {
	txID := c.Param("id")

	tx, err := h.orchMgr.GetTx(txID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"tx": tx})
}

func (h *Handler) ReleaseTx(c *gin.Context) {
	txID := c.Param("id")

	var req model.ReleaseTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tx, err := h.orchMgr.ReleaseTx(txID, req.Holder)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "permission denied") {
			c.JSON(http.StatusForbidden, gin.H{"error": errMsg})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	c.JSON(http.StatusOK, gin.H{"tx": tx})
}

func (h *Handler) GetTxHistory(c *gin.Context) {
	txID := c.Param("id")

	history, err := h.orchMgr.GetTxHistory(txID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) QueryAuditLogs(c *gin.Context) {
	caller := c.Query("caller")
	resource := c.Query("resource")

	var successPtr *bool
	successStr := c.Query("success")
	if successStr != "" {
		s := successStr == "true"
		successPtr = &s
	}

	var startTime, endTime time.Time
	startStr := c.Query("start_time")
	if startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			startTime = t
		}
	}
	endStr := c.Query("end_time")
	if endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			endTime = t
		}
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	result, err := h.auditMgr.QueryAuditLogs(caller, resource, successPtr, startTime, endTime, page, pageSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) CreateCircuitBreakerRule(c *gin.Context) {
	var req model.CreateCircuitBreakerRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rule, err := h.auditMgr.SetCircuitBreakerRule(req.CallerID, req.WindowSec, req.FailureThreshold, req.CooldownSec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) ListCircuitBreakerRules(c *gin.Context) {
	rules, err := h.auditMgr.ListCircuitBreakerRules()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rules": rules})
}

func (h *Handler) GetCircuitBreakerRule(c *gin.Context) {
	callerID := c.Param("caller")
	if callerID == "default" {
		callerID = ""
	}

	rule, err := h.auditMgr.GetCircuitBreakerRule(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if rule == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) DeleteCircuitBreakerRule(c *gin.Context) {
	callerID := c.Param("caller")
	if callerID == "default" {
		callerID = ""
	}

	if err := h.auditMgr.DeleteCircuitBreakerRule(callerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) ListAllCircuitBreakerStatuses(c *gin.Context) {
	statuses, err := h.auditMgr.ListAllCircuitBreakerStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"statuses": statuses})
}

func (h *Handler) ListOpenCircuitBreakers(c *gin.Context) {
	statuses, err := h.auditMgr.ListOpenCircuitBreakers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"open_breakers": statuses})
}

func (h *Handler) GetCircuitBreakerStatus(c *gin.Context) {
	callerID := c.Param("caller")
	status, err := h.auditMgr.GetCircuitBreakerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": status})
}

func (h *Handler) ResetCircuitBreaker(c *gin.Context) {
	callerID := c.Param("caller")
	if err := h.auditMgr.ManuallyCloseCircuitBreaker(callerID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "circuit breaker reset"})
}

func (h *Handler) ListAllCircuitBreakerHistory(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.auditMgr.GetCircuitBreakerHistory("", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetCircuitBreakerHistory(c *gin.Context) {
	callerID := c.Param("caller")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.auditMgr.GetCircuitBreakerHistory(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetAuditGlobalStats(c *gin.Context) {
	stats, err := h.auditMgr.GetGlobalStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) GetAllCallerStats(c *gin.Context) {
	stats, err := h.auditMgr.GetAllCallerStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"caller_stats": stats})
}

func (h *Handler) GetCallerStats(c *gin.Context) {
	callerID := c.Param("id")
	stats, err := h.auditMgr.GetCallerStats(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) RegisterTopoNode(c *gin.Context) {
	var req model.RegisterNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	node, err := h.topoMgr.RegisterNode(req.Name, req.LockName, req.RatePolicy, req.TokenCost)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"node": node})
}

func (h *Handler) ListTopoNodes(c *gin.Context) {
	nodes, err := h.topoMgr.ListNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

func (h *Handler) GetTopoNode(c *gin.Context) {
	name := c.Param("name")
	node, err := h.topoMgr.GetNode(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"node": node})
}

func (h *Handler) DeclareTopoEdge(c *gin.Context) {
	var req model.DeclareEdgeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	edge, err := h.topoMgr.DeclareEdge(req.FromNode, req.ToNode)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"edge": edge})
}

func (h *Handler) ListTopoEdges(c *gin.Context) {
	graph, err := h.topoMgr.GetGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"edges": graph.Edges})
}

func (h *Handler) RemoveTopoEdge(c *gin.Context) {
	fromNode := c.Param("from")
	toNode := c.Param("to")
	if err := h.topoMgr.RemoveEdge(fromNode, toNode); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) GetTopoGraph(c *gin.Context) {
	graph, err := h.topoMgr.GetGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"graph": graph})
}

func (h *Handler) GetNodeAncestors(c *gin.Context) {
	name := c.Param("name")
	result, err := h.topoMgr.GetAncestors(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) GetNodeDescendants(c *gin.Context) {
	name := c.Param("name")
	result, err := h.topoMgr.GetDescendants(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) GetHolderResourceTree(c *gin.Context) {
	holder := c.Param("holder")
	result, err := h.topoMgr.GetHolderResourceTree(holder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tree": result})
}

func (h *Handler) CascadeAcquire(c *gin.Context) {
	var req model.CascadeAcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.topoMgr.CascadeAcquire(req.TargetNode, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusConflict, gin.H{"result": result})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) CascadeRelease(c *gin.Context) {
	var req model.CascadeReleaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.topoMgr.CascadeRelease(req.TargetNode, req.Holder, req.Force)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusConflict, gin.H{"result": result})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) ListTopoHistory(c *gin.Context) {
	holder := c.Query("holder")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.topoMgr.ListHistory(holder, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetTopoStats(c *gin.Context) {
	stats, err := h.topoMgr.GetStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) CreateShadowPlan(c *gin.Context) {
	var req model.CreateShadowPlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	plan, err := h.shadowMgr.CreatePlan(req.Name, req.Description, req.Mode, req.MirrorSec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"plan": plan})
}

func (h *Handler) ListShadowPlans(c *gin.Context) {
	plans, err := h.shadowMgr.ListPlans()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"plans": plans})
}

func (h *Handler) GetShadowPlan(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}
	plan, err := h.shadowMgr.GetPlan(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if plan == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"plan": plan})
}

func (h *Handler) StartShadowPlan(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}
	if err := h.shadowMgr.StartPlan(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "shadow plan started"})
}

func (h *Handler) CancelShadowPlan(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}
	if err := h.shadowMgr.CancelPlan(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "shadow plan cancelled"})
}

func (h *Handler) ApplyShadowPlan(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}
	if err := h.shadowMgr.ApplyPlan(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "shadow plan applied to production atomically"})
}

func (h *Handler) AddShadowOverride(c *gin.Context) {
	planID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}

	var req model.UpdateShadowOverrideRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ov, err := h.shadowMgr.AddOverride(planID, req.Category, req.TargetKey, req.Field, req.NewValue)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"override": ov})
}

func (h *Handler) ListShadowOverrides(c *gin.Context) {
	planID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}

	overrides, err := h.shadowMgr.ListOverrides(planID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"overrides": overrides})
}

func (h *Handler) RemoveShadowOverride(c *gin.Context) {
	ovID, err := strconv.ParseInt(c.Param("ovId"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid override id"})
		return
	}
	if err := h.shadowMgr.RemoveOverride(ovID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) GetShadowDiffs(c *gin.Context) {
	planID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}

	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)

	records, err := h.shadowMgr.GetDiffRecords(planID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"diffs": records})
}

func (h *Handler) GetShadowDiffStats(c *gin.Context) {
	planID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid plan id"})
		return
	}

	stats, err := h.shadowMgr.GetDiffStats(planID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

type DebtBorrowRequest struct {
	Debtor         string `json:"debtor" binding:"required"`
	Creditor       string `json:"creditor" binding:"required"`
	Amount         int    `json:"amount" binding:"required,min=1"`
	ResourceType   string `json:"resource_type" binding:"required"`
	ResourceKey    string `json:"resource_key"`
	GracePeriodSec int    `json:"grace_period_sec"`
}

type DebtReturnRequest struct {
	Debtor       string `json:"debtor" binding:"required"`
	Creditor     string `json:"creditor" binding:"required"`
	Amount       int    `json:"amount" binding:"required,min=1"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceKey  string `json:"resource_key"`
}

type DebtRollbackFailRequest struct {
	Debtor       string `json:"debtor" binding:"required"`
	Amount       int    `json:"amount" binding:"required,min=1"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceKey  string `json:"resource_key"`
	Reason       string `json:"reason"`
}

type DebtReservationExpireRequest struct {
	CallerID   string `json:"caller_id" binding:"required"`
	Tokens     int    `json:"tokens" binding:"required,min=1"`
	PolicyName string `json:"policy_name" binding:"required"`
}

type DebtForceReclaimRequest struct {
	Debtor       string `json:"debtor" binding:"required"`
	ResourceType string `json:"resource_type" binding:"required"`
	ResourceKey  string `json:"resource_key" binding:"required"`
	Amount       int    `json:"amount" binding:"required,min=1"`
}

func (h *Handler) RecordDebtBorrow(c *gin.Context) {
	var req DebtBorrowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	grace := req.GracePeriodSec
	if grace <= 0 {
		grace = h.debtMgr.GetDefaultGracePeriod()
	}
	record, err := h.debtMgr.RecordBorrow(req.Debtor, req.Creditor, req.Amount, req.ResourceType, req.ResourceKey, grace)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) RecordDebtReturn(c *gin.Context) {
	var req DebtReturnRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.debtMgr.RecordReturn(req.Debtor, req.Creditor, req.Amount, req.ResourceType, req.ResourceKey); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) RecordDebtRollbackFail(c *gin.Context) {
	var req DebtRollbackFailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	record, err := h.debtMgr.RecordRollbackFail(req.Debtor, req.Amount, req.ResourceType, req.ResourceKey, req.Reason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) RecordDebtReservationExpire(c *gin.Context) {
	var req DebtReservationExpireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	record, err := h.debtMgr.RecordReservationExpire(req.CallerID, req.Tokens, req.PolicyName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) RecordDebtForceReclaim(c *gin.Context) {
	var req DebtForceReclaimRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	record, err := h.debtMgr.RecordForceReclaim(req.Debtor, req.ResourceType, req.ResourceKey, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) ListDebtRecords(c *gin.Context) {
	debtor := c.Query("debtor")
	status := c.Query("status")
	records, err := h.debtMgr.ListDebtRecords(debtor, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"records": records})
}

func (h *Handler) GetDebtRecord(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid debt id"})
		return
	}
	record, err := h.debtMgr.GetDebtRecord(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if record == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "debt record not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) GetDebtTimeline(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid debt id"})
		return
	}
	timeline, err := h.debtMgr.GetDebtTimeline(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"timeline": timeline})
}

func (h *Handler) ManualCollectDebt(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid debt id"})
		return
	}
	record, err := h.debtMgr.ManualCollect(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"debt": record})
}

func (h *Handler) GetCallerDebtSummary(c *gin.Context) {
	callerID := c.Param("id")
	summary, err := h.debtMgr.GetCallerDebtSummary(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"summary": summary})
}

func (h *Handler) CheckDebtRestriction(c *gin.Context) {
	callerID := c.Param("id")
	scope := c.Query("scope")
	if scope == "" {
		scope = string(model.RestrictionScopeAll)
	}
	result := h.debtMgr.CheckRestriction(callerID, model.RestrictionScope(scope))
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) ListDebtLedgerEvents(c *gin.Context) {
	debtor := c.Query("debtor")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	events, err := h.debtMgr.ListLedgerEvents(debtor, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": events})
}

func (h *Handler) ListDebtRestrictions(c *gin.Context) {
	callerID := c.Query("caller")
	restrictions, err := h.debtMgr.ListRestrictions(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"restrictions": restrictions})
}

func (h *Handler) LiftDebtRestriction(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid restriction id"})
		return
	}
	if err := h.debtMgr.LiftRestriction(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) CreateLiquidationRule(c *gin.Context) {
	var req model.CreateLiquidationRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	rule, err := h.debtMgr.SetLiquidationRule(
		req.CallerID, req.GracePeriodSec, req.OverdueThreshold,
		req.RestrictionType, req.RestrictionScope,
		req.MaxCollectRetries, req.ProtectionAfter,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) ListLiquidationRules(c *gin.Context) {
	rules, err := h.debtMgr.ListLiquidationRules()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rules": rules})
}

func (h *Handler) GetLiquidationRule(c *gin.Context) {
	callerID := c.Param("caller")
	rule, err := h.debtMgr.GetLiquidationRule(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) DeleteLiquidationRule(c *gin.Context) {
	callerID := c.Param("caller")
	if err := h.debtMgr.DeleteLiquidationRule(callerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) ListLiquidationAudit(c *gin.Context) {
	debtor := c.Query("debtor")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)
	entries, err := h.debtMgr.ListLiquidationAudit(debtor, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"audit": entries})
}

func (h *Handler) CreateHandover(c *gin.Context) {
	var req model.CreateHandoverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := h.handoverMgr.CreateHandover(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handover": result})
}

func (h *Handler) ListHandovers(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")
	status := c.Query("status")
	list, err := h.handoverMgr.ListHandovers(from, to, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handovers": list})
}

func (h *Handler) GetHandover(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	hando, err := h.handoverMgr.GetHandover(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if hando == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "handover not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handover": hando})
}

type InitiateHandoverRequest struct {
	Operator string `json:"operator" binding:"required"`
}

func (h *Handler) PreCheckHandover(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	result, err := h.handoverMgr.PreCheck(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) InitiateHandover(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	var req InitiateHandoverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := h.handoverMgr.Initiate(id, req.Operator)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handover": result})
}

func (h *Handler) ConfirmHandover(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	var req model.ConfirmHandoverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := h.handoverMgr.Confirm(id, &req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handover": result})
}

func (h *Handler) CancelHandover(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	var req model.CancelHandoverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := h.handoverMgr.Cancel(id, &req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handover": result})
}

func (h *Handler) ListCallerHandovers(c *gin.Context) {
	callerID := c.Param("caller")
	list, err := h.handoverMgr.ListHandoversForCaller(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"handovers": list})
}

func (h *Handler) GetCallerHandoverSummary(c *gin.Context) {
	callerID := c.Param("caller")
	summary, err := h.handoverMgr.GetCallerSummary(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"summary": summary})
}

func (h *Handler) GetHandoverTimeline(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid handover id"})
		return
	}
	hando, err := h.handoverMgr.GetHandover(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if hando == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "handover not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"timeline": hando.Timeline, "handover_id": id})
}

func (h *Handler) RegisterHeartbeat(c *gin.Context) {
	var req model.RegisterHeartbeatWithGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		var oldReq model.RegisterHeartbeatRequest
		if err2 := c.ShouldBindJSON(&oldReq); err2 != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		req = model.RegisterHeartbeatWithGroupRequest{
			CallerID:    oldReq.CallerID,
			GroupName:   "",
			IntervalSec: oldReq.IntervalSec,
			MaxMissed:   oldReq.MaxMissed,
			Strategy:    oldReq.Strategy,
		}
	}

	reg, err := h.heartbeatMgr.Register(req.CallerID, req.GroupName, req.IntervalSec, req.MaxMissed, req.Strategy)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"registration": reg})
}

func (h *Handler) CreateHeartbeatGroup(c *gin.Context) {
	var req model.CreateHeartbeatGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	group, err := h.heartbeatMgr.CreateGroup(req.Name, req.SurvivalThreshold)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"group": group})
}

func (h *Handler) DeleteHeartbeatGroup(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group name is required"})
		return
	}

	if err := h.heartbeatMgr.DeleteGroup(name); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "group deleted"})
}

func (h *Handler) GetHeartbeatGroup(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group name is required"})
		return
	}

	group, err := h.heartbeatMgr.GetGroup(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"group": group})
}

func (h *Handler) ListHeartbeatGroups(c *gin.Context) {
	groups, err := h.heartbeatMgr.ListGroupStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"groups": groups, "count": len(groups)})
}

func (h *Handler) ListHeartbeatGroupMembers(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group name is required"})
		return
	}

	members, err := h.heartbeatMgr.ListGroupMembers(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"group_name": name, "members": members, "count": len(members)})
}

func (h *Handler) AddGroupDependency(c *gin.Context) {
	var req model.GroupDependencyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.heartbeatMgr.AddGroupDependency(req.GroupName, req.DependsOn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("dependency added: %s -> %s", req.GroupName, req.DependsOn),
	})
}

func (h *Handler) RemoveGroupDependency(c *gin.Context) {
	groupName := c.Param("group_name")
	dependsOn := c.Param("depends_on")
	if groupName == "" || dependsOn == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group_name and depends_on are required"})
		return
	}

	if err := h.heartbeatMgr.RemoveGroupDependency(groupName, dependsOn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("dependency removed: %s -> %s", groupName, dependsOn),
	})
}

func (h *Handler) ListGroupDependencies(c *gin.Context) {
	deps, err := h.heartbeatMgr.ListGroupDependencies()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"dependencies": deps, "count": len(deps)})
}

func (h *Handler) ListDegradedGroups(c *gin.Context) {
	groups, err := h.heartbeatMgr.ListDegradedGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"degraded_groups": groups, "count": len(groups)})
}

func (h *Handler) ReportHeartbeat(c *gin.Context) {
	callerID := c.Param("caller_id")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id is required"})
		return
	}

	reg, err := h.heartbeatMgr.ReportHeartbeat(callerID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "registration": reg})
}

func (h *Handler) GetHeartbeatStatus(c *gin.Context) {
	callerID := c.Param("caller_id")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id is required"})
		return
	}

	status, err := h.heartbeatMgr.GetStatus(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": status})
}

func (h *Handler) ListAllHeartbeatStatuses(c *gin.Context) {
	statuses, err := h.heartbeatMgr.ListAllStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"statuses": statuses, "count": len(statuses)})
}

func (h *Handler) GetHeartbeatReport(c *gin.Context) {
	report, err := h.heartbeatMgr.GetReport()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"report": report})
}

func (h *Handler) ListHeartbeatEvents(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 100
	}

	events, err := h.heartbeatMgr.ListEvents("", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"events": events, "count": len(events)})
}

func (h *Handler) GetCallerHeartbeatEvents(c *gin.Context) {
	callerID := c.Param("caller_id")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id is required"})
		return
	}

	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 100
	}

	events, err := h.heartbeatMgr.ListEvents(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller_id": callerID, "events": events, "count": len(events)})
}

func (h *Handler) ListAllFrozenResources(c *gin.Context) {
	resources, err := h.heartbeatMgr.ListFrozenResources("")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"frozen_resources": resources, "count": len(resources)})
}

func (h *Handler) GetCallerFrozenResources(c *gin.Context) {
	callerID := c.Param("caller_id")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id is required"})
		return
	}

	resources, err := h.heartbeatMgr.ListFrozenResources(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller_id": callerID, "frozen_resources": resources, "count": len(resources)})
}

func (h *Handler) ReleaseFrozenResource(c *gin.Context) {
	callerID := c.Param("caller_id")
	idStr := c.Param("id")
	if callerID == "" || idStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id and id are required"})
		return
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid resource id"})
		return
	}

	if err := h.heartbeatMgr.ReleaseFrozenResource(id, callerID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "frozen resource released"})
}

func (h *Handler) ReleaseAllFrozenResources(c *gin.Context) {
	callerID := c.Param("caller_id")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller_id is required"})
		return
	}

	if err := h.heartbeatMgr.ReleaseAllFrozenResources(callerID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "all frozen resources released"})
}

func (h *Handler) GetHeatmapGlobalStats(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	stats, err := h.heatmapMgr.GetGlobalStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) GetHeatmapConfig(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	cfg := h.heatmapMgr.GetConfig()
	c.JSON(http.StatusOK, gin.H{"config": cfg})
}

func (h *Handler) UpdateHeatmapConfig(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	var req model.UpdateHeatmapConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg, err := h.heatmapMgr.UpdateConfig(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"config": cfg})
}

func (h *Handler) GetTopHeatLocks(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	limitStr := c.DefaultQuery("limit", "10")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 10
	}
	ranking, err := h.heatmapMgr.GetTopHeatLocks(limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"top_locks": ranking, "count": len(ranking)})
}

func (h *Handler) GetLockTrend(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	lockName := c.Param("name")
	if lockName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "lock name is required"})
		return
	}
	minutesStr := c.DefaultQuery("minutes", "60")
	minutes, _ := strconv.Atoi(minutesStr)
	if minutes <= 0 {
		minutes = 60
	}
	trend, err := h.heatmapMgr.GetLockTrend(lockName, minutes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lock_name": lockName, "trend": trend, "minutes": minutes})
}

func (h *Handler) ListHotspotAlerts(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	lockName := c.Query("lock_name")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)

	var ackPtr *bool
	ackStr := c.Query("acknowledged")
	if ackStr != "" {
		ack := ackStr == "true"
		ackPtr = &ack
	}

	alerts, err := h.heatmapMgr.ListAlerts(lockName, ackPtr, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"alerts": alerts, "count": len(alerts)})
}

func (h *Handler) ListActiveHotspotAlerts(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	falseVal := false
	alerts, err := h.heatmapMgr.ListAlerts("", &falseVal, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"active_alerts": alerts, "count": len(alerts)})
}

func (h *Handler) AcknowledgeHotspotAlert(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid alert id"})
		return
	}
	var req model.AcknowledgeAlertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.heatmapMgr.AcknowledgeAlert(id, req.AcknowledgedBy); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "alert acknowledged"})
}

func (h *Handler) ListActiveCooldowns(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}

	cooldowns, err := h.heatmapMgr.ListActiveCooldowns()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"active_cooldowns": cooldowns, "count": len(cooldowns)})
}

func (h *Handler) ListCooldownHistory(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}

	lockName := c.Query("lock_name")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 100
	}

	history, err := h.heatmapMgr.ListCooldownHistory(lockName, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history, "count": len(history)})
}

func (h *Handler) GetCooldownSuggestions(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}

	suggestions, err := h.heatmapMgr.GetCooldownSuggestions()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"suggestions": suggestions, "count": len(suggestions)})
}

func (h *Handler) ManualStartCooldown(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}

	lockName := c.Param("name")
	var req model.ManualCooldownRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	state, err := h.heatmapMgr.ManualStartCooldown(lockName, req.CooldownLeaseSec, req.Reason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "cooldown": state})
}

func (h *Handler) ManualStopCooldown(c *gin.Context) {
	if h.heatmapMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "heatmap manager not initialized"})
		return
	}

	lockName := c.Param("name")
	type stopRequest struct {
		Reason string `json:"reason"`
	}
	var req stopRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req.Reason = ""
	}

	state, err := h.heatmapMgr.ManualStopCooldown(lockName, req.Reason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "cooldown": state})
}

type SetBudgetConfigRequest struct {
	CallerID       string `json:"caller_id" binding:"required"`
	BudgetLimit    int    `json:"budget_limit" binding:"required,min=1"`
	PeriodSec      int    `json:"period_sec" binding:"required,min=1"`
	WarningPct     int    `json:"warning_pct"`
	OverdraftLimit int    `json:"overdraft_limit" binding:"min=0"`
}

func (h *Handler) ListBudgetConfigs(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	configs, err := h.budgetMgr.ListConfigs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, configs)
}

func (h *Handler) GetBudgetConfig(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	cfg, err := h.budgetMgr.GetConfig(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *Handler) SetBudgetConfig(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	var req SetBudgetConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.WarningPct <= 0 || req.WarningPct > 100 {
		req.WarningPct = 80
	}
	if req.OverdraftLimit < 0 {
		req.OverdraftLimit = 0
	}
	cfg, err := h.budgetMgr.SetConfigWithOverdraft(req.CallerID, req.BudgetLimit, req.PeriodSec, req.WarningPct, req.OverdraftLimit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *Handler) DeleteBudgetConfig(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	if err := h.budgetMgr.DeleteConfig(callerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) ListBudgetStatuses(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	statuses, err := h.budgetMgr.ListStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, statuses)
}

func (h *Handler) GetBudgetStatus(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	status, err := h.budgetMgr.GetStatus(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if status == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no budget config for caller"})
		return
	}
	c.JSON(http.StatusOK, status)
}

func (h *Handler) ListHeldLocks(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	now := time.Now()
	holdings, err := h.budgetMgr.ListHeldLocks(callerID, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"caller": callerID, "holdings": holdings})
}

func (h *Handler) ListBudgetExhaustEvents(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 100
	}
	events, err := h.budgetMgr.ListExhaustEvents("", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": events})
}

func (h *Handler) GetCallerBudgetExhaustEvents(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 100
	}
	events, err := h.budgetMgr.ListExhaustEvents(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"caller": callerID, "events": events})
}

func (h *Handler) TransferBudget(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	var req model.BudgetTransferRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	record, err := h.budgetMgr.TransferBudget(req.FromCaller, req.ToCaller, req.Amount, req.Reason)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "record": record})
}

func (h *Handler) ListBudgetTransferRecords(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	limitStr := c.DefaultQuery("limit", "50")
	offsetStr := c.DefaultQuery("offset", "0")
	limit, _ := strconv.Atoi(limitStr)
	offset, _ := strconv.Atoi(offsetStr)
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := &model.BudgetTransferListQuery{
		CallerID:   c.Query("caller_id"),
		FromCaller: c.Query("from_caller"),
		ToCaller:   c.Query("to_caller"),
		Limit:      limit,
		Offset:     offset,
	}
	result, err := h.budgetMgr.ListTransferRecords(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) ListOverdraftCallers(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	result, err := h.budgetMgr.ListOverdraftCallers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetNextPeriodDeduction(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	callerID := c.Param("caller")
	info, err := h.budgetMgr.GetNextPeriodDeduction(callerID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, info)
}

func (h *Handler) ListAllNextPeriodDeductions(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}
	result, err := h.budgetMgr.ListAllNextPeriodDeductions()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": result, "count": len(result)})
}

type BudgetRechargeRequest struct {
	CallerID string `json:"caller_id" binding:"required"`
	Amount   int    `json:"amount" binding:"required,min=1"`
}

func (h *Handler) ListBudgetSettlementBills(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}

	callerID := c.Query("caller_id")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	query := &model.BudgetBillListQuery{
		CallerID: callerID,
		Limit:    limit,
		Offset:   offset,
	}

	result, err := h.budgetMgr.ListSettlementBills(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetBudgetSettlementBillDetail(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}

	billID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bill id"})
		return
	}

	result, err := h.budgetMgr.GetSettlementBillDetail(billID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) ListBudgetArrears(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}

	result, err := h.budgetMgr.ListActiveArrears()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) RechargeBudget(c *gin.Context) {
	if h.budgetMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "budget manager not initialized"})
		return
	}

	var req BudgetRechargeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.budgetMgr.RechargeBudget(req.CallerID, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

type SetRateAlertRuleRequest struct {
	CallerID         string `json:"caller_id" binding:"required"`
	WindowSec        int    `json:"window_sec" binding:"required,min=1"`
	MaxUnitsInWindow int    `json:"max_units_in_window" binding:"required,min=1"`
	FreezeTriggerN   int    `json:"freeze_trigger_n"`
	Enabled          *bool  `json:"enabled"`
}

func (h *Handler) SetRateAlertRule(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	var req SetRateAlertRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	freezeN := req.FreezeTriggerN
	if freezeN <= 0 {
		freezeN = 3
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	rule, err := h.rateAlertMgr.SetRule(req.CallerID, req.WindowSec, req.MaxUnitsInWindow, freezeN, enabled)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) ListRateAlertRules(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	rules, err := h.rateAlertMgr.ListRules()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rules": rules})
}

func (h *Handler) GetRateAlertRule(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	callerID := c.Param("caller")
	rule, err := h.rateAlertMgr.GetRule(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if rule == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) DeleteRateAlertRule(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	callerID := c.Param("caller")
	if err := h.rateAlertMgr.DeleteRule(callerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) ListRateAlertEvents(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 100
	}

	events, err := h.rateAlertMgr.ListEvents("", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"events": events})
}

func (h *Handler) GetCallerRateAlertEvents(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	callerID := c.Param("caller")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 100
	}

	events, err := h.rateAlertMgr.ListEvents(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller_id": callerID, "events": events})
}

func (h *Handler) ListFrozenCallers(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	freezes, err := h.rateAlertMgr.ListFrozenCallers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"frozen_callers": freezes})
}

func (h *Handler) GetCallerFreezeStatus(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	callerID := c.Param("caller")
	freeze, err := h.rateAlertMgr.GetFreezeStatus(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if freeze == nil || !freeze.Active {
		c.JSON(http.StatusOK, gin.H{"caller_id": callerID, "frozen": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller_id": callerID, "frozen": true, "freeze": freeze})
}

type UnfreezeCallerRequest struct {
	UnfrozenBy string `json:"unfrozen_by"`
	Reason     string `json:"reason"`
}

func (h *Handler) UnfreezeCaller(c *gin.Context) {
	if h.rateAlertMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rate alert manager not initialized"})
		return
	}

	callerID := c.Param("caller")

	var req UnfreezeCallerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req.UnfrozenBy = "admin"
		req.Reason = ""
	}

	if err := h.rateAlertMgr.Unfreeze(callerID, req.UnfrozenBy, req.Reason); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "caller unfrozen"})
}

func (h *Handler) GetReputationRanking(c *gin.Context) {
	if h.reputationMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reputation module not enabled"})
		return
	}
	result, err := h.reputationMgr.GetRanking()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetCallerReputationDetail(c *gin.Context) {
	if h.reputationMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reputation module not enabled"})
		return
	}
	callerID := c.Param("caller")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller is required"})
		return
	}
	detail, err := h.reputationMgr.GetCallerDetail(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}

func (h *Handler) ListCallerTierChangeEvents(c *gin.Context) {
	if h.reputationMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reputation module not enabled"})
		return
	}
	callerID := c.Param("caller")
	if callerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "caller is required"})
		return
	}
	limit := 50
	offset := 0
	if l, err := strconv.Atoi(c.DefaultQuery("limit", "50")); err == nil && l > 0 {
		limit = l
	}
	if o, err := strconv.Atoi(c.DefaultQuery("offset", "0")); err == nil && o >= 0 {
		offset = o
	}
	result, err := h.reputationMgr.ListTierChangeEvents(callerID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}
