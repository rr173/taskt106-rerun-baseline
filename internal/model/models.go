package model

import (
	"fmt"
	"time"
)

type LockStatus string

const (
	LockStatusFree    LockStatus = "free"
	LockStatusHeld    LockStatus = "held"
	LockStatusExpired LockStatus = "expired"
)

type Lock struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Status    LockStatus `json:"status"`
	Holder    string     `json:"holder,omitempty"`
	Reentrant bool       `json:"reentrant"`
	Count     int        `json:"count"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type Lease struct {
	ID           int64     `json:"id"`
	LockName     string    `json:"lock_name"`
	Holder       string    `json:"holder"`
	LeaseSec     int       `json:"lease_sec"`
	AcquiredAt   time.Time `json:"acquired_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	Active       bool      `json:"active"`
	FencingToken string    `json:"fencing_token,omitempty"`
	RemainingSec float64   `json:"remaining_sec,omitempty"`
}

type WaitQueueItem struct {
	ID         int64     `json:"id"`
	LockName   string    `json:"lock_name"`
	Holder     string    `json:"holder"`
	Reentrant  bool      `json:"reentrant"`
	LeaseSec   int       `json:"lease_sec"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	TimeoutAt  time.Time `json:"timeout_at"`
}

type OperationType string

const (
	OpAcquire   OperationType = "acquire"
	OpRelease   OperationType = "release"
	OpRenew     OperationType = "renew"
	OpExpire    OperationType = "expire"
	OpTimeout   OperationType = "timeout"
	OpGrantNext OperationType = "grant_next"
)

type OperationHistory struct {
	ID        int64         `json:"id"`
	LockName  string        `json:"lock_name"`
	Holder    string        `json:"holder"`
	Operation OperationType `json:"operation"`
	Detail    string        `json:"detail"`
	CreatedAt time.Time     `json:"created_at"`
}

type LockDetail struct {
	Lock      Lock               `json:"lock"`
	Lease     *Lease             `json:"lease,omitempty"`
	WaitQueue []WaitQueueItem    `json:"wait_queue"`
	History   []OperationHistory `json:"history,omitempty"`
}

type LockStatusInfo struct {
	Name         string     `json:"name"`
	Status       LockStatus `json:"status"`
	Holder       string     `json:"holder,omitempty"`
	Reentrant    bool       `json:"reentrant"`
	Count        int        `json:"count"`
	RemainingSec float64    `json:"remaining_sec,omitempty"`
	WaitQueueLen int        `json:"wait_queue_len"`
}

type WaitGraphEdge struct {
	Waiter   string `json:"waiter"`
	LockName string `json:"lock_name"`
	Holder   string `json:"holder"`
}

type DeadlockCycle struct {
	Cycle []WaitGraphEdge `json:"cycle"`
}

type BatchAcquireRequest struct {
	LockNames []string `json:"lock_names" binding:"required,min=1"`
	Holder    string   `json:"holder" binding:"required"`
	LeaseSec  int      `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool     `json:"reentrant"`
}

type BatchAcquireResult struct {
	Acquired          bool                      `json:"acquired"`
	FailedLock        string                    `json:"failed_lock,omitempty"`
	FailedBy          string                    `json:"failed_by,omitempty"`
	Locks             []Lock                    `json:"locks,omitempty"`
	Leases            []Lease                   `json:"leases,omitempty"`
	BudgetRejected    bool                      `json:"budget_rejected,omitempty"`
	BudgetCheckResult *BudgetAcquireCheckResult `json:"budget_check_result,omitempty"`
}

type WaitGraph struct {
	Edges []WaitGraphEdge `json:"edges"`
	Nodes []string        `json:"nodes"`
}

type AlgorithmType string

const (
	AlgoFixedWindow   AlgorithmType = "fixed_window"
	AlgoSlidingWindow AlgorithmType = "sliding_window"
	AlgoTokenBucket   AlgorithmType = "token_bucket"
)

type RateLimitPolicy struct {
	ID         int64         `json:"id"`
	Name       string        `json:"name"`
	Algorithm  AlgorithmType `json:"algorithm"`
	WindowSec  int           `json:"window_sec,omitempty"`
	MaxTokens  int           `json:"max_tokens"`
	RefillRate float64       `json:"refill_rate,omitempty"`
	RefillUnit string        `json:"refill_unit,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

type CallerBinding struct {
	ID              int64     `json:"id"`
	CallerID        string    `json:"caller_id"`
	PolicyName      string    `json:"policy_name"`
	QuotaLimit      int       `json:"quota_limit"`
	UsedTokens      int       `json:"used_tokens"`
	BorrowedTokens  int       `json:"borrowed_tokens"`
	LentTokens      int       `json:"lent_tokens"`
	ReservedTokens  int       `json:"reserved_tokens"`
	LastRefillAt    time.Time `json:"last_refill_at,omitempty"`
	WindowStartAt   time.Time `json:"window_start_at,omitempty"`
	PrevWindowCount int       `json:"prev_window_count,omitempty"`
	CurrWindowCount int       `json:"curr_window_count,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type RateLimitEvent struct {
	ID         int64     `json:"id"`
	CallerID   string    `json:"caller_id"`
	PolicyName string    `json:"policy_name"`
	Requested  int       `json:"requested"`
	Granted    int       `json:"granted"`
	Allowed    bool      `json:"allowed"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type QuotaBorrowRecord struct {
	ID         int64     `json:"id"`
	FromCaller string    `json:"from_caller"`
	ToCaller   string    `json:"to_caller"`
	Amount     int       `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	ReturnedAt time.Time `json:"returned_at,omitempty"`
}

type TokenRequest struct {
	Tokens   int  `json:"tokens" binding:"required,min=1"`
	Waitable bool `json:"waitable,omitempty"`
	WaitSec  int  `json:"wait_sec,omitempty"`
}

type TokenResult struct {
	Allowed    bool   `json:"allowed"`
	Queued     bool   `json:"queued,omitempty"`
	Position   int    `json:"position,omitempty"`
	Granted    int    `json:"granted"`
	Requested  int    `json:"requested"`
	Remaining  int    `json:"remaining"`
	QuotaLimit int    `json:"quota_limit"`
	UsedTokens int    `json:"used_tokens"`
	Reason     string `json:"reason,omitempty"`
}

type RateLimitWaitItem struct {
	ID         int64     `json:"id"`
	CallerID   string    `json:"caller_id"`
	Tokens     int       `json:"tokens"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	TimeoutAt  time.Time `json:"timeout_at"`
}

type CallerStatus struct {
	CallerID       string `json:"caller_id"`
	PolicyName     string `json:"policy_name"`
	Algorithm      string `json:"algorithm"`
	QuotaLimit     int    `json:"quota_limit"`
	PolicyMax      int    `json:"policy_max"`
	UsedTokens     int    `json:"used_tokens"`
	Remaining      int    `json:"remaining"`
	BorrowedTokens int    `json:"borrowed_tokens"`
	LentTokens     int    `json:"lent_tokens"`
	ReservedTokens int    `json:"reserved_tokens"`
	RateLimited    int64  `json:"rate_limited_count"`
	WaitQueueLen   int    `json:"wait_queue_len,omitempty"`
}

type GlobalStats struct {
	TotalCallers     int   `json:"total_callers"`
	TotalPolicies    int   `json:"total_policies"`
	TotalRequests    int64 `json:"total_requests"`
	TotalAllowed     int64 `json:"total_allowed"`
	TotalRateLimited int64 `json:"total_rate_limited"`
	ActiveBorrows    int   `json:"active_borrows"`
	BorrowedAmount   int   `json:"borrowed_amount"`
}

type BorrowRequest struct {
	FromCaller string `json:"from_caller" binding:"required"`
	ToCaller   string `json:"to_caller" binding:"required"`
	Amount     int    `json:"amount" binding:"required,min=1"`
}

type BorrowResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type ReturnRequest struct {
	FromCaller string `json:"from_caller" binding:"required"`
	ToCaller   string `json:"to_caller" binding:"required"`
	Amount     int    `json:"amount" binding:"required,min=1"`
}

type PolicyCreateRequest struct {
	Name       string        `json:"name" binding:"required"`
	Algorithm  AlgorithmType `json:"algorithm" binding:"required"`
	WindowSec  int           `json:"window_sec"`
	MaxTokens  int           `json:"max_tokens" binding:"required,min=1"`
	RefillRate float64       `json:"refill_rate"`
	RefillUnit string        `json:"refill_unit"`
}

type BindCallerRequest struct {
	CallerID   string `json:"caller_id" binding:"required"`
	PolicyName string `json:"policy_name" binding:"required"`
	QuotaLimit int    `json:"quota_limit" binding:"required,min=1"`
}

type AdjustQuotaRequest struct {
	NewQuotaLimit int `json:"new_quota_limit" binding:"required,min=0"`
}

type ReservationStatus string

const (
	ReservationStatusPending   ReservationStatus = "pending"
	ReservationStatusActive    ReservationStatus = "active"
	ReservationStatusCompleted ReservationStatus = "completed"
	ReservationStatusCancelled ReservationStatus = "cancelled"
)

type QuotaReservation struct {
	ID         int64             `json:"id"`
	PolicyName string            `json:"policy_name"`
	CallerID   string            `json:"caller_id"`
	Tokens     int               `json:"tokens"`
	StartAt    time.Time         `json:"start_at"`
	EndAt      time.Time         `json:"end_at"`
	Status     ReservationStatus `json:"status"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type CreateReservationRequest struct {
	PolicyName string    `json:"policy_name" binding:"required"`
	CallerID   string    `json:"caller_id" binding:"required"`
	Tokens     int       `json:"tokens" binding:"required,min=1"`
	StartAt    time.Time `json:"start_at" binding:"required"`
	EndAt      time.Time `json:"end_at" binding:"required"`
}

type ReservationResult struct {
	Success     bool              `json:"success"`
	Message     string            `json:"message,omitempty"`
	Reservation *QuotaReservation `json:"reservation,omitempty"`
}

type TxStatus string

const (
	TxStatusCreated    TxStatus = "created"
	TxStatusCommitted  TxStatus = "committed"
	TxStatusRolledBack TxStatus = "rolled_back"
	TxStatusReleased   TxStatus = "released"
	TxStatusTimedOut   TxStatus = "timed_out"
)

type TxLockSpec struct {
	LockName string `json:"lock_name" binding:"required"`
	LeaseSec int    `json:"lease_sec" binding:"required,min=1"`
}

type TxTokenSpec struct {
	CallerID string `json:"caller_id" binding:"required"`
	Tokens   int    `json:"tokens" binding:"required,min=1"`
}

type TxLock struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	LockName  string    `json:"lock_name"`
	LeaseSec  int       `json:"lease_sec"`
	Holder    string    `json:"holder"`
	CreatedAt time.Time `json:"created_at"`
}

type TxToken struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	CallerID  string    `json:"caller_id"`
	Tokens    int       `json:"tokens"`
	CreatedAt time.Time `json:"created_at"`
}

type TxStateChange struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	FromState TxStatus  `json:"from_state"`
	ToState   TxStatus  `json:"to_state"`
	Reason    string    `json:"reason,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type OrchestrationTx struct {
	ID           string          `json:"id"`
	Holder       string          `json:"holder"`
	Status       TxStatus        `json:"status"`
	TimeoutSec   int             `json:"timeout_sec"`
	FailReason   string          `json:"fail_reason,omitempty"`
	Locks        []TxLock        `json:"locks,omitempty"`
	Tokens       []TxToken       `json:"tokens,omitempty"`
	StateChanges []TxStateChange `json:"state_changes,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	ExpiresAt    time.Time       `json:"expires_at"`
}

type CreateTxRequest struct {
	Holder     string        `json:"holder" binding:"required"`
	TimeoutSec int           `json:"timeout_sec" binding:"required,min=1"`
	Locks      []TxLockSpec  `json:"locks" binding:"required,min=1"`
	Tokens     []TxTokenSpec `json:"tokens"`
}

type PreCheckResult struct {
	ConflictingLocks  []ConflictingLockInfo   `json:"conflicting_locks,omitempty"`
	InsufficientQuota []InsufficientQuotaInfo `json:"insufficient_quota,omitempty"`
	CanProceed        bool                    `json:"can_proceed"`
}

type ConflictingLockInfo struct {
	LockName string `json:"lock_name"`
	Holder   string `json:"holder"`
}

type InsufficientQuotaInfo struct {
	CallerID  string `json:"caller_id"`
	Requested int    `json:"requested"`
	Remaining int    `json:"remaining"`
}

type ReleaseTxRequest struct {
	Holder string `json:"holder" binding:"required"`
}

type AuditOperationType string

const (
	AuditOpAcquireLock       AuditOperationType = "acquire_lock"
	AuditOpReleaseLock       AuditOperationType = "release_lock"
	AuditOpRenewLock         AuditOperationType = "renew_lock"
	AuditOpAcquireLocksBatch AuditOperationType = "acquire_locks_batch"
	AuditOpRequestTokens     AuditOperationType = "request_tokens"
	AuditOpReturnTokens      AuditOperationType = "return_tokens"
	AuditOpBorrowQuota       AuditOperationType = "borrow_quota"
	AuditOpReturnQuota       AuditOperationType = "return_quota"
	AuditOpAdjustQuota       AuditOperationType = "adjust_quota"
	AuditOpHandover          AuditOperationType = "handover"
)

type AuditLog struct {
	ID         int64              `json:"id"`
	Timestamp  time.Time          `json:"timestamp"`
	Caller     string             `json:"caller"`
	Operation  AuditOperationType `json:"operation"`
	Resource   string             `json:"resource"`
	Success    bool               `json:"success"`
	FailReason string             `json:"fail_reason,omitempty"`
}

type CircuitBreakerRule struct {
	ID               int64     `json:"id"`
	CallerID         string    `json:"caller_id"`
	WindowSec        int       `json:"window_sec"`
	FailureThreshold int       `json:"failure_threshold"`
	CooldownSec      int       `json:"cooldown_sec"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type CircuitBreakerState string

const (
	CircuitBreakerClosed   CircuitBreakerState = "closed"
	CircuitBreakerOpen     CircuitBreakerState = "open"
	CircuitBreakerHalfOpen CircuitBreakerState = "half_open"
)

type CircuitBreakerStatus struct {
	ID               int64               `json:"id"`
	CallerID         string              `json:"caller_id"`
	State            CircuitBreakerState `json:"state"`
	TriggeredAt      time.Time           `json:"triggered_at,omitempty"`
	ExpiresAt        time.Time           `json:"expires_at,omitempty"`
	FailuresInWindow int                 `json:"failures_in_window"`
	TriggerReason    string              `json:"trigger_reason,omitempty"`
	UpdatedAt        time.Time           `json:"updated_at"`
}

type CircuitBreakerHistory struct {
	ID            int64     `json:"id"`
	CallerID      string    `json:"caller_id"`
	State         string    `json:"state"`
	TriggeredAt   time.Time `json:"triggered_at"`
	RecoveredAt   time.Time `json:"recovered_at,omitempty"`
	TriggerReason string    `json:"trigger_reason,omitempty"`
	RecoverReason string    `json:"recover_reason,omitempty"`
}

type CreateCircuitBreakerRuleRequest struct {
	CallerID         string `json:"caller_id"`
	WindowSec        int    `json:"window_sec" binding:"required,min=1"`
	FailureThreshold int    `json:"failure_threshold" binding:"required,min=1"`
	CooldownSec      int    `json:"cooldown_sec" binding:"required,min=1"`
}

type AuditQueryRequest struct {
	Caller    string `form:"caller"`
	Resource  string `form:"resource"`
	Success   *bool  `form:"success"`
	StartTime string `form:"start_time"`
	EndTime   string `form:"end_time"`
	Page      int    `form:"page,default=1"`
	PageSize  int    `form:"page_size,default=20"`
}

type PaginatedAuditLogs struct {
	Logs     []AuditLog `json:"logs"`
	Total    int64      `json:"total"`
	Page     int        `json:"page"`
	PageSize int        `json:"page_size"`
}

type CallerStats struct {
	CallerID      string  `json:"caller_id"`
	TotalRequests int64   `json:"total_requests"`
	SuccessCount  int64   `json:"success_count"`
	FailureCount  int64   `json:"failure_count"`
	SuccessRate   float64 `json:"success_rate"`
	FailureRate   float64 `json:"failure_rate"`
	Requests1Min  int64   `json:"requests_1min"`
	Requests5Min  int64   `json:"requests_5min"`
	Requests15Min int64   `json:"requests_15min"`
}

type GlobalAuditStats struct {
	TotalRequests  int64   `json:"total_requests"`
	SuccessCount   int64   `json:"success_count"`
	FailureCount   int64   `json:"failure_count"`
	SuccessRate    float64 `json:"success_rate"`
	FailureRate    float64 `json:"failure_rate"`
	Requests1Min   int64   `json:"requests_1min"`
	Requests5Min   int64   `json:"requests_5min"`
	Requests15Min  int64   `json:"requests_15min"`
	ActiveBreakers int     `json:"active_breakers"`
}

type TopologyNode struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	LockName   string    `json:"lock_name"`
	RatePolicy string    `json:"rate_policy,omitempty"`
	TokenCost  int       `json:"token_cost"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type TopologyEdge struct {
	ID        int64     `json:"id"`
	FromNode  string    `json:"from_node"`
	ToNode    string    `json:"to_node"`
	CreatedAt time.Time `json:"created_at"`
}

type TopologyGraph struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}

type RegisterNodeRequest struct {
	Name       string `json:"name" binding:"required"`
	LockName   string `json:"lock_name"`
	RatePolicy string `json:"rate_policy,omitempty"`
	TokenCost  int    `json:"token_cost"`
}

type DeclareEdgeRequest struct {
	FromNode string `json:"from_node" binding:"required"`
	ToNode   string `json:"to_node" binding:"required"`
}

type CascadeAcquireRequest struct {
	TargetNode string `json:"target_node" binding:"required"`
	Holder     string `json:"holder" binding:"required"`
	LeaseSec   int    `json:"lease_sec" binding:"required,min=1"`
	Reentrant  bool   `json:"reentrant"`
}

type CascadeAcquireStep struct {
	NodeName string `json:"node_name"`
	LockName string `json:"lock_name"`
	Action   string `json:"action"`
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
}

type CascadeAcquireResult struct {
	Success    bool                 `json:"success"`
	RolledBack bool                 `json:"rolled_back"`
	Steps      []CascadeAcquireStep `json:"steps"`
	Acquired   []string             `json:"acquired,omitempty"`
	Message    string               `json:"message,omitempty"`
	DurationMs int64                `json:"duration_ms"`
}

type CascadeReleaseRequest struct {
	TargetNode string `json:"target_node" binding:"required"`
	Holder     string `json:"holder" binding:"required"`
	Force      bool   `json:"force"`
}

type CascadeReleaseStep struct {
	NodeName string `json:"node_name"`
	LockName string `json:"lock_name"`
	Action   string `json:"action"`
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
}

type CascadeReleaseResult struct {
	Success    bool                 `json:"success"`
	Steps      []CascadeReleaseStep `json:"steps"`
	Released   []string             `json:"released,omitempty"`
	Message    string               `json:"message,omitempty"`
	DurationMs int64                `json:"duration_ms"`
}

type NodeAncestorsResult struct {
	NodeName  string   `json:"node_name"`
	Ancestors []string `json:"ancestors"`
}

type NodeDescendantsResult struct {
	NodeName    string   `json:"node_name"`
	Descendants []string `json:"descendants"`
}

type HolderResourceTree struct {
	Holder    string              `json:"holder"`
	RootNodes []string            `json:"root_nodes"`
	HeldNodes []string            `json:"held_nodes"`
	Tree      map[string][]string `json:"tree"`
}

type TopologyOperationType string

const (
	TopologyOpAcquire TopologyOperationType = "cascade_acquire"
	TopologyOpRelease TopologyOperationType = "cascade_release"
)

type TopologyOperationHistory struct {
	ID           int64                 `json:"id"`
	Operation    TopologyOperationType `json:"operation"`
	TargetNode   string                `json:"target_node"`
	Holder       string                `json:"holder"`
	Success      bool                  `json:"success"`
	RolledBack   bool                  `json:"rolled_back"`
	NodesTouched []string              `json:"nodes_touched"`
	DurationMs   int64                 `json:"duration_ms"`
	Message      string                `json:"message,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
}

type TopologyStats struct {
	TotalNodes      int `json:"total_nodes"`
	TotalEdges      int `json:"total_edges"`
	TotalOperations int `json:"total_operations"`
	AcquireOps      int `json:"acquire_operations"`
	ReleaseOps      int `json:"release_operations"`
}

type ShadowPlanStatus string

const (
	ShadowPlanStatusDraft     ShadowPlanStatus = "draft"
	ShadowPlanStatusRunning   ShadowPlanStatus = "running"
	ShadowPlanStatusCompleted ShadowPlanStatus = "completed"
	ShadowPlanStatusApplied   ShadowPlanStatus = "applied"
	ShadowPlanStatusCancelled ShadowPlanStatus = "cancelled"
)

type ShadowDecision string

const (
	ShadowDecisionAdmit          ShadowDecision = "admit"
	ShadowDecisionWait           ShadowDecision = "wait"
	ShadowDecisionDeadlockReject ShadowDecision = "deadlock_reject"
	ShadowDecisionCircuitBreak   ShadowDecision = "circuit_break"
	ShadowDecisionRateLimit      ShadowDecision = "rate_limit"
	ShadowDecisionTxRollback     ShadowDecision = "tx_rollback"
	ShadowDecisionReject         ShadowDecision = "reject"
)

type DebtStatus string

const (
	DebtStatusActive     DebtStatus = "active"
	DebtStatusCollected  DebtStatus = "collected"
	DebtStatusOverdue    DebtStatus = "overdue"
	DebtStatusWrittenOff DebtStatus = "written_off"
)

type DebtEventType string

const (
	DebtEventBorrow       DebtEventType = "borrow"
	DebtEventReturn       DebtEventType = "return"
	DebtEventRollbackFail DebtEventType = "rollback_fail"
	DebtEventReservExpir  DebtEventType = "reservation_expire"
	DebtEventForceReclaim DebtEventType = "force_reclaim"
	DebtEventCollect      DebtEventType = "collect"
	DebtEventCollectFail  DebtEventType = "collect_fail"
	DebtEventOverdueMark  DebtEventType = "overdue_mark"
	DebtEventRestrict     DebtEventType = "restrict"
	DebtEventRestrictLift DebtEventType = "restrict_lift"
	DebtEventWriteOff     DebtEventType = "write_off"
)

type DebtRecord struct {
	ID              int64      `json:"id"`
	Debtor          string     `json:"debtor"`
	Creditor        string     `json:"creditor"`
	Amount          int        `json:"amount"`
	ResourceType    string     `json:"resource_type"`
	ResourceKey     string     `json:"resource_key"`
	Status          DebtStatus `json:"status"`
	DueAt           time.Time  `json:"due_at"`
	CollectedAt     time.Time  `json:"collected_at,omitempty"`
	OverdueAt       time.Time  `json:"overdue_at,omitempty"`
	WriteOffAt      time.Time  `json:"write_off_at,omitempty"`
	SourceEventID   int64      `json:"source_event_id,omitempty"`
	CollectAttempts int        `json:"collect_attempts"`
	LastCollectAt   time.Time  `json:"last_collect_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type DebtLedgerEvent struct {
	ID           int64         `json:"id"`
	DebtID       int64         `json:"debt_id,omitempty"`
	Debtor       string        `json:"debtor"`
	Creditor     string        `json:"creditor"`
	EventType    DebtEventType `json:"event_type"`
	Amount       int           `json:"amount"`
	ResourceType string        `json:"resource_type"`
	ResourceKey  string        `json:"resource_key"`
	Detail       string        `json:"detail,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
}

type RestrictionType string

const (
	RestrictionTypeReject   RestrictionType = "reject"
	RestrictionTypeDegrade  RestrictionType = "degrade"
	RestrictionTypeThrottle RestrictionType = "throttle"
)

type RestrictionScope string

const (
	RestrictionScopeLock          RestrictionScope = "lock"
	RestrictionScopeToken         RestrictionScope = "token"
	RestrictionScopeOrchestration RestrictionScope = "orchestration"
	RestrictionScopeAll           RestrictionScope = "all"
)

type DebtRestriction struct {
	ID               int64            `json:"id"`
	CallerID         string           `json:"caller_id"`
	RestrictionType  RestrictionType  `json:"restriction_type"`
	Scope            RestrictionScope `json:"scope"`
	OverdueThreshold int              `json:"overdue_threshold"`
	Reason           string           `json:"reason"`
	Active           bool             `json:"active"`
	LiftedAt         time.Time        `json:"lifted_at,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
}

type LiquidationRule struct {
	ID                int64     `json:"id"`
	CallerID          string    `json:"caller_id"`
	GracePeriodSec    int       `json:"grace_period_sec"`
	OverdueThreshold  int       `json:"overdue_threshold"`
	RestrictionType   string    `json:"restriction_type"`
	RestrictionScope  string    `json:"restriction_scope"`
	MaxCollectRetries int       `json:"max_collect_retries"`
	ProtectionAfter   int       `json:"protection_after"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type CreateLiquidationRuleRequest struct {
	CallerID          string `json:"caller_id"`
	GracePeriodSec    int    `json:"grace_period_sec" binding:"required,min=1"`
	OverdueThreshold  int    `json:"overdue_threshold" binding:"required,min=1"`
	RestrictionType   string `json:"restriction_type" binding:"required"`
	RestrictionScope  string `json:"restriction_scope" binding:"required"`
	MaxCollectRetries int    `json:"max_collect_retries" binding:"required,min=1"`
	ProtectionAfter   int    `json:"protection_after" binding:"required,min=1"`
}

type CallerDebtSummary struct {
	CallerID          string            `json:"caller_id"`
	TotalDebt         int               `json:"total_debt"`
	TotalCredit       int               `json:"total_credit"`
	ActiveDebts       int               `json:"active_debts"`
	OverdueDebts      int               `json:"overdue_debts"`
	CollectedDebts    int               `json:"collected_debts"`
	Restricted        bool              `json:"restricted"`
	Restrictions      []DebtRestriction `json:"restrictions,omitempty"`
	LastCollectResult string            `json:"last_collect_result,omitempty"`
	AffectedResources []string          `json:"affected_resources,omitempty"`
}

type DebtTimelineEntry struct {
	ID        int64         `json:"id"`
	EventType DebtEventType `json:"event_type"`
	Amount    int           `json:"amount"`
	Detail    string        `json:"detail"`
	CreatedAt time.Time     `json:"created_at"`
}

type CheckRestrictionResult struct {
	Restricted      bool             `json:"restricted"`
	RestrictionType RestrictionType  `json:"restriction_type,omitempty"`
	Scope           RestrictionScope `json:"scope,omitempty"`
	Reason          string           `json:"reason,omitempty"`
}

type LiquidationAuditEntry struct {
	ID        int64     `json:"id"`
	DebtID    int64     `json:"debt_id"`
	Debtor    string    `json:"debtor"`
	Creditor  string    `json:"creditor"`
	Action    string    `json:"action"`
	Amount    int       `json:"amount"`
	Success   bool      `json:"success"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

type ShadowRuleCategory string

const (
	ShadowRuleLockDependency ShadowRuleCategory = "lock_dependency"
	ShadowRuleRateLimit      ShadowRuleCategory = "rate_limit"
	ShadowRuleReservation    ShadowRuleCategory = "reservation"
	ShadowRuleCircuitBreaker ShadowRuleCategory = "circuit_breaker"
	ShadowRuleDebt           ShadowRuleCategory = "debt"
)

type ShadowPlan struct {
	ID              int64            `json:"id"`
	Name            string           `json:"name"`
	Description     string           `json:"description,omitempty"`
	Status          ShadowPlanStatus `json:"status"`
	Mode            string           `json:"mode"`
	AuditLogStartID int64            `json:"audit_log_start_id,omitempty"`
	AuditLogEndID   int64            `json:"audit_log_end_id,omitempty"`
	MirrorUntil     time.Time        `json:"mirror_until,omitempty"`
	AppliedAt       time.Time        `json:"applied_at,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

type ShadowConfigOverride struct {
	ID        int64              `json:"id"`
	PlanID    int64              `json:"plan_id"`
	Category  ShadowRuleCategory `json:"category"`
	TargetKey string             `json:"target_key"`
	Field     string             `json:"field"`
	OrigValue string             `json:"orig_value"`
	NewValue  string             `json:"new_value"`
	CreatedAt time.Time          `json:"created_at"`
}

type ShadowDiffRecord struct {
	ID              int64              `json:"id"`
	PlanID          int64              `json:"plan_id"`
	AuditLogID      int64              `json:"audit_log_id,omitempty"`
	RequestCaller   string             `json:"request_caller"`
	RequestOp       string             `json:"request_op"`
	RequestResource string             `json:"request_resource"`
	LiveDecision    ShadowDecision     `json:"live_decision"`
	ShadowDecision  ShadowDecision     `json:"shadow_decision"`
	RuleCategory    ShadowRuleCategory `json:"rule_category"`
	Detail          string             `json:"detail,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
}

type CreateShadowPlanRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Mode        string `json:"mode" binding:"required"`
	MirrorSec   int    `json:"mirror_sec"`
}

type UpdateShadowOverrideRequest struct {
	Category  ShadowRuleCategory `json:"category" binding:"required"`
	TargetKey string             `json:"target_key" binding:"required"`
	Field     string             `json:"field" binding:"required"`
	NewValue  string             `json:"new_value" binding:"required"`
}

type ShadowDiffStats struct {
	PlanID             int64                        `json:"plan_id"`
	TotalDiffs         int64                        `json:"total_diffs"`
	ByCategory         map[ShadowRuleCategory]int64 `json:"by_category"`
	ByDecisionPair     map[string]int64             `json:"by_decision_pair"`
	TopCallers         []ShadowCallerImpact         `json:"top_callers"`
	TopResources       []ShadowResourceImpact       `json:"top_resources"`
	TopConflictReasons []ShadowConflictReason       `json:"top_conflict_reasons"`
}

type ShadowCallerImpact struct {
	CallerID  string `json:"caller_id"`
	DiffCount int64  `json:"diff_count"`
}

type ShadowResourceImpact struct {
	Resource  string `json:"resource"`
	DiffCount int64  `json:"diff_count"`
}

type ShadowConflictReason struct {
	Reason   string             `json:"reason"`
	Category ShadowRuleCategory `json:"category"`
	Count    int64              `json:"count"`
}

type HandoverStatus string

const (
	HandoverStatusCreated    HandoverStatus = "created"
	HandoverStatusPreChecked HandoverStatus = "prechecked"
	HandoverStatusPending    HandoverStatus = "pending_receive"
	HandoverStatusCompleted  HandoverStatus = "completed"
	HandoverStatusCancelled  HandoverStatus = "cancelled"
	HandoverStatusRejected   HandoverStatus = "rejected"
)

type HandoverResourceType string

const (
	HandoverResourceLock        HandoverResourceType = "lock"
	HandoverResourceQuota       HandoverResourceType = "quota"
	HandoverResourceOrchTx      HandoverResourceType = "orch_tx"
	HandoverResourceTopology    HandoverResourceType = "topology_subtree"
	HandoverResourceReservation HandoverResourceType = "reservation"
)

type HandoverPreCheckStatus string

const (
	PreCheckOK       HandoverPreCheckStatus = "ok"
	PreCheckConflict HandoverPreCheckStatus = "conflict"
	PreCheckBlocked  HandoverPreCheckStatus = "blocked"
	PreCheckNotFound HandoverPreCheckStatus = "not_found"
)

type HandoverResourceItem struct {
	ID             int64                  `json:"id"`
	HandoverID     int64                  `json:"handover_id,omitempty"`
	ResourceType   HandoverResourceType   `json:"resource_type"`
	ResourceKey    string                 `json:"resource_key"`
	ResourceName   string                 `json:"resource_name,omitempty"`
	PreCheckStatus HandoverPreCheckStatus `json:"precheck_status"`
	PreCheckDetail string                 `json:"precheck_detail,omitempty"`
	CurrentHolder  string                 `json:"current_holder,omitempty"`
	Snapshot       string                 `json:"snapshot,omitempty"`
	Executed       bool                   `json:"executed"`
	RolledBack     bool                   `json:"rolled_back"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

type HandoverTimelineEntry struct {
	ID         int64          `json:"id"`
	HandoverID int64          `json:"handover_id,omitempty"`
	Status     HandoverStatus `json:"status"`
	Operator   string         `json:"operator"`
	Detail     string         `json:"detail,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

type Handover struct {
	ID                int64                   `json:"id"`
	FromCaller        string                  `json:"from_caller"`
	ToCaller          string                  `json:"to_caller"`
	Status            HandoverStatus          `json:"status"`
	Initiator         string                  `json:"initiator"`
	Description       string                  `json:"description,omitempty"`
	NeedConfirm       bool                    `json:"need_confirm"`
	ConfirmTimeoutSec int                     `json:"confirm_timeout_sec"`
	ConfirmDeadline   *time.Time              `json:"confirm_deadline,omitempty"`
	ConfirmedAt       *time.Time              `json:"confirmed_at,omitempty"`
	CancelledAt       *time.Time              `json:"cancelled_at,omitempty"`
	CompletedAt       *time.Time              `json:"completed_at,omitempty"`
	CancelReason      string                  `json:"cancel_reason,omitempty"`
	Resources         []HandoverResourceItem  `json:"resources,omitempty"`
	Timeline          []HandoverTimelineEntry `json:"timeline,omitempty"`
	CreatedAt         time.Time               `json:"created_at"`
	UpdatedAt         time.Time               `json:"updated_at"`
}

type CreateHandoverRequest struct {
	FromCaller        string   `json:"from_caller" binding:"required"`
	ToCaller          string   `json:"to_caller" binding:"required"`
	Initiator         string   `json:"initiator" binding:"required"`
	Description       string   `json:"description"`
	NeedConfirm       bool     `json:"need_confirm"`
	ConfirmTimeoutSec int      `json:"confirm_timeout_sec"`
	LockNames         []string `json:"lock_names,omitempty"`
	QuotaCallers      []string `json:"quota_callers,omitempty"`
	OrchTxIDs         []string `json:"orch_tx_ids,omitempty"`
	TopologyRoots     []string `json:"topology_roots,omitempty"`
	ReservationIDs    []int64  `json:"reservation_ids,omitempty"`
}

type PreCheckHandoverResult struct {
	HandoverID    int64                  `json:"handover_id"`
	CanProceed    bool                   `json:"can_proceed"`
	TotalCount    int                    `json:"total_count"`
	OKCount       int                    `json:"ok_count"`
	ConflictCount int                    `json:"conflict_count"`
	BlockedCount  int                    `json:"blocked_count"`
	NotFoundCount int                    `json:"not_found_count"`
	Resources     []HandoverResourceItem `json:"resources"`
}

type ConfirmHandoverRequest struct {
	Operator string `json:"operator" binding:"required"`
	Accept   bool   `json:"accept"`
	Reason   string `json:"reason,omitempty"`
}

type CancelHandoverRequest struct {
	Operator string `json:"operator" binding:"required"`
	Reason   string `json:"reason"`
}

type CallerHandoverSummary struct {
	CallerID        string `json:"caller_id"`
	TransferringOut int    `json:"transferring_out"`
	TransferringIn  int    `json:"transferring_in"`
	Completed       int    `json:"completed"`
	Cancelled       int    `json:"cancelled"`
}

type HeartbeatStatus string

const (
	HeartbeatStatusHealthy   HeartbeatStatus = "healthy"
	HeartbeatStatusSuspect   HeartbeatStatus = "suspect"
	HeartbeatStatusLost      HeartbeatStatus = "lost"
	HeartbeatStatusFrozen    HeartbeatStatus = "frozen"
	HeartbeatStatusRecovered HeartbeatStatus = "recovered"
)

type DisposalStrategy string

const (
	StrategyReleaseAll  DisposalStrategy = "release_all"
	StrategyReleaseLock DisposalStrategy = "release_lock_only"
	StrategyFreeze      DisposalStrategy = "freeze"
)

type HeartbeatRegistration struct {
	ID              int64            `json:"id"`
	CallerID        string           `json:"caller_id"`
	GroupName       string           `json:"group_name,omitempty"`
	IntervalSec     int              `json:"interval_sec"`
	MaxMissed       int              `json:"max_missed"`
	Strategy        DisposalStrategy `json:"strategy"`
	LastHeartbeatAt time.Time        `json:"last_heartbeat_at"`
	NextExpectedAt  time.Time        `json:"next_expected_at"`
	MissedCount     int              `json:"missed_count"`
	Status          HeartbeatStatus  `json:"status"`
	FrozenAt        *time.Time       `json:"frozen_at,omitempty"`
	LostAt          *time.Time       `json:"lost_at,omitempty"`
	RecoveredAt     *time.Time       `json:"recovered_at,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

type HeartbeatEvent struct {
	ID         int64           `json:"id"`
	CallerID   string          `json:"caller_id"`
	EventType  string          `json:"event_type"`
	FromStatus HeartbeatStatus `json:"from_status,omitempty"`
	ToStatus   HeartbeatStatus `json:"to_status,omitempty"`
	Detail     string          `json:"detail,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

type FrozenResource struct {
	ID           int64      `json:"id"`
	CallerID     string     `json:"caller_id"`
	ResourceType string     `json:"resource_type"`
	ResourceKey  string     `json:"resource_key"`
	FrozenAt     time.Time  `json:"frozen_at"`
	ReleasedAt   *time.Time `json:"released_at,omitempty"`
}

type RegisterHeartbeatRequest struct {
	CallerID    string           `json:"caller_id" binding:"required"`
	IntervalSec int              `json:"interval_sec" binding:"required,min=1"`
	MaxMissed   int              `json:"max_missed" binding:"required,min=1"`
	Strategy    DisposalStrategy `json:"strategy" binding:"required"`
}

type HeartbeatStatusInfo struct {
	CallerID         string           `json:"caller_id"`
	Status           HeartbeatStatus  `json:"status"`
	IntervalSec      int              `json:"interval_sec"`
	MaxMissed        int              `json:"max_missed"`
	LastHeartbeatAt  time.Time        `json:"last_heartbeat_at"`
	NextExpectedAt   time.Time        `json:"next_expected_at"`
	MissedCount      int              `json:"missed_count"`
	Strategy         DisposalStrategy `json:"strategy"`
	SecondsSinceLast float64          `json:"seconds_since_last"`
}

type HeartbeatReport struct {
	RegisteredCount int `json:"registered_count"`
	HealthyCount    int `json:"healthy_count"`
	SuspectCount    int `json:"suspect_count"`
	LostCount       int `json:"lost_count"`
	FrozenCount     int `json:"frozen_count"`
	RecoveredCount  int `json:"recovered_count"`
}

type HeartbeatGroupStatus string

const (
	HeartbeatGroupHealthy   HeartbeatGroupStatus = "healthy"
	HeartbeatGroupUnhealthy HeartbeatGroupStatus = "unhealthy"
	HeartbeatGroupDegraded  HeartbeatGroupStatus = "degraded"
)

type HeartbeatGroup struct {
	ID                int64                `json:"id"`
	Name              string               `json:"name"`
	SurvivalThreshold int                  `json:"survival_threshold"`
	Status            HeartbeatGroupStatus `json:"status"`
	AliveCount        int                  `json:"alive_count"`
	TotalCount        int                  `json:"total_count"`
	Degraded          bool                 `json:"degraded"`
	DegradedReason    string               `json:"degraded_reason,omitempty"`
	DegradedAt        *time.Time           `json:"degraded_at,omitempty"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
}

type HeartbeatGroupMember struct {
	CallerID        string          `json:"caller_id"`
	Status          HeartbeatStatus `json:"status"`
	LastHeartbeatAt time.Time       `json:"last_heartbeat_at"`
}

type HeartbeatGroupInfo struct {
	ID                int64                  `json:"id"`
	Name              string                 `json:"name"`
	SurvivalThreshold int                    `json:"survival_threshold"`
	Status            HeartbeatGroupStatus   `json:"status"`
	AliveCount        int                    `json:"alive_count"`
	TotalCount        int                    `json:"total_count"`
	Degraded          bool                   `json:"degraded"`
	DegradedReason    string                 `json:"degraded_reason,omitempty"`
	DegradedAt        *time.Time             `json:"degraded_at,omitempty"`
	Members           []HeartbeatGroupMember `json:"members"`
	DependsOn         []string               `json:"depends_on,omitempty"`
	DependedBy        []string               `json:"depended_by,omitempty"`
	CreatedAt         time.Time              `json:"created_at"`
}

type HeartbeatGroupDependency struct {
	ID        int64     `json:"id"`
	GroupName string    `json:"group_name"`
	DependsOn string    `json:"depends_on"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateHeartbeatGroupRequest struct {
	Name              string `json:"name" binding:"required"`
	SurvivalThreshold int    `json:"survival_threshold" binding:"required,min=1"`
}

type RegisterHeartbeatWithGroupRequest struct {
	CallerID    string           `json:"caller_id" binding:"required"`
	GroupName   string           `json:"group_name,omitempty"`
	IntervalSec int              `json:"interval_sec" binding:"required,min=1"`
	MaxMissed   int              `json:"max_missed" binding:"required,min=1"`
	Strategy    DisposalStrategy `json:"strategy" binding:"required"`
}

type GroupDependencyRequest struct {
	GroupName string `json:"group_name" binding:"required"`
	DependsOn string `json:"depends_on" binding:"required"`
}

type GroupStatusInfo struct {
	Name              string               `json:"name"`
	Status            HeartbeatGroupStatus `json:"status"`
	SurvivalThreshold int                  `json:"survival_threshold"`
	AliveCount        int                  `json:"alive_count"`
	TotalCount        int                  `json:"total_count"`
	Degraded          bool                 `json:"degraded"`
	DegradedReason    string               `json:"degraded_reason,omitempty"`
}

var ErrGroupDegraded = fmt.Errorf("group is degraded, cannot acquire new locks")

type LockContentionMinuteStat struct {
	ID           int64     `json:"id"`
	LockName     string    `json:"lock_name"`
	MinuteBucket time.Time `json:"minute_bucket"`
	RequestCount int64     `json:"request_count"`
	WaitCount    int64     `json:"wait_count"`
	TotalWaitMs  int64     `json:"total_wait_ms"`
	MaxWaitMs    int64     `json:"max_wait_ms"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type LockHeatInfo struct {
	LockName        string  `json:"lock_name"`
	RequestCount    int64   `json:"request_count"`
	WaitCount       int64   `json:"wait_count"`
	AvgWaitMs       float64 `json:"avg_wait_ms"`
	MaxWaitMs       int64   `json:"max_wait_ms"`
	CurrentQueueLen int     `json:"current_queue_len"`
	HeatScore       float64 `json:"heat_score"`
}

type HotspotAlertEvent struct {
	ID              int64      `json:"id"`
	LockName        string     `json:"lock_name"`
	AvgWaitMs       float64    `json:"avg_wait_ms"`
	ThresholdMs     float64    `json:"threshold_ms"`
	RequestCount    int64      `json:"request_count"`
	WaitCount       int64      `json:"wait_count"`
	MaxWaitMs       int64      `json:"max_wait_ms"`
	CurrentQueueLen int        `json:"current_queue_len"`
	WindowMinutes   int        `json:"window_minutes"`
	AlertType       string     `json:"alert_type"`
	Detail          string     `json:"detail,omitempty"`
	Acknowledged    bool       `json:"acknowledged"`
	AcknowledgedAt  *time.Time `json:"acknowledged_at,omitempty"`
	AcknowledgedBy  string     `json:"acknowledged_by,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

type HeatmapConfig struct {
	WindowMinutes       int            `json:"window_minutes"`
	AlertThresholdMs    float64        `json:"alert_threshold_ms"`
	AlertSuppressMin    int            `json:"alert_suppress_min"`
	TopN                int            `json:"top_n"`
	HistoryRetentionMin int            `json:"history_retention_min"`
	Cooldown            CooldownConfig `json:"cooldown"`
}

type LockTrendPoint struct {
	MinuteBucket time.Time `json:"minute_bucket"`
	RequestCount int64     `json:"request_count"`
	WaitCount    int64     `json:"wait_count"`
	AvgWaitMs    float64   `json:"avg_wait_ms"`
	MaxWaitMs    int64     `json:"max_wait_ms"`
}

type HeatmapGlobalStats struct {
	TotalLocks         int           `json:"total_locks"`
	HotLocks           int           `json:"hot_locks"`
	TotalRequests      int64         `json:"total_requests"`
	TotalWaits         int64         `json:"total_waits"`
	OverallAvgWaitMs   float64       `json:"overall_avg_wait_ms"`
	ActiveAlerts       int           `json:"active_alerts"`
	ActiveCooldowns    int           `json:"active_cooldowns"`
	TotalCooldownToday int64         `json:"total_cooldown_today"`
	Config             HeatmapConfig `json:"config"`
}

type UpdateHeatmapConfigRequest struct {
	WindowMinutes       *int                         `json:"window_minutes"`
	AlertThresholdMs    *float64                     `json:"alert_threshold_ms"`
	AlertSuppressMin    *int                         `json:"alert_suppress_min"`
	TopN                *int                         `json:"top_n"`
	HistoryRetentionMin *int                         `json:"history_retention_min"`
	Cooldown            *UpdateCooldownConfigRequest `json:"cooldown"`
}

type AcknowledgeAlertRequest struct {
	AcknowledgedBy string `json:"acknowledged_by" binding:"required"`
}

type CooldownTriggerType string

const (
	CooldownTriggerAuto   CooldownTriggerType = "auto"
	CooldownTriggerManual CooldownTriggerType = "manual"
)

type CooldownStatus string

const (
	CooldownStatusActive   CooldownStatus = "active"
	CooldownStatusResolved CooldownStatus = "resolved"
)

type LockCooldownState struct {
	ID                   int64               `json:"id"`
	LockName             string              `json:"lock_name"`
	Status               CooldownStatus      `json:"status"`
	TriggerType          CooldownTriggerType `json:"trigger_type"`
	OriginalLeaseSec     int                 `json:"original_lease_sec"`
	CooldownLeaseSec     int                 `json:"cooldown_lease_sec"`
	LeasesShortened      int64               `json:"leases_shortened"`
	ConsecutiveHotCycles int                 `json:"consecutive_hot_cycles"`
	AvgWaitMsAtStart     float64             `json:"avg_wait_ms_at_start"`
	ThresholdMsAtStart   float64             `json:"threshold_ms_at_start"`
	StartedAt            time.Time           `json:"started_at"`
	ResolvedAt           *time.Time          `json:"resolved_at,omitempty"`
	ResolveReason        string              `json:"resolve_reason,omitempty"`
	CreatedAt            time.Time           `json:"created_at"`
	UpdatedAt            time.Time           `json:"updated_at"`
}

type CooldownHistoryRecord struct {
	ID               int64               `json:"id"`
	LockName         string              `json:"lock_name"`
	TriggerType      CooldownTriggerType `json:"trigger_type"`
	OriginalLeaseSec int                 `json:"original_lease_sec"`
	CooldownLeaseSec int                 `json:"cooldown_lease_sec"`
	LeasesShortened  int64               `json:"leases_shortened"`
	AvgWaitMsAtStart float64             `json:"avg_wait_ms_at_start"`
	AvgWaitMsAtEnd   float64             `json:"avg_wait_ms_at_end"`
	ThresholdMs      float64             `json:"threshold_ms"`
	DurationSec      float64             `json:"duration_sec"`
	StartedAt        time.Time           `json:"started_at"`
	EndedAt          time.Time           `json:"ended_at"`
	ResolveReason    string              `json:"resolve_reason,omitempty"`
	CreatedAt        time.Time           `json:"created_at"`
}

type CooldownConfig struct {
	Enabled                  bool    `json:"enabled"`
	ConsecutiveHotCycles     int     `json:"consecutive_hot_cycles"`
	CooldownLeaseSec         int     `json:"cooldown_lease_sec"`
	CooldownLeaseMinPct      float64 `json:"cooldown_lease_min_pct"`
	ResolveThresholdMs       float64 `json:"resolve_threshold_ms"`
	ResolveConsecutiveCycles int     `json:"resolve_consecutive_cycles"`
	MaxCooldownSec           int     `json:"max_cooldown_sec"`
	AcceleratedGrant         bool    `json:"accelerated_grant"`
}

type CooldownStatusInfo struct {
	LockName             string              `json:"lock_name"`
	Status               CooldownStatus      `json:"status"`
	TriggerType          CooldownTriggerType `json:"trigger_type"`
	OriginalLeaseSec     int                 `json:"original_lease_sec"`
	CooldownLeaseSec     int                 `json:"cooldown_lease_sec"`
	LeasesShortened      int64               `json:"leases_shortened"`
	ConsecutiveHotCycles int                 `json:"consecutive_hot_cycles"`
	AvgWaitMsAtStart     float64             `json:"avg_wait_ms_at_start"`
	CurrentAvgWaitMs     float64             `json:"current_avg_wait_ms"`
	ThresholdMs          float64             `json:"threshold_ms"`
	StartedAt            time.Time           `json:"started_at"`
	DurationSec          float64             `json:"duration_sec"`
	CurrentHolder        string              `json:"current_holder,omitempty"`
	RemainingSec         float64             `json:"remaining_sec,omitempty"`
	WaitQueueLen         int                 `json:"wait_queue_len"`
}

type CooldownSuggestion struct {
	LockName          string  `json:"lock_name"`
	AvgWaitMs         float64 `json:"avg_wait_ms"`
	ThresholdMs       float64 `json:"threshold_ms"`
	ConsecutiveHot    int     `json:"consecutive_hot"`
	SuggestedLeaseSec int     `json:"suggested_lease_sec"`
	CurrentLeaseSec   int     `json:"current_lease_sec"`
	QueueLen          int     `json:"queue_len"`
	Reason            string  `json:"reason"`
}

type UpdateCooldownConfigRequest struct {
	Enabled                  *bool    `json:"enabled"`
	ConsecutiveHotCycles     *int     `json:"consecutive_hot_cycles"`
	CooldownLeaseSec         *int     `json:"cooldown_lease_sec"`
	CooldownLeaseMinPct      *float64 `json:"cooldown_lease_min_pct"`
	ResolveThresholdMs       *float64 `json:"resolve_threshold_ms"`
	ResolveConsecutiveCycles *int     `json:"resolve_consecutive_cycles"`
	MaxCooldownSec           *int     `json:"max_cooldown_sec"`
	AcceleratedGrant         *bool    `json:"accelerated_grant"`
}

type ManualCooldownRequest struct {
	LockName         string `json:"lock_name" binding:"required"`
	CooldownLeaseSec int    `json:"cooldown_lease_sec" binding:"required,min=1"`
	Reason           string `json:"reason"`
}

const (
	OpCooldownStart OperationType = "cooldown_start"
	OpCooldownEnd   OperationType = "cooldown_end"
)

type LockBudgetConfig struct {
	ID                 int64     `json:"id"`
	CallerID           string    `json:"caller_id"`
	BudgetLimit        int       `json:"budget_limit"`
	PeriodSec          int       `json:"period_sec"`
	WarningPct         int       `json:"warning_pct"`
	OverdraftLimit     int       `json:"overdraft_limit"`
	MaxConcurrentLocks int       `json:"max_concurrent_locks"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type LockBudgetStatus struct {
	CallerID              string    `json:"caller_id"`
	BudgetLimit           int       `json:"budget_limit"`
	PeriodSec             int       `json:"period_sec"`
	ConsumedUnits         int       `json:"consumed_units"`
	RemainingUnits        int       `json:"remaining_units"`
	WarningPct            int       `json:"warning_pct"`
	WarningTriggered      bool      `json:"warning_triggered"`
	Exhausted             bool      `json:"exhausted"`
	OverdraftLimit        int       `json:"overdraft_limit"`
	CurrentOverdraft      int       `json:"current_overdraft"`
	InOverdraft           bool      `json:"in_overdraft"`
	OverdraftPenaltyUnits int       `json:"overdraft_penalty_units"`
	NextPeriodDeduction   int       `json:"next_period_deduction"`
	TransferredIn         int       `json:"transferred_in"`
	TransferredOut        int       `json:"transferred_out"`
	PeriodStartAt         time.Time `json:"period_start_at"`
	PeriodEndAt           time.Time `json:"period_end_at"`
	ActiveLocks           int       `json:"active_locks"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type HeldLockDetail struct {
	LockName       string    `json:"lock_name"`
	AcquiredAt     time.Time `json:"acquired_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	HeldSec        float64   `json:"held_sec"`
	UnitsConsumed  int       `json:"units_consumed"`
	UnitsProjected int       `json:"units_projected"`
}

type BudgetExhaustEvent struct {
	ID             int64     `json:"id"`
	CallerID       string    `json:"caller_id"`
	ConsumedUnits  int       `json:"consumed_units"`
	BudgetLimit    int       `json:"budget_limit"`
	PeriodStartAt  time.Time `json:"period_start_at"`
	PeriodEndAt    time.Time `json:"period_end_at"`
	AttemptedLock  string    `json:"attempted_lock,omitempty"`
	UnitsRequested int       `json:"units_requested,omitempty"`
	Detail         string    `json:"detail,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type BudgetPeriodSummary struct {
	CallerID           string    `json:"caller_id"`
	PeriodStartAt      time.Time `json:"period_start_at"`
	PeriodEndAt        time.Time `json:"period_end_at"`
	BudgetLimit        int       `json:"budget_limit"`
	OverdraftLimit     int       `json:"overdraft_limit"`
	TotalConsumed      int       `json:"total_consumed"`
	OverdraftUsed      int       `json:"overdraft_used"`
	OverdraftPenalty   int       `json:"overdraft_penalty"`
	TransferredIn      int       `json:"transferred_in"`
	TransferredOut     int       `json:"transferred_out"`
	CarryOverDeduction int       `json:"carry_over_deduction"`
	PeakConcurrent     int       `json:"peak_concurrent"`
	LockCount          int       `json:"lock_count"`
	ExhaustEvents      int       `json:"exhaust_events"`
}

type SetBudgetRequest struct {
	CallerID       string `json:"caller_id" binding:"required"`
	BudgetLimit    int    `json:"budget_limit" binding:"required,min=1"`
	PeriodSec      int    `json:"period_sec" binding:"required,min=1"`
	WarningPct     int    `json:"warning_pct" binding:"min=0,max=100"`
	OverdraftLimit int    `json:"overdraft_limit" binding:"min=0"`
}

type BudgetAcquireCheckResult struct {
	Allowed          bool   `json:"allowed"`
	ConsumedUnits    int    `json:"consumed_units"`
	RemainingUnits   int    `json:"remaining_units"`
	BudgetLimit      int    `json:"budget_limit"`
	OverdraftLimit   int    `json:"overdraft_limit"`
	CurrentOverdraft int    `json:"current_overdraft"`
	UsingOverdraft   bool   `json:"using_overdraft"`
	Reason           string `json:"reason,omitempty"`
	ArrearsRejected  bool   `json:"arrears_rejected,omitempty"`
	ArrearsAmount    int    `json:"arrears_amount,omitempty"`
}

func (r *BudgetAcquireCheckResult) BudgetRejected() bool {
	return !r.Allowed
}

type CallerBudgetStatusInfo struct {
	Config    *LockBudgetConfig `json:"config,omitempty"`
	Status    *LockBudgetStatus `json:"status,omitempty"`
	HeldLocks []HeldLockDetail  `json:"held_locks,omitempty"`
}

type GlobalBudgetStats struct {
	TotalCallers         int   `json:"total_callers"`
	TotalActiveLocks     int   `json:"total_active_locks"`
	TotalConsumedToday   int64 `json:"total_consumed_today"`
	ExhaustEvents24h     int64 `json:"exhaust_events_24h"`
	CallersOverBudget    int   `json:"callers_over_budget"`
	CallersNearBudget    int   `json:"callers_near_budget"`
	CallersInOverdraft   int   `json:"callers_in_overdraft"`
	TotalOverdraftAmount int   `json:"total_overdraft_amount"`
}

type BudgetTransferRecord struct {
	ID         int64     `json:"id"`
	FromCaller string    `json:"from_caller"`
	ToCaller   string    `json:"to_caller"`
	Amount     int       `json:"amount"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type BudgetTransferRequest struct {
	FromCaller string `json:"from_caller" binding:"required"`
	ToCaller   string `json:"to_caller" binding:"required"`
	Amount     int    `json:"amount" binding:"required,min=1"`
	Reason     string `json:"reason,omitempty"`
}

type BudgetTransferResult struct {
	Success bool                  `json:"success"`
	Message string                `json:"message,omitempty"`
	Record  *BudgetTransferRecord `json:"record,omitempty"`
}

type BudgetOverdraftInfo struct {
	CallerID              string    `json:"caller_id"`
	CurrentOverdraft      int       `json:"current_overdraft"`
	OverdraftLimit        int       `json:"overdraft_limit"`
	OverdraftRemaining    int       `json:"overdraft_remaining"`
	OverdraftPenaltyUnits int       `json:"overdraft_penalty_units"`
	NextPeriodDeduction   int       `json:"next_period_deduction"`
	InOverdraft           bool      `json:"in_overdraft"`
	ActiveLocks           int       `json:"active_locks"`
	PeriodStartAt         time.Time `json:"period_start_at"`
	PeriodEndAt           time.Time `json:"period_end_at"`
}

type BudgetOverdraftListResult struct {
	TotalInOverdraft     int                   `json:"total_in_overdraft"`
	TotalOverdraftAmount int                   `json:"total_overdraft_amount"`
	Items                []BudgetOverdraftInfo `json:"items"`
}

type BudgetNextPeriodDeductionInfo struct {
	CallerID              string    `json:"caller_id"`
	NextPeriodDeduction   int       `json:"next_period_deduction"`
	CurrentOverdraft      int       `json:"current_overdraft"`
	OverdraftPenaltyUnits int       `json:"overdraft_penalty_units"`
	PeriodEndAt           time.Time `json:"period_end_at"`
	BudgetLimit           int       `json:"budget_limit"`
	ProjectedRemaining    int       `json:"projected_remaining"`
}

type BudgetTransferListQuery struct {
	CallerID   string `form:"caller_id"`
	FromCaller string `form:"from_caller"`
	ToCaller   string `form:"to_caller"`
	Limit      int    `form:"limit,default=50"`
	Offset     int    `form:"offset,default=0"`
}

type BudgetTransferListResult struct {
	Total  int64                  `json:"total"`
	Items  []BudgetTransferRecord `json:"items"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

const OverdraftPenaltyMultiplier = 1.5

type BudgetBillStatus string

const (
	BudgetBillStatusFinalized BudgetBillStatus = "finalized"
)

type BudgetTransferDirection string

const (
	BudgetTransferOut BudgetTransferDirection = "out"
	BudgetTransferIn  BudgetTransferDirection = "in"
)

type BudgetSettlementBill struct {
	ID                    int64            `json:"id"`
	CallerID              string           `json:"caller_id"`
	PeriodStartAt         time.Time        `json:"period_start_at"`
	PeriodEndAt           time.Time        `json:"period_end_at"`
	BudgetLimit           int              `json:"budget_limit"`
	OverdraftLimit        int              `json:"overdraft_limit"`
	NormalConsumption     int              `json:"normal_consumption"`
	OverdraftConsumption  int              `json:"overdraft_consumption"`
	OverdraftPenalty      int              `json:"overdraft_penalty"`
	TotalConsumption      int              `json:"total_consumption"`
	TransferredIn         int              `json:"transferred_in"`
	TransferredOut        int              `json:"transferred_out"`
	EndingBalance         int              `json:"ending_balance"`
	HadOverdraft          bool             `json:"had_overdraft"`
	PeakOverdraft         int              `json:"peak_overdraft"`
	PeakConcurrent        int              `json:"peak_concurrent"`
	ExhaustEvents         int              `json:"exhaust_events"`
	CarryOverToNextPeriod int              `json:"carry_over_to_next_period"`
	Status                BudgetBillStatus `json:"status"`
	CreatedAt             time.Time        `json:"created_at"`
}

type BudgetBillTransferDetail struct {
	ID         int64                   `json:"id"`
	BillID     int64                   `json:"bill_id"`
	CallerID   string                  `json:"caller_id"`
	Direction  BudgetTransferDirection `json:"direction"`
	PeerCaller string                  `json:"peer_caller"`
	Amount     int                     `json:"amount"`
	Reason     string                  `json:"reason,omitempty"`
	CreatedAt  time.Time               `json:"created_at"`
}

type BudgetArrearsStatus string

const (
	BudgetArrearsStatusActive  BudgetArrearsStatus = "active"
	BudgetArrearsStatusCleared BudgetArrearsStatus = "cleared"
)

type BudgetCallerArrears struct {
	ID                    int64               `json:"id"`
	CallerID              string              `json:"caller_id"`
	ArrearsAmount         int                 `json:"arrears_amount"`
	OriginalBillID        int64               `json:"original_bill_id"`
	OriginalPeriodStartAt time.Time           `json:"original_period_start_at"`
	OriginalPeriodEndAt   time.Time           `json:"original_period_end_at"`
	Status                BudgetArrearsStatus `json:"status"`
	ClearedAt             *time.Time          `json:"cleared_at,omitempty"`
	CreatedAt             time.Time           `json:"created_at"`
	UpdatedAt             time.Time           `json:"updated_at"`
}

type BudgetArrearsCallerInfo struct {
	CallerID              string    `json:"caller_id"`
	ArrearsAmount         int       `json:"arrears_amount"`
	OriginalBillID        int64     `json:"original_bill_id"`
	OriginalPeriodStartAt time.Time `json:"original_period_start_at"`
	OriginalPeriodEndAt   time.Time `json:"original_period_end_at"`
	CurrentBudgetLimit    int       `json:"current_budget_limit"`
	CurrentPeriodStartAt  time.Time `json:"current_period_start_at"`
	CurrentPeriodEndAt    time.Time `json:"current_period_end_at"`
	CreatedAt             time.Time `json:"created_at"`
}

type BudgetArrearsListResult struct {
	TotalInArrears     int                       `json:"total_in_arrears"`
	TotalArrearsAmount int                       `json:"total_arrears_amount"`
	Items              []BudgetArrearsCallerInfo `json:"items"`
}

type BudgetBillDetailResult struct {
	Bill            *BudgetSettlementBill      `json:"bill"`
	TransferDetails []BudgetBillTransferDetail `json:"transfer_details"`
}

type BudgetBillListResult struct {
	Total  int64                  `json:"total"`
	Items  []BudgetSettlementBill `json:"items"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

type BudgetRechargeRequest struct {
	CallerID string `json:"caller_id" binding:"required"`
	Amount   int    `json:"amount" binding:"required,min=1"`
	Reason   string `json:"reason"`
}

type BudgetRechargeResult struct {
	Success          bool   `json:"success"`
	CallerID         string `json:"caller_id"`
	RechargedAmount  int    `json:"recharged_amount"`
	ArrearsCleared   int    `json:"arrears_cleared"`
	RemainingArrears int    `json:"remaining_arrears"`
	NewBudgetLimit   int    `json:"new_budget_limit"`
	Message          string `json:"message,omitempty"`
}

type BudgetBillListQuery struct {
	CallerID string `form:"caller_id"`
	Limit    int    `form:"limit,default=50"`
	Offset   int    `form:"offset,default=0"`
}

type LockBudgetRateAlertRule struct {
	ID               int64     `json:"id"`
	CallerID         string    `json:"caller_id"`
	WindowSec        int       `json:"window_sec"`
	MaxUnitsInWindow int       `json:"max_units_in_window"`
	FreezeTriggerN   int       `json:"freeze_trigger_n"`
	Enabled          bool      `json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type LockBudgetRateAlertEvent struct {
	ID               int64     `json:"id"`
	CallerID         string    `json:"caller_id"`
	WindowSec        int       `json:"window_sec"`
	MaxUnitsInWindow int       `json:"max_units_in_window"`
	ActualRate       float64   `json:"actual_rate"`
	ConsumedInWindow int       `json:"consumed_in_window"`
	Detail           string    `json:"detail,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

type LockBudgetCallerFreeze struct {
	ID               int64      `json:"id"`
	CallerID         string     `json:"caller_id"`
	FrozenAt         time.Time  `json:"frozen_at"`
	FrozenBy         string     `json:"frozen_by"`
	Reason           string     `json:"reason"`
	AlertCountBefore int        `json:"alert_count_before"`
	UnfrozenAt       *time.Time `json:"unfrozen_at,omitempty"`
	UnfrozenBy       string     `json:"unfrozen_by,omitempty"`
	UnfreezeReason   string     `json:"unfreeze_reason,omitempty"`
	Active           bool       `json:"active"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type SetRateAlertRuleRequest struct {
	CallerID         string `json:"caller_id" binding:"required"`
	WindowSec        int    `json:"window_sec" binding:"required,min=1"`
	MaxUnitsInWindow int    `json:"max_units_in_window" binding:"required,min=1"`
	FreezeTriggerN   int    `json:"freeze_trigger_n" binding:"min=1"`
	Enabled          bool   `json:"enabled"`
}

type RateAlertEventListQuery struct {
	CallerID string `form:"caller_id"`
	Limit    int    `form:"limit,default=50"`
	Offset   int    `form:"offset,default=0"`
}

type RateAlertEventListResult struct {
	Total  int64                      `json:"total"`
	Items  []LockBudgetRateAlertEvent `json:"items"`
	Limit  int                        `json:"limit"`
	Offset int                        `json:"offset"`
}

type FreezeListResult struct {
	Total int                      `json:"total"`
	Items []LockBudgetCallerFreeze `json:"items"`
}

type UnfreezeRequest struct {
	CallerID string `json:"caller_id" binding:"required"`
	Operator string `json:"operator" binding:"required"`
	Reason   string `json:"reason,omitempty"`
}

type ServiceTier string

const (
	ServiceTierGold   ServiceTier = "gold"
	ServiceTierSilver ServiceTier = "silver"
	ServiceTierBronze ServiceTier = "bronze"
)

type CallerReputation struct {
	ID                    int64       `json:"id"`
	CallerID              string      `json:"caller_id"`
	Score                 float64     `json:"score"`
	Tier                  ServiceTier `json:"tier"`
	OnTimeReleaseScore    float64     `json:"on_time_release_score"`
	OverdraftReverseScore float64     `json:"overdraft_reverse_score"`
	CircuitBreakerScore   float64     `json:"circuit_breaker_score"`
	ArrearScore           float64     `json:"arrear_score"`
	RateAlertScore        float64     `json:"rate_alert_score"`
	CalculatedAt          time.Time   `json:"calculated_at"`
	CreatedAt             time.Time   `json:"created_at"`
	UpdatedAt             time.Time   `json:"updated_at"`
}

type TierChangeEvent struct {
	ID        int64       `json:"id"`
	CallerID  string      `json:"caller_id"`
	OldTier   ServiceTier `json:"old_tier"`
	NewTier   ServiceTier `json:"new_tier"`
	Score     float64     `json:"score"`
	ChangedAt time.Time   `json:"changed_at"`
	CreatedAt time.Time   `json:"created_at"`
}

type CallerReputationDetail struct {
	CallerID              string      `json:"caller_id"`
	Score                 float64     `json:"score"`
	Tier                  ServiceTier `json:"tier"`
	OnTimeReleaseScore    float64     `json:"on_time_release_score"`
	OverdraftReverseScore float64     `json:"overdraft_reverse_score"`
	CircuitBreakerScore   float64     `json:"circuit_breaker_score"`
	ArrearScore           float64     `json:"arrear_score"`
	RateAlertScore        float64     `json:"rate_alert_score"`
	CalculatedAt          time.Time   `json:"calculated_at"`
}

type CallerReputationRanking struct {
	CallerID string      `json:"caller_id"`
	Score    float64     `json:"score"`
	Tier     ServiceTier `json:"tier"`
	Rank     int         `json:"rank"`
}

type ReputationRankingResult struct {
	Total    int                       `json:"total"`
	Rankings []CallerReputationRanking `json:"rankings"`
}

type TierChangeEventListResult struct {
	Total  int64             `json:"total"`
	Items  []TierChangeEvent `json:"items"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
}

type CallerRateStatus struct {
	CallerID          string                   `json:"caller_id"`
	Rule              *LockBudgetRateAlertRule `json:"rule,omitempty"`
	CurrentRate       float64                  `json:"current_rate"`
	ConsumedInWindow  int                      `json:"consumed_in_window"`
	WindowSec         int                      `json:"window_sec"`
	MaxUnitsInWindow  int                      `json:"max_units_in_window"`
	ConsecutiveAlerts int                      `json:"consecutive_alerts"`
	IsFrozen          bool                     `json:"is_frozen"`
	FreezeInfo        *LockBudgetCallerFreeze  `json:"freeze_info,omitempty"`
}
