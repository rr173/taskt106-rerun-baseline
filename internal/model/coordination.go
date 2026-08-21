package model

import "time"

type ResourceState string

const (
	ResourceActive   ResourceState = "active"
	ResourceDraining ResourceState = "draining"
	ResourceRetired  ResourceState = "retired"
)

type MaintenanceMode string

const (
	MaintenanceDrain MaintenanceMode = "drain"
	MaintenanceForce MaintenanceMode = "force"
)

type Resource struct {
	Path       string            `json:"path"`
	ParentPath string            `json:"parent_path,omitempty"`
	Owner      string            `json:"owner"`
	State      ResourceState     `json:"state"`
	Generation int64             `json:"generation"`
	Labels     map[string]string `json:"labels,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type ResourcePolicy struct {
	Path           string    `json:"path"`
	MaxLeaseSec    int       `json:"max_lease_sec"`
	RequiredHolder string    `json:"required_holder,omitempty"`
	Priority       int       `json:"priority"`
	RequireFencing bool      `json:"require_fencing"`
	AllowedHolders []string  `json:"allowed_holders,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type MaintenanceWindow struct {
	ID           int64           `json:"id"`
	ResourcePath string          `json:"resource_path"`
	Mode         MaintenanceMode `json:"mode"`
	StartAt      time.Time       `json:"start_at"`
	EndAt        time.Time       `json:"end_at"`
	Reason       string          `json:"reason"`
	Operator     string          `json:"operator"`
	Status       string          `json:"status"`
	CreatedAt    time.Time       `json:"created_at"`
}

type FencingToken struct {
	Token        string     `json:"token"`
	ResourcePath string     `json:"resource_path"`
	Holder       string     `json:"holder"`
	Sequence     int64      `json:"sequence"`
	IssuedAt     time.Time  `json:"issued_at"`
	ExpiresAt    time.Time  `json:"expires_at"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	RevokeReason string     `json:"revoke_reason,omitempty"`
}

type TokenValidation struct {
	Valid        bool   `json:"valid"`
	Reason       string `json:"reason,omitempty"`
	Sequence     int64  `json:"sequence,omitempty"`
	ResourcePath string `json:"resource_path,omitempty"`
	Holder       string `json:"holder,omitempty"`
}

type RecoveryCheckpoint struct {
	ID         int64      `json:"id"`
	Scope      string     `json:"scope"`
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Issues     []string   `json:"issues,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

type CoordinationEvent struct {
	ID           int64     `json:"id"`
	EventType    string    `json:"event_type"`
	ResourcePath string    `json:"resource_path,omitempty"`
	Holder       string    `json:"holder,omitempty"`
	Detail       string    `json:"detail,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type ResourceCreateRequest struct {
	Path       string            `json:"path" binding:"required"`
	Owner      string            `json:"owner" binding:"required"`
	ParentPath string            `json:"parent_path"`
	Labels     map[string]string `json:"labels"`
}

type ResourceStateRequest struct {
	State  ResourceState `json:"state" binding:"required"`
	Reason string        `json:"reason"`
}

type ResourcePolicyRequest struct {
	MaxLeaseSec    int      `json:"max_lease_sec" binding:"required,min=1"`
	RequiredHolder string   `json:"required_holder"`
	Priority       int      `json:"priority"`
	RequireFencing bool     `json:"require_fencing"`
	AllowedHolders []string `json:"allowed_holders"`
}

type MaintenanceCreateRequest struct {
	ResourcePath string          `json:"resource_path" binding:"required"`
	Mode         MaintenanceMode `json:"mode" binding:"required"`
	StartAt      time.Time       `json:"start_at" binding:"required"`
	EndAt        time.Time       `json:"end_at" binding:"required"`
	Reason       string          `json:"reason" binding:"required"`
	Operator     string          `json:"operator" binding:"required"`
}

type FencingIssueRequest struct {
	ResourcePath string `json:"resource_path" binding:"required"`
	Holder       string `json:"holder" binding:"required"`
	LeaseSec     int    `json:"lease_sec" binding:"required,min=1"`
}

type FencingValidateRequest struct {
	Token        string `json:"token" binding:"required"`
	ResourcePath string `json:"resource_path" binding:"required"`
	Holder       string `json:"holder" binding:"required"`
}
