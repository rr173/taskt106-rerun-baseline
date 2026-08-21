package api

import (
	"net/http"
	"strconv"
	"task106/internal/model"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *Handler) requireControlPlane(c *gin.Context) bool {
	if h.coordMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "coordination control plane unavailable"})
		return false
	}
	return true
}

func (h *Handler) CreateCoordinationResource(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req model.ResourceCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := h.coordMgr.Resources().Register(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"resource": item})
}

func (h *Handler) ListCoordinationResources(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	items, err := h.coordMgr.Resources().List(c.Query("root"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"resources": items})
}

func (h *Handler) SetCoordinationResourceState(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req struct {
		Path   string              `json:"path" binding:"required"`
		State  model.ResourceState `json:"state" binding:"required"`
		Reason string              `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := h.coordMgr.Resources().SetState(req.Path, req.State, req.Reason)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"resource": item})
}

func (h *Handler) SetCoordinationResourcePolicy(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req struct {
		Path string `json:"path" binding:"required"`
		model.ResourcePolicyRequest
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	policy, err := h.coordMgr.Resources().SetPolicy(req.Path, model.ResourcePolicy{MaxLeaseSec: req.MaxLeaseSec, RequiredHolder: req.RequiredHolder, Priority: req.Priority, RequireFencing: req.RequireFencing, AllowedHolders: req.AllowedHolders})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"policy": policy})
}

func (h *Handler) ListCoordinationPolicies(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"policies": h.coordMgr.Resources().ListPolicies()})
}

func (h *Handler) CreateMaintenanceWindow(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req model.MaintenanceCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	window, err := h.coordMgr.Maintenance().Create(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"window": window})
}

func (h *Handler) ListMaintenanceWindows(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	items, err := h.coordMgr.Maintenance().List(c.Query("resource_path"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"windows": items, "active": h.coordMgr.Maintenance().ActiveWindows(time.Now().UTC())})
}

func (h *Handler) CancelMaintenanceWindow(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid maintenance id"})
		return
	}
	if err := h.coordMgr.Maintenance().Cancel(id, c.Query("operator")); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"cancelled": true})
}

func (h *Handler) IssueFencingToken(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req model.FencingIssueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	token, err := h.coordMgr.Fencing().Issue(req.ResourcePath, req.Holder, req.LeaseSec, time.Now().UTC())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"token": token})
}

func (h *Handler) ValidateFencingToken(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	var req model.FencingValidateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result := h.coordMgr.ValidateToken(req.Token, req.ResourcePath, req.Holder, time.Now().UTC())
	status := http.StatusOK
	if !result.Valid {
		status = http.StatusConflict
	}
	c.JSON(status, result)
}

func (h *Handler) ListFencingTokens(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	items, err := h.coordMgr.Fencing().List(c.Query("resource_path"), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tokens": items})
}

func (h *Handler) RunRecovery(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	checkpoint, err := h.coordMgr.RunRecovery(c.DefaultQuery("scope", "manual"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"checkpoint": checkpoint})
}

func (h *Handler) ListRecoveryCheckpoints(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	items, err := h.coordMgr.Recovery().List(c.Query("scope"), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"checkpoints": items})
}

func (h *Handler) ListCoordinationEvents(c *gin.Context) {
	if !h.requireControlPlane(c) {
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	items, err := h.coordMgr.Events(c.Query("resource_path"), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": items})
}
