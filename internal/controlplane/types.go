package controlplane

import (
	"task106/internal/fencing"
	"task106/internal/maintenance"
	"task106/internal/model"
	"task106/internal/recovery"
	"task106/internal/resource"
)

type Manager struct {
	resources   *resource.Manager
	maintenance *maintenance.Manager
	fencing     *fencing.Manager
	recovery    *recovery.Manager
	events      EventReader
}

type EventReader interface {
	ListCoordinationEvents(string, int) ([]model.CoordinationEvent, error)
}
