package fencing

import (
	"task106/internal/model"
	"time"
)

type Store interface {
	NextFencingSequence(string) (int64, error)
	InsertFencingToken(*model.FencingToken) error
	GetFencingToken(string) (*model.FencingToken, error)
	CurrentFencingSequence(string) (int64, error)
	RevokeFencingToken(string, string, time.Time) error
	ListFencingTokens(string, int) ([]model.FencingToken, error)
	RecordCoordinationEvent(string, string, string, string) error
}

type Manager struct {
	store Store
}

type Issuer interface {
	Issue(resourcePath, holder string, leaseSec int, now time.Time) (string, error)
}
