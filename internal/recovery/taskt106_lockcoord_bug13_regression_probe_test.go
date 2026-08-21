package recovery

import (
    "errors"
    "fmt"
    "task106/internal/model"
    "task106/internal/resource"
    "testing"
    "time"
)

type recoveryProbeStore struct { status string; issues []string }
func (s *recoveryProbeStore) CreateRecoveryCheckpoint(v *model.RecoveryCheckpoint) error { v.ID=1; return nil }
func (s *recoveryProbeStore) FinishRecoveryCheckpoint(_ int64, status string, issues []string, _ time.Time) error { s.status=status; s.issues=issues; return nil }
func (s *recoveryProbeStore) GetRecoveryCheckpoint(int64) (*model.RecoveryCheckpoint, error) { return nil,nil }
func (s *recoveryProbeStore) ListRecoveryCheckpoints(string,int)([]model.RecoveryCheckpoint,error){return nil,nil}
func (s *recoveryProbeStore) RecordCoordinationEvent(string,string,string,string) error{return nil}
type missingResourceReader struct{}
func (missingResourceReader) Get(string)(*model.Resource,error){return nil,fmt.Errorf("lookup failed: %w", resource.ErrNotFound)}
func (missingResourceReader) List(string)([]model.Resource,error){return nil,nil}
type oneLeaseReader struct{}
func (oneLeaseReader) ListActiveLeases()([]model.Lease,error){return []model.Lease{{LockName:"prod/db", ExpiresAt:time.Now().Add(time.Minute)}},nil}

func TestMissingResourceBecomesRecoveryIssue(t *testing.T) {
    store := &recoveryProbeStore{}; m := NewManager(store, missingResourceReader{}, oneLeaseReader{})
    if _, err := m.Run("test"); err != nil { t.Fatalf("recovery failed instead of reporting issue: %v", err) }
    if store.status != "attention" || len(store.issues) != 1 { t.Fatalf("status=%s issues=%v", store.status, store.issues) }
    if !errors.Is(resource.ErrNotFound, resource.ErrNotFound) { t.Fatal("unreachable") }
}
