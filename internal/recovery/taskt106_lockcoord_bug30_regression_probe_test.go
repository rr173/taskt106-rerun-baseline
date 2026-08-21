package recovery

import (
    "fmt"
    "task106/internal/model"
    "task106/internal/resource"
    "testing"
    "time"
)

type wrappedMissingStore struct{issues []string}
func (s *wrappedMissingStore) CreateRecoveryCheckpoint(v *model.RecoveryCheckpoint) error{v.ID=1;return nil}
func (s *wrappedMissingStore) FinishRecoveryCheckpoint(_ int64,_ string,issues []string,_ time.Time) error{s.issues=issues;return nil}
func (*wrappedMissingStore) GetRecoveryCheckpoint(int64)(*model.RecoveryCheckpoint,error){return nil,nil}
func (*wrappedMissingStore) ListRecoveryCheckpoints(string,int)([]model.RecoveryCheckpoint,error){return nil,nil}
func (*wrappedMissingStore) RecordCoordinationEvent(string,string,string,string) error{return nil}
type wrappedMissingResources struct{}
func (wrappedMissingResources) Get(string)(*model.Resource,error){return nil,fmt.Errorf("lookup failed: %w",resource.ErrNotFound)}
func (wrappedMissingResources) List(string)([]model.Resource,error){return nil,nil}
type oneActiveLease struct{}
func (oneActiveLease) ListActiveLeases()([]model.Lease,error){return []model.Lease{{LockName:"prod/db",ExpiresAt:time.Now().Add(time.Minute)}},nil}

func TestMissingResourceWrappedErrorBecomesIssue(t *testing.T){s:=&wrappedMissingStore{};m:=NewManager(s,wrappedMissingResources{},oneActiveLease{});if _,err:=m.Run("daily");err!=nil{t.Fatalf("wrapped not-found should become recovery issue: %v",err)};if len(s.issues)!=1{t.Fatalf("issues=%v",s.issues)}}
