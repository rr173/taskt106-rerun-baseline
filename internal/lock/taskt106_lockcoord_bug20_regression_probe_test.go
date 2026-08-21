package lock

import (
    "path/filepath"
    "task106/internal/controlplane"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestQueuedGrantRechecksAdmission(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"lock.db"));if err!=nil{t.Fatal(err)};defer s.Close();cp:=controlplane.NewManager(s);if err:=cp.Start();err!=nil{t.Fatal(err)};if _,err:=cp.Resources().Register(model.ResourceCreateRequest{Path:"prod",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=cp.Resources().Register(model.ResourceCreateRequest{Path:"prod/db",Owner:"ops"});err!=nil{t.Fatal(err)};m:=NewManager(s);m.SetAdmissionGuard(cp);m.SetFencingIssuer(cp);if err:=m.Start();err!=nil{t.Fatal(err)};defer m.Stop();if _,err:=m.AcquireLock("prod/db","one",30,false);err!=nil{t.Fatal(err)};queued,err:=m.AcquireLock("prod/db","two",30,false);if err!=nil||!queued.Queued{t.Fatalf("queue result=%+v err=%v",queued,err)};now:=time.Now().UTC();if _,err:=cp.Maintenance().Create(model.MaintenanceCreateRequest{ResourcePath:"prod/db",Mode:model.MaintenanceForce,StartAt:now.Add(-time.Minute),EndAt:now.Add(time.Minute),Reason:"freeze",Operator:"ops"});err!=nil{t.Fatal(err)};if _,err:=m.ReleaseLock("prod/db","one");err!=nil{t.Fatal(err)};lease,err:=m.GetActiveLease("prod/db");if err!=nil{t.Fatal(err)};if lease!=nil{t.Fatalf("queued holder was granted during maintenance: %+v",lease)};if n,_:=m.WaitQueueLen("prod/db");n!=1{t.Fatalf("queued request was lost, len=%d",n)}}
