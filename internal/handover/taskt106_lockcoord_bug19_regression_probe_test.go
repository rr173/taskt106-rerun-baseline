package handover

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestLockTransferRollsBackWhenLeaseTransferFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"handover.db"));if err!=nil{t.Fatal(err)};defer s.Close();now:=time.Now().UTC();if err:=s.UpsertLock(&model.Lock{Name:"prod/db",Status:model.LockStatusHeld,Holder:"from",Count:1,CreatedAt:now,UpdatedAt:now});err!=nil{t.Fatal(err)};if err:=s.CreateLease(&model.Lease{LockName:"prod/db",Holder:"from",LeaseSec:60,AcquiredAt:now,ExpiresAt:now.Add(time.Minute),Active:true});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_lease_transfer BEFORE UPDATE ON leases WHEN NEW.holder='to' BEGIN SELECT RAISE(FAIL,'lease transfer failed'); END;`);err!=nil{t.Fatal(err)};m:=&Manager{storage:s};err=m.executeLockTransfer(&execContext{},&model.HandoverResourceItem{ResourceKey:"prod/db"},&model.Handover{FromCaller:"from",ToCaller:"to"},now);if err==nil{t.Fatal("lock transfer succeeded even though lease transfer failed")};lock,err:=s.GetLock("prod/db");if err!=nil{t.Fatal(err)};if lock.Holder!="from"{t.Fatalf("lock holder changed without lease transfer: %+v",lock)}}
