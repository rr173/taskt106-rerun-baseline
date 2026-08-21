package handover

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestOrchTxTransferRollsBackWhenLockRowsFail(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"handover.db"));if err!=nil{t.Fatal(err)};defer s.Close();now:=time.Now().UTC();tx:=&model.OrchestrationTx{ID:"tx-1",Holder:"from",Status:model.TxStatusCommitted,TimeoutSec:60,CreatedAt:now,UpdatedAt:now,ExpiresAt:now.Add(time.Minute)};if err:=s.CreateOrchTx(tx);err!=nil{t.Fatal(err)};if err:=s.AddTxLock(&model.TxLock{TxID:"tx-1",LockName:"prod/db",LeaseSec:60,Holder:"from",CreatedAt:now});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_orch_lock_transfer BEFORE UPDATE ON orch_tx_locks WHEN NEW.holder='to' BEGIN SELECT RAISE(FAIL,'tx lock transfer failed'); END;`);err!=nil{t.Fatal(err)};m:=&Manager{storage:s};err=m.executeOrchTxTransfer(&execContext{},&model.HandoverResourceItem{ResourceKey:"tx-1"},&model.Handover{FromCaller:"from",ToCaller:"to"},now);if err==nil{t.Fatal("tx transfer succeeded even though lock-row transfer failed")};got,err:=s.GetOrchTx("tx-1");if err!=nil{t.Fatal(err)};if got.Holder!="from"{t.Fatalf("tx holder changed without lock rows: %+v",got)}}
