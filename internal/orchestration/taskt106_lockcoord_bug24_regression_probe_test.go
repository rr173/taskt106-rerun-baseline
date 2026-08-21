package orchestration

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestReleaseRollsBackWhenStateHistoryFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"orch.db"));if err!=nil{t.Fatal(err)};defer s.Close();now:=time.Now().UTC();if err:=s.CreateOrchTx(&model.OrchestrationTx{ID:"tx-1",Holder:"worker",Status:model.TxStatusCommitted,TimeoutSec:60,CreatedAt:now,UpdatedAt:now,ExpiresAt:now.Add(time.Minute)});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_release_history BEFORE INSERT ON orch_tx_state_changes WHEN NEW.to_state='released' BEGIN SELECT RAISE(FAIL,'history failed'); END;`);err!=nil{t.Fatal(err)};m:=NewManager(s,nil,nil);_,err=m.ReleaseTx("tx-1","worker");if err==nil{t.Fatal("release succeeded even though state history failed")};got,err:=s.GetOrchTx("tx-1");if err!=nil{t.Fatal(err)};if got.Status!=model.TxStatusCommitted{t.Fatalf("tx status changed without history: %+v",got)}}
