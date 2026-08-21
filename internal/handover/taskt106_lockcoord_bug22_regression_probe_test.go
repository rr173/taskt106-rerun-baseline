package handover

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestQuotaTransferRollsBackWhenSourceUpdateFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"handover.db"));if err!=nil{t.Fatal(err)};defer s.Close();now:=time.Now().UTC();if err:=s.UpsertCallerBinding(&model.CallerBinding{CallerID:"from",PolicyName:"gold",QuotaLimit:100,UsedTokens:40,CreatedAt:now,UpdatedAt:now});err!=nil{t.Fatal(err)};if err:=s.UpsertCallerBinding(&model.CallerBinding{CallerID:"to",PolicyName:"gold",QuotaLimit:100,UsedTokens:5,CreatedAt:now,UpdatedAt:now});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_source_quota BEFORE UPDATE ON rl_caller_bindings WHEN NEW.caller_id='from' BEGIN SELECT RAISE(FAIL,'source update failed'); END;`);err!=nil{t.Fatal(err)};m:=&Manager{storage:s};err=m.executeQuotaTransfer(&execContext{},&model.HandoverResourceItem{ResourceKey:"from"},&model.Handover{FromCaller:"from",ToCaller:"to"},now);if err==nil{t.Fatal("quota transfer succeeded even though source update failed")};to,err:=s.GetCallerBinding("to");if err!=nil{t.Fatal(err)};if to.UsedTokens!=5{t.Fatalf("receiver quota changed without source reset: %+v",to)}}
