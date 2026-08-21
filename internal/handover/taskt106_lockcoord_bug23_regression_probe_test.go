package handover

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestCancelRollsBackWhenTimelineFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"handover.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=&Manager{storage:s};h,err:=m.CreateHandover(&model.CreateHandoverRequest{FromCaller:"from",ToCaller:"to",Initiator:"ops",ConfirmTimeoutSec:30});if err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_cancel_timeline BEFORE INSERT ON handover_timeline WHEN NEW.status='cancelled' BEGIN SELECT RAISE(FAIL,'timeline failed'); END;`);err!=nil{t.Fatal(err)};_,err=m.Cancel(h.ID,&model.CancelHandoverRequest{Operator:"ops",Reason:"abort"});if err==nil{t.Fatal("cancel succeeded even though timeline write failed")};got,err:=s.GetHandover(h.ID);if err!=nil{t.Fatal(err)};if got.Status==model.HandoverStatusCancelled{t.Fatalf("handover status changed without timeline: %+v",got)}}
