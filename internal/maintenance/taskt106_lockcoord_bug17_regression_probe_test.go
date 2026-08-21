package maintenance

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestCancelRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"maintenance.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);now:=time.Now().UTC();w,err:=m.Create(model.MaintenanceCreateRequest{ResourcePath:"prod/db",Mode:model.MaintenanceForce,StartAt:now.Add(time.Minute),EndAt:now.Add(2*time.Minute),Reason:"patch",Operator:"ops"});if err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_maint_cancel BEFORE INSERT ON coordination_events WHEN NEW.event_type='maintenance_cancelled' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};if err:=m.Cancel(w.ID,"ops");err==nil{t.Fatal("cancel succeeded even though event write failed")};items,err:=s.ListMaintenanceWindows("");if err!=nil{t.Fatal(err)};if len(items)!=1||items[0].Status=="cancelled"{t.Fatalf("cancel status persisted without event: %+v",items)}}
