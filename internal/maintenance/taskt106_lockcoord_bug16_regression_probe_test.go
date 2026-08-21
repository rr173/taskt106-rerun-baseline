package maintenance

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestCreateRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"maintenance.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if _,err:=s.DB().Exec(`CREATE TRIGGER fail_maint_create BEFORE INSERT ON coordination_events WHEN NEW.event_type='maintenance_created' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};now:=time.Now().UTC();_,err=m.Create(model.MaintenanceCreateRequest{ResourcePath:"prod/db",Mode:model.MaintenanceForce,StartAt:now.Add(time.Minute),EndAt:now.Add(2*time.Minute),Reason:"patch",Operator:"ops"});if err==nil{t.Fatal("maintenance window persisted while event failed")};items,err:=s.ListMaintenanceWindows("");if err!=nil{t.Fatal(err)};if len(items)!=0{t.Fatalf("window persisted without event: %+v",items)}}
