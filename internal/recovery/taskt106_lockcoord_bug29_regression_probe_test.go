package recovery

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/resource"
    "task106/internal/storage"
    "testing"
)

func TestFinishCheckpointRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"recovery.db"));if err!=nil{t.Fatal(err)};defer s.Close();resources:=resource.NewManager(s);if _,err:=resources.Register(model.ResourceCreateRequest{Path:"prod",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=resources.Register(model.ResourceCreateRequest{Path:"prod/db",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_recovery_event BEFORE INSERT ON coordination_events WHEN NEW.event_type='recovery_checkpoint' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};m:=NewManager(s,resources,s);if _,err:=m.Run("daily");err==nil{t.Fatal("recovery succeeded even though event write failed")};items,err:=s.ListRecoveryCheckpoints("daily",10);if err!=nil{t.Fatal(err)};if len(items)!=1||items[0].Status!="running"{t.Fatalf("checkpoint finished without event: %+v",items)}}
