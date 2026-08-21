package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestSetStateRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"res.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if _,err:=m.Register(model.ResourceCreateRequest{Path:"prod",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=m.Register(model.ResourceCreateRequest{Path:"prod/db",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_state_event BEFORE INSERT ON coordination_events WHEN NEW.event_type='resource_state_changed' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};if _,err:=m.SetState("prod/db",model.ResourceDraining,"maintenance");err==nil{t.Fatal("state change succeeded even though event write failed")};got,err:=s.GetResource("prod/db");if err!=nil{t.Fatal(err)};if got.State!=model.ResourceActive{t.Fatalf("state persisted without event: %s",got.State)}}
