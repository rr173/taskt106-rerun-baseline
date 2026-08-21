package resource

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestSetPolicyRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"policy.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if _,err:=m.Register(model.ResourceCreateRequest{Path:"prod",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=m.Register(model.ResourceCreateRequest{Path:"prod/db",Owner:"ops"});err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_policy_event BEFORE INSERT ON coordination_events WHEN NEW.event_type='resource_policy_changed' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};_,err=m.SetPolicy("prod/db",model.ResourcePolicy{MaxLeaseSec:30,RequiredHolder:"worker"});if err==nil{t.Fatal("policy change succeeded even though event write failed")};got,err:=s.GetResourcePolicy("prod/db");if err!=nil{t.Fatal(err)};if got!=nil{t.Fatalf("policy persisted without event: %+v",got)}}
