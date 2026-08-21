package fencing

import (
    "path/filepath"
    "task106/internal/storage"
    "testing"
    "time"
)

func TestRevokeRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"fence.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);now:=time.Now().UTC();tok,err:=m.Issue("prod/db","worker",60,now);if err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_revoke_event BEFORE INSERT ON coordination_events WHEN NEW.event_type='fencing_revoked' BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};if err:=m.Revoke(tok,"operator",now.Add(time.Second));err==nil{t.Fatal("revoke succeeded even though event write failed")};if got:=m.Validate(tok,"prod/db","worker",now.Add(2*time.Second));!got.Valid{t.Fatalf("token was revoked without event: %+v",got)}}
