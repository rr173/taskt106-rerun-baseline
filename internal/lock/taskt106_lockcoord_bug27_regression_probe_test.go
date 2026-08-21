package lock

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestReleaseRollsBackWhenLockUpdateFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"lock.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if err:=m.Start();err!=nil{t.Fatal(err)};defer m.Stop();if _,err:=m.AcquireLock("prod/db","worker",30,false);err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_lock_release BEFORE UPDATE ON locks WHEN NEW.status='free' BEGIN SELECT RAISE(FAIL,'lock update failed'); END;`);err!=nil{t.Fatal(err)};if _,err:=m.ReleaseLock("prod/db","worker");err==nil{t.Fatal("release succeeded even though lock update failed")};lease,err:=s.GetActiveLease("prod/db");if err!=nil{t.Fatal(err)};if lease==nil{t.Fatal("lease was deactivated even though lock stayed held")};l,err:=s.GetLock("prod/db");if err!=nil{t.Fatal(err)};if l.Status!=model.LockStatusHeld{t.Fatalf("lock status changed unexpectedly: %+v",l)}}
