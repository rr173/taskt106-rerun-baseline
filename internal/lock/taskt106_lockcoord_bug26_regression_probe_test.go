package lock

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestAcquireRollsBackLockWhenLeaseInsertFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"lock.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if err:=m.Start();err!=nil{t.Fatal(err)};defer m.Stop();if _,err:=s.DB().Exec(`CREATE TRIGGER fail_lease_insert BEFORE INSERT ON leases BEGIN SELECT RAISE(FAIL,'lease insert failed'); END;`);err!=nil{t.Fatal(err)};if _,err:=m.AcquireLock("prod/db","worker",30,false);err==nil{t.Fatal("acquire succeeded even though lease insert failed")};l,err:=s.GetLock("prod/db");if err!=nil{t.Fatal(err)};if l!=nil&&l.Status==model.LockStatusHeld{t.Fatalf("lock held without lease: %+v",l)}}
