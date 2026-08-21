package lock

import (
    "path/filepath"
    "task106/internal/storage"
    "testing"
)

func TestShortenLeasePersistsActualDuration(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"lock.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if err:=m.Start();err!=nil{t.Fatal(err)};defer m.Stop();if _,err:=m.AcquireLock("prod/db","worker",30,false);err!=nil{t.Fatal(err)};lease,err:=m.ShortenLease("prod/db",60);if err!=nil{t.Fatal(err)};if lease.LeaseSec>30{t.Fatalf("reported lease_sec=%d for capped lease",lease.LeaseSec)};stored,err:=s.GetActiveLease("prod/db");if err!=nil{t.Fatal(err)};if stored.LeaseSec>30{t.Fatalf("persisted lease_sec=%d for capped lease",stored.LeaseSec)}}
