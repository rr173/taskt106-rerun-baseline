package ratelimit

import (
    "path/filepath"
    "task106/internal/model"
    "task106/internal/storage"
    "testing"
)

func TestRequestTokensRollsBackWhenEventFails(t *testing.T){s,err:=storage.New(filepath.Join(t.TempDir(),"rl.db"));if err!=nil{t.Fatal(err)};defer s.Close();m:=NewManager(s);if _,err:=m.CreatePolicy("gold",model.AlgoFixedWindow,60,10,0,"");err!=nil{t.Fatal(err)};if _,err:=m.BindCaller("caller","gold",10);err!=nil{t.Fatal(err)};if _,err:=s.DB().Exec(`CREATE TRIGGER fail_rl_event BEFORE INSERT ON rl_events BEGIN SELECT RAISE(FAIL,'event failed'); END;`);err!=nil{t.Fatal(err)};if _,err:=m.RequestTokens("caller",3,false,0);err==nil{t.Fatal("token request succeeded even though event write failed")};b,err:=s.GetCallerBinding("caller");if err!=nil{t.Fatal(err)};if b.UsedTokens!=0{t.Fatalf("binding consumed tokens without event: %+v",b)}}
