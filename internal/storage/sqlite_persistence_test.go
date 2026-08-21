package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"task106/internal/model"
)

func TestLockStateSurvivesStorageReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persistence.db")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := &model.Lock{
		Name:      "persisted-resource",
		Status:    model.LockStatusHeld,
		Holder:    "owner-a",
		Reentrant: true,
		Count:     2,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.UpsertLock(want); err != nil {
		store.Close()
		t.Fatalf("UpsertLock returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened, err := New(dbPath)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.GetLock("persisted-resource")
	if err != nil {
		t.Fatalf("GetLock returned error: %v", err)
	}
	if got == nil {
		t.Fatal("GetLock returned nil after reopen")
	}
	if got.Status != want.Status || got.Holder != want.Holder || got.Count != want.Count || !got.Reentrant {
		t.Fatalf("persisted lock mismatch: got %+v, want %+v", got, want)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database file disappeared: %v", err)
	}
}

func TestTransferCallerBindingQuotaAtomic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "transfer.db")
	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)

	// Source caller has used quota to hand over.
	src := &model.CallerBinding{
		CallerID:       "caller-src",
		PolicyName:     "p",
		QuotaLimit:     100,
		UsedTokens:     30,
		BorrowedTokens: 5,
		LentTokens:     2,
		ReservedTokens: 1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := store.UpsertCallerBinding(src); err != nil {
		t.Fatalf("UpsertCallerBinding src: %v", err)
	}

	// Target caller already exists with some used quota.
	dst := &model.CallerBinding{
		CallerID:       "caller-dst",
		PolicyName:     "p",
		QuotaLimit:     100,
		UsedTokens:     10,
		BorrowedTokens: 1,
		LentTokens:     0,
		ReservedTokens: 0,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := store.UpsertCallerBinding(dst); err != nil {
		t.Fatalf("UpsertCallerBinding dst: %v", err)
	}

	// Compute final state exactly as the handover does, then commit atomically.
	src.UsedTokens = 0
	src.BorrowedTokens = 0
	src.LentTokens = 0
	src.ReservedTokens = 0
	src.UpdatedAt = now

	dst.UsedTokens += 30
	dst.BorrowedTokens += 5
	dst.LentTokens += 2
	dst.ReservedTokens += 1
	dst.UpdatedAt = now

	if err := store.TransferCallerBindingQuota(src, dst, now); err != nil {
		t.Fatalf("TransferCallerBindingQuota: %v", err)
	}

	gotSrc, err := store.GetCallerBinding("caller-src")
	if err != nil || gotSrc == nil {
		t.Fatalf("GetCallerBinding src: %v %v", gotSrc, err)
	}
	if gotSrc.UsedTokens != 0 || gotSrc.BorrowedTokens != 0 || gotSrc.LentTokens != 0 || gotSrc.ReservedTokens != 0 {
		t.Fatalf("source not zeroed after transfer: %+v", gotSrc)
	}

	gotDst, err := store.GetCallerBinding("caller-dst")
	if err != nil || gotDst == nil {
		t.Fatalf("GetCallerBinding dst: %v %v", gotDst, err)
	}
	if gotDst.UsedTokens != 40 || gotDst.BorrowedTokens != 6 || gotDst.LentTokens != 2 || gotDst.ReservedTokens != 1 {
		t.Fatalf("target not credited after transfer: %+v", gotDst)
	}

	// A nil binding must fail without touching either side.
	if err := store.TransferCallerBindingQuota(nil, dst, now); err == nil {
		t.Fatal("expected error for nil source binding")
	}
	gotSrc2, _ := store.GetCallerBinding("caller-src")
	if gotSrc2.UsedTokens != 0 {
		t.Fatalf("source changed after failed transfer: %+v", gotSrc2)
	}
}
