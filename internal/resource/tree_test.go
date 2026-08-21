package resource

import (
	"testing"
	"task106/internal/model"
	"task106/internal/storage"
)

func newTestManager(t *testing.T) (*Manager, *storage.Storage) {
	t.Helper()
	db, err := storage.New(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mgr := NewManager(db)
	register := func(path, parent string) {
		t.Helper()
		if _, err := mgr.Register(model.ResourceCreateRequest{Path: path, ParentPath: parent, Owner: "tester"}); err != nil {
			t.Fatalf("register %s: %v", path, err)
		}
	}
	register("db", "")
	register("db/users", "")
	register("db/users/alice", "")
	register("db/users/bob", "")
	if err := mgr.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return mgr, db
}

// Querying children with a whitespace-padded path must match the normalized path.
func TestChildrenPaddedPathMatchesNormalized(t *testing.T) {
	mgr, _ := newTestManager(t)

	normalized := mgr.Children("db/users")
	padded := mgr.Children("  db/users  ")

	if len(normalized) != 2 {
		t.Fatalf("expected 2 children for db/users, got %d", len(normalized))
	}
	if len(padded) != len(normalized) {
		t.Fatalf("padded path returned %d children, normalized returned %d", len(padded), len(normalized))
	}
	for i := range normalized {
		if padded[i].Path != normalized[i].Path {
			t.Fatalf("child %d mismatch: padded=%q normalized=%q", i, padded[i].Path, normalized[i].Path)
		}
	}
}

// Querying descendants with a whitespace-padded path must match the normalized path.
func TestDescendantsPaddedPathMatchesNormalized(t *testing.T) {
	mgr, _ := newTestManager(t)

	normalized := mgr.Descendants("db/users")
	padded := mgr.Descendants("\t db/users \n")

	want := []string{"db/users/alice", "db/users/bob"}
	if len(normalized) != len(want) {
		t.Fatalf("expected %d descendants for db/users, got %d (%+v)", len(want), len(normalized), normalized)
	}
	if len(padded) != len(normalized) {
		t.Fatalf("padded descendants %d, normalized %d", len(padded), len(normalized))
	}
	got := make([]string, 0, len(padded))
	for _, r := range padded {
		got = append(got, r.Path)
	}
	for i, p := range want {
		if got[i] != p {
			t.Fatalf("descendant %d: got %q want %q", i, got[i], p)
		}
	}
}
