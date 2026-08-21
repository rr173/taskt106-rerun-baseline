package namespace

import "testing"

func TestNormalizeAndRelations(t *testing.T) {
	path, err := Normalize(" prod/payments/db ")
	if err != nil || path != "prod/payments/db" {
		t.Fatalf("Normalize returned %q, %v", path, err)
	}
	if !IsAncestor("prod", path) || !IsSameOrDescendant(path, "prod/payments") {
		t.Fatal("expected ancestor relations")
	}
	if got := Ancestors(path); len(got) != 2 || got[1] != "prod/payments" {
		t.Fatalf("unexpected ancestors: %#v", got)
	}
}
