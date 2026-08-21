package namespace

import (
    "reflect"
    "testing"
)

func TestDescendantPathsExcludesRoot(t *testing.T) {
    got := DescendantPaths([]string{"prod", "prod/a", "prod/a/x"}, "prod")
    want := []string{"prod/a", "prod/a/x"}
    if !reflect.DeepEqual(got, want) { t.Fatalf("descendants=%v want=%v", got, want) }
}
