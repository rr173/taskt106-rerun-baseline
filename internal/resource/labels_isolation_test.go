package resource

import (
	"path/filepath"
	"reflect"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func timeNow() time.Time { return time.Now().UTC() }

// TestGetDoesNotLeakInternalLabels verifies that mutating the Labels map of a
// resource returned by the manager does not alter the service's internal state
// nor the labels observed on a subsequent read.
func TestGetDoesNotLeakInternalLabels(t *testing.T) {
	store, manager := newTestManager(t)

	if _, err := manager.Register(model.ResourceCreateRequest{
		Path:  "prod",
		Owner: "platform",
	}); err != nil {
		t.Fatalf("register parent: %v", err)
	}
	source := map[string]string{"env": "prod", "team": "platform"}
	if _, err := manager.Register(model.ResourceCreateRequest{
		Path:   "prod/payments",
		Owner:  "payments",
		Labels: source,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := manager.Get("prod/payments")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Caller mutates the returned labels. This must not touch internal state.
	got.Labels["env"] = "tampered"
	got.Labels["injected"] = "leak"
	delete(got.Labels, "team")

	// The caller's local map is the caller's own business now.
	if got.Labels["env"] != "tampered" {
		t.Fatalf("caller mutation not visible on caller copy: %v", got.Labels)
	}

	// A fresh read must reflect the unmodified internal labels.
	again, err := manager.Get("prod/payments")
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	want := map[string]string{"env": "prod", "team": "platform"}
	if !reflect.DeepEqual(again.Labels, want) {
		t.Fatalf("internal labels leaked: got %v want %v", again.Labels, want)
	}

	// And the source map the caller originally handed to Register must be
	// untouched too (Register must not store an alias to the caller's map).
	if got := source["env"]; got != "prod" {
		t.Fatalf("source labels map was mutated by the manager: env=%q", got)
	}
	_ = store
}

// TestReadPathsDoNotLeakInternalLabels sweeps every public read path that
// returns model.Resource values and confirms none of them share the internal
// Labels map with the caller.
func TestReadPathsDoNotLeakInternalLabels(t *testing.T) {
	_, manager := newTestManager(t)
	register := func(path string, labels map[string]string) {
		t.Helper()
		if _, err := manager.Register(model.ResourceCreateRequest{
			Path: path, Owner: "platform", Labels: labels,
		}); err != nil {
			t.Fatalf("register %s: %v", path, err)
		}
	}
	register("root", map[string]string{"tier": "root"})
	register("root/child", map[string]string{"tier": "child"})

	wantRoot := map[string]string{"tier": "root"}
	wantChild := map[string]string{"tier": "child"}
	mutate := func(r *model.Resource) {
		r.Labels["tier"] = "tampered"
		r.Labels["leak"] = "yes"
		delete(r.Labels, "tier")
	}
	assert := func(name string, gotLabels, want map[string]string) {
		t.Helper()
		if !reflect.DeepEqual(gotLabels, want) {
			t.Fatalf("%s: internal labels leaked after caller mutation: got %v want %v", name, gotLabels, want)
		}
	}

	// Get
	if r, err := manager.Get("root/child"); err != nil {
		t.Fatalf("Get: %v", err)
	} else {
		mutate(r)
	}
	r2, _ := manager.Get("root/child")
	assert("Get", r2.Labels, wantChild)

	// Ensure (hit path returns an existing resource)
	if r, err := manager.Ensure("root/child", "platform"); err != nil {
		t.Fatalf("Ensure: %v", err)
	} else {
		mutate(r)
	}
	r3, _ := manager.Get("root/child")
	assert("Ensure", r3.Labels, wantChild)

	// List
	if items, err := manager.List("root"); err != nil {
		t.Fatalf("List: %v", err)
	} else {
		for i := range items {
			mutate(&items[i])
		}
	}
	r4, _ := manager.Get("root/child")
	assert("List", r4.Labels, wantChild)
	r4b, _ := manager.Get("root")
	assert("List-root", r4b.Labels, wantRoot)

	// Children
	if children := manager.Children("root"); len(children) == 0 {
		t.Fatal("Children: empty")
	} else {
		for i := range children {
			mutate(&children[i])
		}
	}
	r5, _ := manager.Get("root/child")
	assert("Children", r5.Labels, wantChild)

	// Descendants
	if desc := manager.Descendants("root"); len(desc) == 0 {
		t.Fatal("Descendants: empty")
	} else {
		for i := range desc {
			mutate(&desc[i])
		}
	}
	r6, _ := manager.Get("root/child")
	assert("Descendants", r6.Labels, wantChild)

	// SetState returns a resource whose labels the caller must not be able to
	// mutate into internal state.
	if r, err := manager.SetState("root/child", model.ResourceDraining, "test"); err != nil {
		t.Fatalf("SetState: %v", err)
	} else {
		mutate(r)
	}
	r7, _ := manager.Get("root/child")
	assert("SetState", r7.Labels, wantChild)

	// Decide exposes the resource via Decision.Resource.
	if d, err := manager.Decide("root/child", "platform", 5, timeNow()); err != nil {
		t.Fatalf("Decide: %v", err)
	} else if d.Resource != nil {
		mutate(d.Resource)
	}
	r8, _ := manager.Get("root/child")
	assert("Decide", r8.Labels, wantChild)

	// Restore state to keep the tree tidy for any later assertions.
	_, _ = manager.SetState("root/child", model.ResourceActive, "restored")
}

func newTestManager(t *testing.T) (*storage.Storage, *Manager) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "resource.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(store)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	return store, manager
}
