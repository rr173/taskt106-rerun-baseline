package topology

import (
	"os"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"testing"
)

func setupTestManagers(t *testing.T) (*Manager, *lock.Manager, *ratelimit.Manager, *storage.Storage) {
	t.Helper()

	dbPath := "./test_topology.db"
	os.Remove(dbPath)

	s, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	lm := lock.NewManager(s)
	if err := lm.Start(); err != nil {
		t.Fatalf("failed to start lock manager: %v", err)
	}

	rm := ratelimit.NewManager(s)
	if err := rm.Start(); err != nil {
		t.Fatalf("failed to start ratelimit manager: %v", err)
	}

	tm := NewManager(s, lm, rm)
	if err := tm.Start(); err != nil {
		t.Fatalf("failed to start topology manager: %v", err)
	}

	t.Cleanup(func() {
		tm.Stop()
		rm.Stop()
		lm.Stop()
		s.Close()
		os.Remove(dbPath)
	})

	return tm, lm, rm, s
}

func TestRegisterNode(t *testing.T) {
	tm, _, _, _ := setupTestManagers(t)

	node, err := tm.RegisterNode("node1", "lock-node1", "", 0)
	if err != nil {
		t.Fatalf("failed to register node: %v", err)
	}
	if node.Name != "node1" {
		t.Errorf("expected node name node1, got %s", node.Name)
	}
	if node.LockName != "lock-node1" {
		t.Errorf("expected lock name lock-node1, got %s", node.LockName)
	}

	_, err = tm.RegisterNode("node1", "lock-node1", "", 0)
	if err == nil {
		t.Error("expected error when registering duplicate node")
	}
}

func TestRegisterNodeWithRatePolicy(t *testing.T) {
	tm, _, rm, _ := setupTestManagers(t)

	_, err := rm.CreatePolicy("test-policy", model.AlgoTokenBucket, 0, 100, 10.0, "per_second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	node, err := tm.RegisterNode("node-with-policy", "lock-nwp", "test-policy", 5)
	if err != nil {
		t.Fatalf("failed to register node with policy: %v", err)
	}
	if node.RatePolicy != "test-policy" {
		t.Errorf("expected policy test-policy, got %s", node.RatePolicy)
	}
	if node.TokenCost != 5 {
		t.Errorf("expected token cost 5, got %d", node.TokenCost)
	}
}

func TestDeclareEdgeAndCycleDetection(t *testing.T) {
	tm, _, _, _ := setupTestManagers(t)

	_, err := tm.RegisterNode("A", "lock-A", "", 0)
	if err != nil {
		t.Fatalf("failed to register A: %v", err)
	}
	_, err = tm.RegisterNode("B", "lock-B", "", 0)
	if err != nil {
		t.Fatalf("failed to register B: %v", err)
	}
	_, err = tm.RegisterNode("C", "lock-C", "", 0)
	if err != nil {
		t.Fatalf("failed to register C: %v", err)
	}

	_, err = tm.DeclareEdge("A", "B")
	if err != nil {
		t.Fatalf("failed to declare A->B: %v", err)
	}
	_, err = tm.DeclareEdge("B", "C")
	if err != nil {
		t.Fatalf("failed to declare B->C: %v", err)
	}

	_, err = tm.DeclareEdge("C", "A")
	if err == nil {
		t.Error("expected cycle detection error for C->A")
	} else {
		t.Logf("correctly detected cycle: %v", err)
	}

	_, err = tm.DeclareEdge("A", "A")
	if err == nil {
		t.Error("expected self-loop error")
	}
}

func TestGetAncestorsAndDescendants(t *testing.T) {
	tm, _, _, _ := setupTestManagers(t)

	nodes := []string{"A", "B", "C", "D"}
	for _, n := range nodes {
		_, err := tm.RegisterNode(n, "lock-"+n, "", 0)
		if err != nil {
			t.Fatalf("failed to register %s: %v", n, err)
		}
	}

	edges := [][2]string{{"A", "B"}, {"B", "C"}, {"C", "D"}}
	for _, e := range edges {
		_, err := tm.DeclareEdge(e[0], e[1])
		if err != nil {
			t.Fatalf("failed to declare edge %s->%s: %v", e[0], e[1], err)
		}
	}

	ancestors, err := tm.GetAncestors("D")
	if err != nil {
		t.Fatalf("failed to get ancestors: %v", err)
	}
	t.Logf("ancestors of D: %v", ancestors.Ancestors)
	if len(ancestors.Ancestors) != 3 {
		t.Errorf("expected 3 ancestors, got %d", len(ancestors.Ancestors))
	}

	descendants, err := tm.GetDescendants("A")
	if err != nil {
		t.Fatalf("failed to get descendants: %v", err)
	}
	t.Logf("descendants of A: %v", descendants.Descendants)
	if len(descendants.Descendants) != 3 {
		t.Errorf("expected 3 descendants, got %d", len(descendants.Descendants))
	}
}

func TestCascadeAcquireAndRelease(t *testing.T) {
	tm, lm, rm, _ := setupTestManagers(t)

	_, err := rm.CreatePolicy("cluster-policy", model.AlgoTokenBucket, 0, 100, 10.0, "per_second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = tm.RegisterNode("cluster", "lock-cluster", "cluster-policy", 2)
	if err != nil {
		t.Fatalf("failed to register cluster: %v", err)
	}
	_, err = tm.RegisterNode("namespace", "lock-namespace", "", 0)
	if err != nil {
		t.Fatalf("failed to register namespace: %v", err)
	}
	_, err = tm.RegisterNode("pod", "lock-pod", "", 0)
	if err != nil {
		t.Fatalf("failed to register pod: %v", err)
	}

	_, err = tm.DeclareEdge("namespace", "pod")
	if err != nil {
		t.Fatalf("failed to declare namespace->pod: %v", err)
	}
	_, err = tm.DeclareEdge("cluster", "namespace")
	if err != nil {
		t.Fatalf("failed to declare cluster->namespace: %v", err)
	}

	result, err := tm.CascadeAcquire("pod", "holder1", 60, false)
	if err != nil {
		t.Fatalf("failed to cascade acquire: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got failure: %s", result.Message)
	}
	if result.RolledBack {
		t.Error("expected no rollback")
	}
	t.Logf("acquired: %v", result.Acquired)
	t.Logf("steps: %+v", result.Steps)
	if len(result.Acquired) != 3 {
		t.Errorf("expected 3 nodes acquired, got %d", len(result.Acquired))
	}

	locks, _ := lm.ListAllLocks()
	heldCount := 0
	for _, l := range locks {
		if l.Holder == "holder1" && l.Status == model.LockStatusHeld {
			heldCount++
		}
	}
	if heldCount != 3 {
		t.Errorf("expected 3 locks held, got %d", heldCount)
	}

	tree, err := tm.GetHolderResourceTree("holder1")
	if err != nil {
		t.Fatalf("failed to get holder tree: %v", err)
	}
	t.Logf("holder tree: %+v", tree)
	if len(tree.HeldNodes) != 3 {
		t.Errorf("expected 3 held nodes, got %d", len(tree.HeldNodes))
	}

	relResult, err := tm.CascadeRelease("cluster", "holder1", false)
	if err == nil && relResult.Success {
		t.Error("expected error when releasing cluster while namespace held")
	} else {
		t.Logf("correctly prevented release: %s", relResult.Message)
	}

	forceResult, err := tm.CascadeRelease("cluster", "holder1", true)
	if err != nil {
		t.Fatalf("failed to force release: %v", err)
	}
	if !forceResult.Success {
		t.Errorf("expected force release success: %s", forceResult.Message)
	}
	t.Logf("force released: %v", forceResult.Released)

	locks, _ = lm.ListAllLocks()
	heldCount = 0
	for _, l := range locks {
		if l.Holder == "holder1" && l.Status == model.LockStatusHeld {
			heldCount++
		}
	}
	if heldCount != 0 {
		t.Errorf("expected 0 locks held after force release, got %d", heldCount)
	}
}

func TestCascadeAcquireRollback(t *testing.T) {
	tm, _, rm, _ := setupTestManagers(t)

	_, err := rm.CreatePolicy("limited-policy", model.AlgoTokenBucket, 0, 5, 10.0, "per_second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = rm.BindCaller("topo-holder2-parent", "limited-policy", 5)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	_, err = tm.RegisterNode("parent", "lock-parent", "limited-policy", 10)
	if err != nil {
		t.Fatalf("failed to register parent: %v", err)
	}
	_, err = tm.RegisterNode("child", "lock-child", "", 0)
	if err != nil {
		t.Fatalf("failed to register child: %v", err)
	}

	_, err = tm.DeclareEdge("parent", "child")
	if err != nil {
		t.Fatalf("failed to declare parent->child: %v", err)
	}

	result, err := tm.CascadeAcquire("child", "holder2", 60, false)
	if err != nil {
		t.Fatalf("cascade acquire error: %v", err)
	}
	if result.Success {
		t.Error("expected failure due to insufficient tokens")
	}
	if !result.RolledBack {
		t.Error("expected rollback")
	}
	t.Logf("rollback result: %s", result.Message)
	t.Logf("steps: %+v", result.Steps)
}

func TestGraphAndStats(t *testing.T) {
	tm, _, _, _ := setupTestManagers(t)

	nodes := []string{"X", "Y", "Z"}
	for _, n := range nodes {
		_, err := tm.RegisterNode(n, "lock-"+n, "", 0)
		if err != nil {
			t.Fatalf("failed to register %s: %v", n, err)
		}
	}
	_, err := tm.DeclareEdge("X", "Y")
	if err != nil {
		t.Fatalf("failed to declare edge: %v", err)
	}
	_, err = tm.DeclareEdge("Y", "Z")
	if err != nil {
		t.Fatalf("failed to declare edge: %v", err)
	}

	graph, err := tm.GetGraph()
	if err != nil {
		t.Fatalf("failed to get graph: %v", err)
	}
	if len(graph.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(graph.Edges))
	}

	_, err = tm.CascadeAcquire("Z", "holder-stats", 60, false)
	if err != nil {
		t.Fatalf("acquire error: %v", err)
	}

	stats, err := tm.GetStats()
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	t.Logf("stats: %+v", stats)
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 nodes in stats, got %d", stats.TotalNodes)
	}
	if stats.TotalEdges != 2 {
		t.Errorf("expected 2 edges in stats, got %d", stats.TotalEdges)
	}
	if stats.AcquireOps < 1 {
		t.Errorf("expected at least 1 acquire op, got %d", stats.AcquireOps)
	}

	history, err := tm.ListHistory("", 10)
	if err != nil {
		t.Fatalf("failed to list history: %v", err)
	}
	if len(history) < 1 {
		t.Errorf("expected at least 1 history entry, got %d", len(history))
	}
	t.Logf("history: %+v", history)
}

func TestPersistentAfterRestart(t *testing.T) {
	dbPath := "./test_persist_topology.db"
	os.Remove(dbPath)

	s, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	lm := lock.NewManager(s)
	rm := ratelimit.NewManager(s)
	tm := NewManager(s, lm, rm)

	lm.Start()
	rm.Start()
	tm.Start()

	tm.RegisterNode("persist-A", "lock-pA", "", 0)
	tm.RegisterNode("persist-B", "lock-pB", "", 0)
	tm.DeclareEdge("persist-A", "persist-B")

	tm.Stop()
	rm.Stop()
	lm.Stop()
	s.Close()

	s2, err := storage.New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}

	lm2 := lock.NewManager(s2)
	rm2 := ratelimit.NewManager(s2)
	tm2 := NewManager(s2, lm2, rm2)

	lm2.Start()
	rm2.Start()
	tm2.Start()

	defer func() {
		tm2.Stop()
		rm2.Stop()
		lm2.Stop()
		s2.Close()
		os.Remove(dbPath)
	}()

	nodes, err := tm2.ListNodes()
	if err != nil {
		t.Fatalf("failed to list nodes after restart: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes after restart, got %d", len(nodes))
	}

	graph, err := tm2.GetGraph()
	if err != nil {
		t.Fatalf("failed to get graph after restart: %v", err)
	}
	if len(graph.Edges) != 1 {
		t.Errorf("expected 1 edge after restart, got %d", len(graph.Edges))
	}
	t.Logf("persistence verified: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))
}
