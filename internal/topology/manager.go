package topology

import (
	"fmt"
	"log"
	"task106/internal/lock"
	"task106/internal/model"
	"task106/internal/ratelimit"
	"task106/internal/storage"
	"sync"
	"time"
)

type Manager struct {
	storage   *storage.Storage
	lockMgr   *lock.Manager
	rateMgr   *ratelimit.Manager
	mu        sync.Mutex
	nodes     map[string]*model.TopologyNode
	children  map[string][]string
	parents   map[string][]string
	stopCh    chan struct{}
}

func NewManager(s *storage.Storage, lm *lock.Manager, rm *ratelimit.Manager) *Manager {
	return &Manager{
		storage:  s,
		lockMgr:  lm,
		rateMgr:  rm,
		nodes:    make(map[string]*model.TopologyNode),
		children: make(map[string][]string),
		parents:  make(map[string][]string),
		stopCh:   make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadGraphLocked(); err != nil {
		return fmt.Errorf("load topology graph: %w", err)
	}

	log.Println("[topology-manager] started")
	log.Printf("[topology-manager] loaded %d nodes, %d edges", len(m.nodes), m.countEdgesLocked())
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	log.Println("[topology-manager] stopped")
}

func (m *Manager) countEdgesLocked() int {
	count := 0
	for _, kids := range m.children {
		count += len(kids)
	}
	return count
}

func (m *Manager) loadGraphLocked() error {
	nodes, err := m.storage.ListTopoNodes()
	if err != nil {
		return err
	}
	for i := range nodes {
		n := nodes[i]
		m.nodes[n.Name] = &n
	}

	edges, err := m.storage.ListTopoEdges()
	if err != nil {
		return err
	}
	for _, e := range edges {
		m.children[e.FromNode] = append(m.children[e.FromNode], e.ToNode)
		m.parents[e.ToNode] = append(m.parents[e.ToNode], e.FromNode)
	}
	return nil
}

func (m *Manager) RegisterNode(name, lockName, ratePolicy string, tokenCost int) (*model.TopologyNode, error) {
	if name == "" {
		return nil, fmt.Errorf("node name is required")
	}
	if tokenCost < 0 {
		return nil, fmt.Errorf("token_cost cannot be negative")
	}
	if lockName == "" {
		lockName = name
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[name]; exists {
		return nil, fmt.Errorf("node already exists: %s", name)
	}

	now := time.Now()
	node := &model.TopologyNode{
		Name:       name,
		LockName:   lockName,
		RatePolicy: ratePolicy,
		TokenCost:  tokenCost,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := m.storage.CreateTopoNode(node); err != nil {
		return nil, err
	}
	m.nodes[name] = node

	log.Printf("[topology] node registered: name=%s lock=%s policy=%s cost=%d", name, lockName, ratePolicy, tokenCost)
	return node, nil
}

func (m *Manager) DeclareEdge(fromNode, toNode string) (*model.TopologyEdge, error) {
	if fromNode == "" || toNode == "" {
		return nil, fmt.Errorf("both from_node and to_node are required")
	}
	if fromNode == toNode {
		return nil, fmt.Errorf("cannot create self-loop on %s", fromNode)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[fromNode]; !exists {
		return nil, fmt.Errorf("from_node not found: %s", fromNode)
	}
	if _, exists := m.nodes[toNode]; !exists {
		return nil, fmt.Errorf("to_node not found: %s", toNode)
	}

	for _, existing := range m.children[fromNode] {
		if existing == toNode {
			return nil, fmt.Errorf("edge already exists: %s -> %s", fromNode, toNode)
		}
	}

	tempChildren := make(map[string][]string)
	for k, v := range m.children {
		tempChildren[k] = append([]string{}, v...)
	}
	tempChildren[fromNode] = append(tempChildren[fromNode], toNode)

	if m.hasCycleDFS(tempChildren) {
		return nil, fmt.Errorf("edge would create a cycle: %s -> %s", fromNode, toNode)
	}

	now := time.Now()
	edge := &model.TopologyEdge{
		FromNode:  fromNode,
		ToNode:    toNode,
		CreatedAt: now,
	}

	if err := m.storage.CreateTopoEdge(edge); err != nil {
		return nil, err
	}

	m.children[fromNode] = append(m.children[fromNode], toNode)
	m.parents[toNode] = append(m.parents[toNode], fromNode)

	log.Printf("[topology] edge declared: %s -> %s", fromNode, toNode)
	return edge, nil
}

func (m *Manager) hasCycleDFS(children map[string][]string) bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(node string) bool
	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true

		for _, child := range children[node] {
			if !visited[child] {
				if dfs(child) {
					return true
				}
			} else if recStack[child] {
				return true
			}
		}

		recStack[node] = false
		return false
	}

	for node := range m.nodes {
		if !visited[node] {
			if dfs(node) {
				return true
			}
		}
	}
	return false
}

type acquiredItem struct {
	nodeName   string
	lockName   string
	lockHolder string
	wasHeld    bool
	ratePolicy string
	tokenCost  int
	callerID   string
	tokensPaid bool
}

func (m *Manager) CascadeAcquire(targetNode, holder string, leaseSec int, reentrant bool) (*model.CascadeAcquireResult, error) {
	start := time.Now()
	result := &model.CascadeAcquireResult{
		Steps: make([]model.CascadeAcquireStep, 0),
	}

	if targetNode == "" || holder == "" {
		result.Message = "target_node and holder are required"
		result.Success = false
		result.DurationMs = time.Since(start).Milliseconds()
		return result, nil
	}
	if leaseSec <= 0 {
		result.Message = "lease_sec must be positive"
		result.Success = false
		result.DurationMs = time.Since(start).Milliseconds()
		return result, nil
	}

	m.mu.Lock()

	if _, exists := m.nodes[targetNode]; !exists {
		m.mu.Unlock()
		result.Message = fmt.Sprintf("target node not found: %s", targetNode)
		result.Success = false
		result.DurationMs = time.Since(start).Milliseconds()
		m.recordOp(model.TopologyOpAcquire, targetNode, holder, false, false, nil, result.DurationMs, result.Message)
		return result, nil
	}

	ancestors := m.collectAncestorsLocked(targetNode)
	acquireOrder := append(ancestors, targetNode)

	touched := make([]string, 0, len(acquireOrder))
	acquired := make([]*acquiredItem, 0, len(acquireOrder))

	m.mu.Unlock()

	overallSuccess := true
	failMsg := ""

	for _, nodeName := range acquireOrder {
		m.mu.Lock()
		node, exists := m.nodes[nodeName]
		m.mu.Unlock()
		if !exists {
			continue
		}

		step := model.CascadeAcquireStep{
			NodeName: nodeName,
			LockName: node.LockName,
			Action:   "acquire_lock",
		}

		lockResult, err := m.lockMgr.AcquireLock(node.LockName, holder, leaseSec, reentrant)
		if err != nil {
			overallSuccess = false
			failMsg = fmt.Sprintf("failed to acquire lock %s: %v", node.LockName, err)
			step.Success = false
			step.Message = err.Error()
			result.Steps = append(result.Steps, step)
			break
		}
		if lockResult.Queued {
			overallSuccess = false
			failMsg = fmt.Sprintf("lock %s is held by another, queued at position %d", node.LockName, lockResult.Position)
			step.Success = false
			step.Message = failMsg
			result.Steps = append(result.Steps, step)
			break
		}
		if lockResult.Deadlock {
			overallSuccess = false
			failMsg = fmt.Sprintf("deadlock detected on lock %s", node.LockName)
			step.Success = false
			step.Message = failMsg
			result.Steps = append(result.Steps, step)
			break
		}

		step.Success = true
		wasHeld := false
		if lockResult.Lock != nil && lockResult.Lock.Count > 1 {
			wasHeld = true
			step.Message = "reentrant, already held"
		} else {
			step.Message = "acquired"
		}
		result.Steps = append(result.Steps, step)
		touched = append(touched, nodeName)
		acquired = append(acquired, &acquiredItem{
			nodeName:   nodeName,
			lockName:   node.LockName,
			lockHolder: holder,
			wasHeld:    wasHeld,
			ratePolicy: node.RatePolicy,
			tokenCost:  node.TokenCost,
		})

		if node.RatePolicy != "" && node.TokenCost > 0 {
			callerID := fmt.Sprintf("topo-%s-%s", holder, nodeName)
			rateStep := model.CascadeAcquireStep{
				NodeName: nodeName,
				LockName: node.LockName,
				Action:   "consume_tokens",
			}

			_, _ = m.rateMgr.BindCaller(callerID, node.RatePolicy, 10000)

			tokenResult, err := m.rateMgr.RequestTokens(callerID, node.TokenCost, false, 0)
			if err != nil {
				overallSuccess = false
				failMsg = fmt.Sprintf("failed to consume tokens for %s: %v", nodeName, err)
				rateStep.Success = false
				rateStep.Message = err.Error()
				result.Steps = append(result.Steps, rateStep)
				break
			}
			if !tokenResult.Allowed {
				overallSuccess = false
				failMsg = fmt.Sprintf("insufficient tokens for %s: %s", nodeName, tokenResult.Reason)
				rateStep.Success = false
				rateStep.Message = tokenResult.Reason
				result.Steps = append(result.Steps, rateStep)
				break
			}

			rateStep.Success = true
			rateStep.Message = fmt.Sprintf("consumed %d tokens from %s", node.TokenCost, node.RatePolicy)
			result.Steps = append(result.Steps, rateStep)
			acquired[len(acquired)-1].callerID = callerID
			acquired[len(acquired)-1].tokensPaid = true
		}
	}

	duration := time.Since(start).Milliseconds()

	if !overallSuccess {
		result.RolledBack = true
		for i := len(acquired) - 1; i >= 0; i-- {
			item := acquired[i]
			rollbackStep := model.CascadeAcquireStep{
				NodeName: item.nodeName,
				LockName: item.lockName,
				Action:   "rollback",
			}

			if item.tokensPaid && item.callerID != "" {
				if err := m.rateMgr.ReturnTokens(item.callerID, item.tokenCost); err != nil {
					log.Printf("[topology-rollback] return tokens failed for %s: %v", item.nodeName, err)
				}
			}

			if !item.wasHeld {
				relResult, err := m.lockMgr.ReleaseLock(item.lockName, item.lockHolder)
				if err != nil {
					log.Printf("[topology-rollback] release lock failed for %s: %v", item.nodeName, err)
					rollbackStep.Success = false
					rollbackStep.Message = fmt.Sprintf("release failed: %v", err)
				} else {
					rollbackStep.Success = relResult.Released
					if relResult.Released {
						rollbackStep.Message = "lock released"
					} else {
						rollbackStep.Message = "lock not released"
					}
				}
			} else {
				rollbackStep.Success = true
				rollbackStep.Message = "reentrant lock, skipped release"
			}

			result.Steps = append(result.Steps, rollbackStep)
		}

		result.Success = false
		result.Message = failMsg
		result.DurationMs = duration
		m.recordOp(model.TopologyOpAcquire, targetNode, holder, false, true, touched, duration, failMsg)
		return result, nil
	}

	result.Success = true
	result.RolledBack = false
	result.Acquired = touched
	result.Message = fmt.Sprintf("successfully acquired %d nodes", len(touched))
	result.DurationMs = duration

	m.recordOp(model.TopologyOpAcquire, targetNode, holder, true, false, touched, duration, result.Message)
	log.Printf("[topology] cascade acquire success: target=%s holder=%s nodes=%d duration=%dms", targetNode, holder, len(touched), duration)
	return result, nil
}

func (m *Manager) collectAncestorsLocked(nodeName string) []string {
	var ancestors []string
	visited := make(map[string]bool)

	var dfs func(n string)
	dfs = func(n string) {
		if visited[n] {
			return
		}
		visited[n] = true

		for _, parent := range m.parents[n] {
			dfs(parent)
			ancestors = append(ancestors, parent)
		}
	}

	dfs(nodeName)
	return ancestors
}

func (m *Manager) CascadeRelease(targetNode, holder string, force bool) (*model.CascadeReleaseResult, error) {
	start := time.Now()
	result := &model.CascadeReleaseResult{
		Steps: make([]model.CascadeReleaseStep, 0),
	}

	if targetNode == "" || holder == "" {
		result.Message = "target_node and holder are required"
		result.Success = false
		result.DurationMs = time.Since(start).Milliseconds()
		return result, nil
	}

	m.mu.Lock()

	if _, exists := m.nodes[targetNode]; !exists {
		m.mu.Unlock()
		result.Message = fmt.Sprintf("target node not found: %s", targetNode)
		result.Success = false
		result.DurationMs = time.Since(start).Milliseconds()
		m.recordOp(model.TopologyOpRelease, targetNode, holder, false, false, nil, result.DurationMs, result.Message)
		return result, nil
	}

	var releaseOrder []string
	var touched []string

	if force {
		descendants := m.collectDescendantsLocked(targetNode)
		for i := len(descendants) - 1; i >= 0; i-- {
			releaseOrder = append(releaseOrder, descendants[i])
		}
		releaseOrder = append(releaseOrder, targetNode)
	} else {
		childrenList := m.children[targetNode]
		for _, child := range childrenList {
			childNode := m.nodes[child]
			if childNode != nil {
				locks, _ := m.lockMgr.ListAllLocks()
				for _, l := range locks {
					if l.Name == childNode.LockName && l.Holder == holder && l.Status == model.LockStatusHeld {
						m.mu.Unlock()
						result.Message = fmt.Sprintf("cannot release: child node %s is still held by %s (release children first, or use force=true)", child, holder)
						result.Success = false
						result.DurationMs = time.Since(start).Milliseconds()
						m.recordOp(model.TopologyOpRelease, targetNode, holder, false, false, nil, result.DurationMs, result.Message)
						return result, nil
					}
				}
			}
		}
		releaseOrder = []string{targetNode}
	}

	m.mu.Unlock()

	overallSuccess := true
	failMsg := ""

	for _, nodeName := range releaseOrder {
		m.mu.Lock()
		node, exists := m.nodes[nodeName]
		m.mu.Unlock()
		if !exists {
			continue
		}

		step := model.CascadeReleaseStep{
			NodeName: nodeName,
			LockName: node.LockName,
			Action:   "release_lock",
		}

		relResult, err := m.lockMgr.ReleaseLock(node.LockName, holder)
		if err != nil {
			failMsg = fmt.Sprintf("failed to release lock %s: %v", node.LockName, err)
			step.Success = false
			step.Message = err.Error()
			result.Steps = append(result.Steps, step)
			if !force {
				overallSuccess = false
				break
			}
		} else if !relResult.Released {
			msg := "lock not held"
			step.Success = true
			step.Message = msg
			result.Steps = append(result.Steps, step)
		} else {
			step.Success = true
			step.Message = "released"
			result.Steps = append(result.Steps, step)
			touched = append(touched, nodeName)
		}
	}

	duration := time.Since(start).Milliseconds()

	if !overallSuccess {
		result.Success = false
		result.Message = failMsg
		result.DurationMs = duration
		m.recordOp(model.TopologyOpRelease, targetNode, holder, false, false, touched, duration, failMsg)
		return result, nil
	}

	result.Success = true
	result.Released = touched
	if force {
		result.Message = fmt.Sprintf("force released %d nodes", len(touched))
	} else {
		result.Message = fmt.Sprintf("successfully released %d nodes", len(touched))
	}
	result.DurationMs = duration

	m.recordOp(model.TopologyOpRelease, targetNode, holder, true, false, touched, duration, result.Message)
	log.Printf("[topology] cascade release %s: target=%s holder=%s nodes=%d duration=%dms",
		map[bool]string{true: "success", false: "fail"}[overallSuccess], targetNode, holder, len(touched), duration)
	return result, nil
}

func (m *Manager) collectDescendantsLocked(nodeName string) []string {
	var descendants []string
	visited := make(map[string]bool)

	var dfs func(n string)
	dfs = func(n string) {
		if visited[n] {
			return
		}
		visited[n] = true
		descendants = append(descendants, n)

		for _, child := range m.children[n] {
			dfs(child)
		}
	}

	for _, child := range m.children[nodeName] {
		dfs(child)
	}

	return descendants
}

func (m *Manager) GetGraph() (*model.TopologyGraph, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	graph := &model.TopologyGraph{
		Nodes: make([]model.TopologyNode, 0, len(m.nodes)),
		Edges: make([]model.TopologyEdge, 0),
	}

	for _, node := range m.nodes {
		graph.Nodes = append(graph.Nodes, *node)
	}

	for from, kids := range m.children {
		for _, to := range kids {
			graph.Edges = append(graph.Edges, model.TopologyEdge{
				FromNode: from,
				ToNode:   to,
			})
		}
	}

	return graph, nil
}

func (m *Manager) GetAncestors(nodeName string) (*model.NodeAncestorsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[nodeName]; !exists {
		return nil, fmt.Errorf("node not found: %s", nodeName)
	}

	ancestors := m.collectAncestorsLocked(nodeName)
	return &model.NodeAncestorsResult{
		NodeName:  nodeName,
		Ancestors: ancestors,
	}, nil
}

func (m *Manager) GetDescendants(nodeName string) (*model.NodeDescendantsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[nodeName]; !exists {
		return nil, fmt.Errorf("node not found: %s", nodeName)
	}

	descendants := m.collectDescendantsLocked(nodeName)
	return &model.NodeDescendantsResult{
		NodeName:    nodeName,
		Descendants: descendants,
	}, nil
}

func (m *Manager) GetHolderResourceTree(holder string) (*model.HolderResourceTree, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := &model.HolderResourceTree{
		Holder:    holder,
		RootNodes: make([]string, 0),
		HeldNodes: make([]string, 0),
		Tree:      make(map[string][]string),
	}

	locks, err := m.lockMgr.ListAllLocks()
	if err != nil {
		return nil, err
	}

	lockHeldSet := make(map[string]bool)
	for _, l := range locks {
		if l.Holder == holder && l.Status == model.LockStatusHeld {
			lockHeldSet[l.Name] = true
		}
	}

	heldNodesSet := make(map[string]bool)
	for name, node := range m.nodes {
		if lockHeldSet[node.LockName] {
			heldNodesSet[name] = true
			result.HeldNodes = append(result.HeldNodes, name)
		}
	}

	childrenMap := make(map[string][]string)
	for _, name := range result.HeldNodes {
		for _, child := range m.children[name] {
			if heldNodesSet[child] {
				childrenMap[name] = append(childrenMap[name], child)
			}
		}
	}

	for _, name := range result.HeldNodes {
		isRoot := true
		for _, parent := range m.parents[name] {
			if heldNodesSet[parent] {
				isRoot = false
				break
			}
		}
		if isRoot {
			result.RootNodes = append(result.RootNodes, name)
		}
	}
	result.Tree = childrenMap

	return result, nil
}

func (m *Manager) recordOp(op model.TopologyOperationType, targetNode, holder string, success, rolledBack bool, touched []string, durationMs int64, message string) {
	h := &model.TopologyOperationHistory{
		Operation:    op,
		TargetNode:   targetNode,
		Holder:       holder,
		Success:      success,
		RolledBack:   rolledBack,
		NodesTouched: touched,
		DurationMs:   durationMs,
		Message:      message,
		CreatedAt:    time.Now(),
	}
	_ = m.storage.AddTopoOpHistory(h)
}

func (m *Manager) ListHistory(holder string, limit int) ([]model.TopologyOperationHistory, error) {
	return m.storage.ListTopoOpHistory(holder, limit)
}

func (m *Manager) GetStats() (*model.TopologyStats, error) {
	m.mu.Lock()
	nodeCount := len(m.nodes)
	edgeCount := m.countEdgesLocked()
	m.mu.Unlock()

	total, acquire, release, err := m.storage.CountTopoOps()
	if err != nil {
		return nil, err
	}

	return &model.TopologyStats{
		TotalNodes:      nodeCount,
		TotalEdges:      edgeCount,
		TotalOperations: total,
		AcquireOps:      acquire,
		ReleaseOps:      release,
	}, nil
}

func (m *Manager) ListNodes() ([]model.TopologyNode, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodes := make([]model.TopologyNode, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, *n)
	}
	return nodes, nil
}

func (m *Manager) GetNode(name string) (*model.TopologyNode, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[name]
	if !exists {
		return nil, fmt.Errorf("node not found: %s", name)
	}
	n := *node
	return &n, nil
}

func (m *Manager) RemoveEdge(fromNode, toNode string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, c := range m.children[fromNode] {
		if c == toNode {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("edge not found: %s -> %s", fromNode, toNode)
	}

	if err := m.storage.DeleteTopoEdge(fromNode, toNode); err != nil {
		return err
	}

	m.children[fromNode] = append(m.children[fromNode][:idx], m.children[fromNode][idx+1:]...)

	pIdx := -1
	for i, p := range m.parents[toNode] {
		if p == fromNode {
			pIdx = i
			break
		}
	}
	if pIdx >= 0 {
		m.parents[toNode] = append(m.parents[toNode][:pIdx], m.parents[toNode][pIdx+1:]...)
	}

	log.Printf("[topology] edge removed: %s -> %s", fromNode, toNode)
	return nil
}
