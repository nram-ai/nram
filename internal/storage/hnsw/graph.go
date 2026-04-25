package hnsw

import (
	"container/heap"
	"errors"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/google/uuid"
)

var (
	// ErrDimensionMismatch is returned when a vector's dimension does not match the graph dimension.
	ErrDimensionMismatch = errors.New("hnsw: vector dimension does not match graph dimension")
	// ErrEmptyVector is returned when an empty vector is provided.
	ErrEmptyVector = errors.New("hnsw: empty vector")
	// ErrEmptyGraph is returned when searching an empty graph.
	ErrEmptyGraph = errors.New("hnsw: graph is empty")
)

// Graph is a Hierarchical Navigable Small World graph for approximate nearest neighbor search.
type Graph struct {
	mu             sync.RWMutex
	dimension      int
	m              int     // max neighbors per upper layer
	mMax0          int     // max neighbors for layer 0 (2*M)
	efConstruction int     // candidate pool size during construction
	efSearch       int     // candidate pool size during search
	mL             float64 // level multiplier: 1/ln(M)
	entryPoint     *graphNode
	maxLevel       int
	nodes          map[uuid.UUID]*graphNode
	rng            *rand.Rand
}

// Option configures Graph construction parameters.
type Option func(*Graph)

// WithM sets the maximum number of neighbors per upper layer. Default is 16.
func WithM(m int) Option {
	return func(g *Graph) {
		if m > 0 {
			g.m = m
			g.mMax0 = 2 * m
			g.mL = 1.0 / math.Log(float64(m))
		}
	}
}

// WithEfConstruction sets the construction candidate pool size. Default is 200.
func WithEfConstruction(ef int) Option {
	return func(g *Graph) {
		if ef > 0 {
			g.efConstruction = ef
		}
	}
}

// WithEfSearch sets the search candidate pool size. Default is 50.
func WithEfSearch(ef int) Option {
	return func(g *Graph) {
		if ef > 0 {
			g.efSearch = ef
		}
	}
}

// WithSeed sets the random seed for reproducible level assignment.
func WithSeed(seed int64) Option {
	return func(g *Graph) {
		g.rng = rand.New(rand.NewSource(seed))
	}
}

// NewGraph creates a new HNSW graph for vectors of the given dimension.
func NewGraph(dimension int, opts ...Option) *Graph {
	g := &Graph{
		dimension:      dimension,
		m:              16,
		mMax0:          32,
		efConstruction: 200,
		efSearch:       50,
		mL:             1.0 / math.Log(16.0),
		maxLevel:       -1,
		nodes:          make(map[uuid.UUID]*graphNode),
		rng:            rand.New(rand.NewSource(42)),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Len returns the number of nodes in the graph.
func (g *Graph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}

// Dimension returns the vector dimension of the graph.
func (g *Graph) Dimension() int {
	return g.dimension
}

// Has returns true if a node with the given ID exists in the graph.
func (g *Graph) Has(id uuid.UUID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.nodes[id]
	return ok
}

// GetVectors returns defensive copies of the stored vectors for the given
// IDs. Missing IDs (no node in the graph) are absent from the returned map.
// Copies are made under the read lock so callers cannot observe partial
// mutations from a concurrent Add or Delete.
func (g *Graph) GetVectors(ids []uuid.UUID) map[uuid.UUID][]float32 {
	out := make(map[uuid.UUID][]float32, len(ids))
	if len(ids) == 0 {
		return out
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, id := range ids {
		n, ok := g.nodes[id]
		if !ok {
			continue
		}
		cp := make([]float32, len(n.vector))
		copy(cp, n.vector)
		out[id] = cp
	}
	return out
}

// Add inserts one or more nodes into the graph. Thread-safe.
// Returns ErrEmptyVector if any vector is empty, ErrDimensionMismatch if dimensions don't match.
func (g *Graph) Add(nodes ...Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, n := range nodes {
		if err := g.addSingle(n); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) addSingle(n Node) error {
	if len(n.Vector) == 0 {
		return ErrEmptyVector
	}
	if len(n.Vector) != g.dimension {
		return ErrDimensionMismatch
	}

	// If the node already exists, remove it first (update semantics).
	if existing, ok := g.nodes[n.ID]; ok {
		g.removeLocked(existing)
	}

	level := g.randomLevel()
	gn := newGraphNode(n.ID, n.Vector, level)

	if g.entryPoint == nil {
		// First node in the graph.
		g.entryPoint = gn
		g.maxLevel = level
		g.nodes[n.ID] = gn
		return nil
	}

	ep := g.entryPoint

	// Phase 1: Greedily descend through upper layers above the new node's level.
	for lc := g.maxLevel; lc > level; lc-- {
		ep = g.greedyClosest(ep, gn.vector, gn.norm, lc)
	}

	// Phase 2: At each layer from min(level, maxLevel) down to 0,
	// find efConstruction nearest neighbors and connect.
	for lc := min(level, g.maxLevel); lc >= 0; lc-- {
		neighbors := g.searchLayer(ep, gn.vector, gn.norm, g.efConstruction, lc)
		maxConn := g.m
		if lc == 0 {
			maxConn = g.mMax0
		}
		selected := g.selectNeighborsHeuristic(gn, neighbors, maxConn, lc)

		// Connect the new node to selected neighbors.
		gn.friends[lc] = selected

		// Add back-connections from each selected neighbor to the new node.
		for _, neighbor := range selected {
			neighbor.friends[lc] = append(neighbor.friends[lc], gn)
			nMaxConn := g.m
			if lc == 0 {
				nMaxConn = g.mMax0
			}
			if len(neighbor.friends[lc]) > nMaxConn {
				// Prune the neighbor's connections using the heuristic.
				neighbor.friends[lc] = g.selectNeighborsHeuristic(neighbor, neighbor.friends[lc], nMaxConn, lc)
			}
		}

		if len(neighbors) > 0 {
			ep = neighbors[0]
		}
	}

	// Update entry point if this node has a higher level.
	if level > g.maxLevel {
		g.entryPoint = gn
		g.maxLevel = level
	}

	g.nodes[n.ID] = gn
	return nil
}

// Search finds the k nearest neighbors to the query vector. Thread-safe.
// Returns results sorted by descending similarity score.
func (g *Graph) Search(query []float32, k int) ([]SearchResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(query) == 0 {
		return nil, ErrEmptyVector
	}
	if len(query) != g.dimension {
		return nil, ErrDimensionMismatch
	}
	if g.entryPoint == nil {
		return nil, ErrEmptyGraph
	}

	queryNorm := Norm(query)

	ep := g.entryPoint

	// Phase 1: Greedily descend from the top layer to layer 1.
	for lc := g.maxLevel; lc > 0; lc-- {
		ep = g.greedyClosest(ep, query, queryNorm, lc)
	}

	// Phase 2: Search layer 0 with efSearch candidates.
	ef := g.efSearch
	if ef < k {
		ef = k
	}
	candidates := g.searchLayer(ep, query, queryNorm, ef, 0)

	// Build results from top candidates.
	if k > len(candidates) {
		k = len(candidates)
	}

	// Score and sort candidates.
	type scored struct {
		node  *graphNode
		score float64
	}
	all := make([]scored, len(candidates))
	for i, c := range candidates {
		all[i] = scored{
			node:  c,
			score: CosineSimilarityWithNorms(query, c.vector, queryNorm, c.norm),
		}
	}

	// Sort descending by score.
	sort.Slice(all, func(i, j int) bool {
		return all[i].score > all[j].score
	})

	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = SearchResult{
			ID:    all[i].node.id,
			Score: all[i].score,
		}
	}
	return results, nil
}

// Delete removes a node from the graph. Thread-safe. Returns true if the node was found and removed.
func (g *Graph) Delete(id uuid.UUID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	gn, ok := g.nodes[id]
	if !ok {
		return false
	}

	g.removeLocked(gn)
	return true
}

// removeLocked removes a node from the graph. Caller must hold write lock.
func (g *Graph) removeLocked(gn *graphNode) {
	// For each layer, repair neighbor connections.
	for lc := 0; lc <= gn.level; lc++ {
		maxConn := g.m
		if lc == 0 {
			maxConn = g.mMax0
		}

		// Collect all neighbors of the deleted node at this layer.
		neighbors := gn.friends[lc]

		// Remove the deleted node from all neighbor friend lists.
		for _, neighbor := range neighbors {
			neighbor.friends[lc] = removeFromSlice(neighbor.friends[lc], gn)
		}

		// For each orphaned neighbor, try to reconnect through the deleted node's neighborhood.
		for _, neighbor := range neighbors {
			if len(neighbor.friends[lc]) < maxConn/2 {
				// Find replacement connections from the deleted node's other neighbors.
				for _, candidate := range neighbors {
					if candidate == neighbor {
						continue
					}
					if containsNode(neighbor.friends[lc], candidate) {
						continue
					}
					neighbor.friends[lc] = append(neighbor.friends[lc], candidate)
					if len(neighbor.friends[lc]) >= maxConn {
						break
					}
				}
			}
		}
	}

	delete(g.nodes, gn.id)

	// Update entry point if we just deleted it.
	if g.entryPoint == gn {
		g.entryPoint = nil
		g.maxLevel = -1
		// Find the node with the highest level as the new entry point.
		for _, n := range g.nodes {
			if n.level > g.maxLevel {
				g.maxLevel = n.level
				g.entryPoint = n
			}
		}
	}
}

// randomLevel generates a random level for a new node using the HNSW formula:
// floor(-ln(uniform(0,1)) * mL)
func (g *Graph) randomLevel() int {
	r := g.rng.Float64()
	if r == 0 {
		r = 1e-10
	}
	return int(math.Floor(-math.Log(r) * g.mL))
}

// greedyClosest performs a greedy search from ep to find the closest node to the query at the given layer.
func (g *Graph) greedyClosest(ep *graphNode, query []float32, queryNorm float32, layer int) *graphNode {
	current := ep
	currentScore := CosineSimilarityWithNorms(query, current.vector, queryNorm, current.norm)

	for {
		improved := false
		if layer < len(current.friends) {
			for _, neighbor := range current.friends[layer] {
				score := CosineSimilarityWithNorms(query, neighbor.vector, queryNorm, neighbor.norm)
				if score > currentScore {
					current = neighbor
					currentScore = score
					improved = true
				}
			}
		}
		if !improved {
			break
		}
	}
	return current
}

// searchLayer performs a beam search at a given layer, returning up to ef closest nodes.
func (g *Graph) searchLayer(ep *graphNode, query []float32, queryNorm float32, ef int, layer int) []*graphNode {
	visited := make(map[*graphNode]bool, ef*2)
	visited[ep] = true

	epScore := CosineSimilarityWithNorms(query, ep.vector, queryNorm, ep.norm)

	// candidates is a max-heap so we can pop the best (highest similarity) candidate.
	candidatesSlice := make(maxHeap, 1, ef)
	candidatesSlice[0] = candidate{node: ep, score: epScore}
	candidates := &candidatesSlice
	heap.Init(candidates)

	// results is a min-heap so we can check/pop the worst (lowest similarity) result.
	resultsSlice := make(minHeap, 1, ef)
	resultsSlice[0] = candidate{node: ep, score: epScore}
	results := &resultsSlice
	heap.Init(results)

	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(candidate)

		// If the best candidate is worse than the worst result, stop.
		worst := (*results)[0]
		if c.score < worst.score && results.Len() >= ef {
			break
		}

		if layer >= len(c.node.friends) {
			continue
		}

		for _, neighbor := range c.node.friends[layer] {
			if visited[neighbor] {
				continue
			}
			visited[neighbor] = true

			score := CosineSimilarityWithNorms(query, neighbor.vector, queryNorm, neighbor.norm)

			worstResult := (*results)[0]
			if results.Len() < ef || score > worstResult.score {
				heap.Push(candidates, candidate{node: neighbor, score: score})
				heap.Push(results, candidate{node: neighbor, score: score})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// Extract results from the min-heap.
	out := make([]*graphNode, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(candidate).node
	}
	return out
}

// selectNeighborsHeuristic implements Algorithm 4 from the HNSW paper.
// It selects up to maxConn neighbors that provide diverse coverage.
func (g *Graph) selectNeighborsHeuristic(target *graphNode, candidates []*graphNode, maxConn int, layer int) []*graphNode {
	if len(candidates) <= maxConn {
		result := make([]*graphNode, len(candidates))
		copy(result, candidates)
		return result
	}

	// Build a working set sorted by distance to target (descending similarity = ascending distance).
	type scoredNode struct {
		node  *graphNode
		score float64
	}
	working := make([]scoredNode, len(candidates))
	for i, c := range candidates {
		working[i] = scoredNode{
			node:  c,
			score: CosineSimilarityWithNorms(target.vector, c.vector, target.norm, c.norm),
		}
	}

	// Sort descending by score (closest first).
	sort.Slice(working, func(i, j int) bool {
		return working[i].score > working[j].score
	})

	selected := make([]*graphNode, 0, maxConn)
	for _, w := range working {
		if len(selected) >= maxConn {
			break
		}

		// Check if this candidate is closer to the target than to any already selected neighbor.
		// This is the diversity heuristic from Algorithm 4.
		good := true
		for _, s := range selected {
			simToSelected := CosineSimilarityWithNorms(w.node.vector, s.vector, w.node.norm, s.norm)
			if simToSelected > w.score {
				good = false
				break
			}
		}
		if good {
			selected = append(selected, w.node)
		}
	}

	// If we didn't fill up with the heuristic, add remaining candidates by proximity.
	if len(selected) < maxConn {
		selectedSet := make(map[uuid.UUID]bool, len(selected))
		for _, s := range selected {
			selectedSet[s.id] = true
		}
		for _, w := range working {
			if len(selected) >= maxConn {
				break
			}
			if !selectedSet[w.node.id] {
				selected = append(selected, w.node)
				selectedSet[w.node.id] = true
			}
		}
	}

	return selected
}

// removeFromSlice removes a specific node from a slice of graphNode pointers.
func removeFromSlice(s []*graphNode, target *graphNode) []*graphNode {
	for i, n := range s {
		if n == target {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// containsNode checks if a slice contains a specific graphNode.
func containsNode(s []*graphNode, target *graphNode) bool {
	for _, n := range s {
		if n == target {
			return true
		}
	}
	return false
}
