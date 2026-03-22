package hnsw

// candidate is an item in the priority queue, pairing a graph node with its similarity score.
type candidate struct {
	node  *graphNode
	score float64
}

// minHeap is a min-heap of candidates ordered by score (lowest score at top).
// Used during search to track the closest candidates found so far.
type minHeap []candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].score < h[j].score }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// maxHeap is a max-heap of candidates ordered by score (highest score at top).
// Used to maintain the top-k results and prune the candidate set.
type maxHeap []candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].score > h[j].score }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// resultHeap is a min-heap of SearchResult ordered by score (lowest first).
// Used to maintain top-k search results efficiently.
type resultHeap []SearchResult

func (h resultHeap) Len() int            { return len(h) }
func (h resultHeap) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h resultHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x interface{}) { *h = append(*h, x.(SearchResult)) }
func (h *resultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
