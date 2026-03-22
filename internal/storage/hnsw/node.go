package hnsw

import "github.com/google/uuid"

// Node represents a vector with its unique identifier for insertion into the graph.
type Node struct {
	ID     uuid.UUID
	Vector []float32
}

// SearchResult represents a search hit with its cosine similarity score.
type SearchResult struct {
	ID    uuid.UUID
	Score float64 // cosine similarity: 1.0 = identical, 0.0 = orthogonal
}

// graphNode is the internal representation of a node within the HNSW graph.
type graphNode struct {
	id      uuid.UUID
	vector  []float32
	norm    float32
	level   int
	friends [][]*graphNode // friends[layer] = neighbor list for that layer
}

// newGraphNode creates an internal graph node with precomputed norm.
func newGraphNode(id uuid.UUID, vector []float32, level int) *graphNode {
	n := &graphNode{
		id:     id,
		vector: vector,
		norm:   Norm(vector),
		level:  level,
	}
	n.friends = make([][]*graphNode, level+1)
	for i := range n.friends {
		n.friends[i] = make([]*graphNode, 0)
	}
	return n
}
