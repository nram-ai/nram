package hnsw

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"

	"github.com/google/uuid"
)

var magicBytes = [4]byte{'H', 'N', 'S', 'W'}

const formatVersion uint8 = 1

// Export writes the graph to a binary format. Thread-safe (holds read lock).
func (g *Graph) Export(w io.Writer) error {
	g.mu.RLock()
	defer g.mu.RUnlock()

	bw := bufio.NewWriter(w)

	// Header: magic bytes
	if _, err := bw.Write(magicBytes[:]); err != nil {
		return fmt.Errorf("hnsw: export: write magic: %w", err)
	}

	// Header: version
	if err := bw.WriteByte(formatVersion); err != nil {
		return fmt.Errorf("hnsw: export: write version: %w", err)
	}

	// Header: parameters
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.dimension)); err != nil {
		return fmt.Errorf("hnsw: export: write dimension: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.m)); err != nil {
		return fmt.Errorf("hnsw: export: write M: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.mMax0)); err != nil {
		return fmt.Errorf("hnsw: export: write MMax0: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.efConstruction)); err != nil {
		return fmt.Errorf("hnsw: export: write EfConstruction: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.efSearch)); err != nil {
		return fmt.Errorf("hnsw: export: write EfSearch: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, math.Float64bits(g.mL)); err != nil {
		return fmt.Errorf("hnsw: export: write Ml: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(g.nodes))); err != nil {
		return fmt.Errorf("hnsw: export: write NodeCount: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(g.maxLevel)); err != nil {
		return fmt.Errorf("hnsw: export: write MaxLevel: %w", err)
	}

	// EntryPointID: 16 bytes UUID or all zeros
	var epID uuid.UUID
	if g.entryPoint != nil {
		epID = g.entryPoint.id
	}
	if _, err := bw.Write(epID[:]); err != nil {
		return fmt.Errorf("hnsw: export: write EntryPointID: %w", err)
	}

	// Per node
	for _, gn := range g.nodes {
		// NodeID
		if _, err := bw.Write(gn.id[:]); err != nil {
			return fmt.Errorf("hnsw: export: write NodeID: %w", err)
		}

		// Vector: dimension * float32
		for _, v := range gn.vector {
			if err := binary.Write(bw, binary.LittleEndian, v); err != nil {
				return fmt.Errorf("hnsw: export: write vector component: %w", err)
			}
		}

		// NodeLevel
		if err := binary.Write(bw, binary.LittleEndian, uint32(gn.level)); err != nil {
			return fmt.Errorf("hnsw: export: write NodeLevel: %w", err)
		}

		// For each layer 0..NodeLevel
		for layer := 0; layer <= gn.level; layer++ {
			var friends []*graphNode
			if layer < len(gn.friends) {
				// Filter out any neighbors that have been deleted from the graph
				// (the delete heuristic may leave stale pointers).
				for _, f := range gn.friends[layer] {
					if _, ok := g.nodes[f.id]; ok {
						friends = append(friends, f)
					}
				}
			}
			if err := binary.Write(bw, binary.LittleEndian, uint32(len(friends))); err != nil {
				return fmt.Errorf("hnsw: export: write NeighborCount layer %d: %w", layer, err)
			}
			for _, neighbor := range friends {
				if _, err := bw.Write(neighbor.id[:]); err != nil {
					return fmt.Errorf("hnsw: export: write NeighborID: %w", err)
				}
			}
		}
	}

	return bw.Flush()
}

// Import reads a graph from the binary format produced by Export.
func Import(r io.Reader) (*Graph, error) {
	br := bufio.NewReader(r)

	// Header: magic bytes
	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("hnsw: import: read magic: %w", err)
	}
	if magic != magicBytes {
		return nil, fmt.Errorf("hnsw: import: invalid magic bytes %q, expected %q", magic[:], magicBytes[:])
	}

	// Header: version
	version, err := br.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("hnsw: import: read version: %w", err)
	}
	if version != formatVersion {
		return nil, fmt.Errorf("hnsw: import: unsupported version %d, expected %d", version, formatVersion)
	}

	// Header: parameters
	var dimension, m, mMax0, efConstruction, efSearch, nodeCount, maxLevel uint32
	var mlBits uint64

	if err := binary.Read(br, binary.LittleEndian, &dimension); err != nil {
		return nil, fmt.Errorf("hnsw: import: read dimension: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &m); err != nil {
		return nil, fmt.Errorf("hnsw: import: read M: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &mMax0); err != nil {
		return nil, fmt.Errorf("hnsw: import: read MMax0: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &efConstruction); err != nil {
		return nil, fmt.Errorf("hnsw: import: read EfConstruction: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &efSearch); err != nil {
		return nil, fmt.Errorf("hnsw: import: read EfSearch: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &mlBits); err != nil {
		return nil, fmt.Errorf("hnsw: import: read Ml: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &nodeCount); err != nil {
		return nil, fmt.Errorf("hnsw: import: read NodeCount: %w", err)
	}
	if err := binary.Read(br, binary.LittleEndian, &maxLevel); err != nil {
		return nil, fmt.Errorf("hnsw: import: read MaxLevel: %w", err)
	}

	var epID uuid.UUID
	if _, err := io.ReadFull(br, epID[:]); err != nil {
		return nil, fmt.Errorf("hnsw: import: read EntryPointID: %w", err)
	}

	mL := math.Float64frombits(mlBits)

	g := &Graph{
		dimension:      int(dimension),
		m:              int(m),
		mMax0:          int(mMax0),
		efConstruction: int(efConstruction),
		efSearch:       int(efSearch),
		mL:             mL,
		maxLevel:       int(maxLevel),
		nodes:          make(map[uuid.UUID]*graphNode, nodeCount),
		rng:            nil, // will be set after
	}

	// First pass: read all nodes (without resolving neighbor pointers).
	// Store neighbor IDs temporarily.
	type layerNeighborIDs struct {
		ids []uuid.UUID
	}
	type pendingNode struct {
		gn     *graphNode
		layers []layerNeighborIDs
	}
	pending := make([]pendingNode, 0, nodeCount)

	for i := uint32(0); i < nodeCount; i++ {
		var nodeID uuid.UUID
		if _, err := io.ReadFull(br, nodeID[:]); err != nil {
			return nil, fmt.Errorf("hnsw: import: read NodeID [%d]: %w", i, err)
		}

		vector := make([]float32, dimension)
		for d := uint32(0); d < dimension; d++ {
			if err := binary.Read(br, binary.LittleEndian, &vector[d]); err != nil {
				return nil, fmt.Errorf("hnsw: import: read vector[%d][%d]: %w", i, d, err)
			}
		}

		var nodeLevel uint32
		if err := binary.Read(br, binary.LittleEndian, &nodeLevel); err != nil {
			return nil, fmt.Errorf("hnsw: import: read NodeLevel [%d]: %w", i, err)
		}

		gn := &graphNode{
			id:     nodeID,
			vector: vector,
			norm:   Norm(vector),
			level:  int(nodeLevel),
		}
		gn.friends = make([][]*graphNode, nodeLevel+1)

		layers := make([]layerNeighborIDs, nodeLevel+1)
		for layer := uint32(0); layer <= nodeLevel; layer++ {
			var neighborCount uint32
			if err := binary.Read(br, binary.LittleEndian, &neighborCount); err != nil {
				return nil, fmt.Errorf("hnsw: import: read NeighborCount [%d][%d]: %w", i, layer, err)
			}
			ids := make([]uuid.UUID, neighborCount)
			for n := uint32(0); n < neighborCount; n++ {
				if _, err := io.ReadFull(br, ids[n][:]); err != nil {
					return nil, fmt.Errorf("hnsw: import: read NeighborID [%d][%d][%d]: %w", i, layer, n, err)
				}
			}
			layers[layer] = layerNeighborIDs{ids: ids}
		}

		g.nodes[nodeID] = gn
		pending = append(pending, pendingNode{gn: gn, layers: layers})
	}

	// Second pass: resolve neighbor pointers.
	for _, p := range pending {
		for layer, ln := range p.layers {
			friends := make([]*graphNode, 0, len(ln.ids))
			for _, nid := range ln.ids {
				neighbor, ok := g.nodes[nid]
				if !ok {
					return nil, fmt.Errorf("hnsw: import: neighbor %s not found for node %s layer %d", nid, p.gn.id, layer)
				}
				friends = append(friends, neighbor)
			}
			p.gn.friends[layer] = friends
		}
	}

	// Set entry point.
	if epID != (uuid.UUID{}) {
		ep, ok := g.nodes[epID]
		if !ok {
			return nil, fmt.Errorf("hnsw: import: entry point %s not found in nodes", epID)
		}
		g.entryPoint = ep
	}

	// Initialize RNG with default seed (matches NewGraph default).
	g.rng = newDefaultRNG()

	return g, nil
}

// newDefaultRNG creates the default RNG matching NewGraph.
func newDefaultRNG() *rand.Rand {
	return rand.New(rand.NewSource(42))
}
