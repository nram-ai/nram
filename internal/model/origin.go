package model

import "encoding/json"

// MemoryOrigin is the coarse, server-assigned provenance category for a memory.
// It is the authoritative discriminator that internal logic branches on,
// replacing the historical practice of overloading the free-form Source string
// (which carried the literal "dream" value). Origin is assigned on the write
// path and is never accepted from request input.
type MemoryOrigin string

const (
	// OriginUser marks a memory created through a user-facing store path.
	// This is the column default, so any row without an explicit origin reads
	// back as OriginUser.
	OriginUser MemoryOrigin = "user"
	// OriginDream marks a memory synthesized by the dream/consolidation cycle.
	// This is the load-bearing flag for the dream-recursion guard: dreams must
	// never be re-clustered, re-enriched, or fed back into the dirty tracker.
	OriginDream MemoryOrigin = "dream"
	// OriginImport marks a memory created through the bulk import path.
	OriginImport MemoryOrigin = "import"
)

// OrDefault returns o, or OriginUser when o is empty. The storage boundary
// uses it so a Memory built without an explicit origin persists and reads back
// as user rather than as an empty (invalid) enum value; the column's
// DEFAULT 'user' only applies when the column is omitted from the INSERT, and
// the repo always writes it explicitly.
func (o MemoryOrigin) OrDefault() MemoryOrigin {
	if o == "" {
		return OriginUser
	}
	return o
}

// IsDream reports whether the memory was produced by the dream cycle. This is
// the canonical replacement for the old `Source == DreamSource` check.
func (m *Memory) IsDream() bool {
	return m.Origin == OriginDream
}

// IsConsolidationDream reports whether the memory is a consolidation synthesis,
// as opposed to other dream-origin memories (notably project-description blurbs
// written by ProjectDescriptionPhase, which carry nram_kind=project_description
// and no source IDs). Consolidation syntheses are the only dreams whose source
// memories get superseded and their entities reaped, so they are the only
// dreams eligible for entity extraction (the consolidation-erases-coverage fix).
//
// The discriminator is the DreamMetaSourceMemoryIDs ("source_memory_ids")
// metadata key: the consolidation phase records the IDs of the source memories
// it consumed, and no other dream-origin writer sets it. A dream with an empty
// or absent source_memory_ids list is not treated as a consolidation synthesis.
func (m *Memory) IsConsolidationDream() bool {
	if m.Origin != OriginDream || len(m.Metadata) == 0 {
		return false
	}
	var meta struct {
		// Tag mirrors DreamMetaSourceMemoryIDs (model/dream.go).
		SourceMemoryIDs []string `json:"source_memory_ids"`
	}
	if err := json.Unmarshal(m.Metadata, &meta); err != nil {
		return false
	}
	return len(meta.SourceMemoryIDs) > 0
}
