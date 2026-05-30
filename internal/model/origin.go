package model

// MemoryOrigin is the coarse, server-assigned provenance category for a memory.
// It is the authoritative discriminator that internal logic branches on —
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
// as user rather than as an empty (invalid) enum value — the column's
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
