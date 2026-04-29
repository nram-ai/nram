package dreaming

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryReader retrieves memories.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error)
}

// MemoryWriter creates and updates memories.
type MemoryWriter interface {
	Create(ctx context.Context, mem *model.Memory) error
	Update(ctx context.Context, mem *model.Memory) error
	// UpdateMetadata writes only the metadata column without bumping
	// updated_at. Phases use it to record visit stamps so the staleness
	// check (stamp < updated_at) does not immediately re-invalidate the
	// stamp on the next cycle.
	UpdateMetadata(ctx context.Context, id, namespaceID uuid.UUID, metadata json.RawMessage) error
	SoftDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	HardDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	// DecayConfidence multiplicatively scales confidence for the given IDs,
	// clamped to floor. Used by the pruning phase to fade idle memories.
	DecayConfidence(ctx context.Context, ids []uuid.UUID, multiplier, floor float64) (int64, error)
}

// EntityReader reads entity data.
type EntityReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Entity, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
}

// EntityWriter creates and updates entities.
type EntityWriter interface {
	Upsert(ctx context.Context, entity *model.Entity) error
}

// EntityAliasWriter creates entity aliases.
type EntityAliasWriter interface {
	Create(ctx context.Context, alias *model.EntityAlias) error
}

// RelationshipReader reads relationship data.
type RelationshipReader interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Relationship, error)
	ListByEntity(ctx context.Context, entityID uuid.UUID) ([]model.Relationship, error)
	TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops int) ([]model.Relationship, error)
	FindActiveByTriple(ctx context.Context, namespaceID, sourceID, targetID uuid.UUID, relation string) (*model.Relationship, error)
	CountActiveByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error)
}

// RelationshipWriter creates and modifies relationships.
type RelationshipWriter interface {
	Create(ctx context.Context, rel *model.Relationship) error
	Reinforce(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	Expire(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	DeleteByID(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	UpdateWeight(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, weight float64) error
	ExpireLowWeight(ctx context.Context, namespaceID uuid.UUID, threshold float64) (int64, error)
}

// LineageWriter creates memory lineage records and answers questions a
// dreaming phase needs to ask about prior lineage state at write time.
// CountConflictsBetween reads the table but lives here because the
// contradiction phase needs both the read and the write to make a single
// detection decision; splitting them across two interfaces would force
// callers to wire in two dependencies for one operation.
type LineageWriter interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
	CountConflictsBetween(ctx context.Context, namespaceID, aID, bID uuid.UUID) (int, error)
}

// LineageReader reads memory lineage records.
type LineageReader interface {
	FindConflicts(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error)
}

// LLMProviderFunc returns an LLM provider or nil if unavailable.
type LLMProviderFunc func() provider.LLMProvider

// EmbeddingProviderFunc returns an embedding provider or nil if unavailable.
type EmbeddingProviderFunc func() provider.EmbeddingProvider

// SettingsResolver resolves configuration values.
type SettingsResolver interface {
	Resolve(ctx context.Context, key string, scope string) (string, error)
	ResolveFloat(ctx context.Context, key string, scope string) (float64, error)
	ResolveInt(ctx context.Context, key string, scope string) (int, error)
	ResolveBool(ctx context.Context, key string, scope string) bool
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
	ResolveFloatWithDefault(ctx context.Context, key, scope string) float64
	ResolveDurationSecondsWithDefault(ctx context.Context, key, scope string) time.Duration
}

// VectorPurger removes a vector from the active vector store. The dreaming
// system calls it whenever a memory or entity transitions to a state in which
// it should no longer surface via vector search: soft-delete, novelty-audit
// demotion, or supersession. Implementations should be idempotent — calling
// Delete on an already-absent id is a no-op. Kind selects the table family.
type VectorPurger interface {
	Delete(ctx context.Context, kind storage.VectorKind, id uuid.UUID) error
}

// MemoryHardDeleter deletes memory rows past their soft-delete retention
// window. Implementations drive the retention sweep's hard-delete pass so
// vector rows cascade and disk/index space is reclaimed.
type MemoryHardDeleter interface {
	HardDeleteSoftDeletedBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// MemoryDimRepairer locates memories whose embedding_dim is set but whose
// matching memory_vectors_<dim> row is missing. The embedding-backfill
// phase pages through divergent rows per supported dim and either re-
// embeds them or clears embedding_dim so the row state matches the
// vector store.
type MemoryDimRepairer interface {
	FindMemoriesMissingVector(ctx context.Context, namespaceID uuid.UUID, dim, limit int) ([]model.Memory, error)
}
