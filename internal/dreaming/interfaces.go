package dreaming

import (
	"context"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// MemoryReader retrieves memories.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error)
}

// MemoryWriter creates and updates memories.
type MemoryWriter interface {
	Create(ctx context.Context, mem *model.Memory) error
	Update(ctx context.Context, mem *model.Memory) error
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

// LineageWriter creates memory lineage records.
type LineageWriter interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
}

// LineageReader reads memory lineage records.
type LineageReader interface {
	FindConflicts(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error)
}

// LLMProviderFunc returns an LLM provider or nil if unavailable.
type LLMProviderFunc func() provider.LLMProvider

// SettingsResolver resolves configuration values.
type SettingsResolver interface {
	Resolve(ctx context.Context, key string, scope string) (string, error)
	ResolveFloat(ctx context.Context, key string, scope string) (float64, error)
	ResolveInt(ctx context.Context, key string, scope string) (int, error)
	ResolveBool(ctx context.Context, key string, scope string) bool
}

// TokenRecorder persists token-usage telemetry. Dreaming phases attribute
// every LLM call to the token_usage table via this interface so the UI's
// analytics view reflects actual consumption.
type TokenRecorder interface {
	Record(ctx context.Context, usage *model.TokenUsage) error
}

// UsageContextResolver resolves org, user, and project IDs from a namespace
// ID so that token usage records written by dreaming can be attributed to
// the correct owner in analytics.
type UsageContextResolver interface {
	ResolveUsageContext(ctx context.Context, namespaceID uuid.UUID) (*model.UsageContext, error)
}
