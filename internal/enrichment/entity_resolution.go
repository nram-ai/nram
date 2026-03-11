package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ---------------------------------------------------------------------------
// Dependency-inversion interfaces for entity resolution
// ---------------------------------------------------------------------------

// EntityFinder provides entity lookup and persistence operations needed by the
// entity resolution pipeline.
type EntityFinder interface {
	Upsert(ctx context.Context, entity *model.Entity) error
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
}

// AliasManager provides alias lookup and creation operations.
type AliasManager interface {
	Create(ctx context.Context, alias *model.EntityAlias) error
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.EntityAlias, error)
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

// EntityInput represents an extracted entity to be resolved against existing
// entities in the store. It is the enrichment-package equivalent of the
// service-layer ExtractedEntityData type.
type EntityInput struct {
	Name       string
	Type       string
	Properties map[string]interface{}
}

// ---------------------------------------------------------------------------
// EntityResolver
// ---------------------------------------------------------------------------

// EntityResolver resolves extracted entity names to existing or newly created
// entity records. The resolution order is:
//  1. Canonical name match (exact, via Upsert dedup key)
//  2. Alias lookup
//  3. Name similarity fallback (LIKE match)
//  4. Create new entity
type EntityResolver struct {
	entities EntityFinder
	aliases  AliasManager
}

// NewEntityResolver constructs an EntityResolver with the given storage
// dependencies.
func NewEntityResolver(entities EntityFinder, aliases AliasManager) *EntityResolver {
	return &EntityResolver{
		entities: entities,
		aliases:  aliases,
	}
}

// canonicalize normalises a name to its canonical form: lowercase, trimmed,
// with interior whitespace collapsed to single spaces.
func canonicalize(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	fields := strings.Fields(name)
	return strings.Join(fields, " ")
}

// Resolve attempts to match the given entity name against existing entities
// within the namespace. It returns the resolved entity, whether a new entity
// was created, and any error encountered.
//
// Resolution order:
//  1. Canonical name match — search by similarity for the name+type, check for
//     an exact canonical match.
//  2. Alias lookup — check the alias table for a matching alias.
//  3. Name similarity — re-use FindBySimilarity results to find a fuzzy match,
//     then register the name as an alias.
//  4. Create new — upsert a brand-new entity.
func (r *EntityResolver) Resolve(
	ctx context.Context,
	namespaceID uuid.UUID,
	name string,
	entityType string,
	properties map[string]interface{},
) (*model.Entity, bool, error) {
	canonical := canonicalize(name)

	// Step 1: Canonical name match via FindBySimilarity + exact canonical
	// comparison. We ask for a small limit since we only need an exact hit.
	similar, err := r.entities.FindBySimilarity(ctx, namespaceID, canonical, entityType, 10)
	if err != nil {
		return nil, false, fmt.Errorf("entity resolution: find by similarity: %w", err)
	}

	for i := range similar {
		if similar[i].Canonical == canonical {
			// Exact canonical match — increment mention count and upsert.
			similar[i].MentionCount++
			similar[i].UpdatedAt = time.Now().UTC()
			if err := r.entities.Upsert(ctx, &similar[i]); err != nil {
				return nil, false, fmt.Errorf("entity resolution: upsert existing: %w", err)
			}
			return &similar[i], false, nil
		}
	}

	// Step 2: Alias lookup.
	aliases, err := r.aliases.FindByAlias(ctx, namespaceID, name)
	if err != nil {
		return nil, false, fmt.Errorf("entity resolution: alias lookup: %w", err)
	}
	if len(aliases) > 0 {
		// Found via alias — look up the linked entity through FindBySimilarity
		// using the alias's entity. Use FindByAlias on the entity finder which
		// returns full Entity records.
		entities, err := r.entities.FindByAlias(ctx, namespaceID, name)
		if err != nil {
			return nil, false, fmt.Errorf("entity resolution: find entity by alias: %w", err)
		}
		if len(entities) > 0 {
			entities[0].MentionCount++
			entities[0].UpdatedAt = time.Now().UTC()
			if err := r.entities.Upsert(ctx, &entities[0]); err != nil {
				return nil, false, fmt.Errorf("entity resolution: upsert alias match: %w", err)
			}
			return &entities[0], false, nil
		}
	}

	// Step 3: Name similarity fallback — we already have the results from step
	// 1. If any entity of the same type was returned by FindBySimilarity (but
	// was not an exact canonical match), treat the first one as a fuzzy match.
	if len(similar) > 0 {
		match := similar[0]
		match.MentionCount++
		match.UpdatedAt = time.Now().UTC()
		if err := r.entities.Upsert(ctx, &match); err != nil {
			return nil, false, fmt.Errorf("entity resolution: upsert similar: %w", err)
		}

		// Register the new name as an alias for the matched entity.
		alias := &model.EntityAlias{
			ID:        uuid.New(),
			EntityID:  match.ID,
			Alias:     name,
			AliasType: "similar_name",
		}
		if err := r.aliases.Create(ctx, alias); err != nil {
			return nil, false, fmt.Errorf("entity resolution: create alias: %w", err)
		}

		return &match, false, nil
	}

	// Step 4: Create new entity.
	propsJSON, err := json.Marshal(properties)
	if err != nil {
		return nil, false, fmt.Errorf("entity resolution: marshal properties: %w", err)
	}

	now := time.Now().UTC()
	entity := &model.Entity{
		ID:           uuid.New(),
		NamespaceID:  namespaceID,
		Name:         name,
		Canonical:    canonical,
		EntityType:   entityType,
		Properties:   propsJSON,
		MentionCount: 1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := r.entities.Upsert(ctx, entity); err != nil {
		return nil, false, fmt.Errorf("entity resolution: create new: %w", err)
	}

	return entity, true, nil
}

// ResolveAll resolves a batch of extracted entities, returning a map keyed by
// the original input name to the resolved model.Entity. Entities that appear
// multiple times in the input are resolved only once; subsequent references
// share the same entity and contribute to the mention count.
func (r *EntityResolver) ResolveAll(
	ctx context.Context,
	namespaceID uuid.UUID,
	entities []EntityInput,
) (map[string]*model.Entity, error) {
	result := make(map[string]*model.Entity, len(entities))

	for _, input := range entities {
		// Dedup within batch: if we already resolved this name, just bump the
		// mention count and skip.
		if existing, ok := result[input.Name]; ok {
			existing.MentionCount++
			existing.UpdatedAt = time.Now().UTC()
			if err := r.entities.Upsert(ctx, existing); err != nil {
				return nil, fmt.Errorf("entity resolution: batch dedup upsert: %w", err)
			}
			continue
		}

		entity, _, err := r.Resolve(ctx, namespaceID, input.Name, input.Type, input.Properties)
		if err != nil {
			return nil, fmt.Errorf("entity resolution: resolve %q: %w", input.Name, err)
		}
		result[input.Name] = entity
	}

	return result, nil
}
