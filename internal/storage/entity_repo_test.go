package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func newTestEntity(namespaceID uuid.UUID) *model.Entity {
	return &model.Entity{
		NamespaceID:  namespaceID,
		Name:         "John Doe",
		Canonical:    "john_doe",
		EntityType:   "person",
		MentionCount: 1,
		Properties:   json.RawMessage(`{"role":"engineer"}`),
		Metadata:     json.RawMessage(`{"source":"test"}`),
	}
}

func createTestEntityAlias(t *testing.T, ctx context.Context, db DB, namespaceID, entityID uuid.UUID, alias, aliasType string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	query := `INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type) VALUES (?, ?, ?, ?, ?)`
	if db.Backend() == BackendPostgres {
		query = `INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type) VALUES ($1, $2, $3, $4, $5)`
	}
	_, err := db.Exec(ctx, query, id.String(), namespaceID.String(), entityID.String(), alias, aliasType)
	if err != nil {
		t.Fatalf("failed to create test entity alias: %v", err)
	}
	return id
}

func TestEntityRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := newTestEntity(nsID)
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create entity: %v", err)
		}

		if entity.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if entity.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, entity.NamespaceID)
		}
		if entity.Name != "John Doe" {
			t.Fatalf("unexpected name: %q", entity.Name)
		}
		if entity.Canonical != "john_doe" {
			t.Fatalf("unexpected canonical: %q", entity.Canonical)
		}
		if entity.EntityType != "person" {
			t.Fatalf("unexpected entity_type: %q", entity.EntityType)
		}
		if entity.MentionCount != 1 {
			t.Fatalf("expected mention_count 1, got %d", entity.MentionCount)
		}
		if !jsonEqual(string(entity.Properties), `{"role":"engineer"}`) {
			t.Fatalf("unexpected properties: %q", string(entity.Properties))
		}
		if !jsonEqual(string(entity.Metadata), `{"source":"test"}`) {
			t.Fatalf("unexpected metadata: %q", string(entity.Metadata))
		}
		if entity.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if entity.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
	})
}

func TestEntityRepo_DeleteByIDs_CascadesRelationships(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		src := &model.Entity{NamespaceID: nsID, Name: "Source", Canonical: "source", EntityType: "concept", MentionCount: 1}
		tgt := &model.Entity{NamespaceID: nsID, Name: "Target", Canonical: "target", EntityType: "concept", MentionCount: 1}
		if err := repo.Create(ctx, src); err != nil {
			t.Fatalf("create source: %v", err)
		}
		if err := repo.Create(ctx, tgt); err != nil {
			t.Fatalf("create target: %v", err)
		}
		rel := &model.Relationship{NamespaceID: nsID, SourceID: src.ID, TargetID: tgt.ID, Relation: "related_to", Weight: 1}
		if err := relRepo.Create(ctx, rel); err != nil {
			t.Fatalf("create relationship: %v", err)
		}

		deleted, err := repo.DeleteByIDs(ctx, []uuid.UUID{src.ID})
		if err != nil {
			t.Fatalf("DeleteByIDs: %v", err)
		}
		if len(deleted) != 1 || deleted[0] != src.ID {
			t.Fatalf("deleted = %v, want [%s]", deleted, src.ID)
		}

		// Source is gone.
		if got, _ := repo.GetByID(ctx, src.ID, nsID); got != nil {
			t.Fatalf("source entity still present after delete")
		}
		// Target survives.
		if got, _ := repo.GetByID(ctx, tgt.ID, nsID); got == nil {
			t.Fatalf("target entity was unexpectedly removed")
		}
		// The relationship cascaded away with its source endpoint.
		rels, err := relRepo.ListByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("list relationships: %v", err)
		}
		if len(rels) != 0 {
			t.Fatalf("relationship not cascade-deleted: %d remain", len(rels))
		}
	})
}

func TestEntityRepo_DeleteByIDs_EmptyIsNoop(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		deleted, err := repo.DeleteByIDs(ctx, nil)
		if err != nil {
			t.Fatalf("DeleteByIDs(nil): %v", err)
		}
		if len(deleted) != 0 {
			t.Fatalf("expected no deletions, got %v", deleted)
		}
	})
}

func TestEntityRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := &model.Entity{
			NamespaceID: nsID,
			Name:        "Auto ID",
			Canonical:   "auto_id",
			EntityType:  "person",
		}
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if entity.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestEntityRepo_Create_NilDefaults(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := &model.Entity{
			NamespaceID: nsID,
			Name:        "Defaults",
			Canonical:   "defaults",
			EntityType:  "concept",
		}
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if string(entity.Properties) != "{}" {
			t.Fatalf("expected properties '{}', got %q", string(entity.Properties))
		}
		if string(entity.Metadata) != "{}" {
			t.Fatalf("expected metadata '{}', got %q", string(entity.Metadata))
		}
	})
}

func TestEntityRepo_Create_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		explicitID := uuid.New()
		entity := &model.Entity{
			ID:          explicitID,
			NamespaceID: nsID,
			Name:        "Explicit",
			Canonical:   "explicit",
			EntityType:  "person",
		}
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if entity.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, entity.ID)
		}
	})
}

func TestEntityRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := newTestEntity(nsID)
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, entity.ID, nsID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != entity.ID {
			t.Fatalf("expected ID %s, got %s", entity.ID, fetched.ID)
		}
		if fetched.Name != entity.Name {
			t.Fatalf("expected name %q, got %q", entity.Name, fetched.Name)
		}
		if fetched.EntityType != entity.EntityType {
			t.Fatalf("expected entity_type %q, got %q", entity.EntityType, fetched.EntityType)
		}
	})
}

func TestEntityRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)

		_, err := repo.GetByID(ctx, uuid.New(), uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestEntityRepo_Upsert_Insert(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := newTestEntity(nsID)
		if err := repo.Upsert(ctx, entity); err != nil {
			t.Fatalf("failed to upsert (insert): %v", err)
		}

		if entity.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after upsert")
		}
		if entity.Name != "John Doe" {
			t.Fatalf("unexpected name after upsert: %q", entity.Name)
		}
	})
}

func TestEntityRepo_Upsert_Update(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// First insert
		entity := newTestEntity(nsID)
		if err := repo.Upsert(ctx, entity); err != nil {
			t.Fatalf("failed to upsert (insert): %v", err)
		}
		originalID := entity.ID
		originalCreatedAt := entity.CreatedAt

		// Second upsert with same canonical key but different data
		entity2 := &model.Entity{
			NamespaceID:  nsID,
			Name:         "John D.",
			Canonical:    "john_doe",
			EntityType:   "person",
			MentionCount: 5,
			Properties:   json.RawMessage(`{"role":"manager"}`),
			Metadata:     json.RawMessage(`{"source":"updated"}`),
		}
		if err := repo.Upsert(ctx, entity2); err != nil {
			t.Fatalf("failed to upsert (update): %v", err)
		}

		// Should have the same ID as the original
		if entity2.ID != originalID {
			t.Fatalf("expected same ID %s after upsert update, got %s", originalID, entity2.ID)
		}
		// Name should be updated
		if entity2.Name != "John D." {
			t.Fatalf("expected name 'John D.', got %q", entity2.Name)
		}
		// MentionCount should be incremented (1 from first insert + 1 from second upsert = 2)
		if entity2.MentionCount != 2 {
			t.Fatalf("expected mention_count 2, got %d", entity2.MentionCount)
		}
		// Properties should be updated
		if !jsonEqual(string(entity2.Properties), `{"role":"manager"}`) {
			t.Fatalf("expected updated properties, got %q", string(entity2.Properties))
		}
		// CreatedAt should remain unchanged
		if !entity2.CreatedAt.Equal(originalCreatedAt) {
			t.Fatalf("expected created_at to remain %v, got %v", originalCreatedAt, entity2.CreatedAt)
		}

		// Verify only one entity exists
		all, err := repo.ListByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 entity after upsert, got %d", len(all))
		}
	})
}

func TestEntityRepo_FindBySimilarity(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Create multiple entities
		entities := []*model.Entity{
			{NamespaceID: nsID, Name: "John Doe", Canonical: "john_doe", EntityType: "person", MentionCount: 3},
			{NamespaceID: nsID, Name: "John Smith", Canonical: "john_smith", EntityType: "person", MentionCount: 1},
			{NamespaceID: nsID, Name: "Jane Doe", Canonical: "jane_doe", EntityType: "person", MentionCount: 2},
			{NamespaceID: nsID, Name: "Acme Corp", Canonical: "acme_corp", EntityType: "organization", MentionCount: 1},
		}
		for _, e := range entities {
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("failed to create entity %q: %v", e.Name, err)
			}
		}

		// Search for "John" in person type
		results, err := repo.FindBySimilarity(ctx, nsID, "John", "person", 10)
		if err != nil {
			t.Fatalf("failed to find by similarity: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results for 'John' person, got %d", len(results))
		}
		// Should be ordered by mention_count DESC
		if results[0].Name != "John Doe" {
			t.Fatalf("expected first result 'John Doe' (mention_count 3), got %q", results[0].Name)
		}

		// Search for "Doe" in person type
		results, err = repo.FindBySimilarity(ctx, nsID, "Doe", "person", 10)
		if err != nil {
			t.Fatalf("failed to find by similarity: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results for 'Doe' person, got %d", len(results))
		}

		// Search in organization type should not find persons
		results, err = repo.FindBySimilarity(ctx, nsID, "John", "organization", 10)
		if err != nil {
			t.Fatalf("failed to find by similarity: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for 'John' organization, got %d", len(results))
		}

		// Limit works
		results, err = repo.FindBySimilarity(ctx, nsID, "Doe", "person", 1)
		if err != nil {
			t.Fatalf("failed to find by similarity with limit: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result with limit 1, got %d", len(results))
		}
	})
}

func TestEntityRepo_FindBySimilarity_CaseInsensitive(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := newTestEntity(nsID)
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Lowercase search should find uppercase name
		results, err := repo.FindBySimilarity(ctx, nsID, "john", "person", 10)
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for case-insensitive search, got %d", len(results))
		}
	})
}

// TestEntityRepo_FindBySimilarity_MultiWordIsLiteral pins the contract
// that FindBySimilarity is LITERAL substring match for every input,
// including multi-word ones. The previous rebalance branched on token
// count and silently turned multi-word inputs into a token-OR query,
// which broke EntityResolver.Resolve's Step 3 fuzzy-fallback by
// returning unrelated rows that the resolver then aliased onto the wrong
// entity. The token-OR semantics moved to SearchEntities; this test
// guards against re-introducing it on FindBySimilarity.
//
// Assertion: against ["John Doe", "Jane Smith"], the multi-word query
// "John Smith" matches NEITHER (no entity name literally contains the
// phrase); token-OR would return both as fuzzy hits.
func TestEntityRepo_FindBySimilarity_MultiWordIsLiteral(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entities := []*model.Entity{
			{NamespaceID: nsID, Name: "John Doe", Canonical: "john doe", EntityType: "person", MentionCount: 1},
			{NamespaceID: nsID, Name: "Jane Smith", Canonical: "jane smith", EntityType: "person", MentionCount: 5},
		}
		for _, e := range entities {
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create %q: %v", e.Name, err)
			}
		}

		results, err := repo.FindBySimilarity(ctx, nsID, "John Smith", "", 10)
		if err != nil {
			t.Fatalf("FindBySimilarity err: %v", err)
		}
		if len(results) != 0 {
			gotNames := make([]string, len(results))
			for i, r := range results {
				gotNames[i] = r.Name
			}
			t.Fatalf("FindBySimilarity('John Smith') must be literal, expected 0 results, got %d: %v (token-OR semantics belong on SearchEntities, not here)", len(results), gotNames)
		}
	})
}

// TestEntityRepo_SearchEntities_MultiToken pins the agent-facing matcher.
// SearchEntities tokenizes on whitespace and ORs LIKE clauses across
// tokens, ranking by name-token-match-count DESC.
//
// Assertions:
//  1. A query "John Doe" returns BOTH "John Doe" and "John Smith" (each
//     matches at least one token) plus "Jane Doe" (matches "Doe").
//  2. "John Doe" returns "John Doe" first (matches 2 tokens: name AND
//     surname) before "John Smith" / "Jane Doe" (1 token each).
//  3. mention_count breaks the tie within an equal-score bucket.
func TestEntityRepo_SearchEntities_MultiToken(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entities := []*model.Entity{
			{NamespaceID: nsID, Name: "John Doe", Canonical: "john_doe", EntityType: "person", MentionCount: 1},
			{NamespaceID: nsID, Name: "John Smith", Canonical: "john_smith", EntityType: "person", MentionCount: 5},
			{NamespaceID: nsID, Name: "Jane Doe", Canonical: "jane_doe", EntityType: "person", MentionCount: 3},
			{NamespaceID: nsID, Name: "Acme Corp", Canonical: "acme_corp", EntityType: "organization", MentionCount: 1},
		}
		for _, e := range entities {
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create %q: %v", e.Name, err)
			}
		}

		results, err := repo.SearchEntities(ctx, nsID, "John Doe", "", 10)
		if err != nil {
			t.Fatalf("multi-token search failed: %v", err)
		}
		if len(results) != 3 {
			gotNames := make([]string, len(results))
			for i, r := range results {
				gotNames[i] = r.Name
			}
			t.Fatalf("expected 3 person matches for 'John Doe' (John Doe, John Smith, Jane Doe), got %d: %v", len(results), gotNames)
		}

		// John Doe matches 2 tokens (John + Doe) and must rank first
		// despite having lower mention_count than John Smith (which
		// matches only 1 token but has mention_count=5). The ranking
		// rule is score DESC FIRST, then mention_count DESC.
		if results[0].Name != "John Doe" {
			gotNames := make([]string, len(results))
			for i, r := range results {
				gotNames[i] = r.Name
			}
			t.Errorf("expected 'John Doe' first (matches 2 tokens), got order: %v", gotNames)
		}
	})
}

// TestEntityRepo_SearchEntities_AliasMatch pins that SearchEntities
// surfaces entities whose ALIAS contains one or more tokens, even when
// the entity's own name matches no tokens. The MCP graph tool would
// otherwise need a second FindByAlias call.
//
// Alias matches do not contribute to the score axis (a deliberate
// trade-off documented on searchEntitiesMultiToken), so an alias-only
// match ranks below name matches but still surfaces.
func TestEntityRepo_SearchEntities_AliasMatch(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		alice := &model.Entity{NamespaceID: nsID, Name: "Alice Walker", Canonical: "alice_walker", EntityType: "person", MentionCount: 2}
		bob := &model.Entity{NamespaceID: nsID, Name: "Bob Stevens", Canonical: "bob_stevens", EntityType: "person", MentionCount: 1}
		for _, e := range []*model.Entity{alice, bob} {
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create %q: %v", e.Name, err)
			}
		}

		// Register "AW" as an alias of Alice. The query "AW author" has
		// no name match against either entity but should surface Alice
		// via the alias-OR clause.
		createTestEntityAlias(t, ctx, db, nsID, alice.ID, "AW", "abbreviation")

		results, err := repo.SearchEntities(ctx, nsID, "AW author", "", 10)
		if err != nil {
			t.Fatalf("multi-token + alias search failed: %v", err)
		}
		var sawAlice bool
		for _, r := range results {
			if r.ID == alice.ID {
				sawAlice = true
				break
			}
		}
		if !sawAlice {
			gotNames := make([]string, len(results))
			for i, r := range results {
				gotNames[i] = r.Name
			}
			t.Errorf("expected Alice (matched via alias 'AW') in results; got: %v", gotNames)
		}
	})
}

func TestEntityRepo_FindByAlias(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Create entity
		entity := newTestEntity(nsID)
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create entity: %v", err)
		}

		// Create aliases
		createTestEntityAlias(t, ctx, db, nsID, entity.ID, "JD", "abbreviation")
		createTestEntityAlias(t, ctx, db, nsID, entity.ID, "Johnny", "nickname")

		// Find by alias
		results, err := repo.FindByAlias(ctx, nsID, "JD")
		if err != nil {
			t.Fatalf("failed to find by alias: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for alias 'JD', got %d", len(results))
		}
		if results[0].ID != entity.ID {
			t.Fatalf("expected entity ID %s, got %s", entity.ID, results[0].ID)
		}

		// Find by other alias
		results, err = repo.FindByAlias(ctx, nsID, "Johnny")
		if err != nil {
			t.Fatalf("failed to find by alias 'Johnny': %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for alias 'Johnny', got %d", len(results))
		}

		// Non-existent alias
		results, err = repo.FindByAlias(ctx, nsID, "nonexistent")
		if err != nil {
			t.Fatalf("failed to find by non-existent alias: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for non-existent alias, got %d", len(results))
		}
	})
}

func TestEntityRepo_FindByAlias_CaseInsensitive(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entity := newTestEntity(nsID)
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create entity: %v", err)
		}

		createTestEntityAlias(t, ctx, db, nsID, entity.ID, "JohnDoe", "username")

		// Case-insensitive search
		results, err := repo.FindByAlias(ctx, nsID, "johndoe")
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for case-insensitive alias, got %d", len(results))
		}
	})
}

func TestEntityRepo_ListByNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Create 3 entities
		for i, name := range []string{"Alpha", "Beta", "Gamma"} {
			e := &model.Entity{
				NamespaceID: nsID,
				Name:        name,
				Canonical:   name,
				EntityType:  "concept",
			}
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("failed to create entity %d: %v", i, err)
			}
		}

		results, err := repo.ListByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// Should be ordered by created_at DESC
		for i := 1; i < len(results); i++ {
			if results[i].CreatedAt.After(results[i-1].CreatedAt) {
				t.Fatal("expected results ordered by created_at DESC")
			}
		}
	})
}

func TestEntityRepo_ListByNamespace_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)

		results, err := repo.ListByNamespace(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestEntityRepo_ListByNamespace_Isolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID1 := createTestNamespace(t, ctx, db)
		nsID2 := createTestNamespace(t, ctx, db)

		// Create entity in ns1
		e1 := &model.Entity{
			NamespaceID: nsID1,
			Name:        "Entity1",
			Canonical:   "entity1",
			EntityType:  "concept",
		}
		if err := repo.Create(ctx, e1); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Create entity in ns2
		e2 := &model.Entity{
			NamespaceID: nsID2,
			Name:        "Entity2",
			Canonical:   "entity2",
			EntityType:  "concept",
		}
		if err := repo.Create(ctx, e2); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// List ns1 should only see e1
		results, err := repo.ListByNamespace(ctx, nsID1)
		if err != nil {
			t.Fatalf("failed to list ns1: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for ns1, got %d", len(results))
		}
		if results[0].ID != e1.ID {
			t.Fatalf("expected entity ID %s, got %s", e1.ID, results[0].ID)
		}
	})
}

func TestEntityRepo_Create_WithEmbeddingDim(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		dim := 768
		entity := &model.Entity{
			NamespaceID:  nsID,
			Name:         "Embedded Entity",
			Canonical:    "embedded_entity",
			EntityType:   "concept",
			EmbeddingDim: &dim,
		}
		if err := repo.Create(ctx, entity); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, entity.ID, nsID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.EmbeddingDim == nil || *fetched.EmbeddingDim != 768 {
			t.Fatalf("expected embedding_dim 768, got %v", fetched.EmbeddingDim)
		}
	})
}

// TestEntityRepo_Upsert_PromoteStub_MergesConflicts verifies that when
// promoteStub runs against a stub whose relationships and aliases collide
// with ones already owned by the real entity, the merge absorbs the
// conflicts (taking max(weight), dropping duplicate aliases) instead of
// crashing on UNIQUE constraint violations. Server previously emitted
// `entity promote stub: reassign target relationships: duplicate key`
// warnings every time this happened.
func TestEntityRepo_Upsert_PromoteStub_MergesConflicts(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)
		aliasRepo := NewEntityAliasRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Stub entity for "apple", created as type=unknown.
		stub := &model.Entity{
			NamespaceID: nsID,
			Name:        "apple",
			Canonical:   "apple",
			EntityType:  "unknown",
		}
		if err := repo.Create(ctx, stub); err != nil {
			t.Fatalf("create stub: %v", err)
		}

		// Real entity for "apple" as type=organization. Create directly so
		// we can seed state before triggering promoteStub via Upsert below.
		real := &model.Entity{
			NamespaceID: nsID,
			Name:        "Apple Inc.",
			Canonical:   "apple",
			EntityType:  "organization",
		}
		if err := repo.Create(ctx, real); err != nil {
			t.Fatalf("create real: %v", err)
		}

		// A third entity to connect relationships to.
		acquirer := &model.Entity{
			NamespaceID: nsID,
			Name:        "microsoft",
			Canonical:   "microsoft",
			EntityType:  "organization",
		}
		if err := repo.Create(ctx, acquirer); err != nil {
			t.Fatalf("create acquirer: %v", err)
		}

		validFrom, err := time.Parse(time.RFC3339, "2026-04-01T00:00:00Z")
		if err != nil {
			t.Fatalf("parse valid_from: %v", err)
		}

		// Conflicting relationship: microsoft --acquired--> apple, both as
		// stub-target and real-target, same valid_from. Stub has the
		// larger weight; the merge must preserve it, not regress.
		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: acquirer.ID, TargetID: stub.ID,
			Relation: "acquired", Weight: 0.95, ValidFrom: validFrom,
		}); err != nil {
			t.Fatalf("seed stub-target rel: %v", err)
		}
		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: acquirer.ID, TargetID: real.ID,
			Relation: "acquired", Weight: 0.50, ValidFrom: validFrom,
		}); err != nil {
			t.Fatalf("seed real-target rel: %v", err)
		}

		// Stub-only relationship: apple --competes_with--> microsoft.
		// No conflict on real side; must migrate cleanly.
		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: stub.ID, TargetID: acquirer.ID,
			Relation: "competes_with", Weight: 0.40, ValidFrom: validFrom,
		}); err != nil {
			t.Fatalf("seed stub-source rel: %v", err)
		}

		// Overlapping alias: both hold "Apple". Plus a stub-only alias.
		if err := aliasRepo.Create(ctx, &model.EntityAlias{
			NamespaceID: nsID, EntityID: stub.ID, Alias: "Apple", AliasType: "variant",
		}); err != nil {
			t.Fatalf("seed stub alias: %v", err)
		}
		if err := aliasRepo.Create(ctx, &model.EntityAlias{
			NamespaceID: nsID, EntityID: real.ID, Alias: "Apple", AliasType: "variant",
		}); err != nil {
			t.Fatalf("seed real alias: %v", err)
		}
		if err := aliasRepo.Create(ctx, &model.EntityAlias{
			NamespaceID: nsID, EntityID: stub.ID, Alias: "AAPL", AliasType: "ticker",
		}); err != nil {
			t.Fatalf("seed stub-only alias: %v", err)
		}

		// Trigger promoteStub: Upsert the real-shaped entity again.
		trigger := &model.Entity{
			NamespaceID: nsID,
			Name:        "Apple Inc.",
			Canonical:   "apple",
			EntityType:  "organization",
		}
		if err := repo.Upsert(ctx, trigger); err != nil {
			t.Fatalf("upsert trigger (this is the bug path): %v", err)
		}

		// Stub must be gone.
		if _, err := repo.GetByID(ctx, stub.ID, nsID); err == nil {
			t.Errorf("expected stub %s deleted after promote, still exists", stub.ID)
		}

		// Relationship collision: exactly one microsoft --acquired--> apple
		// row remains, targeting real, with the larger weight (0.95) preserved.
		var acqCount int
		var acqWeight float64
		countQuery := `SELECT COUNT(*), COALESCE(MAX(weight), 0) FROM relationships
			WHERE namespace_id = ? AND source_id = ? AND relation = 'acquired'`
		if db.Backend() == BackendPostgres {
			countQuery = `SELECT COUNT(*), COALESCE(MAX(weight), 0) FROM relationships
				WHERE namespace_id = $1 AND source_id = $2 AND relation = 'acquired'`
		}
		if err := db.QueryRow(ctx, countQuery, nsID.String(), acquirer.ID.String()).Scan(&acqCount, &acqWeight); err != nil {
			t.Fatalf("count acquired rels: %v", err)
		}
		if acqCount != 1 {
			t.Errorf("expected 1 acquired relationship after merge, got %d", acqCount)
		}
		if acqWeight != 0.95 {
			t.Errorf("expected merged weight 0.95 (max of stub 0.95 and real 0.50), got %f", acqWeight)
		}

		// Target of the surviving acquired row must be real.
		var acqTargetStr string
		tgtQuery := `SELECT target_id FROM relationships
			WHERE namespace_id = ? AND source_id = ? AND relation = 'acquired'`
		if db.Backend() == BackendPostgres {
			tgtQuery = `SELECT target_id FROM relationships
				WHERE namespace_id = $1 AND source_id = $2 AND relation = 'acquired'`
		}
		if err := db.QueryRow(ctx, tgtQuery, nsID.String(), acquirer.ID.String()).Scan(&acqTargetStr); err != nil {
			t.Fatalf("read acquired rel target: %v", err)
		}
		if acqTargetStr != real.ID.String() {
			t.Errorf("expected surviving acquired rel to point at real (%s), got %s", real.ID, acqTargetStr)
		}

		// Stub-only relationship must be reassigned to real.
		var compCount int
		var compSrcStr string
		// Stored relations are canonicalized, so "competes_with" persists as
		// "competes with"; assert against the canonical form.
		compQuery := `SELECT COUNT(*), COALESCE(MAX(CAST(source_id AS TEXT)), '') FROM relationships
			WHERE namespace_id = ? AND target_id = ? AND relation = 'competes with'`
		if db.Backend() == BackendPostgres {
			compQuery = `SELECT COUNT(*), COALESCE(MAX(source_id::text), '') FROM relationships
				WHERE namespace_id = $1 AND target_id = $2 AND relation = 'competes with'`
		}
		if err := db.QueryRow(ctx, compQuery, nsID.String(), acquirer.ID.String()).Scan(&compCount, &compSrcStr); err != nil {
			t.Fatalf("count competes_with rels: %v", err)
		}
		if compCount != 1 {
			t.Errorf("expected 1 competes_with relationship after reassign, got %d", compCount)
		}
		if compSrcStr != real.ID.String() {
			t.Errorf("expected competes_with source to be real (%s), got %s", real.ID, compSrcStr)
		}

		// Aliases on real: "Apple" (deduped) + "AAPL" (migrated). Exactly 2.
		aliases, err := aliasRepo.ListByEntity(ctx, real.ID, []uuid.UUID{nsID})
		if err != nil {
			t.Fatalf("list real aliases: %v", err)
		}
		if len(aliases) != 2 {
			t.Errorf("expected 2 aliases on real after merge, got %d: %+v", len(aliases), aliases)
		}
		seen := map[string]bool{}
		for _, a := range aliases {
			seen[a.Alias] = true
		}
		if !seen["Apple"] || !seen["AAPL"] {
			t.Errorf("expected aliases 'Apple' and 'AAPL' on real, got %v", seen)
		}
	})
}

// TestEntityRepo_ListAll exercises the cross-namespace pagination used by
// the embedding-model switch cascade's entity re-embed loop.
func TestEntityRepo_ListAll(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		// ListAll is a whole-DB scan; the shared Postgres schema retains
		// rows from prior tests, so the row-count assertions below need a
		// blank slate.
		truncateAllForTest(t, db)
		ctx := context.Background()
		repo := NewEntityRepo(db)
		ns1 := createTestNamespace(t, ctx, db)
		ns2 := createTestNamespace(t, ctx, db)

		// Seed 5 entities across two namespaces.
		want := 5
		for i := range 3 {
			e := newTestEntity(ns1)
			e.Canonical = fmt.Sprintf("ns1_e%d", i)
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create ns1 entity: %v", err)
			}
		}
		for i := range 2 {
			e := newTestEntity(ns2)
			e.Canonical = fmt.Sprintf("ns2_e%d", i)
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create ns2 entity: %v", err)
			}
		}

		// First page covers all 5; pageSize default is 500.
		page, err := repo.ListAll(ctx, 0, 0)
		if err != nil {
			t.Fatalf("list all: %v", err)
		}
		if len(page) != want {
			t.Errorf("page 0: want %d entities, got %d", want, len(page))
		}

		// Pagination: limit=2 returns first 2; offset=2 returns next 2; offset=4 returns 1.
		first, _ := repo.ListAll(ctx, 2, 0)
		if len(first) != 2 {
			t.Errorf("limit=2 offset=0: want 2, got %d", len(first))
		}
		next, _ := repo.ListAll(ctx, 2, 2)
		if len(next) != 2 {
			t.Errorf("limit=2 offset=2: want 2, got %d", len(next))
		}
		last, _ := repo.ListAll(ctx, 2, 4)
		if len(last) != 1 {
			t.Errorf("limit=2 offset=4: want 1, got %d", len(last))
		}

		// Stable order: same call returns same id sequence.
		again, _ := repo.ListAll(ctx, 0, 0)
		if len(again) != want {
			t.Fatalf("re-list size mismatch: %d vs %d", len(again), want)
		}
		for i := range page {
			if page[i].ID != again[i].ID {
				t.Errorf("ListAll order is not stable: pos=%d id1=%s id2=%s", i, page[i].ID, again[i].ID)
			}
		}
	})
}

// TestEntityRepo_ClearAllEmbeddingDims is the load-bearing assertion for
// the cascade: every row's embedding_dim must be NULL'd in one call so
// the re-embed pipeline treats every entity as needing fresh vectors.
func TestEntityRepo_ClearAllEmbeddingDims(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		// ClearAllEmbeddingDims is a whole-DB UPDATE; the shared Postgres
		// schema retains entities from prior tests with embedding_dim set,
		// inflating the rows-affected count.
		truncateAllForTest(t, db)
		ctx := context.Background()
		repo := NewEntityRepo(db)
		ns := createTestNamespace(t, ctx, db)

		// 3 entities with embedding_dim set, 1 with NULL.
		dim := 768
		for i := range 3 {
			e := newTestEntity(ns)
			e.Canonical = fmt.Sprintf("e%d", i)
			e.EmbeddingDim = &dim
			if err := repo.Create(ctx, e); err != nil {
				t.Fatalf("create with dim: %v", err)
			}
		}
		eNull := newTestEntity(ns)
		eNull.Canonical = "e_null"
		if err := repo.Create(ctx, eNull); err != nil {
			t.Fatalf("create null: %v", err)
		}

		n, err := repo.ClearAllEmbeddingDims(ctx)
		if err != nil {
			t.Fatalf("clear: %v", err)
		}
		if n != 3 {
			t.Errorf("expected 3 rows affected (only the non-NULL ones), got %d", n)
		}

		// Every entity now has NULL embedding_dim.
		page, _ := repo.ListAll(ctx, 0, 0)
		for _, e := range page {
			if e.EmbeddingDim != nil {
				t.Errorf("entity %s embedding_dim should be NULL after clear, got %d", e.ID, *e.EmbeddingDim)
			}
		}

		// Idempotent: second call affects 0 rows.
		n2, err := repo.ClearAllEmbeddingDims(ctx)
		if err != nil {
			t.Fatalf("second clear: %v", err)
		}
		if n2 != 0 {
			t.Errorf("second clear should be a no-op, affected %d rows", n2)
		}
	})
}

// backdateEntityCreatedAt forces an entity's created_at to a specific past
// instant. The DEFAULT clause sets created_at = now() at insert time, so
// orphan-sweep tests need this hook to make rows eligible for the cutoff.
func backdateEntityCreatedAt(t *testing.T, ctx context.Context, db DB, entityID uuid.UUID, when time.Time) {
	t.Helper()
	stamp := when.UTC().Format(time.RFC3339)
	query := `UPDATE entities SET created_at = ? WHERE id = ?`
	if db.Backend() == BackendPostgres {
		query = `UPDATE entities SET created_at = $1 WHERE id = $2`
	}
	if _, err := db.Exec(ctx, query, stamp, entityID.String()); err != nil {
		t.Fatalf("backdate entity created_at: %v", err)
	}
}

// TestEntityRepo_DeleteOrphaned_ReturnsIDsAndCascadesAliases verifies that
// the lifecycle orphan sweep can delete an entity that has an alias attached.
// Pre-cascade migration 000032 / 000035, this raised SQLSTATE 23503 on
// entity_aliases_entity_id_fkey and the sweep silently failed every tick.
func TestEntityRepo_DeleteOrphaned_ReturnsIDsAndCascadesAliases(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		entityID := createTestEntity(t, ctx, db, nsID, "orphan_with_alias_"+uuid.NewString()[:8])
		createTestEntityAlias(t, ctx, db, nsID, entityID, "OWA", "ticker")
		backdateEntityCreatedAt(t, ctx, db, entityID, time.Now().Add(-2*time.Hour))

		ids, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("DeleteOrphaned: %v", err)
		}

		found := slices.Contains(ids, entityID)
		if !found {
			t.Fatalf("expected returned IDs to include %s, got %v", entityID, ids)
		}

		if _, err := repo.GetByID(ctx, entityID, nsID); err == nil {
			t.Errorf("entity %s still exists after orphan sweep", entityID)
		}

		if aliasCount := countAliasesForTest(t, ctx, db, entityID); aliasCount != 0 {
			t.Errorf("expected 0 aliases after cascade delete, got %d", aliasCount)
		}
	})
}

// TestEntityRepo_DeleteOrphaned_SkipsEntitiesWithRelationships verifies the
// NOT IN filter: an entity referenced by any relationship endpoint stays put
// even when its created_at predates the cutoff.
func TestEntityRepo_DeleteOrphaned_SkipsEntitiesWithRelationships(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.NewString()[:8]
		srcID := createTestEntity(t, ctx, db, nsID, "src_"+suffix)
		tgtID := createTestEntity(t, ctx, db, nsID, "tgt_"+suffix)

		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: tgtID,
			Relation: "knows", Weight: 1.0,
		}); err != nil {
			t.Fatalf("create relationship: %v", err)
		}

		backdateEntityCreatedAt(t, ctx, db, srcID, time.Now().Add(-2*time.Hour))
		backdateEntityCreatedAt(t, ctx, db, tgtID, time.Now().Add(-2*time.Hour))

		ids, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("DeleteOrphaned: %v", err)
		}
		for _, id := range ids {
			if id == srcID || id == tgtID {
				t.Errorf("DeleteOrphaned returned referenced entity %s", id)
			}
		}

		if _, err := repo.GetByID(ctx, srcID, nsID); err != nil {
			t.Errorf("source entity unexpectedly deleted: %v", err)
		}
		if _, err := repo.GetByID(ctx, tgtID, nsID); err != nil {
			t.Errorf("target entity unexpectedly deleted: %v", err)
		}
	})
}

// TestEntityRepo_DeleteByNamespaceTx_ReturnsIDsAndCascadesChildren verifies
// the bulk-namespace delete used by ProjectDeleteService. Aliases and
// relationships referencing namespace entities must be cleared by the
// schema cascade (no manual pre-delete step) and the returned IDs must
// match the actual deleted entities for downstream Qdrant cleanup.
func TestEntityRepo_DeleteByNamespaceTx_ReturnsIDsAndCascadesChildren(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.NewString()[:8]
		e1 := createTestEntity(t, ctx, db, nsID, "ent1_"+suffix)
		e2 := createTestEntity(t, ctx, db, nsID, "ent2_"+suffix)

		createTestEntityAlias(t, ctx, db, nsID, e1, "E1", "ticker")
		createTestEntityAlias(t, ctx, db, nsID, e2, "E2", "ticker")

		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: e1, TargetID: e2,
			Relation: "knows", Weight: 1.0,
		}); err != nil {
			t.Fatalf("create relationship: %v", err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		// Schema cascade deletes relationships when entities are deleted, so
		// the per-namespace entity delete must succeed without a manual
		// relationship pre-delete here.
		ids, err := repo.DeleteByNamespaceTx(ctx, tx, nsID)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("DeleteByNamespaceTx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		want := map[uuid.UUID]bool{e1: false, e2: false}
		for _, id := range ids {
			if _, ok := want[id]; ok {
				want[id] = true
			}
		}
		for id, seen := range want {
			if !seen {
				t.Errorf("expected returned IDs to include %s, got %v", id, ids)
			}
		}

		var entCount, aliasCount, relCount int
		entQuery := `SELECT COUNT(*) FROM entities WHERE namespace_id = ?`
		aliasQuery := `SELECT COUNT(*) FROM entity_aliases WHERE namespace_id = ?`
		relQuery := `SELECT COUNT(*) FROM relationships WHERE namespace_id = ?`
		if db.Backend() == BackendPostgres {
			entQuery = `SELECT COUNT(*) FROM entities WHERE namespace_id = $1`
			aliasQuery = `SELECT COUNT(*) FROM entity_aliases WHERE namespace_id = $1`
			relQuery = `SELECT COUNT(*) FROM relationships WHERE namespace_id = $1`
		}
		if err := db.QueryRow(ctx, entQuery, nsID.String()).Scan(&entCount); err != nil {
			t.Fatalf("count entities: %v", err)
		}
		if err := db.QueryRow(ctx, aliasQuery, nsID.String()).Scan(&aliasCount); err != nil {
			t.Fatalf("count aliases: %v", err)
		}
		if err := db.QueryRow(ctx, relQuery, nsID.String()).Scan(&relCount); err != nil {
			t.Fatalf("count relationships: %v", err)
		}
		if entCount != 0 || aliasCount != 0 || relCount != 0 {
			t.Errorf("expected 0 entities/aliases/relationships in namespace after delete, got %d/%d/%d",
				entCount, aliasCount, relCount)
		}
	})
}

// TestEntityRepo_PromoteStub_DeletesStubVector verifies that when a stub
// entity is merged into a real one inside Upsert, the stub's vector store
// entry is also cleaned up. Without this, Qdrant accumulates dead points
// keyed by the stub's UUID with no SQL row to ever reclaim them.
func TestEntityRepo_PromoteStub_DeletesStubVector(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		vec := &recordingVectorStore{}
		repo.SetVectorStore(vec)
		nsID := createTestNamespace(t, ctx, db)

		canonical := "promote_stub_vec_" + uuid.NewString()[:8]

		stub := &model.Entity{
			NamespaceID: nsID, Name: canonical, Canonical: canonical, EntityType: "unknown",
		}
		if err := repo.Create(ctx, stub); err != nil {
			t.Fatalf("create stub: %v", err)
		}
		real := &model.Entity{
			NamespaceID: nsID, Name: canonical, Canonical: canonical, EntityType: "person",
		}
		if err := repo.Create(ctx, real); err != nil {
			t.Fatalf("create real: %v", err)
		}

		// Trigger promoteStub. The conflict path (both rows exist) is the one
		// that deletes the stub row, which is the path that needs vector cleanup.
		trigger := &model.Entity{
			NamespaceID: nsID, Name: canonical, Canonical: canonical, EntityType: "person",
		}
		if err := repo.Upsert(ctx, trigger); err != nil {
			t.Fatalf("upsert trigger: %v", err)
		}

		var stubDeletes int
		for _, c := range vec.deleteCalls {
			if c.kind == VectorKindEntity && c.id == stub.ID {
				stubDeletes++
			}
		}
		if stubDeletes != 1 {
			t.Fatalf("expected exactly 1 vector delete for stub %s, got %d (deleteCalls=%v)",
				stub.ID, stubDeletes, vec.deleteCalls)
		}
	})
}

// countAliasesForTest counts an entity's alias rows. Shared by the sweep tests
// that assert aliases either cascade away with a genuine orphan or survive an
// entity the sweep must not touch.
func countAliasesForTest(t *testing.T, ctx context.Context, db DB, entityID uuid.UUID) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM entity_aliases WHERE entity_id = ?`
	if db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM entity_aliases WHERE entity_id = $1`
	}
	var n int
	if err := db.QueryRow(ctx, query, entityID.String()).Scan(&n); err != nil {
		t.Fatalf("count aliases: %v", err)
	}
	return n
}

// seedReExtractWindow reproduces the state ReExtract leaves behind between
// enrich.go's ReapMemoryFootprint and its Enqueue: two long-lived entities
// (one carrying an alias) whose only edge was sourced by memID and has just
// been reaped, so both sit edge-less holding a months-old created_at.
func seedReExtractWindow(t *testing.T, ctx context.Context, db DB) (nsID, memID, srcID, tgtID uuid.UUID) {
	t.Helper()
	nsID = createTestNamespace(t, ctx, db)
	memID = createTestMemoryForLineage(t, ctx, db, nsID)
	suffix := uuid.NewString()[:8]
	srcID = createTestEntity(t, ctx, db, nsID, "reextract_src_"+suffix)
	tgtID = createTestEntity(t, ctx, db, nsID, "reextract_tgt_"+suffix)
	createTestEntityAlias(t, ctx, db, nsID, srcID, "RSRC_"+suffix, "acronym")

	if err := NewRelationshipRepo(db).Create(ctx, &model.Relationship{
		NamespaceID: nsID, SourceID: srcID, TargetID: tgtID,
		Relation: "knows", Weight: 1.0, SourceMemory: &memID,
	}); err != nil {
		t.Fatalf("seed relationship: %v", err)
	}
	backdateEntityCreatedAt(t, ctx, db, srcID, time.Now().Add(-30*24*time.Hour))
	backdateEntityCreatedAt(t, ctx, db, tgtID, time.Now().Add(-30*24*time.Hour))

	if _, err := NewRelationshipRepo(db).DeleteBySourceMemory(ctx, nsID, memID); err != nil {
		t.Fatalf("reap memory footprint: %v", err)
	}
	return nsID, memID, srcID, tgtID
}

// TestEntityRepo_DeleteOrphaned_SparesNamespaceWithPendingJob pins the fix for
// the ReExtract window. ReExtract reaps a memory's edges (enrich.go) and only
// then enqueues the re-extraction job, so entities sourced solely by that
// memory are transiently edge-less while still carrying their original
// created_at. Age-gating alone deleted them mid-re-extract, taking the alias
// rows with them via CASCADE; the pending job must hold the sweep off.
func TestEntityRepo_DeleteOrphaned_SparesNamespaceWithPendingJob(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID, memID, srcID, tgtID := seedReExtractWindow(t, ctx, db)

		if _, err := NewEnrichmentQueueRepo(db).Enqueue(ctx, &model.EnrichmentJob{
			MemoryID: memID, NamespaceID: nsID, Priority: 0,
		}); err != nil {
			t.Fatalf("enqueue re-extract job: %v", err)
		}

		ids, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("DeleteOrphaned: %v", err)
		}
		if slices.Contains(ids, srcID) || slices.Contains(ids, tgtID) {
			t.Errorf("sweep collected entities with a job still in flight: %v", ids)
		}
		if _, err := repo.GetByID(ctx, srcID, nsID); err != nil {
			t.Errorf("source entity deleted during the re-extraction window: %v", err)
		}
		if _, err := repo.GetByID(ctx, tgtID, nsID); err != nil {
			t.Errorf("target entity deleted during the re-extraction window: %v", err)
		}

		if aliasCount := countAliasesForTest(t, ctx, db, srcID); aliasCount != 1 {
			t.Errorf("alias lost to the sweep's CASCADE: want 1, got %d", aliasCount)
		}
	})
}

// TestEntityRepo_DeleteOrphaned_CollectsWhenQueueQuiet guards against
// over-correction: with no job in flight, a genuine orphan is still collected.
func TestEntityRepo_DeleteOrphaned_CollectsWhenQueueQuiet(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		id := createTestEntity(t, ctx, db, nsID, "genuine_orphan_"+uuid.NewString()[:8])
		backdateEntityCreatedAt(t, ctx, db, id, time.Now().Add(-90*24*time.Hour))

		ids, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("DeleteOrphaned: %v", err)
		}
		if !slices.Contains(ids, id) {
			t.Errorf("expected returned ids to include genuine orphan %s, got %v", id, ids)
		}
		if _, err := repo.GetByID(ctx, id, nsID); err == nil {
			t.Error("genuine orphan survived a sweep with no job in flight")
		}
	})
}

// TestEntityRepo_DeleteOrphaned_CollectsOnceJobTerminal proves the queue guard
// defers collection rather than pinning orphans forever: once the job leaves
// pending/processing, the next tick collects the row.
func TestEntityRepo_DeleteOrphaned_CollectsOnceJobTerminal(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)
		nsID, memID, srcID, _ := seedReExtractWindow(t, ctx, db)

		job := &model.EnrichmentJob{MemoryID: memID, NamespaceID: nsID, Priority: 0}
		if _, err := NewEnrichmentQueueRepo(db).Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour)); err != nil {
			t.Fatalf("DeleteOrphaned (job pending): %v", err)
		}
		if _, err := repo.GetByID(ctx, srcID, nsID); err != nil {
			t.Fatalf("entity collected while the job was still pending: %v", err)
		}

		// Drive the row terminal directly: Complete requires an active claim,
		// and the guard predicate only reads status.
		termQuery := `UPDATE enrichment_queue SET status = 'completed' WHERE id = ?`
		if db.Backend() == BackendPostgres {
			termQuery = `UPDATE enrichment_queue SET status = 'completed' WHERE id = $1`
		}
		if _, err := db.Exec(ctx, termQuery, job.ID.String()); err != nil {
			t.Fatalf("terminalize job: %v", err)
		}

		ids, err := repo.DeleteOrphaned(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("DeleteOrphaned (job terminal): %v", err)
		}
		if !slices.Contains(ids, srcID) {
			t.Errorf("deferred orphan not collected once the job went terminal, got %v", ids)
		}
	})
}

// TestEntityRepo_RecomputeMentionCounts_ScopedStillCorrectsRealChanges pins the
// ids-scoped change-guard on both sides: an endpoint already at its canonical
// count is not rewritten (and keeps its updated_at), but one whose derived count
// actually moves is still corrected.
//
// The nil-scoped path is deliberately left unguarded (see RecomputeMentionCounts)
// and is covered by TestEntityRepo_RecomputeMentionCounts in graph_reap_test.go.
func TestEntityRepo_RecomputeMentionCounts_ScopedStillCorrectsRealChanges(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityRepo(db)

		nsID := createTestNamespace(t, ctx, db)
		memID := createTestMemoryForLineage(t, ctx, db, nsID)
		suffix := uuid.NewString()[:8]
		srcID := createTestEntity(t, ctx, db, nsID, "scoped_src_"+suffix)
		tgtID := createTestEntity(t, ctx, db, nsID, "scoped_tgt_"+suffix)
		relRepo := NewRelationshipRepo(db)
		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: tgtID,
			Relation: "knows", Weight: 1.0, SourceMemory: &memID,
		}); err != nil {
			t.Fatalf("seed relationship: %v", err)
		}

		ids := []uuid.UUID{srcID, tgtID}
		if _, err := repo.RecomputeMentionCounts(ctx, ids); err != nil {
			t.Fatalf("RecomputeMentionCounts (converge): %v", err)
		}
		// Already canonical: the guard must make this a no-op.
		corrected, err := repo.RecomputeMentionCounts(ctx, ids)
		if err != nil {
			t.Fatalf("RecomputeMentionCounts (no-op): %v", err)
		}
		if corrected != 0 {
			t.Errorf("scoped recompute rewrote %d already-canonical rows, want 0", corrected)
		}

		// Reap the edge: both endpoints now derive 0 and MUST be rewritten.
		if _, err := relRepo.DeleteBySourceMemory(ctx, nsID, memID); err != nil {
			t.Fatalf("reap: %v", err)
		}
		corrected, err = repo.RecomputeMentionCounts(ctx, ids)
		if err != nil {
			t.Fatalf("RecomputeMentionCounts (after reap): %v", err)
		}
		if corrected != 2 {
			t.Errorf("scoped recompute after reap: want 2 rows corrected, got %d", corrected)
		}
		got, err := repo.GetByID(ctx, srcID, nsID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.MentionCount != 0 {
			t.Errorf("mention_count not corrected after reap: want 0, got %d", got.MentionCount)
		}
	})
}
