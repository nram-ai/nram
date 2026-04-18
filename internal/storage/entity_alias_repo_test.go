package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func createEntityForAliasTest(t *testing.T, ctx context.Context, db DB, nsID uuid.UUID, name string) *model.Entity {
	t.Helper()
	repo := NewEntityRepo(db)
	entity := &model.Entity{
		NamespaceID:  nsID,
		Name:         name,
		Canonical:    name,
		EntityType:   "person",
		MentionCount: 1,
		Properties:   json.RawMessage(`{}`),
		Metadata:     json.RawMessage(`{}`),
	}
	if err := repo.Create(ctx, entity); err != nil {
		t.Fatalf("failed to create entity %q: %v", name, err)
	}
	return entity
}

func TestEntityAliasRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "alice")
		repo := NewEntityAliasRepo(db)

		alias := &model.EntityAlias{
			NamespaceID: nsID,
			EntityID:    entity.ID,
			Alias:       "Al",
			AliasType:   "nickname",
		}
		if err := repo.Create(ctx, alias); err != nil {
			t.Fatalf("failed to create alias: %v", err)
		}

		if alias.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if alias.EntityID != entity.ID {
			t.Fatalf("expected entity_id %s, got %s", entity.ID, alias.EntityID)
		}
		if alias.Alias != "Al" {
			t.Fatalf("expected alias 'Al', got %q", alias.Alias)
		}
		if alias.AliasType != "nickname" {
			t.Fatalf("expected alias_type 'nickname', got %q", alias.AliasType)
		}
		if alias.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestEntityAliasRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "bob")
		repo := NewEntityAliasRepo(db)

		alias := &model.EntityAlias{
			NamespaceID: nsID,
			EntityID:    entity.ID,
			Alias:       "Bobby",
			AliasType:   "nickname",
		}
		if err := repo.Create(ctx, alias); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if alias.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestEntityAliasRepo_Create_ExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "charlie")
		repo := NewEntityAliasRepo(db)

		explicitID := uuid.New()
		alias := &model.EntityAlias{
			ID:          explicitID,
			NamespaceID: nsID,
			EntityID:    entity.ID,
			Alias:       "Chuck",
			AliasType:   "nickname",
		}
		if err := repo.Create(ctx, alias); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if alias.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, alias.ID)
		}
	})
}

func TestEntityAliasRepo_Create_DefaultAliasType(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "dave")
		repo := NewEntityAliasRepo(db)

		alias := &model.EntityAlias{
			NamespaceID: nsID,
			EntityID:    entity.ID,
			Alias:       "David",
			AliasType:   "name",
		}
		if err := repo.Create(ctx, alias); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if alias.AliasType != "name" {
			t.Fatalf("expected alias_type 'name', got %q", alias.AliasType)
		}
	})
}

func TestEntityAliasRepo_FindByAlias(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "eve")
		repo := NewEntityAliasRepo(db)

		// Create two aliases for the same entity
		a1 := &model.EntityAlias{NamespaceID: nsID, EntityID: entity.ID, Alias: "Evie", AliasType: "nickname"}
		a2 := &model.EntityAlias{NamespaceID: nsID, EntityID: entity.ID, Alias: "E", AliasType: "abbreviation"}
		if err := repo.Create(ctx, a1); err != nil {
			t.Fatalf("failed to create alias 1: %v", err)
		}
		if err := repo.Create(ctx, a2); err != nil {
			t.Fatalf("failed to create alias 2: %v", err)
		}

		// Find by alias text
		results, err := repo.FindByAlias(ctx, nsID, "Evie")
		if err != nil {
			t.Fatalf("failed to find by alias: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for 'Evie', got %d", len(results))
		}
		if results[0].Alias != "Evie" {
			t.Fatalf("expected alias 'Evie', got %q", results[0].Alias)
		}
		if results[0].EntityID != entity.ID {
			t.Fatalf("expected entity_id %s, got %s", entity.ID, results[0].EntityID)
		}

		// Find by other alias
		results, err = repo.FindByAlias(ctx, nsID, "E")
		if err != nil {
			t.Fatalf("failed to find by alias 'E': %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for 'E', got %d", len(results))
		}

		// Non-existent alias returns empty
		results, err = repo.FindByAlias(ctx, nsID, "nonexistent")
		if err != nil {
			t.Fatalf("failed to find non-existent alias: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for non-existent alias, got %d", len(results))
		}
	})
}

func TestEntityAliasRepo_FindByAlias_CaseInsensitive(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "frank")
		repo := NewEntityAliasRepo(db)

		a := &model.EntityAlias{NamespaceID: nsID, EntityID: entity.ID, Alias: "Frankie", AliasType: "nickname"}
		if err := repo.Create(ctx, a); err != nil {
			t.Fatalf("failed to create alias: %v", err)
		}

		// Search with different case
		results, err := repo.FindByAlias(ctx, nsID, "frankie")
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for case-insensitive search, got %d", len(results))
		}

		results, err = repo.FindByAlias(ctx, nsID, "FRANKIE")
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for uppercase search, got %d", len(results))
		}
	})
}

func TestEntityAliasRepo_FindByAlias_NamespaceIsolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID1 := createTestNamespace(t, ctx, db)
		nsID2 := createTestNamespace(t, ctx, db)
		entity1 := createEntityForAliasTest(t, ctx, db, nsID1, "grace_ns1")
		entity2 := createEntityForAliasTest(t, ctx, db, nsID2, "grace_ns2")
		repo := NewEntityAliasRepo(db)

		// Same alias text in different namespaces
		a1 := &model.EntityAlias{NamespaceID: nsID1, EntityID: entity1.ID, Alias: "Gracie", AliasType: "nickname"}
		a2 := &model.EntityAlias{NamespaceID: nsID2, EntityID: entity2.ID, Alias: "Gracie", AliasType: "nickname"}
		if err := repo.Create(ctx, a1); err != nil {
			t.Fatalf("failed to create alias in ns1: %v", err)
		}
		if err := repo.Create(ctx, a2); err != nil {
			t.Fatalf("failed to create alias in ns2: %v", err)
		}

		// Should only find alias in ns1
		results, err := repo.FindByAlias(ctx, nsID1, "Gracie")
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result in ns1, got %d", len(results))
		}
		if results[0].EntityID != entity1.ID {
			t.Fatalf("expected entity from ns1, got entity_id %s", results[0].EntityID)
		}

		// Should only find alias in ns2
		results, err = repo.FindByAlias(ctx, nsID2, "Gracie")
		if err != nil {
			t.Fatalf("failed to find: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result in ns2, got %d", len(results))
		}
		if results[0].EntityID != entity2.ID {
			t.Fatalf("expected entity from ns2, got entity_id %s", results[0].EntityID)
		}
	})
}

func TestEntityAliasRepo_ListByEntity(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "heidi")
		repo := NewEntityAliasRepo(db)

		// Create multiple aliases
		aliases := []struct {
			alias     string
			aliasType string
		}{
			{"Heids", "nickname"},
			{"H", "abbreviation"},
			{"Heidi M.", "formal"},
		}
		for _, a := range aliases {
			ea := &model.EntityAlias{NamespaceID: nsID, EntityID: entity.ID, Alias: a.alias, AliasType: a.aliasType}
			if err := repo.Create(ctx, ea); err != nil {
				t.Fatalf("failed to create alias %q: %v", a.alias, err)
			}
		}

		results, err := repo.ListByEntity(ctx, entity.ID)
		if err != nil {
			t.Fatalf("failed to list by entity: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 aliases, got %d", len(results))
		}

		// Should be ordered by created_at DESC
		for i := 1; i < len(results); i++ {
			if results[i].CreatedAt.After(results[i-1].CreatedAt) {
				t.Fatal("expected results ordered by created_at DESC")
			}
		}
	})
}

func TestEntityAliasRepo_ListByEntity_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEntityAliasRepo(db)

		results, err := repo.ListByEntity(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestEntityAliasRepo_ListByEntity_Isolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity1 := createEntityForAliasTest(t, ctx, db, nsID, "ivan")
		entity2 := createEntityForAliasTest(t, ctx, db, nsID, "judy")
		repo := NewEntityAliasRepo(db)

		// Aliases for entity1
		a1 := &model.EntityAlias{NamespaceID: nsID, EntityID: entity1.ID, Alias: "Ivy", AliasType: "nickname"}
		if err := repo.Create(ctx, a1); err != nil {
			t.Fatalf("failed to create alias for entity1: %v", err)
		}

		// Aliases for entity2
		a2 := &model.EntityAlias{NamespaceID: nsID, EntityID: entity2.ID, Alias: "Jules", AliasType: "nickname"}
		if err := repo.Create(ctx, a2); err != nil {
			t.Fatalf("failed to create alias for entity2: %v", err)
		}

		// List entity1 should only see its aliases
		results, err := repo.ListByEntity(ctx, entity1.ID)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 alias for entity1, got %d", len(results))
		}
		if results[0].Alias != "Ivy" {
			t.Fatalf("expected alias 'Ivy', got %q", results[0].Alias)
		}

		// List entity2 should only see its aliases
		results, err = repo.ListByEntity(ctx, entity2.ID)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 alias for entity2, got %d", len(results))
		}
		if results[0].Alias != "Jules" {
			t.Fatalf("expected alias 'Jules', got %q", results[0].Alias)
		}
	})
}

// TestEntityAliasRepo_Create_IdempotentOnDuplicate pins the
// ON CONFLICT DO NOTHING contract added to silence the
// `dreaming: alias creation failed (may already exist)` warning spam.
// Re-registering the same (entity_id, alias) mapping must return nil, not
// a unique-constraint error, and must leave exactly one row in the table.
func TestEntityAliasRepo_Create_IdempotentOnDuplicate(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		entity := createEntityForAliasTest(t, ctx, db, nsID, "alice")
		repo := NewEntityAliasRepo(db)

		first := &model.EntityAlias{
			NamespaceID: nsID, EntityID: entity.ID,
			Alias: "Al", AliasType: "nickname",
		}
		if err := repo.Create(ctx, first); err != nil {
			t.Fatalf("first create: %v", err)
		}

		// Same (entity_id, alias) with a different proposed ID + alias_type.
		// Must be absorbed silently.
		second := &model.EntityAlias{
			NamespaceID: nsID, EntityID: entity.ID,
			Alias: "Al", AliasType: "dream_dedup",
		}
		if err := repo.Create(ctx, second); err != nil {
			t.Fatalf("duplicate create must not error: %v", err)
		}

		results, err := repo.ListByEntity(ctx, entity.ID)
		if err != nil {
			t.Fatalf("list aliases: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected exactly 1 alias row after duplicate insert, got %d: %+v", len(results), results)
		}
	})
}
