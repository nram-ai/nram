package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

		fetched, err := repo.GetByID(ctx, entity.ID)
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

		_, err := repo.GetByID(ctx, uuid.New())
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

		fetched, err := repo.GetByID(ctx, entity.ID)
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

		// Stub entity for "apple" — created as type=unknown.
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
		// larger weight — the merge must preserve it, not regress.
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
		// No conflict on real side — must migrate cleanly.
		if err := relRepo.Create(ctx, &model.Relationship{
			NamespaceID: nsID, SourceID: stub.ID, TargetID: acquirer.ID,
			Relation: "competes_with", Weight: 0.40, ValidFrom: validFrom,
		}); err != nil {
			t.Fatalf("seed stub-source rel: %v", err)
		}

		// Overlapping alias — both hold "Apple". Plus a stub-only alias.
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
		if _, err := repo.GetByID(ctx, stub.ID); err == nil {
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
		compQuery := `SELECT COUNT(*), COALESCE(MAX(CAST(source_id AS TEXT)), '') FROM relationships
			WHERE namespace_id = ? AND target_id = ? AND relation = 'competes_with'`
		if db.Backend() == BackendPostgres {
			compQuery = `SELECT COUNT(*), COALESCE(MAX(source_id::text), '') FROM relationships
				WHERE namespace_id = $1 AND target_id = $2 AND relation = 'competes_with'`
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
		aliases, err := aliasRepo.ListByEntity(ctx, real.ID)
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
