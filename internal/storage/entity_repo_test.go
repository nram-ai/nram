package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

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

func createTestEntityAlias(t *testing.T, ctx context.Context, db DB, entityID uuid.UUID, alias, aliasType string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	query := `INSERT INTO entity_aliases (id, entity_id, alias, alias_type) VALUES (?, ?, ?, ?)`
	if db.Backend() == BackendPostgres {
		query = `INSERT INTO entity_aliases (id, entity_id, alias, alias_type) VALUES ($1, $2, $3, $4)`
	}
	_, err := db.Exec(ctx, query, id.String(), entityID.String(), alias, aliasType)
	if err != nil {
		t.Fatalf("failed to create test entity alias: %v", err)
	}
	return id
}

func TestEntityRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
	if string(entity.Properties) != `{"role":"engineer"}` {
		t.Fatalf("unexpected properties: %q", string(entity.Properties))
	}
	if string(entity.Metadata) != `{"source":"test"}` {
		t.Fatalf("unexpected metadata: %q", string(entity.Metadata))
	}
	if entity.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if entity.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestEntityRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_Create_NilDefaults(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_Create_WithExplicitID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEntityRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestEntityRepo_Upsert_Insert(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_Upsert_Update(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
	// MentionCount should be updated
	if entity2.MentionCount != 5 {
		t.Fatalf("expected mention_count 5, got %d", entity2.MentionCount)
	}
	// Properties should be updated
	if string(entity2.Properties) != `{"role":"manager"}` {
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
}

func TestEntityRepo_FindBySimilarity(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_FindBySimilarity_CaseInsensitive(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_FindByAlias(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEntityRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Create entity
	entity := newTestEntity(nsID)
	if err := repo.Create(ctx, entity); err != nil {
		t.Fatalf("failed to create entity: %v", err)
	}

	// Create aliases
	createTestEntityAlias(t, ctx, db, entity.ID, "JD", "abbreviation")
	createTestEntityAlias(t, ctx, db, entity.ID, "Johnny", "nickname")

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
}

func TestEntityRepo_FindByAlias_CaseInsensitive(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEntityRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	entity := newTestEntity(nsID)
	if err := repo.Create(ctx, entity); err != nil {
		t.Fatalf("failed to create entity: %v", err)
	}

	createTestEntityAlias(t, ctx, db, entity.ID, "JohnDoe", "username")

	// Case-insensitive search
	results, err := repo.FindByAlias(ctx, nsID, "johndoe")
	if err != nil {
		t.Fatalf("failed to find: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for case-insensitive alias, got %d", len(results))
	}
}

func TestEntityRepo_ListByNamespace(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_ListByNamespace_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEntityRepo(db)

	results, err := repo.ListByNamespace(ctx, uuid.New())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestEntityRepo_ListByNamespace_Isolation(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}

func TestEntityRepo_Create_WithEmbeddingDim(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
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
}
