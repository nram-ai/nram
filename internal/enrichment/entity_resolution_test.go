package enrichment

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type mockEntityFinder struct {
	mu       sync.Mutex
	entities map[uuid.UUID]*model.Entity // keyed by ID
}

func newMockEntityFinder() *mockEntityFinder {
	return &mockEntityFinder{entities: make(map[uuid.UUID]*model.Entity)}
}

func (m *mockEntityFinder) Upsert(_ context.Context, entity *model.Entity) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for canonical conflict (simulate ON CONFLICT behaviour).
	for _, e := range m.entities {
		if e.NamespaceID == entity.NamespaceID &&
			e.Canonical == entity.Canonical &&
			e.EntityType == entity.EntityType &&
			e.ID != entity.ID {
			// Conflict — update existing record.
			e.Name = entity.Name
			e.MentionCount = entity.MentionCount
			e.Properties = entity.Properties
			e.UpdatedAt = entity.UpdatedAt
			// Point caller's struct at the stored record so it picks up the
			// existing ID (mirrors reloadByCanonical in storage).
			*entity = *e
			return nil
		}
	}

	stored := *entity
	m.entities[entity.ID] = &stored
	return nil
}

func (m *mockEntityFinder) FindBySimilarity(_ context.Context, namespaceID uuid.UUID, name string, kind string, _ int) ([]model.Entity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var results []model.Entity
	for _, e := range m.entities {
		if e.NamespaceID != namespaceID || e.EntityType != kind {
			continue
		}
		// Exact canonical match.
		if e.Canonical == canonicalize(name) {
			results = append(results, *e)
			continue
		}
		// Simple substring similarity (mirrors SQL LIKE %name%).
		if contains(e.Canonical, canonicalize(name)) || contains(canonicalize(name), e.Canonical) {
			results = append(results, *e)
		}
	}
	return results, nil
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) > 0 &&
		len(haystack) >= len(needle) &&
		indexOf(haystack, needle) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (m *mockEntityFinder) FindByAlias(_ context.Context, _ uuid.UUID, _ string) ([]model.Entity, error) {
	// The real implementation joins entity_aliases with entities. We delegate
	// alias resolution to the AliasManager mock — this method returns no
	// results by default but can be overridden via aliasEntityMap below.
	return nil, nil
}

// mockEntityFinderWithAlias extends mockEntityFinder to support FindByAlias
// with actual entity lookup.
type mockEntityFinderWithAlias struct {
	*mockEntityFinder
	aliasEntities map[string]*model.Entity // alias -> entity
}

func newMockEntityFinderWithAlias() *mockEntityFinderWithAlias {
	return &mockEntityFinderWithAlias{
		mockEntityFinder: newMockEntityFinder(),
		aliasEntities:    make(map[string]*model.Entity),
	}
}

func (m *mockEntityFinderWithAlias) FindByAlias(_ context.Context, _ uuid.UUID, alias string) ([]model.Entity, error) {
	if e, ok := m.aliasEntities[canonicalize(alias)]; ok {
		return []model.Entity{*e}, nil
	}
	return nil, nil
}

func (m *mockEntityFinderWithAlias) registerAlias(alias string, entity *model.Entity) {
	m.aliasEntities[canonicalize(alias)] = entity
}

type mockAliasManager struct {
	mu      sync.Mutex
	aliases []model.EntityAlias
}

func newMockAliasManager() *mockAliasManager {
	return &mockAliasManager{}
}

func (m *mockAliasManager) Create(_ context.Context, alias *model.EntityAlias) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aliases = append(m.aliases, *alias)
	return nil
}

func (m *mockAliasManager) FindByAlias(_ context.Context, _ uuid.UUID, alias string) ([]model.EntityAlias, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var results []model.EntityAlias
	for _, a := range m.aliases {
		if canonicalize(a.Alias) == canonicalize(alias) {
			results = append(results, a)
		}
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCanonicalize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  Hello  World  ", "hello world"},
		{"Alice", "alice"},
		{"   ", ""},
		{"UPPER CASE", "upper case"},
		{"tabs\tand\nnewlines", "tabs and newlines"},
		{"  multiple   spaces   between  ", "multiple spaces between"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.input), func(t *testing.T) {
			got := canonicalize(tt.input)
			if got != tt.want {
				t.Errorf("canonicalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolve_NewEntity(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()
	entity, isNew, err := resolver.Resolve(ctx, nsID, "Alice", "person", map[string]interface{}{"role": "engineer"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isNew {
		t.Error("expected isNew=true for brand-new entity")
	}
	if entity.Name != "Alice" {
		t.Errorf("entity.Name = %q, want %q", entity.Name, "Alice")
	}
	if entity.Canonical != "alice" {
		t.Errorf("entity.Canonical = %q, want %q", entity.Canonical, "alice")
	}
	if entity.EntityType != "person" {
		t.Errorf("entity.EntityType = %q, want %q", entity.EntityType, "person")
	}
	if entity.MentionCount != 1 {
		t.Errorf("entity.MentionCount = %d, want 1", entity.MentionCount)
	}
	if entity.NamespaceID != nsID {
		t.Errorf("entity.NamespaceID = %v, want %v", entity.NamespaceID, nsID)
	}
}

func TestResolve_ExistingByCanonicalName(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Create entity first.
	entity1, isNew, err := resolver.Resolve(ctx, nsID, "Alice", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isNew {
		t.Fatal("expected first resolve to create new entity")
	}

	// Resolve again with different casing and whitespace.
	entity2, isNew2, err := resolver.Resolve(ctx, nsID, "  ALICE  ", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isNew2 {
		t.Error("expected isNew=false for canonical match")
	}
	if entity2.ID != entity1.ID {
		t.Errorf("expected same entity ID, got %v and %v", entity1.ID, entity2.ID)
	}
	if entity2.MentionCount != 2 {
		t.Errorf("entity.MentionCount = %d, want 2", entity2.MentionCount)
	}
}

func TestResolve_ExistingByAlias(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinderWithAlias()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Create entity first.
	entity1, _, err := resolver.Resolve(ctx, nsID, "Robert Smith", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Register "Bob Smith" as an alias in both the alias manager and the
	// entity finder's alias map.
	alias := &model.EntityAlias{
		ID:        uuid.New(),
		EntityID:  entity1.ID,
		Alias:     "Bob Smith",
		AliasType: "nickname",
	}
	if err := am.Create(ctx, alias); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ef.registerAlias("Bob Smith", entity1)

	// Resolve via alias.
	entity2, isNew, err := resolver.Resolve(ctx, nsID, "Bob Smith", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isNew {
		t.Error("expected isNew=false for alias match")
	}
	if entity2.ID != entity1.ID {
		t.Errorf("expected same entity ID, got %v and %v", entity1.ID, entity2.ID)
	}
}

func TestResolve_ExistingBySimilarity(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Create "International Business Machines".
	entity1, _, err := resolver.Resolve(ctx, nsID, "International Business Machines", "organization", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Resolve "Business Machines" — should match via similarity (substring).
	entity2, isNew, err := resolver.Resolve(ctx, nsID, "Business Machines", "organization", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isNew {
		t.Error("expected isNew=false for similarity match")
	}
	if entity2.ID != entity1.ID {
		t.Errorf("expected same entity ID, got %v and %v", entity1.ID, entity2.ID)
	}
}

func TestResolve_AliasCreatedOnSimilarMatch(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Create base entity.
	_, _, err := resolver.Resolve(ctx, nsID, "International Business Machines", "organization", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Resolve via similarity.
	_, _, err = resolver.Resolve(ctx, nsID, "Business Machines", "organization", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify alias was created.
	if len(am.aliases) == 0 {
		t.Fatal("expected an alias to be created for the similar name")
	}

	found := false
	for _, a := range am.aliases {
		if a.Alias == "Business Machines" && a.AliasType == "similar_name" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected alias 'Business Machines' with type 'similar_name' to be created")
	}
}

func TestResolve_MentionCountIncremented(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Create entity.
	entity, _, err := resolver.Resolve(ctx, nsID, "Alice", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entity.MentionCount != 1 {
		t.Fatalf("initial mention count = %d, want 1", entity.MentionCount)
	}

	// Resolve same entity three more times.
	for i := 2; i <= 4; i++ {
		e, isNew, err := resolver.Resolve(ctx, nsID, "Alice", "person", nil)
		if err != nil {
			t.Fatalf("unexpected error on resolve %d: %v", i, err)
		}
		if isNew {
			t.Errorf("resolve %d: expected isNew=false", i)
		}
		if e.MentionCount != i {
			t.Errorf("resolve %d: mention count = %d, want %d", i, e.MentionCount, i)
		}
	}
}

func TestResolveAll_MixOfNewAndExisting(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	// Pre-create one entity.
	_, _, err := resolver.Resolve(ctx, nsID, "Alice", "person", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inputs := []EntityInput{
		{Name: "Alice", Type: "person"},         // existing
		{Name: "Bob", Type: "person"},            // new
		{Name: "Acme Corp", Type: "organization"}, // new
	}

	result, err := resolver.ResolveAll(ctx, nsID, inputs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	if result["Alice"] == nil {
		t.Error("missing result for Alice")
	}
	if result["Bob"] == nil {
		t.Error("missing result for Bob")
	}
	if result["Acme Corp"] == nil {
		t.Error("missing result for Acme Corp")
	}

	// Alice should have mention count 2 (initial + re-resolve).
	if result["Alice"].MentionCount != 2 {
		t.Errorf("Alice mention count = %d, want 2", result["Alice"].MentionCount)
	}
	if result["Bob"].MentionCount != 1 {
		t.Errorf("Bob mention count = %d, want 1", result["Bob"].MentionCount)
	}
}

func TestResolveAll_DeduplicatesWithinBatch(t *testing.T) {
	ctx := context.Background()
	ef := newMockEntityFinder()
	am := newMockAliasManager()
	resolver := NewEntityResolver(ef, am)

	nsID := uuid.New()

	inputs := []EntityInput{
		{Name: "Alice", Type: "person"},
		{Name: "Bob", Type: "person"},
		{Name: "Alice", Type: "person"}, // duplicate
	}

	result, err := resolver.ResolveAll(ctx, nsID, inputs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 unique results, got %d", len(result))
	}

	// Alice should have mention count 2 (created once + dedup bump).
	if result["Alice"].MentionCount != 2 {
		t.Errorf("Alice mention count = %d, want 2", result["Alice"].MentionCount)
	}
}
