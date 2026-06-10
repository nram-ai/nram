package service

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

func createRoundTripProject(t *testing.T, ctx context.Context, nsRepo *storage.NamespaceRepo, projRepo *storage.ProjectRepo, slug string) *model.Project {
	t.Helper()
	nsID := uuid.New()
	ns := &model.Namespace{
		ID:    nsID,
		Name:  slug,
		Slug:  slug,
		Kind:  "project",
		Path:  "/" + slug,
		Depth: 1,
	}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace %s: %v", slug, err)
	}
	proj := &model.Project{
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             slug,
		Slug:             slug,
	}
	if err := projRepo.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", slug, err)
	}
	return proj
}

// TestExportImportGraphRoundTripIntegration exercises the full native
// round-trip against a real database: it seeds a source project with memories,
// entities, and a relationship; serializes it with the real ExportService;
// then feeds those exact bytes to the real ImportService targeting a fresh
// project and confirms the graph is recreated with relationship endpoints
// remapped to the new namespace's entities.
func TestExportImportGraphRoundTripIntegration(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	memRepo := storage.NewMemoryRepo(db)
	projRepo := storage.NewProjectRepo(db)
	nsRepo := storage.NewNamespaceRepo(db)
	entRepo := storage.NewEntityRepo(db)
	relRepo := storage.NewRelationshipRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	ingRepo := storage.NewIngestionLogRepo(db)
	settingsSvc := NewSettingsService(storage.NewSettingsRepo(db))

	src := createRoundTripProject(t, ctx, nsRepo, projRepo, "source-proj")
	dst := createRoundTripProject(t, ctx, nsRepo, projRepo, "dest-proj")

	// Seed memories.
	now := time.Now().UTC().Truncate(time.Second)
	var firstMemID, secondMemID uuid.UUID
	for i, content := range []string{"Alice knows Bob", "Bob lives in Berlin"} {
		mem := &model.Memory{
			ID:          uuid.New(),
			NamespaceID: src.NamespaceID,
			Content:     content,
			ContentHash: storage.HashContent(content),
			Origin:      model.OriginUser,
			Tags:        []string{"people"},
			Confidence:  0.9,
			Importance:  0.8,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := memRepo.Create(ctx, mem); err != nil {
			t.Fatalf("seed memory: %v", err)
		}
		if i == 0 {
			firstMemID = mem.ID
		} else {
			secondMemID = mem.ID
		}
	}

	// Seed a lineage link: the second memory is synthesized from the first.
	if err := lineageRepo.Create(ctx, &model.MemoryLineage{
		NamespaceID: src.NamespaceID,
		MemoryID:    secondMemID,
		ParentID:    &firstMemID,
		Relation:    model.LineageSynthesizedFrom,
	}); err != nil {
		t.Fatalf("seed lineage: %v", err)
	}

	// Seed entities (Upsert assigns the persisted IDs).
	alice := &model.Entity{NamespaceID: src.NamespaceID, Name: "Alice", Canonical: "alice", EntityType: "person", MentionCount: 2}
	bob := &model.Entity{NamespaceID: src.NamespaceID, Name: "Bob", Canonical: "bob", EntityType: "person", MentionCount: 1}
	if err := entRepo.Upsert(ctx, alice); err != nil {
		t.Fatalf("upsert alice: %v", err)
	}
	if err := entRepo.Upsert(ctx, bob); err != nil {
		t.Fatalf("upsert bob: %v", err)
	}

	// Seed a relationship between them, anchored to the first memory.
	rel := &model.Relationship{
		ID:           uuid.New(),
		NamespaceID:  src.NamespaceID,
		SourceID:     alice.ID,
		TargetID:     bob.ID,
		Relation:     "knows",
		Weight:       0.75,
		SourceMemory: &firstMemID,
		ValidFrom:    now,
	}
	if err := relRepo.Create(ctx, rel); err != nil {
		t.Fatalf("seed relationship: %v", err)
	}

	// Export the source project with the real service.
	exportSvc := NewExportService(memRepo, entRepo, relRepo, lineageRepo, projRepo, settingsSvc)
	exportData, err := exportSvc.Export(ctx, &ExportRequest{ProjectID: src.ID, Format: ExportFormatJSON})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exportData.Memories) != 2 || len(exportData.Entities) != 2 || len(exportData.Relationships) != 1 {
		t.Fatalf("export shape unexpected: %d memories, %d entities, %d relationships",
			len(exportData.Memories), len(exportData.Entities), len(exportData.Relationships))
	}
	raw, err := json.Marshal(exportData)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}

	// Import those exact bytes into the destination project.
	importSvc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, entRepo, relRepo, lineageRepo, settingsSvc)
	resp, err := importSvc.Import(ctx, &ImportRequest{
		ProjectID: dst.ID,
		Format:    ImportFormatNRAM,
		Data:      bytes.NewReader(raw),
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if resp.Imported != 2 {
		t.Errorf("imported memories = %d, want 2", resp.Imported)
	}
	if resp.EntitiesImported != 2 {
		t.Errorf("imported entities = %d, want 2", resp.EntitiesImported)
	}
	if resp.RelationshipsImported != 1 {
		t.Errorf("imported relationships = %d, want 1", resp.RelationshipsImported)
	}

	// Verify the graph landed in the destination namespace.
	dstEntities, err := entRepo.ListByNamespace(ctx, dst.NamespaceID)
	if err != nil {
		t.Fatalf("list dest entities: %v", err)
	}
	if len(dstEntities) != 2 {
		t.Fatalf("dest entities = %d, want 2", len(dstEntities))
	}
	dstEntityIDs := map[uuid.UUID]string{}
	for _, e := range dstEntities {
		dstEntityIDs[e.ID] = e.Name
		if e.NamespaceID != dst.NamespaceID {
			t.Errorf("entity %q landed in wrong namespace", e.Name)
		}
	}

	dstRels, err := relRepo.ListByNamespace(ctx, dst.NamespaceID)
	if err != nil {
		t.Fatalf("list dest relationships: %v", err)
	}
	if len(dstRels) != 1 {
		t.Fatalf("dest relationships = %d, want 1", len(dstRels))
	}
	dstRel := dstRels[0]
	if dstRel.Relation != "knows" {
		t.Errorf("relation = %q, want knows", dstRel.Relation)
	}
	// Endpoints must be remapped to the destination entities, not the source.
	if _, ok := dstEntityIDs[dstRel.SourceID]; !ok {
		t.Errorf("relationship source %s not among dest entities", dstRel.SourceID)
	}
	if _, ok := dstEntityIDs[dstRel.TargetID]; !ok {
		t.Errorf("relationship target %s not among dest entities", dstRel.TargetID)
	}
	if dstRel.SourceID == alice.ID || dstRel.TargetID == bob.ID {
		t.Errorf("relationship endpoints were not remapped from the source namespace")
	}

	// Memories round-tripped too.
	dstMems, err := memRepo.ListByNamespaceFiltered(ctx, dst.NamespaceID, storage.MemoryListFilters{}, 100, 0)
	if err != nil {
		t.Fatalf("list dest memories: %v", err)
	}
	if len(dstMems) != 2 {
		t.Errorf("dest memories = %d, want 2", len(dstMems))
	}

	// Provenance must be re-anchored to a live destination memory, never left
	// NULL (the lost-provenance sweep reaps NULL-source edges) and never the
	// source namespace's memory ID.
	if dstRel.SourceMemory == nil {
		t.Fatalf("relationship source_memory is NULL; the lost-provenance sweep would reap it")
	}
	if *dstRel.SourceMemory == firstMemID {
		t.Errorf("relationship source_memory was not remapped from the source memory")
	}
	dstMemIDs := map[uuid.UUID]bool{}
	for _, m := range dstMems {
		dstMemIDs[m.ID] = true
	}
	if !dstMemIDs[*dstRel.SourceMemory] {
		t.Errorf("relationship source_memory %s is not a live destination memory", *dstRel.SourceMemory)
	}

	// Lineage round-trips: the synthesized_from link is recreated between the
	// remapped child and parent memories.
	if resp.LineageImported != 1 {
		t.Errorf("imported lineage = %d, want 1", resp.LineageImported)
	}
	byContent := map[string]uuid.UUID{}
	for _, m := range dstMems {
		byContent[m.Content] = m.ID
	}
	newChild := byContent["Bob lives in Berlin"]
	newParent := byContent["Alice knows Bob"]
	lineages, err := lineageRepo.ListByMemory(ctx, dst.NamespaceID, newChild)
	if err != nil {
		t.Fatalf("list dest lineage: %v", err)
	}
	var found bool
	for _, l := range lineages {
		if l.MemoryID == newChild && l.ParentID != nil && *l.ParentID == newParent &&
			l.Relation == model.LineageSynthesizedFrom {
			found = true
		}
		// Endpoints must be remapped away from the source namespace's ids.
		if l.MemoryID == secondMemID || (l.ParentID != nil && *l.ParentID == firstMemID) {
			t.Errorf("lineage endpoints were not remapped from the source namespace")
		}
	}
	if !found {
		t.Errorf("expected a synthesized_from lineage row linking remapped child %s -> parent %s", newChild, newParent)
	}
}
