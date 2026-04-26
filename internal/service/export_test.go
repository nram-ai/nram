package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Mock implementations for export tests ---

type mockExportMemoryReader struct {
	memories map[uuid.UUID][]model.Memory // keyed by namespaceID
}

func (m *mockExportMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for _, mems := range m.memories {
		for i := range mems {
			if mems[i].ID == id {
				return &mems[i], nil
			}
		}
	}
	return nil, fmt.Errorf("memory not found")
}

func (m *mockExportMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var result []model.Memory
	idSet := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for _, mems := range m.memories {
		for _, mem := range mems {
			if _, ok := idSet[mem.ID]; ok {
				result = append(result, mem)
			}
		}
	}
	return result, nil
}

func (m *mockExportMemoryReader) ListByNamespace(_ context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	mems := m.memories[namespaceID]
	if offset >= len(mems) {
		return nil, nil
	}
	end := offset + limit
	if end > len(mems) {
		end = len(mems)
	}
	return mems[offset:end], nil
}

func (m *mockExportMemoryReader) ListByNamespaceFiltered(_ context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	rows := m.memories[namespaceID]
	if filters.HideSuperseded {
		filtered := make([]model.Memory, 0, len(rows))
		for _, mem := range rows {
			if mem.SupersededBy != nil {
				continue
			}
			filtered = append(filtered, mem)
		}
		rows = filtered
	}
	if offset >= len(rows) {
		return nil, nil
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end], nil
}

type mockEntityLister struct {
	entities map[uuid.UUID][]model.Entity // keyed by namespaceID
}

func (m *mockEntityLister) ListByNamespace(_ context.Context, namespaceID uuid.UUID) ([]model.Entity, error) {
	return m.entities[namespaceID], nil
}

type mockRelationshipLister struct {
	relationships map[uuid.UUID][]model.Relationship // keyed by entityID
}

func (m *mockRelationshipLister) ListByEntity(_ context.Context, entityID uuid.UUID) ([]model.Relationship, error) {
	return m.relationships[entityID], nil
}

type mockLineageReader struct {
	lineage map[uuid.UUID][]model.MemoryLineage // keyed by memoryID
}

func (m *mockLineageReader) ListByMemory(_ context.Context, _ uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error) {
	return m.lineage[memoryID], nil
}

type mockExportProjectRepo struct {
	projects map[uuid.UUID]*model.Project
}

func (m *mockExportProjectRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

// --- Helper to build test fixtures ---

func newExportTestFixtures() (
	projectID uuid.UUID,
	nsID uuid.UUID,
	mem1ID uuid.UUID,
	mem2ID uuid.UUID,
	ent1ID uuid.UUID,
	ent2ID uuid.UUID,
	rel1ID uuid.UUID,
	projects *mockExportProjectRepo,
	memories *mockExportMemoryReader,
	entities *mockEntityLister,
	relationships *mockRelationshipLister,
	lineageReader *mockLineageReader,
) {
	projectID = uuid.New()
	nsID = uuid.New()
	mem1ID = uuid.New()
	mem2ID = uuid.New()
	ent1ID = uuid.New()
	ent2ID = uuid.New()
	rel1ID = uuid.New()

	now := time.Now()
	parentID := mem1ID

	projects = &mockExportProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Test Project",
				Slug:        "test-project",
			},
		},
	}

	memories = &mockExportMemoryReader{
		memories: map[uuid.UUID][]model.Memory{
			nsID: {
				{
					ID:          mem1ID,
					NamespaceID: nsID,
					Content:     "First memory",
					Tags:        []string{"tag1"},
					Confidence:  0.9,
					Importance:  0.7,
					CreatedAt:   now,
					UpdatedAt:   now,
				},
				{
					ID:          mem2ID,
					NamespaceID: nsID,
					Content:     "Second memory",
					Tags:        []string{"tag2"},
					Confidence:  0.8,
					Importance:  0.5,
					Enriched:    true,
					Metadata:    json.RawMessage(`{"key":"value"}`),
					CreatedAt:   now,
					UpdatedAt:   now,
				},
			},
		},
	}

	entities = &mockEntityLister{
		entities: map[uuid.UUID][]model.Entity{
			nsID: {
				{
					ID:           ent1ID,
					NamespaceID:  nsID,
					Name:         "Alice",
					Canonical:    "alice",
					EntityType:   "person",
					MentionCount: 5,
					CreatedAt:    now,
					UpdatedAt:    now,
				},
				{
					ID:           ent2ID,
					NamespaceID:  nsID,
					Name:         "Acme Corp",
					Canonical:    "acme-corp",
					EntityType:   "organization",
					Properties:   json.RawMessage(`{"industry":"tech"}`),
					MentionCount: 3,
					CreatedAt:    now,
					UpdatedAt:    now,
				},
			},
		},
	}

	relationships = &mockRelationshipLister{
		relationships: map[uuid.UUID][]model.Relationship{
			ent1ID: {
				{
					ID:         rel1ID,
					SourceID:   ent1ID,
					TargetID:   ent2ID,
					Relation:   "works_at",
					Weight:     0.9,
					ValidFrom:  now,
					ValidUntil: nil,
					CreatedAt:  now,
				},
			},
			ent2ID: {
				// Same relationship returned from the other side.
				{
					ID:         rel1ID,
					SourceID:   ent1ID,
					TargetID:   ent2ID,
					Relation:   "works_at",
					Weight:     0.9,
					ValidFrom:  now,
					ValidUntil: nil,
					CreatedAt:  now,
				},
			},
		},
	}

	lineageReader = &mockLineageReader{
		lineage: map[uuid.UUID][]model.MemoryLineage{
			mem2ID: {
				{
					ID:        uuid.New(),
					MemoryID:  mem2ID,
					ParentID:  &parentID,
					Relation:  "derived_from",
					CreatedAt: now,
				},
			},
		},
	}

	return projectID, nsID, mem1ID, mem2ID, ent1ID, ent2ID, rel1ID, projects, memories, entities, relationships, lineageReader
}

func TestExport_JSONFormat_WithData(t *testing.T) {
	projectID, _, mem1ID, mem2ID, ent1ID, ent2ID, rel1ID, projects, memories, entities, relationships, lineage := newExportTestFixtures()

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	data, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatJSON,
	})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if data.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", data.Version)
	}
	if data.Project.ID != projectID {
		t.Errorf("expected project ID %s, got %s", projectID, data.Project.ID)
	}
	if data.Project.Name != "Test Project" {
		t.Errorf("expected project name 'Test Project', got %s", data.Project.Name)
	}
	if data.Project.Slug != "test-project" {
		t.Errorf("expected project slug 'test-project', got %s", data.Project.Slug)
	}

	// Memories.
	if len(data.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(data.Memories))
	}

	memIDs := map[uuid.UUID]ExportMemory{}
	for _, m := range data.Memories {
		memIDs[m.ID] = m
	}

	m1, ok := memIDs[mem1ID]
	if !ok {
		t.Fatal("mem1 not found in export")
	}
	if m1.Content != "First memory" {
		t.Errorf("expected 'First memory', got %s", m1.Content)
	}
	if len(m1.Lineage) != 0 {
		t.Errorf("expected no lineage for mem1, got %d", len(m1.Lineage))
	}

	m2, ok := memIDs[mem2ID]
	if !ok {
		t.Fatal("mem2 not found in export")
	}
	if m2.Content != "Second memory" {
		t.Errorf("expected 'Second memory', got %s", m2.Content)
	}
	if !m2.Enriched {
		t.Error("expected mem2 to be enriched")
	}

	// Lineage for mem2.
	if len(m2.Lineage) != 1 {
		t.Fatalf("expected 1 lineage for mem2, got %d", len(m2.Lineage))
	}
	if m2.Lineage[0].Relation != "derived_from" {
		t.Errorf("expected lineage relation 'derived_from', got %s", m2.Lineage[0].Relation)
	}
	if m2.Lineage[0].ParentID == nil || *m2.Lineage[0].ParentID != mem1ID {
		t.Error("expected lineage parent_id to be mem1ID")
	}

	// Entities.
	if len(data.Entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(data.Entities))
	}

	entIDs := map[uuid.UUID]ExportEntity{}
	for _, e := range data.Entities {
		entIDs[e.ID] = e
	}
	if _, ok := entIDs[ent1ID]; !ok {
		t.Error("ent1 not found in export")
	}
	if _, ok := entIDs[ent2ID]; !ok {
		t.Error("ent2 not found in export")
	}
	if entIDs[ent1ID].Type != "person" {
		t.Errorf("expected entity type 'person', got %s", entIDs[ent1ID].Type)
	}

	// Relationships (deduplicated).
	if len(data.Relationships) != 1 {
		t.Fatalf("expected 1 relationship (deduplicated), got %d", len(data.Relationships))
	}
	if data.Relationships[0].ID != rel1ID {
		t.Errorf("expected relationship ID %s, got %s", rel1ID, data.Relationships[0].ID)
	}
	if data.Relationships[0].Relation != "works_at" {
		t.Errorf("expected relation 'works_at', got %s", data.Relationships[0].Relation)
	}

	// Stats.
	if data.Stats.MemoryCount != 2 {
		t.Errorf("expected memory_count 2, got %d", data.Stats.MemoryCount)
	}
	if data.Stats.EntityCount != 2 {
		t.Errorf("expected entity_count 2, got %d", data.Stats.EntityCount)
	}
	if data.Stats.RelationshipCount != 1 {
		t.Errorf("expected relationship_count 1, got %d", data.Stats.RelationshipCount)
	}
}

func TestExport_JSONFormat_EmptyProject(t *testing.T) {
	projectID := uuid.New()
	nsID := uuid.New()

	projects := &mockExportProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Empty Project",
				Slug:        "empty-project",
			},
		},
	}
	memories := &mockExportMemoryReader{memories: map[uuid.UUID][]model.Memory{}}
	entities := &mockEntityLister{entities: map[uuid.UUID][]model.Entity{}}
	relationships := &mockRelationshipLister{relationships: map[uuid.UUID][]model.Relationship{}}
	lineage := &mockLineageReader{lineage: map[uuid.UUID][]model.MemoryLineage{}}

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	data, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatJSON,
	})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if len(data.Memories) != 0 {
		t.Errorf("expected 0 memories, got %d", len(data.Memories))
	}
	if len(data.Entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(data.Entities))
	}
	if len(data.Relationships) != 0 {
		t.Errorf("expected 0 relationships, got %d", len(data.Relationships))
	}
	if data.Stats.MemoryCount != 0 || data.Stats.EntityCount != 0 || data.Stats.RelationshipCount != 0 {
		t.Errorf("expected all stats to be 0, got %+v", data.Stats)
	}

	// Ensure slices are non-nil for clean JSON marshalling.
	b, _ := json.Marshal(data)
	var raw map[string]interface{}
	_ = json.Unmarshal(b, &raw)
	if raw["memories"] == nil {
		t.Error("memories should be non-nil empty array in JSON")
	}
	if raw["entities"] == nil {
		t.Error("entities should be non-nil empty array in JSON")
	}
	if raw["relationships"] == nil {
		t.Error("relationships should be non-nil empty array in JSON")
	}
}

func TestExport_ProjectNotFound(t *testing.T) {
	projects := &mockExportProjectRepo{projects: map[uuid.UUID]*model.Project{}}
	memories := &mockExportMemoryReader{memories: map[uuid.UUID][]model.Memory{}}
	entities := &mockEntityLister{entities: map[uuid.UUID][]model.Entity{}}
	relationships := &mockRelationshipLister{relationships: map[uuid.UUID][]model.Relationship{}}
	lineage := &mockLineageReader{lineage: map[uuid.UUID][]model.MemoryLineage{}}

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	_, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: uuid.New(),
		Format:    ExportFormatJSON,
	})
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}

func TestExport_InvalidFormat(t *testing.T) {
	projectID := uuid.New()
	nsID := uuid.New()

	projects := &mockExportProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Test",
				Slug:        "test",
			},
		},
	}
	memories := &mockExportMemoryReader{memories: map[uuid.UUID][]model.Memory{}}
	entities := &mockEntityLister{entities: map[uuid.UUID][]model.Entity{}}
	relationships := &mockRelationshipLister{relationships: map[uuid.UUID][]model.Relationship{}}
	lineage := &mockLineageReader{lineage: map[uuid.UUID][]model.MemoryLineage{}}

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	_, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormat("xml"),
	})
	if err == nil {
		t.Fatal("expected error for unsupported format")
	}
}

func TestExportNDJSON(t *testing.T) {
	projectID, _, _, _, _, _, _, projects, memories, entities, relationships, lineage := newExportTestFixtures()

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	var buf bytes.Buffer
	err := svc.ExportNDJSON(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatNDJSON,
	}, &buf)
	if err != nil {
		t.Fatalf("ExportNDJSON failed: %v", err)
	}

	// Parse each line.
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) < 1 {
		t.Fatal("expected at least 1 line of output")
	}

	// First line should be project.
	var firstRecord ndjsonRecord
	if err := json.Unmarshal(lines[0], &firstRecord); err != nil {
		t.Fatalf("failed to parse first NDJSON line: %v", err)
	}
	if firstRecord.Type != "project" {
		t.Errorf("expected first record type 'project', got %s", firstRecord.Type)
	}

	// Count record types.
	typeCounts := map[string]int{}
	for _, line := range lines {
		var rec struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("failed to parse NDJSON line: %v", err)
		}
		typeCounts[rec.Type]++
	}

	if typeCounts["project"] != 1 {
		t.Errorf("expected 1 project record, got %d", typeCounts["project"])
	}
	if typeCounts["memory"] != 2 {
		t.Errorf("expected 2 memory records, got %d", typeCounts["memory"])
	}
	if typeCounts["entity"] != 2 {
		t.Errorf("expected 2 entity records, got %d", typeCounts["entity"])
	}
	if typeCounts["relationship"] != 1 {
		t.Errorf("expected 1 relationship record (deduplicated), got %d", typeCounts["relationship"])
	}

	// Total lines: 1 project + 2 memories + 2 entities + 1 relationship = 6
	expectedTotal := 6
	if len(lines) != expectedTotal {
		t.Errorf("expected %d total lines, got %d", expectedTotal, len(lines))
	}
}

func TestExportNDJSON_ProjectNotFound(t *testing.T) {
	projects := &mockExportProjectRepo{projects: map[uuid.UUID]*model.Project{}}
	memories := &mockExportMemoryReader{memories: map[uuid.UUID][]model.Memory{}}
	entities := &mockEntityLister{entities: map[uuid.UUID][]model.Entity{}}
	relationships := &mockRelationshipLister{relationships: map[uuid.UUID][]model.Relationship{}}
	lineage := &mockLineageReader{lineage: map[uuid.UUID][]model.MemoryLineage{}}

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	var buf bytes.Buffer
	err := svc.ExportNDJSON(context.Background(), &ExportRequest{
		ProjectID: uuid.New(),
	}, &buf)
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}

func TestExport_StatsCalculatedCorrectly(t *testing.T) {
	projectID, _, _, _, _, _, _, projects, memories, entities, relationships, lineage := newExportTestFixtures()

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	data, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatJSON,
	})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if data.Stats.MemoryCount != len(data.Memories) {
		t.Errorf("stats memory_count %d does not match actual %d", data.Stats.MemoryCount, len(data.Memories))
	}
	if data.Stats.EntityCount != len(data.Entities) {
		t.Errorf("stats entity_count %d does not match actual %d", data.Stats.EntityCount, len(data.Entities))
	}
	if data.Stats.RelationshipCount != len(data.Relationships) {
		t.Errorf("stats relationship_count %d does not match actual %d", data.Stats.RelationshipCount, len(data.Relationships))
	}
}

func TestExport_LineageIncludedForMemories(t *testing.T) {
	projectID, _, mem1ID, mem2ID, _, _, _, projects, memories, entities, relationships, lineage := newExportTestFixtures()

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	data, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatJSON,
	})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	memMap := map[uuid.UUID]ExportMemory{}
	for _, m := range data.Memories {
		memMap[m.ID] = m
	}

	// mem1 should have no lineage.
	m1 := memMap[mem1ID]
	if len(m1.Lineage) != 0 {
		t.Errorf("expected 0 lineage for mem1, got %d", len(m1.Lineage))
	}

	// mem2 should have 1 lineage entry.
	m2 := memMap[mem2ID]
	if len(m2.Lineage) != 1 {
		t.Fatalf("expected 1 lineage for mem2, got %d", len(m2.Lineage))
	}
	if m2.Lineage[0].ParentID == nil {
		t.Fatal("expected non-nil parent_id in lineage")
	}
	if *m2.Lineage[0].ParentID != mem1ID {
		t.Errorf("expected parent_id %s, got %s", mem1ID, *m2.Lineage[0].ParentID)
	}
	if m2.Lineage[0].Relation != "derived_from" {
		t.Errorf("expected relation 'derived_from', got %s", m2.Lineage[0].Relation)
	}
}

func TestExportNDJSON_LineageInMemoryRecords(t *testing.T) {
	projectID, _, _, mem2ID, _, _, _, projects, memories, entities, relationships, lineage := newExportTestFixtures()

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	var buf bytes.Buffer
	err := svc.ExportNDJSON(context.Background(), &ExportRequest{
		ProjectID: projectID,
		Format:    ExportFormatNDJSON,
	}, &buf)
	if err != nil {
		t.Fatalf("ExportNDJSON failed: %v", err)
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))

	// Find the memory record for mem2 and check its lineage.
	found := false
	for _, line := range lines {
		var rec struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("failed to parse line: %v", err)
		}
		if rec.Type != "memory" {
			continue
		}
		var mem ExportMemory
		if err := json.Unmarshal(rec.Data, &mem); err != nil {
			t.Fatalf("failed to parse memory data: %v", err)
		}
		if mem.ID == mem2ID {
			found = true
			if len(mem.Lineage) != 1 {
				t.Fatalf("expected 1 lineage for mem2 in NDJSON, got %d", len(mem.Lineage))
			}
			if mem.Lineage[0].Relation != "derived_from" {
				t.Errorf("expected lineage relation 'derived_from', got %s", mem.Lineage[0].Relation)
			}
		}
	}
	if !found {
		t.Fatal("mem2 not found in NDJSON output")
	}
}

func TestExport_SupersededExcludedByDefault(t *testing.T) {
	projectID, nsID, mem1ID, mem2ID, _, _, _, projects, memories, entities, relationships, lineage := newExportTestFixtures()
	// Mark mem1 as superseded by mem2.
	winnerID := mem2ID
	for i := range memories.memories[nsID] {
		if memories.memories[nsID][i].ID == mem1ID {
			memories.memories[nsID][i].SupersededBy = &winnerID
		}
	}

	svc := NewExportService(memories, entities, relationships, lineage, projects)

	defaulted, err := svc.Export(context.Background(), &ExportRequest{ProjectID: projectID, Format: ExportFormatJSON})
	if err != nil {
		t.Fatalf("Export default: %v", err)
	}
	if len(defaulted.Memories) != 1 {
		t.Fatalf("default should drop superseded loser; got %d memories", len(defaulted.Memories))
	}
	if defaulted.Memories[0].ID != mem2ID {
		t.Fatalf("survivor should be mem2; got %s", defaulted.Memories[0].ID)
	}

	included, err := svc.Export(context.Background(), &ExportRequest{
		ProjectID:         projectID,
		Format:            ExportFormatJSON,
		IncludeSuperseded: true,
	})
	if err != nil {
		t.Fatalf("Export include: %v", err)
	}
	if len(included.Memories) != 2 {
		t.Fatalf("IncludeSuperseded should surface both rows; got %d", len(included.Memories))
	}
}
