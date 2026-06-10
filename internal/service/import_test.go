package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Import-specific mock with create error support ---

type mockMemoryRepoForImport struct {
	created  []*model.Memory
	createFn func(mem *model.Memory) error
}

func (m *mockMemoryRepoForImport) Create(_ context.Context, mem *model.Memory) error {
	if m.createFn != nil {
		if err := m.createFn(mem); err != nil {
			return err
		}
	}
	m.created = append(m.created, mem)
	return nil
}

func (m *mockMemoryRepoForImport) GetByID(_ context.Context, id uuid.UUID, _ uuid.UUID) (*model.Memory, error) {
	for _, mem := range m.created {
		if mem.ID == id {
			return mem, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockMemoryRepoForImport) LookupByContentHash(_ context.Context, namespaceID uuid.UUID, hash string) (*model.Memory, error) {
	for _, mem := range m.created {
		if mem.NamespaceID != namespaceID {
			continue
		}
		memHash := mem.ContentHash
		if memHash == "" {
			memHash = storage.HashContent(mem.Content)
		}
		if memHash == hash {
			return mem, nil
		}
	}
	return nil, sql.ErrNoRows
}

// --- Helper to build standard test fixtures ---

func newImportTestFixtures() (
	*mockMemoryRepoForImport,
	*mockProjectRepo,
	*mockNamespaceRepo,
	*mockIngestionLogRepo,
	uuid.UUID, // projectID
	uuid.UUID, // namespaceID
) {
	projectID := uuid.New()
	namespaceID := uuid.New()

	memRepo := &mockMemoryRepoForImport{}
	projRepo := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: namespaceID,
				Name:        "Test Project",
				Slug:        "test-project",
			},
		},
	}
	nsRepo := &mockNamespaceRepo{
		namespaces: map[uuid.UUID]*model.Namespace{
			namespaceID: {
				ID:   namespaceID,
				Name: "test-ns",
				Slug: "test-ns",
				Kind: "project",
			},
		},
	}
	ingRepo := &mockIngestionLogRepo{}

	return memRepo, projRepo, nsRepo, ingRepo, projectID, namespaceID
}

func TestImportNRAMJSON(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, namespaceID := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	src := "test-source"
	exportData := ExportData{
		Version:    "1.0",
		ExportedAt: time.Now(),
		Project:    ExportProject{ID: projectID, Name: "Test", Slug: "test"},
		Memories: []ExportMemory{
			{
				ID:         uuid.New(),
				Content:    "Memory one",
				Tags:       []string{"tag1"},
				Source:     &src,
				Confidence: 0.9,
				Importance: 0.8,
				Metadata:   json.RawMessage(`{"key":"value"}`),
				CreatedAt:  ts,
			},
			{
				ID:         uuid.New(),
				Content:    "Memory two",
				Tags:       []string{"tag2"},
				Confidence: 0.7,
				Importance: 0.6,
				CreatedAt:  ts,
			},
		},
		Entities: []ExportEntity{},
		Stats:    ExportStats{MemoryCount: 2},
	}

	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 2 {
		t.Errorf("expected 2 imported, got %d", resp.Imported)
	}
	if resp.Skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", resp.Skipped)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(resp.Errors))
	}
	if len(memRepo.created) != 2 {
		t.Fatalf("expected 2 memories created, got %d", len(memRepo.created))
	}

	// Verify first memory fields.
	mem := memRepo.created[0]
	if mem.Content != "Memory one" {
		t.Errorf("expected content 'Memory one', got %q", mem.Content)
	}
	if mem.NamespaceID != namespaceID {
		t.Errorf("expected namespace %s, got %s", namespaceID, mem.NamespaceID)
	}
	if mem.Confidence != 0.9 {
		t.Errorf("expected confidence 0.9, got %f", mem.Confidence)
	}
	if mem.Importance != 0.8 {
		t.Errorf("expected importance 0.8, got %f", mem.Importance)
	}
	if mem.Source == nil || *mem.Source != "test-source" {
		t.Errorf("expected source 'test-source', got %v", mem.Source)
	}
	if !mem.CreatedAt.Equal(ts) {
		t.Errorf("expected created_at %v, got %v", ts, mem.CreatedAt)
	}
}

func TestImportNRAMNDJSON(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	ts := time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC)
	src := "ndjson-source"

	projLine, _ := json.Marshal(ndjsonRecord{
		Type: "project",
		Data: ExportProject{ID: projectID, Name: "Test", Slug: "test"},
	})
	memLine1, _ := json.Marshal(ndjsonRecord{
		Type: "memory",
		Data: ExportMemory{
			ID:         uuid.New(),
			Content:    "NDJSON memory",
			Tags:       []string{"nd"},
			Source:     &src,
			Confidence: 0.95,
			Importance: 0.75,
			CreatedAt:  ts,
		},
	})

	ndjsonData := string(projLine) + "\n" + string(memLine1) + "\n"

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(ndjsonData),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 1 {
		t.Errorf("expected 1 imported, got %d", resp.Imported)
	}
	if len(memRepo.created) != 1 {
		t.Fatalf("expected 1 memory created, got %d", len(memRepo.created))
	}
	if memRepo.created[0].Content != "NDJSON memory" {
		t.Errorf("expected content 'NDJSON memory', got %q", memRepo.created[0].Content)
	}
}

func TestImportMem0Format(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mem0Data := fmt.Sprintf(`{
		"results": [
			{
				"id": "abc123",
				"memory": "User prefers dark mode",
				"hash": "h1",
				"metadata": {"category": "preferences"},
				"created_at": "%s",
				"updated_at": "%s"
			},
			{
				"id": "def456",
				"memory": "User speaks English",
				"hash": "h2",
				"metadata": {},
				"created_at": "%s",
				"updated_at": "%s"
			}
		]
	}`, ts.Format(time.RFC3339), ts.Format(time.RFC3339), ts.Format(time.RFC3339), ts.Format(time.RFC3339))

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(mem0Data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 2 {
		t.Errorf("expected 2 imported, got %d", resp.Imported)
	}
	if len(memRepo.created) != 2 {
		t.Fatalf("expected 2 memories created, got %d", len(memRepo.created))
	}

	mem := memRepo.created[0]
	if mem.Content != "User prefers dark mode" {
		t.Errorf("expected 'User prefers dark mode', got %q", mem.Content)
	}
	if mem.Source == nil || *mem.Source != "mem0-import" {
		t.Errorf("expected source 'mem0-import', got %v", mem.Source)
	}
	if !mem.CreatedAt.Equal(ts) {
		t.Errorf("expected created_at %v, got %v", ts, mem.CreatedAt)
	}
}

func TestImportZepFormat(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	zepData := fmt.Sprintf(`{
		"messages": [
			{
				"uuid": "aaa-bbb",
				"role": "human",
				"content": "Hello there",
				"metadata": {},
				"created_at": "%s"
			},
			{
				"uuid": "ccc-ddd",
				"role": "assistant",
				"content": "Hi! How can I help?",
				"metadata": {"key": "val"},
				"created_at": "%s"
			}
		]
	}`, ts.Format(time.RFC3339), ts.Format(time.RFC3339))

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatZep,
		Data:      strings.NewReader(zepData),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 2 {
		t.Errorf("expected 2 imported, got %d", resp.Imported)
	}

	// Check role mapped to tags.
	mem0 := memRepo.created[0]
	if len(mem0.Tags) != 1 || mem0.Tags[0] != "human" {
		t.Errorf("expected tags [human], got %v", mem0.Tags)
	}
	if mem0.Source == nil || *mem0.Source != "zep-import" {
		t.Errorf("expected source 'zep-import', got %v", mem0.Source)
	}

	mem1 := memRepo.created[1]
	if len(mem1.Tags) != 1 || mem1.Tags[0] != "assistant" {
		t.Errorf("expected tags [assistant], got %v", mem1.Tags)
	}
}

func TestImportProjectNotFound(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, _, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	_, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: uuid.New(), // unknown project
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(`{"memories":[]}`),
	})

	if err == nil {
		t.Fatal("expected error for unknown project")
	}
	if !strings.Contains(err.Error(), "project not found") {
		t.Errorf("expected 'project not found' error, got: %v", err)
	}
}

func TestImportInvalidFormat(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	_, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormat("unknown"),
		Data:      strings.NewReader("{}"),
	})

	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "unsupported import format") {
		t.Errorf("expected 'unsupported import format' error, got: %v", err)
	}
}

func TestImportMalformedJSON(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	_, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader("{invalid json"),
	})

	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
	if !strings.Contains(err.Error(), "failed to parse") {
		t.Errorf("expected parse error, got: %v", err)
	}
}

func TestImportPerItemErrors(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	// One valid, one empty content.
	mem0Data := `{
		"results": [
			{
				"id": "abc",
				"memory": "Valid memory",
				"hash": "h1",
				"metadata": {}
			},
			{
				"id": "def",
				"memory": "",
				"hash": "h2",
				"metadata": {}
			},
			{
				"id": "ghi",
				"memory": "   ",
				"hash": "h3",
				"metadata": {}
			}
		]
	}`

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(mem0Data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 1 {
		t.Errorf("expected 1 imported, got %d", resp.Imported)
	}
	if resp.Skipped != 2 {
		t.Errorf("expected 2 skipped, got %d", resp.Skipped)
	}
	if len(resp.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(resp.Errors))
	}
	// Check error indices.
	if resp.Errors[0].Index != 1 {
		t.Errorf("expected error index 1, got %d", resp.Errors[0].Index)
	}
	if resp.Errors[1].Index != 2 {
		t.Errorf("expected error index 2, got %d", resp.Errors[1].Index)
	}
	if !strings.Contains(resp.Errors[0].Message, "empty content") {
		t.Errorf("expected 'empty content' message, got %q", resp.Errors[0].Message)
	}
}

func TestImportIngestionLogCreated(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, namespaceID := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	mem0Data := `{
		"results": [
			{
				"id": "abc",
				"memory": "A memory",
				"hash": "h1",
				"metadata": {}
			}
		]
	}`

	_, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(mem0Data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ingRepo.logs) != 1 {
		t.Fatalf("expected 1 ingestion log, got %d", len(ingRepo.logs))
	}

	log := ingRepo.logs[0]
	if log.NamespaceID != namespaceID {
		t.Errorf("expected namespace %s, got %s", namespaceID, log.NamespaceID)
	}
	if log.Source != "mem0-import" {
		t.Errorf("expected source 'mem0-import', got %q", log.Source)
	}
	if log.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", log.Status)
	}
	if len(log.MemoryIDs) != 1 {
		t.Errorf("expected 1 memory ID in log, got %d", len(log.MemoryIDs))
	}
}

func TestImportLatencyTracked(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	data := `{"results": [{"id": "a", "memory": "test", "hash": "h", "metadata": {}}]}`

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestImportEmptyData(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	// Empty results array.
	data := `{"results": []}`

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 0 {
		t.Errorf("expected 0 imported, got %d", resp.Imported)
	}
	if resp.Skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", resp.Skipped)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(resp.Errors))
	}
}

func TestImportPartialStatusInIngestionLog(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	// One valid, one empty -> partial status.
	data := `{
		"results": [
			{"id": "a", "memory": "valid", "hash": "h", "metadata": {}},
			{"id": "b", "memory": "", "hash": "h2", "metadata": {}}
		]
	}`

	_, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ingRepo.logs) != 1 {
		t.Fatalf("expected 1 ingestion log, got %d", len(ingRepo.logs))
	}
	if ingRepo.logs[0].Status != "partial" {
		t.Errorf("expected status 'partial', got %q", ingRepo.logs[0].Status)
	}
}

func TestImportCreateError(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()

	callCount := 0
	memRepo.createFn = func(_ *model.Memory) error {
		callCount++
		if callCount == 2 {
			return fmt.Errorf("db error")
		}
		return nil
	}

	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, nil, nil)

	data := `{
		"results": [
			{"id": "a", "memory": "one", "hash": "h1", "metadata": {}},
			{"id": "b", "memory": "two", "hash": "h2", "metadata": {}},
			{"id": "c", "memory": "three", "hash": "h3", "metadata": {}}
		]
	}`

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatMem0,
		Data:      strings.NewReader(data),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 2 {
		t.Errorf("expected 2 imported, got %d", resp.Imported)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", resp.Skipped)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(resp.Errors))
	}
	if resp.Errors[0].Index != 1 {
		t.Errorf("expected error at index 1, got %d", resp.Errors[0].Index)
	}
	if !strings.Contains(resp.Errors[0].Message, "db error") {
		t.Errorf("expected 'db error' in message, got %q", resp.Errors[0].Message)
	}
}

func TestImportNRAMGraphRoundTripJSON(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, namespaceID := newImportTestFixtures()
	entRepo := &mockEntityRepo{}
	relRepo := &mockRelationshipRepo{}
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, entRepo, relRepo, &mockLineageRepo{}, nil)

	// Old IDs as they appear in the export. After import these must be remapped
	// to the freshly-created entity IDs in the target namespace.
	oldE1 := uuid.New()
	oldE2 := uuid.New()
	oldMem1 := uuid.New()
	ts := time.Date(2024, 3, 1, 9, 0, 0, 0, time.UTC)

	exportData := ExportData{
		Version:    "1.0",
		ExportedAt: ts,
		Project:    ExportProject{ID: projectID, Name: "Test", Slug: "test"},
		Memories: []ExportMemory{
			{ID: oldMem1, Content: "Alice knows Bob", Tags: []string{"people"}, Confidence: 0.9, Importance: 0.8, CreatedAt: ts},
			{ID: uuid.New(), Content: "A second memory", Confidence: 0.7, Importance: 0.6, CreatedAt: ts},
		},
		Entities: []ExportEntity{
			{ID: oldE1, Name: "Alice", Type: "person", Canonical: "alice", Properties: json.RawMessage(`{"k":"v"}`), MentionCount: 3},
			{ID: oldE2, Name: "Bob", Type: "person", Canonical: "bob", MentionCount: 1},
		},
		Relationships: []ExportRelationship{
			{ID: uuid.New(), SourceID: oldE1, TargetID: oldE2, Relation: "knows", Weight: 0.75, SourceMemory: &oldMem1, ValidFrom: ts},
		},
		Stats: ExportStats{MemoryCount: 2, EntityCount: 2, RelationshipCount: 1},
	}

	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Imported != 2 {
		t.Errorf("expected 2 memories imported, got %d", resp.Imported)
	}
	if resp.EntitiesImported != 2 {
		t.Errorf("expected 2 entities imported, got %d", resp.EntitiesImported)
	}
	if resp.RelationshipsImported != 1 {
		t.Errorf("expected 1 relationship imported, got %d", resp.RelationshipsImported)
	}
	if len(entRepo.entities) != 2 {
		t.Fatalf("expected 2 entities upserted, got %d", len(entRepo.entities))
	}
	if len(relRepo.relationships) != 1 {
		t.Fatalf("expected 1 relationship created, got %d", len(relRepo.relationships))
	}

	// Entities land in the target namespace with canonical preserved.
	for _, e := range entRepo.entities {
		if e.NamespaceID != namespaceID {
			t.Errorf("entity %q in namespace %s, want %s", e.Name, e.NamespaceID, namespaceID)
		}
	}
	if entRepo.entities[0].Canonical != "alice" || entRepo.entities[0].EntityType != "person" {
		t.Errorf("entity 0 canonical/type not preserved: %q/%q", entRepo.entities[0].Canonical, entRepo.entities[0].EntityType)
	}

	// The relationship endpoints must point at the newly-created entity IDs,
	// not the old export IDs.
	rel := relRepo.relationships[0]
	newE1 := entRepo.entities[0].ID
	newE2 := entRepo.entities[1].ID
	if rel.SourceID != newE1 {
		t.Errorf("relationship source = %s, want remapped %s", rel.SourceID, newE1)
	}
	if rel.TargetID != newE2 {
		t.Errorf("relationship target = %s, want remapped %s", rel.TargetID, newE2)
	}
	if rel.SourceID == oldE1 || rel.TargetID == oldE2 {
		t.Errorf("relationship endpoints were not remapped from export IDs")
	}
	if rel.NamespaceID != namespaceID {
		t.Errorf("relationship namespace = %s, want %s", rel.NamespaceID, namespaceID)
	}
	if rel.Relation != "knows" || rel.Weight != 0.75 {
		t.Errorf("relationship fields not preserved: %q/%f", rel.Relation, rel.Weight)
	}
	if !rel.ValidFrom.Equal(ts) {
		t.Errorf("relationship valid_from = %v, want %v", rel.ValidFrom, ts)
	}
	// Provenance must be remapped to the newly-created memory, never the old
	// export id and never NULL.
	if rel.SourceMemory == nil {
		t.Fatalf("relationship source_memory is NULL")
	}
	if *rel.SourceMemory == oldMem1 {
		t.Errorf("relationship source_memory not remapped from export id")
	}
	if *rel.SourceMemory != memRepo.created[0].ID {
		t.Errorf("relationship source_memory = %s, want first imported memory %s", *rel.SourceMemory, memRepo.created[0].ID)
	}
}

func TestImportNRAMGraphSkipsUnmappedRelationship(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	entRepo := &mockEntityRepo{}
	relRepo := &mockRelationshipRepo{}
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, entRepo, relRepo, &mockLineageRepo{}, nil)

	e1 := uuid.New()
	dangling := uuid.New() // referenced by a relationship but absent from Entities
	exportData := ExportData{
		Version:  "1.0",
		Project:  ExportProject{ID: projectID},
		Memories: []ExportMemory{},
		Entities: []ExportEntity{
			{ID: e1, Name: "Alice", Type: "person", Canonical: "alice"},
		},
		Relationships: []ExportRelationship{
			{ID: uuid.New(), SourceID: e1, TargetID: dangling, Relation: "knows", Weight: 1},
		},
	}
	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.EntitiesImported != 1 {
		t.Errorf("expected 1 entity imported, got %d", resp.EntitiesImported)
	}
	if resp.RelationshipsImported != 0 {
		t.Errorf("expected 0 relationships imported, got %d", resp.RelationshipsImported)
	}
	if len(relRepo.relationships) != 0 {
		t.Errorf("expected no relationships created, got %d", len(relRepo.relationships))
	}
	if resp.Skipped != 1 {
		t.Errorf("expected 1 skipped (dangling relationship), got %d", resp.Skipped)
	}
}

func TestImportNRAMGraphNDJSON(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	entRepo := &mockEntityRepo{}
	relRepo := &mockRelationshipRepo{}
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, entRepo, relRepo, &mockLineageRepo{}, nil)

	oldE1 := uuid.New()
	oldE2 := uuid.New()
	oldMem := uuid.New()
	ts := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)

	projLine, _ := json.Marshal(ndjsonRecord{Type: "project", Data: ExportProject{ID: projectID}})
	memLine, _ := json.Marshal(ndjsonRecord{Type: "memory", Data: ExportMemory{ID: oldMem, Content: "ND memory", Confidence: 0.9, Importance: 0.8, CreatedAt: ts}})
	entLine1, _ := json.Marshal(ndjsonRecord{Type: "entity", Data: ExportEntity{ID: oldE1, Name: "Alice", Type: "person", Canonical: "alice"}})
	entLine2, _ := json.Marshal(ndjsonRecord{Type: "entity", Data: ExportEntity{ID: oldE2, Name: "Bob", Type: "person", Canonical: "bob"}})
	relLine, _ := json.Marshal(ndjsonRecord{Type: "relationship", Data: ExportRelationship{ID: uuid.New(), SourceID: oldE1, TargetID: oldE2, Relation: "knows", Weight: 0.5, SourceMemory: &oldMem, ValidFrom: ts}})

	nd := strings.Join([]string{string(projLine), string(memLine), string(entLine1), string(entLine2), string(relLine)}, "\n") + "\n"

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(nd),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 1 {
		t.Errorf("expected 1 memory imported, got %d", resp.Imported)
	}
	if resp.EntitiesImported != 2 {
		t.Errorf("expected 2 entities imported, got %d", resp.EntitiesImported)
	}
	if resp.RelationshipsImported != 1 {
		t.Errorf("expected 1 relationship imported, got %d", resp.RelationshipsImported)
	}
	if len(relRepo.relationships) != 1 {
		t.Fatalf("expected 1 relationship created, got %d", len(relRepo.relationships))
	}
	rel := relRepo.relationships[0]
	if rel.SourceID != entRepo.entities[0].ID || rel.TargetID != entRepo.entities[1].ID {
		t.Errorf("NDJSON relationship endpoints not remapped correctly")
	}
}

func TestImportNRAMDropsOrphanEdge(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	entRepo := &mockEntityRepo{}
	relRepo := &mockRelationshipRepo{}
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, entRepo, relRepo, &mockLineageRepo{}, nil)

	e1 := uuid.New()
	e2 := uuid.New()
	orphan := uuid.New() // a source memory that is NOT part of the export
	ts := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	exportData := ExportData{
		Version: "1.0",
		Project: ExportProject{ID: projectID},
		Memories: []ExportMemory{
			{ID: uuid.New(), Content: "a memory", Confidence: 0.9, Importance: 0.8, CreatedAt: ts},
		},
		Entities: []ExportEntity{
			{ID: e1, Name: "Alice", Type: "person", Canonical: "alice"},
			{ID: e2, Name: "Bob", Type: "person", Canonical: "bob"},
		},
		Relationships: []ExportRelationship{
			{ID: uuid.New(), SourceID: e1, TargetID: e2, Relation: "knows", Weight: 1, SourceMemory: &orphan, ValidFrom: ts},
		},
	}
	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.EntitiesImported != 2 {
		t.Errorf("expected 2 entities imported, got %d", resp.EntitiesImported)
	}
	if resp.RelationshipsImported != 0 {
		t.Errorf("expected 0 relationships imported (orphan dropped), got %d", resp.RelationshipsImported)
	}
	if len(relRepo.relationships) != 0 {
		t.Errorf("expected no relationships created, got %d", len(relRepo.relationships))
	}
	if resp.Skipped != 1 {
		t.Errorf("expected 1 skipped (orphan edge), got %d", resp.Skipped)
	}
}

func TestImportNRAMRoundTripsLineage(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, namespaceID := newImportTestFixtures()
	lineageRepo := &mockLineageRepo{}
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil, nil, lineageRepo, nil)

	childID := uuid.New()
	parentID := uuid.New()
	ts := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	exportData := ExportData{
		Version: "1.1",
		Project: ExportProject{ID: projectID},
		Memories: []ExportMemory{
			{ID: childID, Content: "synthesis", Confidence: 0.9, Importance: 0.8, CreatedAt: ts},
			{ID: parentID, Content: "source", Confidence: 0.7, Importance: 0.6, CreatedAt: ts},
		},
		// Lineage is a top-level explicit child -> parent edge.
		Lineage: []ExportLineage{
			{MemoryID: childID, ParentID: &parentID, Relation: model.LineageSynthesizedFrom},
		},
	}
	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Imported != 2 {
		t.Fatalf("expected 2 memories imported, got %d", resp.Imported)
	}
	if resp.LineageImported != 1 {
		t.Errorf("expected 1 lineage row imported, got %d", resp.LineageImported)
	}
	if len(lineageRepo.lineages) != 1 {
		t.Fatalf("expected 1 lineage row created, got %d", len(lineageRepo.lineages))
	}

	// memRepo.created order matches the export memory order: [child, parent].
	newChild := memRepo.created[0].ID
	newParent := memRepo.created[1].ID
	lr := lineageRepo.lineages[0]
	if lr.NamespaceID != namespaceID {
		t.Errorf("lineage namespace = %s, want %s", lr.NamespaceID, namespaceID)
	}
	if lr.MemoryID != newChild {
		t.Errorf("lineage child = %s, want remapped %s", lr.MemoryID, newChild)
	}
	if lr.ParentID == nil || *lr.ParentID != newParent {
		t.Errorf("lineage parent = %v, want remapped %s", lr.ParentID, newParent)
	}
	if lr.ParentID != nil && (*lr.ParentID == parentID || lr.MemoryID == childID) {
		t.Errorf("lineage endpoints were not remapped from export ids")
	}
	if lr.Relation != model.LineageSynthesizedFrom {
		t.Errorf("lineage relation = %q, want %q", lr.Relation, model.LineageSynthesizedFrom)
	}
}
