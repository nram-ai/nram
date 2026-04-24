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

func (m *mockMemoryRepoForImport) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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

	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo)

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
