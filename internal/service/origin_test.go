package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestStore_AssignsUserOrigin pins that the user-facing store path stamps
// Origin=OriginUser server-side regardless of any client-supplied source.
func TestStore_AssignsUserOrigin(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _ := newTestService(projects, namespaces)

	if _, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "a user memory",
		Source:    "api",
	}); err != nil {
		t.Fatalf("store: %v", err)
	}

	if len(memories.created) != 1 {
		t.Fatalf("expected 1 memory created, got %d", len(memories.created))
	}
	if got := memories.created[0].Origin; got != model.OriginUser {
		t.Errorf("expected origin %q, got %q", model.OriginUser, got)
	}
}

// TestStore_RejectsReservedDreamSource confirms the user store path refuses the
// reserved "dream" source (case/space-insensitive) and writes nothing, so the
// string can never re-enter the source column and resurrect the discriminator
// footgun.
func TestStore_RejectsReservedDreamSource(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _ := newTestService(projects, namespaces)

	for _, src := range []string{"dream", "Dream", "  DREAM  "} {
		_, err := svc.Store(context.Background(), &StoreRequest{
			ProjectID: projectID,
			Content:   "trying to forge a dream",
			Source:    src,
		})
		if err == nil {
			t.Errorf("source %q: expected rejection, got nil error", src)
		}
	}
	if len(memories.created) != 0 {
		t.Fatalf("expected no memories created, got %d", len(memories.created))
	}
}

// TestBatchStore_RejectsReservedDreamSourcePerItem confirms a reserved-source
// item fails in isolation (a per-item error) while its siblings still store
// with Origin=OriginUser.
func TestBatchStore_RejectsReservedDreamSourcePerItem(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _ := newBatchTestService(projects, namespaces)

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "good one", Source: "api"},
			{Content: "forged dream", Source: "dream"},
			{Content: "good two", Source: "cli"},
		},
	})
	if err != nil {
		t.Fatalf("batch store: %v", err)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected 2 created, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 1 || resp.Errors[0].Index != 1 {
		t.Errorf("expected one error at index 1, got %+v", resp.Errors)
	}
	for _, m := range memories.created {
		if m.Origin != model.OriginUser {
			t.Errorf("expected origin %q, got %q", model.OriginUser, m.Origin)
		}
		if m.Source != nil && strings.EqualFold(*m.Source, model.DreamSource) {
			t.Errorf("reserved source leaked into a stored memory: %q", *m.Source)
		}
	}
}

// TestImport_AssignsImportOriginAndSanitizesDreamSource confirms imported
// memories are stamped Origin=OriginImport and that a legacy "dream" source
// carried by export data is dropped rather than re-entering the source column.
func TestImport_AssignsImportOriginAndSanitizesDreamSource(t *testing.T) {
	memRepo, projRepo, nsRepo, ingRepo, projectID, _ := newImportTestFixtures()
	svc := NewImportService(memRepo, projRepo, nsRepo, ingRepo, nil)

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	normalSrc := "linkedin"
	dreamSrc := model.DreamSource
	exportData := ExportData{
		Version:    "1.0",
		ExportedAt: time.Now(),
		Project:    ExportProject{ID: projectID, Name: "Test", Slug: "test"},
		Memories: []ExportMemory{
			{ID: uuid.New(), Content: "kept source", Source: &normalSrc, CreatedAt: ts},
			{ID: uuid.New(), Content: "forged dream", Source: &dreamSrc, CreatedAt: ts},
		},
		Stats: ExportStats{MemoryCount: 2},
	}
	data, _ := json.Marshal(exportData)

	resp, err := svc.Import(context.Background(), &ImportRequest{
		ProjectID: projectID,
		Format:    ImportFormatNRAM,
		Data:      strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if resp.Imported != 2 {
		t.Fatalf("expected 2 imported, got %d", resp.Imported)
	}
	if len(memRepo.created) != 2 {
		t.Fatalf("expected 2 created, got %d", len(memRepo.created))
	}
	for _, m := range memRepo.created {
		if m.Origin != model.OriginImport {
			t.Errorf("expected origin %q, got %q", model.OriginImport, m.Origin)
		}
		if m.Source != nil && strings.EqualFold(*m.Source, model.DreamSource) {
			t.Errorf("reserved dream source survived import: %q", *m.Source)
		}
	}
}
