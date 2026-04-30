package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Mock implementations for update tests ---

type mockMemoryUpdater struct {
	memories         map[uuid.UUID]*model.Memory
	updated          []*model.Memory
	supersedes       []supersedeCall
	getErr           error
	updateErr        error
	supersedeErr     error
}

type supersedeCall struct {
	oldID   uuid.UUID
	newMem  *model.Memory
	lineage *model.MemoryLineage
}

func (m *mockMemoryUpdater) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	// Return a copy to avoid test aliasing issues.
	cp := *mem
	return &cp, nil
}

func (m *mockMemoryUpdater) Update(_ context.Context, mem *model.Memory) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.updated = append(m.updated, mem)
	m.memories[mem.ID] = mem
	return nil
}

func (m *mockMemoryUpdater) SupersedeReplacing(_ context.Context, oldID uuid.UUID, newMem *model.Memory, lineage *model.MemoryLineage) error {
	if m.supersedeErr != nil {
		return m.supersedeErr
	}
	if newMem.ID == uuid.Nil {
		newMem.ID = uuid.New()
	}
	if lineage.ID == uuid.Nil {
		lineage.ID = uuid.New()
	}
	if lineage.MemoryID == uuid.Nil {
		lineage.MemoryID = newMem.ID
	}
	now := time.Now().UTC()
	old, ok := m.memories[oldID]
	if !ok {
		return fmt.Errorf("supersede: old memory %s not found", oldID)
	}
	if old.SupersededBy != nil {
		return fmt.Errorf("supersede: old memory %s already superseded", oldID)
	}
	old.SupersededBy = &newMem.ID
	old.SupersededAt = &now
	old.UpdatedAt = now
	m.memories[oldID] = old

	cp := *newMem
	m.memories[newMem.ID] = &cp
	m.supersedes = append(m.supersedes, supersedeCall{
		oldID:   oldID,
		newMem:  &cp,
		lineage: lineage,
	})
	return nil
}


// --- Test helpers ---

func setupUpdateFixtures() (uuid.UUID, uuid.UUID, uuid.UUID, *mockProjectRepo, *mockMemoryUpdater) {
	projectID := uuid.New()
	nsID := uuid.New()
	memID := uuid.New()

	projects := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Test Project",
				Slug:        "test-project",
			},
		},
	}

	memories := &mockMemoryUpdater{
		memories: map[uuid.UUID]*model.Memory{
			memID: {
				ID:          memID,
				NamespaceID: nsID,
				Content:     "original content",
				Tags:        []string{"old-tag"},
				Metadata:    json.RawMessage(`{"old":"data"}`),
				Confidence:  1.0,
				Importance:  0.5,
				CreatedAt:   time.Now().Add(-time.Hour),
				UpdatedAt:   time.Now().Add(-time.Hour),
			},
		},
	}

	return projectID, nsID, memID, projects, memories
}

func newUpdateService(
	memories *mockMemoryUpdater,
	projects *mockProjectRepo,
	embedFn func() provider.EmbeddingProvider,
) (*UpdateService, *mockEnrichmentQueueRepo, *mockTokenUsageRepo, *mockVectorStore) {
	enrichmentQueue := &mockEnrichmentQueueRepo{}
	tokenUsage := &mockTokenUsageRepo{}
	vectors := &mockVectorStore{}

	// Wrap embedFn so the middleware writes token_usage rows on every
	// Embed call — matches production wiring.
	wrapped := provider.WrapEmbeddingForTest(embedFn, tokenUsage)

	svc := NewUpdateService(memories, projects, vectors, wrapped, enrichmentQueue)
	return svc, enrichmentQueue, tokenUsage, vectors
}

// --- Tests ---

func TestUpdate_ContentOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 10, TotalTokens: 10},
		},
	}

	svc, enrichment, tokenUsage, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})

	newContent := "updated content"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID == memID {
		t.Errorf("content change must return a new memory ID; got original %s", memID)
	}
	if resp.PreviousMemoryID != memID {
		t.Errorf("PreviousMemoryID = %s, want %s", resp.PreviousMemoryID, memID)
	}
	if !resp.Superseded {
		t.Error("expected Superseded=true on content change")
	}
	if resp.Content != "updated content" {
		t.Errorf("expected content 'updated content', got %q", resp.Content)
	}
	if resp.PreviousContent != "original content" {
		t.Errorf("expected previous content 'original content', got %q", resp.PreviousContent)
	}
	if !resp.ReEmbedded {
		t.Error("expected re-embedded=true")
	}

	// Vector upsert lands at the NEW ID.
	if len(vectors.upserted) != 1 {
		t.Fatalf("expected 1 vector upsert, got %d", len(vectors.upserted))
	}
	if vectors.upserted[0].ID != resp.ID {
		t.Errorf("vector upsert id = %s, want resp.ID %s", vectors.upserted[0].ID, resp.ID)
	}

	// Supersede transaction recorded with old=memID -> new=resp.ID and a
	// supersedes lineage edge.
	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	sc := memories.supersedes[0]
	if sc.oldID != memID {
		t.Errorf("supersede old id = %s, want %s", sc.oldID, memID)
	}
	if sc.newMem.ID != resp.ID {
		t.Errorf("supersede new id = %s, want %s", sc.newMem.ID, resp.ID)
	}
	if sc.lineage.Relation != "supersedes" {
		t.Errorf("supersede lineage relation = %q, want supersedes", sc.lineage.Relation)
	}
	if sc.lineage.ParentID == nil || *sc.lineage.ParentID != memID {
		t.Errorf("supersede lineage parent = %v, want %s", sc.lineage.ParentID, memID)
	}

	// Fresh enrichment job enqueued for the new ID.
	if len(enrichment.jobs) != 1 {
		t.Fatalf("expected 1 enrichment job, got %d", len(enrichment.jobs))
	}
	if enrichment.jobs[0].MemoryID != resp.ID {
		t.Errorf("enrichment.MemoryID = %s, want resp.ID %s", enrichment.jobs[0].MemoryID, resp.ID)
	}

	// Verify token usage was recorded.
	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_TagsOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, enrichment, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newTags := []string{"new-tag1", "new-tag2"}
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID != memID {
		t.Errorf("tags-only update must keep input ID; got %s, want %s", resp.ID, memID)
	}
	if resp.Superseded {
		t.Error("expected Superseded=false for tags-only update")
	}
	if len(resp.Tags) != 2 || resp.Tags[0] != "new-tag1" || resp.Tags[1] != "new-tag2" {
		t.Errorf("expected tags [new-tag1 new-tag2], got %v", resp.Tags)
	}
	if resp.ReEmbedded {
		t.Error("expected re-embedded=false for tags-only update")
	}
	if resp.Content != "original content" {
		t.Errorf("expected content unchanged, got %q", resp.Content)
	}

	// No re-embedding, no supersede, no enrichment.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(memories.supersedes) != 0 {
		t.Errorf("expected 0 supersede calls, got %d", len(memories.supersedes))
	}
	if len(enrichment.jobs) != 0 {
		t.Errorf("expected 0 enrichment jobs, got %d", len(enrichment.jobs))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_MetadataOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, enrichment, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newMeta := json.RawMessage(`{"new":"metadata"}`)
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Metadata:  &newMeta,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID != memID {
		t.Errorf("metadata-only update must keep input ID; got %s, want %s", resp.ID, memID)
	}
	if resp.Superseded {
		t.Error("expected Superseded=false for metadata-only update")
	}
	if resp.ReEmbedded {
		t.Error("expected re-embedded=false for metadata-only update")
	}

	// In-place mutation: 1 row updated, metadata replaced.
	if len(memories.updated) != 1 {
		t.Fatalf("expected 1 in-place Update, got %d", len(memories.updated))
	}
	updated := memories.updated[0]
	if string(updated.Metadata) != `{"new":"metadata"}` {
		t.Errorf("expected metadata '{\"new\":\"metadata\"}', got %s", string(updated.Metadata))
	}

	// No re-embedding, no supersede, no enrichment.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(memories.supersedes) != 0 {
		t.Errorf("expected 0 supersede calls, got %d", len(memories.supersedes))
	}
	if len(enrichment.jobs) != 0 {
		t.Errorf("expected 0 enrichment jobs, got %d", len(enrichment.jobs))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_ContentAndTags(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{256},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 256)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 8, TotalTokens: 8},
		},
	}

	svc, _, _, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})

	newContent := "new content"
	newTags := []string{"combined-tag"}
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Superseded {
		t.Error("expected Superseded=true on content change")
	}
	if resp.ID == memID {
		t.Error("expected new ID for content change")
	}
	if resp.Content != "new content" {
		t.Errorf("expected content 'new content', got %q", resp.Content)
	}
	if len(resp.Tags) != 1 || resp.Tags[0] != "combined-tag" {
		t.Errorf("expected tags [combined-tag], got %v", resp.Tags)
	}
	if !resp.ReEmbedded {
		t.Error("expected re-embedded=true")
	}
	if len(vectors.upserted) != 1 {
		t.Errorf("expected 1 vector upsert, got %d", len(vectors.upserted))
	}
	if len(memories.supersedes) != 1 {
		t.Errorf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	if len(memories.supersedes) > 0 && memories.supersedes[0].newMem.Tags[0] != "combined-tag" {
		t.Errorf("expected new memory to carry the new tag, got %v", memories.supersedes[0].newMem.Tags)
	}
}

func TestUpdate_NoEmbeddingProvider(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, enrichment, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newContent := "changed without embedding"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Superseded {
		t.Error("expected Superseded=true on content change")
	}
	if resp.Content != "changed without embedding" {
		t.Errorf("expected updated content, got %q", resp.Content)
	}
	if resp.ReEmbedded {
		t.Error("expected re-embedded=false without embedding provider")
	}

	// Content changed → supersede chain link still recorded.
	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	// Enrichment job still queued so the dream cycle picks up the embed.
	if len(enrichment.jobs) != 1 {
		t.Fatalf("expected 1 enrichment job, got %d", len(enrichment.jobs))
	}

	// No vectors or token usage (no embed provider).
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_SupersedeOnContentChange(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "supersede test content"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	sc := memories.supersedes[0]
	if sc.oldID != memID {
		t.Errorf("supersede oldID = %s, want %s", sc.oldID, memID)
	}
	if sc.newMem.ID != resp.ID {
		t.Errorf("supersede newMem.ID = %s, want resp.ID %s", sc.newMem.ID, resp.ID)
	}
	if sc.lineage.MemoryID != resp.ID {
		t.Errorf("lineage.MemoryID = %s, want resp.ID %s", sc.lineage.MemoryID, resp.ID)
	}
	if sc.lineage.ParentID == nil || *sc.lineage.ParentID != memID {
		t.Errorf("lineage.ParentID = %v, want %s", sc.lineage.ParentID, memID)
	}
	if sc.lineage.Relation != "supersedes" {
		t.Errorf("lineage.Relation = %q, want supersedes", sc.lineage.Relation)
	}
	if sc.lineage.Context == nil {
		t.Error("expected non-nil lineage context (carries previous_content)")
	}
}

func TestUpdate_NoSupersedeOnTagsOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newTags := []string{"just-tags"}
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.supersedes) != 0 {
		t.Errorf("expected 0 supersede calls for tags-only update, got %d", len(memories.supersedes))
	}
}

func TestUpdate_NoSupersedeOnMetadataOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newMeta := json.RawMessage(`{"only":"meta"}`)
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Metadata:  &newMeta,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.supersedes) != 0 {
		t.Errorf("expected 0 supersede calls for metadata-only update, got %d", len(memories.supersedes))
	}
}

func TestUpdate_TokenUsageRecordedOnReEmbed(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	userID := uuid.New()
	orgID := uuid.New()
	apiKeyID := uuid.New()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{128},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 128)},
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 12, CompletionTokens: 0, TotalTokens: 12},
		},
	}

	svc, _, tokenUsage, _ := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})

	newContent := "re-embed token tracking"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
		UserID:    &userID,
		OrgID:     &orgID,
		APIKeyID:  &apiKeyID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}

	tu := tokenUsage.usages[0]
	if tu.Operation != "embedding" {
		t.Errorf("expected operation 'embedding', got %q", tu.Operation)
	}
	if tu.Provider != "test-provider" {
		t.Errorf("expected provider 'test-provider', got %q", tu.Provider)
	}
	if tu.Model != "embed-model" {
		t.Errorf("expected model 'embed-model', got %q", tu.Model)
	}
	if tu.TokensInput != 12 {
		t.Errorf("expected 12 input tokens, got %d", tu.TokensInput)
	}
	if *tu.UserID != userID {
		t.Errorf("expected user ID %s, got %s", userID, *tu.UserID)
	}
	if *tu.OrgID != orgID {
		t.Errorf("expected org ID %s, got %s", orgID, *tu.OrgID)
	}
	if *tu.APIKeyID != apiKeyID {
		t.Errorf("expected API key ID %s, got %s", apiKeyID, *tu.APIKeyID)
	}
	// Token usage attributes the embed call to the NEW memory ID, since
	// the supersede path embeds the new content under newID.
	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call to read newID from, got %d", len(memories.supersedes))
	}
	newID := memories.supersedes[0].newMem.ID
	if tu.MemoryID == nil || *tu.MemoryID != newID {
		t.Errorf("expected memory ID %s (new), got %v", newID, tu.MemoryID)
	}
	_ = memID
}

func TestUpdate_MemoryNotFound(t *testing.T) {
	projectID, _, _, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "does not matter"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  uuid.New(), // non-existent
		Content:   &newContent,
	})
	if err == nil {
		t.Error("expected error for non-existent memory")
	}
}

func TestUpdate_ProjectNotFound(t *testing.T) {
	_, _, memID, _, memories := setupUpdateFixtures()
	// Empty project repo.
	emptyProjects := &mockProjectRepo{projects: map[uuid.UUID]*model.Project{}}
	svc, _, _, _ := newUpdateService(memories, emptyProjects, nil)

	newContent := "does not matter"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: uuid.New(), // non-existent
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err == nil {
		t.Error("expected error for non-existent project")
	}
}

func TestUpdate_NothingToUpdate(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		// All update fields nil.
	})
	if err == nil {
		t.Error("expected error when nothing to update")
	}
}

func TestUpdate_MemoryWrongNamespace(t *testing.T) {
	projectID, _, _, projects, _ := setupUpdateFixtures()

	// Create a memory in a different namespace.
	memID := uuid.New()
	differentNS := uuid.New()
	memories := &mockMemoryUpdater{
		memories: map[uuid.UUID]*model.Memory{
			memID: {
				ID:          memID,
				NamespaceID: differentNS, // does not match project namespace
				Content:     "wrong namespace",
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			},
		},
	}

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "should fail"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err == nil {
		t.Error("expected error for memory in wrong namespace")
	}
}

func TestUpdate_LatencyTracked(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newTags := []string{"latency-test"}
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestUpdate_PreviousContentReturned(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "brand new content"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.PreviousContent != "original content" {
		t.Errorf("expected previous content 'original content', got %q", resp.PreviousContent)
	}
	if resp.Content != "brand new content" {
		t.Errorf("expected content 'brand new content', got %q", resp.Content)
	}
}

func TestUpdate_SameContentNoReEmbed(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 10, TotalTokens: 10},
		},
	}

	svc, _, tokenUsage, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})

	// Set content to the same value — should not trigger re-embed
	// nor a supersede chain link; tags-only path runs in place.
	sameContent := "original content"
	newTags := []string{"new-tag"}
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &sameContent,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID != memID {
		t.Errorf("identical-content update must keep input ID; got %s, want %s", resp.ID, memID)
	}
	if resp.Superseded {
		t.Error("expected Superseded=false when content unchanged")
	}
	if resp.ReEmbedded {
		t.Error("expected re-embedded=false when content unchanged")
	}
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(memories.supersedes) != 0 {
		t.Errorf("expected 0 supersede calls, got %d", len(memories.supersedes))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_RejectsSupersededMemory(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	winnerID := uuid.New()
	memories.memories[memID].SupersededBy = &winnerID

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "trying to edit a loser"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err == nil {
		t.Fatal("expected error when updating superseded memory")
	}
	msg := err.Error()
	if !strings.Contains(msg, "superseded by") {
		t.Errorf("expected error to mention 'superseded by'; got %q", msg)
	}
	if !strings.Contains(msg, winnerID.String()) {
		t.Errorf("expected error to surface winner ID %s; got %q", winnerID, msg)
	}
}

// TestUpdate_VectorUpsertFailure_ClearsEmbeddingDim verifies that when
// the vector store rejects an Upsert during a content update, the memory
// row is persisted WITHOUT an embedding_dim rather than carrying a stale
// dim that has no matching vector. The embedding-backfill phase is the
// owner of repair on the next dream cycle.
func TestUpdate_VectorUpsertFailure_ClearsEmbeddingDim(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 10, TotalTokens: 10},
		},
	}

	svc, _, _, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})
	vectors.upsertErr = fmt.Errorf("vector store offline")

	newContent := "edit that triggers re-embed"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("Update should succeed even when vector Upsert fails; got err=%v", err)
	}
	if resp.ReEmbedded {
		t.Errorf("ReEmbedded should be false when vector Upsert failed; the row claims no vector")
	}
	if !resp.Superseded {
		t.Error("supersede chain link must still form when only the vector Upsert fails")
	}

	// Supersede transaction lands first (creates new row in mock), then
	// the vector upsert at the new ID fails, then the service patches the
	// new row via Update to clear embedding_dim.
	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call; got %d", len(memories.supersedes))
	}
	if len(memories.updated) != 1 {
		t.Fatalf("expected 1 follow-up Update to clear embedding_dim; got %d", len(memories.updated))
	}
	persisted := memories.updated[0]
	if persisted.ID != resp.ID {
		t.Errorf("follow-up Update should target the new ID %s; got %s", resp.ID, persisted.ID)
	}
	if persisted.EmbeddingDim != nil {
		t.Errorf("EmbeddingDim must be cleared when vector Upsert failed; got %v", *persisted.EmbeddingDim)
	}
	if len(vectors.upserted) != 0 {
		t.Errorf("upsert call should have failed; got %d successful upserts", len(vectors.upserted))
	}
}

// TestUpdate_ContentChange_OldRowFrozen verifies that the supersede path
// leaves the old memory row's enrichment state intact. Reinforcement
// signal accumulated on the old trace (Enriched=true, AccessCount, the
// confidence the row had when the user edited content) must survive so
// that recall against the old version (via include_superseded) returns
// faithful state, and so the existing entities/relationships keyed by
// source_memory remain valid.
func TestUpdate_ContentChange_OldRowFrozen(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	// Stamp meaningful state on the old row before the update so the
	// "frozen" assertion has something to defend.
	old := memories.memories[memID]
	old.Enriched = true
	old.AccessCount = 7
	old.Confidence = 0.42
	now := time.Now().Add(-30 * time.Minute)
	old.LastAccessed = &now
	memories.memories[memID] = old

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "new content that will create a chain link"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ID == memID {
		t.Fatal("content change must produce a new ID")
	}

	frozen := memories.memories[memID]
	if !frozen.Enriched {
		t.Error("old row Enriched should stay true (frozen with original enrichment)")
	}
	if frozen.AccessCount != 7 {
		t.Errorf("old row AccessCount = %d, want 7 (frozen)", frozen.AccessCount)
	}
	if frozen.Confidence != 0.42 {
		t.Errorf("old row Confidence = %f, want 0.42 (frozen)", frozen.Confidence)
	}
	if frozen.LastAccessed == nil {
		t.Error("old row LastAccessed should stay set (frozen)")
	}
	if frozen.Content != "original content" {
		t.Errorf("old row Content = %q, want unchanged", frozen.Content)
	}
	// Old row IS marked superseded (that's the whole point) — but the
	// "frozen" semantic is: enrichment-derived state stays put.
	if frozen.SupersededBy == nil || *frozen.SupersededBy != resp.ID {
		t.Errorf("old row SupersededBy = %v, want %s", frozen.SupersededBy, resp.ID)
	}
}

// TestUpdate_ContentChange_TagsInherited verifies that when the request
// has Tags=nil the new memory row carries the old tags forward.
func TestUpdate_ContentChange_TagsInherited(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	// Set distinctive tags on the old row.
	old := memories.memories[memID]
	old.Tags = []string{"alpha", "beta"}
	memories.memories[memID] = old

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "fresh content; tags should inherit"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
		// Tags deliberately omitted.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	got := memories.supersedes[0].newMem.Tags
	if len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Errorf("new memory tags = %v, want [alpha beta] (inherited)", got)
	}
	if !reflect.DeepEqual(resp.Tags, []string{"alpha", "beta"}) {
		t.Errorf("response tags = %v, want [alpha beta]", resp.Tags)
	}
}

// TestUpdate_ContentChange_StateFields: access metrics reset on the new
// row (AccessCount=0, LastAccessed=nil, Confidence=1.0, Enriched=false),
// policy fields inherit from the old row (Importance, ExpiresAt,
// PurgeAfter, Source).
func TestUpdate_ContentChange_StateFields(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()

	// Seed the old row with non-default state across both buckets.
	src := "audit-source"
	expires := time.Now().Add(48 * time.Hour)
	purge := time.Now().Add(96 * time.Hour)
	now := time.Now().Add(-15 * time.Minute)
	old := memories.memories[memID]
	old.Source = &src
	old.Importance = 0.91
	old.ExpiresAt = &expires
	old.PurgeAfter = &purge
	old.AccessCount = 12
	old.LastAccessed = &now
	old.Confidence = 0.6
	old.Enriched = true
	memories.memories[memID] = old

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "state-field test content"
	if _, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.supersedes) != 1 {
		t.Fatalf("expected 1 supersede call, got %d", len(memories.supersedes))
	}
	newMem := memories.supersedes[0].newMem

	// Reset (fresh-trace) fields:
	if newMem.AccessCount != 0 {
		t.Errorf("new.AccessCount = %d, want 0 (reset)", newMem.AccessCount)
	}
	if newMem.LastAccessed != nil {
		t.Errorf("new.LastAccessed = %v, want nil (reset)", newMem.LastAccessed)
	}
	if newMem.Confidence != 1.0 {
		t.Errorf("new.Confidence = %f, want 1.0 (reset)", newMem.Confidence)
	}
	if newMem.Enriched {
		t.Error("new.Enriched should be false (fresh enrichment needed)")
	}

	// Inherited (caller-intent) fields:
	if newMem.Source == nil || *newMem.Source != "audit-source" {
		t.Errorf("new.Source = %v, want inherited 'audit-source'", newMem.Source)
	}
	if newMem.Importance != 0.91 {
		t.Errorf("new.Importance = %f, want inherited 0.91", newMem.Importance)
	}
	if newMem.ExpiresAt == nil || !newMem.ExpiresAt.Equal(expires) {
		t.Errorf("new.ExpiresAt = %v, want inherited %v", newMem.ExpiresAt, expires)
	}
	if newMem.PurgeAfter == nil || !newMem.PurgeAfter.Equal(purge) {
		t.Errorf("new.PurgeAfter = %v, want inherited %v", newMem.PurgeAfter, purge)
	}
}

// TestUpdate_ConcurrentSupersede_PropagatesStorageError ensures the
// service surfaces storage.ErrConcurrentSupersede so callers can react
// (refresh the active head, retry against it). Today the service wraps
// the error in "failed to supersede memory: ..."; this test pins that
// behavior so future refactors don't silently swallow the signal.
func TestUpdate_ConcurrentSupersede_PropagatesStorageError(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	memories.supersedeErr = storage.ErrConcurrentSupersede

	svc, _, _, _ := newUpdateService(memories, projects, nil)

	newContent := "edit losing the race"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err == nil {
		t.Fatal("expected error when SupersedeReplacing returns ErrConcurrentSupersede")
	}
	if !strings.Contains(err.Error(), "supersede memory") {
		t.Errorf("error %q should mention supersede so callers can branch", err.Error())
	}
}
