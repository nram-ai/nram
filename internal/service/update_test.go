package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// --- Mock implementations for update tests ---

type mockMemoryUpdater struct {
	memories map[uuid.UUID]*model.Memory
	updated  []*model.Memory
	getErr   error
	updateErr error
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

type mockLineageCreator struct {
	lineages []*model.MemoryLineage
}

func (m *mockLineageCreator) Create(_ context.Context, lineage *model.MemoryLineage) error {
	m.lineages = append(m.lineages, lineage)
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
) (*UpdateService, *mockLineageCreator, *mockTokenUsageRepo, *mockVectorStore) {
	lineage := &mockLineageCreator{}
	tokenUsage := &mockTokenUsageRepo{}
	vectors := &mockVectorStore{}

	// Wrap embedFn so the middleware writes token_usage rows on every
	// Embed call — matches production wiring.
	wrapped := provider.WrapEmbeddingForTest(embedFn, tokenUsage)

	svc := NewUpdateService(memories, projects, lineage, vectors, wrapped)
	return svc, lineage, tokenUsage, vectors
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

	svc, lineage, tokenUsage, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
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

	if resp.Content != "updated content" {
		t.Errorf("expected content 'updated content', got %q", resp.Content)
	}
	if resp.PreviousContent != "original content" {
		t.Errorf("expected previous content 'original content', got %q", resp.PreviousContent)
	}
	if !resp.ReEmbedded {
		t.Error("expected re-embedded=true")
	}

	// Verify vector was upserted.
	if len(vectors.upserted) != 1 {
		t.Fatalf("expected 1 vector upsert, got %d", len(vectors.upserted))
	}

	// Verify lineage record was created.
	if len(lineage.lineages) != 1 {
		t.Fatalf("expected 1 lineage record, got %d", len(lineage.lineages))
	}
	if lineage.lineages[0].Relation != "supersedes" {
		t.Errorf("expected relation 'supersedes', got %q", lineage.lineages[0].Relation)
	}

	// Verify token usage was recorded.
	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_TagsOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newTags := []string{"new-tag1", "new-tag2"}
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

	// No re-embedding artifacts.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(lineage.lineages) != 0 {
		t.Errorf("expected 0 lineage records, got %d", len(lineage.lineages))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_MetadataOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newMeta := json.RawMessage(`{"new":"metadata"}`)
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Metadata:  &newMeta,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ReEmbedded {
		t.Error("expected re-embedded=false for metadata-only update")
	}

	// Verify the memory was updated with new metadata.
	updated := memories.updated[0]
	if string(updated.Metadata) != `{"new":"metadata"}` {
		t.Errorf("expected metadata '{\"new\":\"metadata\"}', got %s", string(updated.Metadata))
	}

	// No re-embedding artifacts.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(lineage.lineages) != 0 {
		t.Errorf("expected 0 lineage records, got %d", len(lineage.lineages))
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

	svc, lineage, _, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
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
	if len(lineage.lineages) != 1 {
		t.Errorf("expected 1 lineage record, got %d", len(lineage.lineages))
	}
}

func TestUpdate_NoEmbeddingProvider(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, tokenUsage, vectors := newUpdateService(memories, projects, nil)

	newContent := "changed without embedding"
	resp, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Content != "changed without embedding" {
		t.Errorf("expected updated content, got %q", resp.Content)
	}
	if resp.ReEmbedded {
		t.Error("expected re-embedded=false without embedding provider")
	}

	// Content changed, so lineage should still be created.
	if len(lineage.lineages) != 1 {
		t.Fatalf("expected 1 lineage record, got %d", len(lineage.lineages))
	}

	// No vectors or token usage.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestUpdate_LineageOnContentChange(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, _, _ := newUpdateService(memories, projects, nil)

	newContent := "lineage test content"
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(lineage.lineages) != 1 {
		t.Fatalf("expected 1 lineage record, got %d", len(lineage.lineages))
	}

	lr := lineage.lineages[0]
	if lr.MemoryID != memID {
		t.Errorf("expected memory ID %s, got %s", memID, lr.MemoryID)
	}
	if lr.ParentID == nil || *lr.ParentID != memID {
		t.Errorf("expected parent ID %s, got %v", memID, lr.ParentID)
	}
	if lr.Relation != "supersedes" {
		t.Errorf("expected relation 'supersedes', got %q", lr.Relation)
	}
	if lr.Context == nil {
		t.Error("expected non-nil context")
	}
}

func TestUpdate_NoLineageOnTagsOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, _, _ := newUpdateService(memories, projects, nil)

	newTags := []string{"just-tags"}
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Tags:      &newTags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(lineage.lineages) != 0 {
		t.Errorf("expected 0 lineage records for tags-only update, got %d", len(lineage.lineages))
	}
}

func TestUpdate_NoLineageOnMetadataOnly(t *testing.T) {
	projectID, _, memID, projects, memories := setupUpdateFixtures()
	svc, lineage, _, _ := newUpdateService(memories, projects, nil)

	newMeta := json.RawMessage(`{"only":"meta"}`)
	_, err := svc.Update(context.Background(), &UpdateRequest{
		ProjectID: projectID,
		MemoryID:  memID,
		Metadata:  &newMeta,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(lineage.lineages) != 0 {
		t.Errorf("expected 0 lineage records for metadata-only update, got %d", len(lineage.lineages))
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
	if tu.MemoryID == nil || *tu.MemoryID != memID {
		t.Errorf("expected memory ID %s, got %v", memID, tu.MemoryID)
	}
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

	svc, lineage, tokenUsage, vectors := newUpdateService(memories, projects, func() provider.EmbeddingProvider {
		return embProvider
	})

	// Set content to the same value — should not trigger re-embed.
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

	if resp.ReEmbedded {
		t.Error("expected re-embedded=false when content unchanged")
	}
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}
	if len(lineage.lineages) != 0 {
		t.Errorf("expected 0 lineage records, got %d", len(lineage.lineages))
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
	if !resp.ReEmbedded {
		t.Errorf("ReEmbedded should be true (we did call the embedder); got false")
	}

	// The persisted memory must NOT advertise an embedding_dim, since
	// the vector write failed.
	if len(memories.updated) != 1 {
		t.Fatalf("expected 1 memory Update; got %d", len(memories.updated))
	}
	persisted := memories.updated[0]
	if persisted.EmbeddingDim != nil {
		t.Errorf("EmbeddingDim must be cleared when vector Upsert failed; got %v", *persisted.EmbeddingDim)
	}
	if len(vectors.upserted) != 0 {
		t.Errorf("upsert call should have failed; got %d successful upserts", len(vectors.upserted))
	}
}
