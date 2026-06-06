package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// shareIsolationFixture builds a recall scenario where a PRIMARY-namespace seed
// entity is connected by TWO traversed edges: one legitimate in-primary edge,
// and one CROSS-NAMESPACE edge that lives in the global tier but references the
// primary seed entity (the exact shape that would leak reserved-tier structure
// if the graph output were not aperture-bounded). The traverser returns both for
// the seed, mimicking ListByEntity (which has no namespace filter).
func shareIsolationFixture(t *testing.T) (
	svc *RecallService, primaryID uuid.UUID, globalNs uuid.UUID,
	primaryRelID, foreignRelID, foreignEntityID uuid.UUID,
) {
	t.Helper()
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()
	now := time.Now()

	seedEntityID := uuid.New()
	primaryMemID := uuid.New()
	foreignMemID := uuid.New()
	primaryRelID = uuid.New()
	foreignRelID = uuid.New()
	foreignEntityID = uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			primaryMemID: makeTestMemory(primaryMemID, primaryNs, "primary memory", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: primaryMemID, Score: 0.50, NamespaceID: primaryNs},
		},
	}
	// Seed entity lives in the primary namespace and is found lexically.
	entityReader := &mockEntityReader{
		entities: []model.Entity{{ID: seedEntityID, NamespaceID: primaryNs, Name: "Seed", EntityType: "concept"}},
	}
	traverser := &mockRelTraverser{
		rels: []model.Relationship{
			{ // legitimate in-aperture (primary) edge
				ID: primaryRelID, NamespaceID: primaryNs,
				SourceID: seedEntityID, TargetID: uuid.New(),
				Relation: "rel", Weight: 1.0, SourceMemory: &primaryMemID,
				CreatedAt: now, ValidFrom: now,
			},
			{ // CROSS-NAMESPACE foreign edge referencing the primary seed entity
				ID: foreignRelID, NamespaceID: globalNs,
				SourceID: seedEntityID, TargetID: foreignEntityID,
				Relation: "secret", Weight: 1.0, SourceMemory: &foreignMemID,
				CreatedAt: now, ValidFrom: now,
			},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{384},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 384)}, Model: "m"},
	}
	svc, _ = newRecallService(memReader, projects, namespaces, vectorSearcher, entityReader, traverser, func() provider.EmbeddingProvider { return embProvider })
	repo := newMockSettingsRepo()
	settings := NewSettingsService(repo)
	svc.SetSettings(settings)
	settings.InvalidateAllCache()
	return svc, primaryID, globalNs, primaryRelID, foreignRelID, foreignEntityID
}

func hasRel(resp *RecallResponse, id uuid.UUID) bool {
	for _, r := range resp.Graph.Relationships {
		if r.ID == id {
			return true
		}
	}
	return false
}

func hasEntity(resp *RecallResponse, id uuid.UUID) bool {
	for _, e := range resp.Graph.Entities {
		if e.ID == id {
			return true
		}
	}
	return false
}

// TestRecall_ShareBearerAperture_SuppressesForeignGraphEdges is the security
// regression: a share-bearer scoped to one project (aperture = [primary], no
// Global/AboutMe namespace) must never see foreign-namespace relationships or
// entities in the recall graph, even when a cross-namespace edge exists.
func TestRecall_ShareBearerAperture_SuppressesForeignGraphEdges(t *testing.T) {
	svc, primaryID, _, primaryRelID, foreignRelID, foreignEntityID := shareIsolationFixture(t)

	// Share-bearer aperture: primary only (GlobalNamespaceID/AboutMeNamespaceID nil).
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:    primaryID,
		Query:        "Seed",
		Limit:        10,
		IncludeGraph: true,
		GraphDepth:   2,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}

	if hasRel(resp, foreignRelID) {
		t.Error("LEAK: foreign-namespace relationship surfaced to a share-bearer aperture")
	}
	if hasEntity(resp, foreignEntityID) {
		t.Error("LEAK: foreign-namespace entity surfaced to a share-bearer aperture")
	}
	if !hasRel(resp, primaryRelID) {
		t.Error("the legitimate in-aperture relationship must still be present")
	}
}

// TestRecall_OwnerAperture_IncludesInApertureForeignEdges is the non-regression
// counter-test: an owner whose aperture includes the global tier DOES see the
// global-namespace edge (cross-tier graph still works). Same fixture, but with
// GlobalNamespaceID set so global is inside the aperture.
func TestRecall_OwnerAperture_IncludesInApertureForeignEdges(t *testing.T) {
	svc, primaryID, globalNs, _, foreignRelID, _ := shareIsolationFixture(t)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "Seed",
		Limit:             10,
		IncludeGraph:      true,
		GraphDepth:        2,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if !hasRel(resp, foreignRelID) {
		t.Error("owner aperture includes global: the global-tier edge should be present")
	}
}
