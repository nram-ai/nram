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

// TestRecall_AboutMeAperture_AttributesPersonaNamespace verifies that setting
// RecallRequest.AboutMeNamespaceID brings the about_me namespace into the recall
// aperture and that memories from it are attributed to the about_me project
// (not mis-stamped with the primary project's slug). When the field is unset,
// the same memory falls back to the primary attribution — proving the wiring,
// not the mock, drives the behavior.
func TestRecall_AboutMeAperture_AttributesPersonaNamespace(t *testing.T) {
	primaryID, primaryNs, _, _, projects, namespaces := setupPrimaryGlobalFixtures()

	aboutID := uuid.New()
	aboutNs := uuid.New()
	projects.projects[aboutID] = &model.Project{ID: aboutID, NamespaceID: aboutNs, Name: "about_me", Slug: model.ReservedProjectSlugAboutMe}
	namespaces.namespaces[aboutNs] = &model.Namespace{ID: aboutNs, Slug: model.ReservedProjectSlugAboutMe, Kind: "project", Path: "about_me"}

	projectMemID := uuid.New()
	aboutMemID := uuid.New()
	now := time.Now()

	newSvc := func() *RecallService {
		memReader := &mockMemoryReader{
			memories: map[uuid.UUID]*model.Memory{
				projectMemID: makeTestMemory(projectMemID, primaryNs, "project content", nil, 0.5, 0, now),
				aboutMemID:   makeTestMemory(aboutMemID, aboutNs, "about content", nil, 0.5, 0, now),
			},
		}
		vectorSearcher := &mockVectorSearcher{
			results: []storage.VectorSearchResult{
				{ID: projectMemID, Score: 0.60, NamespaceID: primaryNs},
				{ID: aboutMemID, Score: 0.65, NamespaceID: aboutNs},
			},
		}
		embProvider := &mockEmbeddingProvider{
			name: "test-embed", dimensions: []int{128},
			resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
		}
		svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
		return svc
	}

	slugOf := func(mems []RecallResult, id uuid.UUID) (string, bool) {
		for _, m := range mems {
			if m.ID == id {
				return m.ProjectSlug, true
			}
		}
		return "", false
	}

	// With AboutMeNamespaceID set: about_me memory attributed to about_me.
	resp, err := newSvc().Recall(context.Background(), &RecallRequest{
		ProjectID:          primaryID,
		AboutMeNamespaceID: &aboutNs,
		Query:              "anything",
	})
	if err != nil {
		t.Fatalf("recall (with about_me): %v", err)
	}
	slug, ok := slugOf(resp.Memories, aboutMemID)
	if !ok {
		t.Fatal("about_me memory should be in the aperture when AboutMeNamespaceID is set")
	}
	if slug != model.ReservedProjectSlugAboutMe {
		t.Errorf("about_me memory attribution: want %q, got %q", model.ReservedProjectSlugAboutMe, slug)
	}

	// Without it: no about_me namespace in projectByNamespace, so the same
	// memory falls back to the primary project's slug.
	resp2, err := newSvc().Recall(context.Background(), &RecallRequest{
		ProjectID: primaryID,
		Query:     "anything",
	})
	if err != nil {
		t.Fatalf("recall (without about_me): %v", err)
	}
	if slug, ok := slugOf(resp2.Memories, aboutMemID); ok && slug == model.ReservedProjectSlugAboutMe {
		t.Error("about_me attribution must not appear when AboutMeNamespaceID is unset")
	}
}
