package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// activationWeights pins ranking weights so the cross-namespace graph boost is
// the only thing that can flip ordering between two memories: similarity and
// graph_relevance carry all the weight, everything else (recency, importance,
// frequency, confidence, origin) is zeroed, and MMR is disabled (lambda=1).
func activationWeights(repo *mockSettingsRepo) {
	repo.put(SettingRankWeightSim, "global", "1")
	repo.put(SettingRankWeightRec, "global", "0")
	repo.put(SettingRankWeightImp, "global", "0")
	repo.put(SettingRankWeightFreq, "global", "0")
	repo.put(SettingRankWeightGraph, "global", "1")
	repo.put(SettingRankWeightConf, "global", "0")
	repo.put(SettingRankWeightOrigin, "global", "0")
	repo.put(SettingRankWeightMmr, "global", "1")
}

// activationFixture builds a recall scenario where:
//   - connectedMem lives in the PRIMARY tier with a LOW cosine (0.30).
//   - unconnectedMem lives in the GLOBAL tier with a HIGHER cosine (0.50).
//   - an entity lives in the GLOBAL tier, reachable only by the vector channel
//     (lexical FindBySimilarity returns nothing), and the graph connects that
//     entity to connectedMem.
//
// On pure similarity, unconnectedMem outranks connectedMem. With cross-namespace
// vector activation ON, the global entity is surfaced, traversed, and boosts the
// connected primary memory above the unconnected global one. With activation OFF
// (or when the entity vector search errors), no entity is found and the original
// similarity order stands.
func activationFixture(t *testing.T, lexicalEntity bool) (
	svc *RecallService, repo *mockSettingsRepo, settings *SettingsService,
	connectedID uuid.UUID, unconnectedID uuid.UUID, primaryID uuid.UUID, globalNs uuid.UUID,
) {
	t.Helper()
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	now := time.Now()
	connectedID = uuid.New()
	unconnectedID = uuid.New()
	entityID := uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			connectedID:   makeTestMemory(connectedID, primaryNs, "connected primary memory", nil, 0, 0, now),
			unconnectedID: makeTestMemory(unconnectedID, globalNs, "unconnected global memory", nil, 0, 0, now),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: connectedID, Score: 0.30, NamespaceID: primaryNs},
			{ID: unconnectedID, Score: 0.50, NamespaceID: globalNs},
		},
		// The activating entity is only reachable in the global tier.
		entityResults: []storage.VectorSearchResult{
			{ID: entityID, Score: 0.99, NamespaceID: globalNs},
		},
	}

	entityReader := &mockEntityReader{
		// GetBatch hydrates the vector-surfaced entity by ID.
		byID: map[uuid.UUID]model.Entity{
			entityID: {ID: entityID, NamespaceID: globalNs, Name: "CrossEntity", EntityType: "concept"},
		},
	}
	if lexicalEntity {
		// Also make the entity discoverable lexically so the fail-soft test can
		// prove the lexical channel still activates when the vector channel errors.
		entityReader.entities = []model.Entity{{ID: entityID, NamespaceID: globalNs, Name: "CrossEntity", EntityType: "concept"}}
	}

	traverser := &mockRelTraverser{
		rels: []model.Relationship{{
			ID:           uuid.New(),
			NamespaceID:  globalNs,
			SourceID:     entityID,
			TargetID:     uuid.New(),
			Relation:     "describes",
			Weight:       1.0,
			SourceMemory: &connectedID,
			CreatedAt:    now,
			ValidFrom:    now,
		}},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
		},
	}

	svc, _ = newRecallService(memReader, projects, namespaces, vectorSearcher, entityReader, traverser, func() provider.EmbeddingProvider {
		return embProvider
	})
	repo = newMockSettingsRepo()
	settings = NewSettingsService(repo)
	svc.SetSettings(settings)
	activationWeights(repo)
	settings.InvalidateAllCache()

	return svc, repo, settings, connectedID, unconnectedID, primaryID, globalNs
}

func rankOf(resp *RecallResponse, id uuid.UUID) int {
	for i, m := range resp.Memories {
		if m.ID == id {
			return i
		}
	}
	return -1
}

// TestRecall_VectorActivationOn_BoostsCrossNamespaceConnectedMemory is the
// core ON case: a global-tier entity surfaced only by the vector channel boosts
// its connected low-cosine primary memory above a higher-cosine unconnected one.
func TestRecall_VectorActivationOn_BoostsCrossNamespaceConnectedMemory(t *testing.T) {
	svc, repo, settings, connectedID, unconnectedID, primaryID, globalNs := activationFixture(t, false)
	repo.put(SettingRecallGraphVectorActivationEnabled, "global", "true")
	settings.InvalidateAllCache()

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
		Limit:             10,
		IncludeGraph:      true,
		GraphDepth:        2,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	cr, ur := rankOf(resp, connectedID), rankOf(resp, unconnectedID)
	if cr == -1 || ur == -1 {
		t.Fatalf("expected both memories in results, got connected=%d unconnected=%d", cr, ur)
	}
	if cr >= ur {
		t.Errorf("activation ON: connected memory (rank %d) should outrank unconnected (rank %d) via cross-namespace boost", cr, ur)
	}
	// The cross-namespace entity must surface in the response graph block.
	if len(resp.Graph.Entities) == 0 {
		t.Errorf("activation ON: expected the vector-surfaced entity in the response graph, got none")
	}
}

// TestRecall_VectorActivationOff_NoBoost is the OFF case: with the switch off,
// the vector channel does not run, no entity is found (lexical returns none),
// and pure similarity order stands — unconnected outranks connected.
func TestRecall_VectorActivationOff_NoBoost(t *testing.T) {
	svc, repo, settings, connectedID, unconnectedID, primaryID, globalNs := activationFixture(t, false)
	repo.put(SettingRecallGraphVectorActivationEnabled, "global", "false")
	settings.InvalidateAllCache()

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
		Limit:             10,
		IncludeGraph:      true,
		GraphDepth:        2,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	cr, ur := rankOf(resp, connectedID), rankOf(resp, unconnectedID)
	if cr == -1 || ur == -1 {
		t.Fatalf("expected both memories in results, got connected=%d unconnected=%d", cr, ur)
	}
	if ur >= cr {
		t.Errorf("activation OFF: unconnected (rank %d) should outrank connected (rank %d) on pure similarity", ur, cr)
	}
	if len(resp.Graph.Entities) != 0 {
		t.Errorf("activation OFF: expected no entity (lexical found none), got %d", len(resp.Graph.Entities))
	}
}

// TestRecall_VectorActivationFailSoft_FallsBackToLexical asserts that an error
// from the entity vector search does not fail the recall: the lexical channel
// still activates the entity and boosts the connected memory.
func TestRecall_VectorActivationFailSoft_FallsBackToLexical(t *testing.T) {
	svc, repo, settings, connectedID, unconnectedID, primaryID, globalNs := activationFixture(t, true)
	repo.put(SettingRecallGraphVectorActivationEnabled, "global", "true")
	settings.InvalidateAllCache()

	// Force the entity vector search to error.
	svc.vectorSearch.(*mockVectorSearcher).entityErr = errors.New("entity vector store down")

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "CrossEntity",
		Limit:             10,
		IncludeGraph:      true,
		GraphDepth:        2,
	})
	if err != nil {
		t.Fatalf("fail-soft: recall must succeed despite entity-vector error, got %v", err)
	}
	cr, ur := rankOf(resp, connectedID), rankOf(resp, unconnectedID)
	if cr == -1 || ur == -1 {
		t.Fatalf("expected both memories in results, got connected=%d unconnected=%d", cr, ur)
	}
	if cr >= ur {
		t.Errorf("fail-soft: lexical activation should still boost connected (rank %d) above unconnected (rank %d)", cr, ur)
	}
}

// perEntityTraverser returns a distinct edge set per entity and faithfully
// truncates to the passed maxEdges (as a real traverser does), recording each
// call so a test can assert that every seed actually got to traverse.
type perEntityTraverser struct {
	relsByEntity map[uuid.UUID][]model.Relationship
	calls        []traverseCall
}

type traverseCall struct {
	entity   uuid.UUID
	maxEdges int
}

func (t *perEntityTraverser) TraverseFromEntity(_ context.Context, entityID uuid.UUID, _, maxEdges int) (storage.TraversalResult, error) {
	t.calls = append(t.calls, traverseCall{entity: entityID, maxEdges: maxEdges})
	rels := t.relsByEntity[entityID]
	if maxEdges > 0 && len(rels) > maxEdges {
		rels = rels[:maxEdges]
	}
	return storage.TraversalResult{Relationships: rels}, nil
}

// TestRecall_VectorActivationPerSeedBudget_CrossTierStillTraverses is the
// load-bearing starvation regression. A hot lexical seed (first in interleave
// order) wants far more edges than the whole budget; a cross-tier vector seed
// is second. Under the old all-remaining cap the hot seed drained the budget
// and the cross-tier seed never traversed. With per-seed fair share, both seeds
// get an equal slice and the cross-tier seed STILL traverses.
func TestRecall_VectorActivationPerSeedBudget_CrossTierStillTraverses(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()
	now := time.Now()

	hotEntityID := uuid.New()
	crossEntityID := uuid.New()
	crossMemID := uuid.New()
	hotMemID := uuid.New()

	// The hot entity wants 10 edges (each to its own primary memory); the
	// cross-tier entity has a single edge to the cross-tier memory.
	hotRels := make([]model.Relationship, 0, 10)
	for range 10 {
		src := uuid.New()
		hotRels = append(hotRels, model.Relationship{
			ID: uuid.New(), NamespaceID: primaryNs, SourceID: hotEntityID, TargetID: uuid.New(),
			Relation: "rel", Weight: 1.0, SourceMemory: &src, CreatedAt: now, ValidFrom: now,
		})
	}
	traverser := &perEntityTraverser{relsByEntity: map[uuid.UUID][]model.Relationship{
		hotEntityID: hotRels,
		crossEntityID: {{
			ID: uuid.New(), NamespaceID: globalNs, SourceID: crossEntityID, TargetID: uuid.New(),
			Relation: "rel", Weight: 1.0, SourceMemory: &crossMemID, CreatedAt: now, ValidFrom: now,
		}},
	}}

	memReader := &mockMemoryReader{memories: map[uuid.UUID]*model.Memory{
		hotMemID:   makeTestMemory(hotMemID, primaryNs, "hot primary memory", nil, 0, 0, now),
		crossMemID: makeTestMemory(crossMemID, globalNs, "cross tier memory", nil, 0, 0, now),
	}}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: hotMemID, Score: 0.40, NamespaceID: primaryNs},
			{ID: crossMemID, Score: 0.20, NamespaceID: globalNs},
		},
		entityResults: []storage.VectorSearchResult{
			{ID: crossEntityID, Score: 0.99, NamespaceID: globalNs},
		},
	}

	entityReader := &mockEntityReader{
		// Hot entity is the lexical hit (seeded first by interleave).
		entities: []model.Entity{{ID: hotEntityID, NamespaceID: primaryNs, Name: "HotEntity", EntityType: "concept"}},
		// Cross entity is hydrated from the vector channel.
		byID: map[uuid.UUID]model.Entity{
			crossEntityID: {ID: crossEntityID, NamespaceID: globalNs, Name: "CrossEntity", EntityType: "concept"},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{384},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 384)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, entityReader, traverser, func() provider.EmbeddingProvider { return embProvider })
	repo := newMockSettingsRepo()
	settings := NewSettingsService(repo)
	svc.SetSettings(settings)
	repo.put(SettingRecallGraphVectorActivationEnabled, "global", "true")
	// Tiny edge budget: 2 seeds → per-seed share of ceil(4/2)=2.
	repo.put(SettingRecallGraphMaxEdges, "global", "4")
	settings.InvalidateAllCache()

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "HotEntity",
		Limit:             10,
		IncludeGraph:      true,
		GraphDepth:        2,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}

	if len(traverser.calls) != 2 {
		t.Fatalf("expected both seeds to traverse (per-seed budget), got %d call(s): %+v", len(traverser.calls), traverser.calls)
	}
	if traverser.calls[0].entity != hotEntityID {
		t.Errorf("expected hot lexical seed first, got %v", traverser.calls[0].entity)
	}
	// The cross-tier seed is the regression target: it must be reached and
	// handed a positive per-seed budget, not starved to zero.
	if traverser.calls[1].entity != crossEntityID {
		t.Errorf("expected cross-tier vector seed second, got %v", traverser.calls[1].entity)
	}
	if traverser.calls[1].maxEdges != 2 {
		t.Errorf("expected cross-tier seed per-seed budget=2, got %d", traverser.calls[1].maxEdges)
	}
	// And the hot seed must be capped at its fair share (not the whole budget).
	if traverser.calls[0].maxEdges != 2 {
		t.Errorf("expected hot seed per-seed budget=2 (fair share), got %d", traverser.calls[0].maxEdges)
	}
	if rankOf(resp, crossMemID) == -1 {
		t.Errorf("cross-tier memory should be present in results")
	}
}
