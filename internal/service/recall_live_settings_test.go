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

// liveSettingsLexicalSearcher returns a fixed lexical ranking on every call.
// Used by TestRecall_FusionConfigLiveReload to drive the fusion gate without
// pulling in the full lexical fixture stack.
type liveSettingsLexicalSearcher struct {
	ranks []storage.MemoryRank
}

func (l *liveSettingsLexicalSearcher) SearchByText(_ context.Context, _ uuid.UUID, _ string, _ int) ([]storage.MemoryRank, error) {
	return l.ranks, nil
}

// TestRecall_RankingWeightsLiveReload exercises the regression that motivated
// dropping the cached RecallService.weights struct: editing a ranking.weight.*
// key via the settings service between two Recall calls must change the
// composite-score ordering on the second call without any restart or setter
// reinvocation. Concretely we flip ranking.weight.similarity from 0 to 1 (and
// every other weight to 0) so the second sort order matches the raw cosine.
func TestRecall_RankingWeightsLiveReload(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	highSimLowImp := uuid.New()
	lowSimHighImp := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highSimLowImp: makeTestMemory(highSimLowImp, nsID, "alpha", nil, 0.0, 0, now),
			lowSimHighImp: makeTestMemory(lowSimHighImp, nsID, "beta", nil, 1.0, 0, now),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highSimLowImp, Score: 0.95, NamespaceID: nsID},
			{ID: lowSimHighImp, Score: 0.10, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	repo := newMockSettingsRepo()
	settings := NewSettingsService(repo)
	svc.SetSettings(settings)

	// Phase 1: importance-dominant weights. With similarity=0 and
	// importance=1, the low-similarity high-importance memory wins.
	repo.put(SettingRankWeightSim, "global", "0")
	repo.put(SettingRankWeightRec, "global", "0")
	repo.put(SettingRankWeightImp, "global", "1")
	repo.put(SettingRankWeightFreq, "global", "0")
	repo.put(SettingRankWeightGraph, "global", "0")
	repo.put(SettingRankWeightConf, "global", "0")
	repo.put(SettingRankWeightOrigin, "global", "0")
	repo.put(SettingRankWeightMmr, "global", "1")
	settings.InvalidateAllCache()

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find something",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("phase 1 recall: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("phase 1: expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != lowSimHighImp {
		t.Fatalf("phase 1: expected importance to dominate, got %s first", resp.Memories[0].ID)
	}

	// Phase 2: flip to similarity-dominant. No setter, no restart — only
	// the registry value changes. The cached settings cache is invalidated
	// to match what the admin PUT handler does on a real edit.
	repo.put(SettingRankWeightSim, "global", "1")
	repo.put(SettingRankWeightImp, "global", "0")
	settings.InvalidateAllCache()

	resp, err = svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find something",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("phase 2 recall: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("phase 2: expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != highSimLowImp {
		t.Fatalf("phase 2: expected similarity to dominate after live edit, got %s first", resp.Memories[0].ID)
	}
}

// TestRecall_FusionConfigLiveReload covers the matching regression for
// recall.fusion.enabled. With fusion off, the fused_combined similarity
// threshold mode is rejected at the request gate. Flipping the setting
// between two calls must reopen the gate without a restart.
func TestRecall_FusionConfigLiveReload(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	now := time.Now()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			memID: makeTestMemory(memID, nsID, "alpha", nil, 0.5, 0, now),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: memID, Score: 0.95, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})
	svc.SetLexical(&liveSettingsLexicalSearcher{ranks: []storage.MemoryRank{{ID: memID, Rank: 0.5}}})

	repo := newMockSettingsRepo()
	settings := NewSettingsService(repo)
	svc.SetSettings(settings)

	// Phase 1: fusion off (set explicitly — the registered default is now on).
	// fused_combined with a non-zero threshold must be rejected.
	repo.put(SettingRecallFusionEnabled, "global", "false")
	settings.InvalidateAllCache()
	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "find something",
		Limit:                   1,
		SimilarityThreshold:     0.1,
		SimilarityThresholdMode: SimilarityThresholdModeFusedCombined,
	})
	if err == nil {
		t.Fatal("phase 1: expected fused_combined to be rejected with fusion disabled")
	}

	// Phase 2: enable fusion via the settings registry only (no SetFusion
	// call). The same request must now succeed.
	repo.put(SettingRecallFusionEnabled, "global", "true")
	settings.InvalidateAllCache()

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "find something",
		Limit:                   1,
		SimilarityThreshold:     0.1,
		SimilarityThresholdMode: SimilarityThresholdModeFusedCombined,
	})
	if err != nil {
		t.Fatalf("phase 2 recall: %v", err)
	}
	if len(resp.Memories) == 0 {
		t.Fatal("phase 2: expected at least one memory once fusion is enabled")
	}
}
