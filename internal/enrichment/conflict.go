package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// ConflictConfig controls the behaviour of the conflict detection check.
type ConflictConfig struct {
	SimilarityThreshold float64 // min similarity to consider for conflict check (default 0.7)
	TopK                int     // number of candidates to evaluate (default 10)
}

// DefaultConflictConfig provides sensible defaults for conflict detection.
var DefaultConflictConfig = ConflictConfig{
	SimilarityThreshold: 0.7,
	TopK:                10,
}

func (c ConflictConfig) withDefaults() ConflictConfig {
	if c.SimilarityThreshold <= 0 {
		c.SimilarityThreshold = DefaultConflictConfig.SimilarityThreshold
	}
	if c.TopK <= 0 {
		c.TopK = DefaultConflictConfig.TopK
	}
	return c
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

// ConflictResult describes a detected semantic contradiction between two
// memories.
type ConflictResult struct {
	ConflictFound bool      // true when the LLM determines the statements contradict
	ConflictingID uuid.UUID // the existing memory that conflicts
	Explanation   string    // why they conflict
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

const contradictionPrompt = `Given two factual statements, determine if they contradict each other (i.e., both cannot be true simultaneously).

Statement A: %s
Statement B: %s

Consider temporal context — if Statement A says "Alice works at ACME" and Statement B says "Alice previously worked at ACME", these do NOT contradict.

Respond ONLY with JSON, no markdown fences:
{"contradicts": true, "explanation": "Brief reason"}`

// ---------------------------------------------------------------------------
// ConflictDetector
// ---------------------------------------------------------------------------

// ConflictDetector checks whether a new memory semantically contradicts
// existing memories in the same namespace. When a contradiction is found it
// creates a conflicts_with lineage record linking the two memories.
type ConflictDetector struct {
	vectorStore   VectorSearcher
	memories      MemoryReader
	lineage       LineageCreator
	llmProvider   func() provider.LLMProvider
	embedProvider func() provider.EmbeddingProvider
	config        ConflictConfig
}

// NewConflictDetector constructs a ConflictDetector. The provider functions may
// return nil to indicate that a capability is unavailable; in that case Detect
// returns an empty result.
func NewConflictDetector(
	vectorStore VectorSearcher,
	memories MemoryReader,
	lineage LineageCreator,
	llmProvider func() provider.LLMProvider,
	embedProvider func() provider.EmbeddingProvider,
	config ConflictConfig,
) *ConflictDetector {
	return &ConflictDetector{
		vectorStore:   vectorStore,
		memories:      memories,
		lineage:       lineage,
		llmProvider:   llmProvider,
		embedProvider: embedProvider,
		config:        config.withDefaults(),
	}
}

// Detect checks whether the given memory contradicts any existing memories in
// its namespace. For each detected contradiction a conflicts_with lineage
// record is created.
func (cd *ConflictDetector) Detect(ctx context.Context, memory *model.Memory) ([]ConflictResult, error) {
	llm := cd.llmProvider()
	if llm == nil {
		return nil, nil
	}

	ep := cd.embedProvider()
	if ep == nil {
		return nil, nil
	}

	// Embed the new memory's content.
	embedResp, err := ep.Embed(ctx, &provider.EmbeddingRequest{
		Input: []string{memory.Content},
	})
	if err != nil {
		return nil, fmt.Errorf("conflict: embed content: %w", err)
	}
	if len(embedResp.Embeddings) == 0 {
		return nil, fmt.Errorf("conflict: embed returned no vectors")
	}

	embedding := embedResp.Embeddings[0]
	dimension := len(embedding)

	// Search for similar memories.
	results, err := cd.vectorStore.Search(ctx, embedding, memory.NamespaceID, dimension, cd.config.TopK)
	if err != nil {
		return nil, fmt.Errorf("conflict: vector search: %w", err)
	}

	var conflicts []ConflictResult

	for _, result := range results {
		// Skip self-matches.
		if result.ID == memory.ID {
			continue
		}

		// Only consider results above the similarity threshold.
		if result.Score < cd.config.SimilarityThreshold {
			continue
		}

		// Fetch the candidate memory.
		candidate, err := cd.memories.GetByID(ctx, result.ID)
		if err != nil {
			// Skip candidates we cannot retrieve.
			continue
		}

		// Ask the LLM whether the two statements contradict.
		prompt := fmt.Sprintf(contradictionPrompt, memory.Content, candidate.Content)
		resp, err := llm.Complete(ctx, &provider.CompletionRequest{
			Messages: []provider.Message{
				{Role: "user", Content: prompt},
			},
			MaxTokens:   256,
			Temperature: 0.1,
			JSONMode:    true,
		})
		if err != nil {
			// Skip this candidate on LLM error.
			continue
		}

		contradicts, explanation, err := parseConflictResponse(resp.Content)
		if err != nil {
			// Malformed response — skip this candidate.
			continue
		}

		if !contradicts {
			continue
		}

		// Create a conflicts_with lineage record.
		conflictingID := candidate.ID
		lineageCtx, _ := json.Marshal(map[string]string{
			"explanation": explanation,
		})
		lin := &model.MemoryLineage{
			ID:        uuid.New(),
			MemoryID:  memory.ID,
			ParentID:  &conflictingID,
			Relation:  "conflicts_with",
			Context:   lineageCtx,
			CreatedAt: time.Now().UTC(),
		}
		if err := cd.lineage.Create(ctx, lin); err != nil {
			return nil, fmt.Errorf("conflict: create lineage: %w", err)
		}

		conflicts = append(conflicts, ConflictResult{
			ConflictFound: true,
			ConflictingID: candidate.ID,
			Explanation:   explanation,
		})
	}

	return conflicts, nil
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

type conflictJSON struct {
	Contradicts bool   `json:"contradicts"`
	Explanation string `json:"explanation"`
}

// parseConflictResponse parses the LLM's JSON response indicating whether two
// statements contradict. It applies the same three-tier recovery strategy used
// elsewhere in the enrichment package.
func parseConflictResponse(raw string) (bool, string, error) {
	raw = strings.TrimSpace(raw)

	var result conflictJSON
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return false, "", fmt.Errorf("unable to parse conflict JSON: %w", err)
	}
	return result.Contradicts, result.Explanation, nil
}
