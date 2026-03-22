package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryReader provides read access to stored memories.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
}

// VectorSearcher provides vector similarity search.
type VectorSearcher interface {
	Search(ctx context.Context, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error)
}

// EntityReader provides entity lookup operations.
type EntityReader interface {
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
}

// RelationshipTraverser provides graph traversal from entities.
type RelationshipTraverser interface {
	TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops int) ([]model.Relationship, error)
}

// MemoryShareReader provides access to memory sharing records.
type MemoryShareReader interface {
	ListSharedToNamespace(ctx context.Context, targetNamespaceID uuid.UUID) ([]model.MemoryShare, error)
}

// RecallRequest contains all parameters needed to recall memories.
type RecallRequest struct {
	ProjectID   uuid.UUID  `json:"project_id"`
	Query       string     `json:"query"`
	Limit       int        `json:"limit"`
	Threshold   float64    `json:"threshold"`
	Tags        []string   `json:"tags"`
	IncludeGraph bool      `json:"include_graph"`
	GraphDepth  int        `json:"graph_depth"`
	// Caller context
	UserID   *uuid.UUID `json:"-"`
	OrgID    *uuid.UUID `json:"-"`
	APIKeyID *uuid.UUID `json:"-"`
	// Scope overrides (for user/org-level recall)
	NamespaceID *uuid.UUID `json:"-"` // if set, search this namespace instead of project's
	// GlobalNamespaceID, when set, causes the recall to also search the global
	// project's namespace and merge results with the primary project's results.
	GlobalNamespaceID *uuid.UUID `json:"-"`
}

// RecallResult holds a single recalled memory with its computed score.
type RecallResult struct {
	ID          uuid.UUID       `json:"id"`
	ProjectID   uuid.UUID       `json:"project_id"`
	ProjectSlug string          `json:"project_slug"`
	Path        string          `json:"path"`
	Content     string          `json:"content"`
	Tags        []string        `json:"tags"`
	Source      *string         `json:"source,omitempty"`
	Score       float64         `json:"score"`
	Similarity  *float64        `json:"similarity,omitempty"`
	Confidence  float64         `json:"confidence"`
	SharedFrom  *string         `json:"shared_from"`
	AccessCount int             `json:"access_count"`
	Enriched    bool            `json:"enriched"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// RecallGraph holds the graph entities and relationships found during graph traversal.
type RecallGraph struct {
	Entities      []RecallEntity      `json:"entities"`
	Relationships []RecallRelationship `json:"relationships"`
}

// RecallResponse contains the full recall result including optional graph data.
type RecallResponse struct {
	Memories      []RecallResult `json:"memories"`
	Graph         RecallGraph    `json:"graph"`
	TotalSearched int            `json:"total_searched"`
	LatencyMs     int64          `json:"latency_ms"`
}

// RecallEntity represents an entity found during graph traversal.
type RecallEntity struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	EntityType string    `json:"type"`
}

// RecallRelationship represents a relationship found during graph traversal.
type RecallRelationship struct {
	ID       uuid.UUID `json:"id"`
	SourceID uuid.UUID `json:"source_id"`
	TargetID uuid.UUID `json:"target_id"`
	Relation string    `json:"relation"`
	Weight   float64   `json:"weight"`
}

// RankingWeights controls the relative importance of each scoring factor.
type RankingWeights struct {
	Similarity     float64
	Recency        float64
	Importance     float64
	Frequency      float64
	GraphRelevance float64
}

// DefaultRankingWeights provides sensible defaults for ranking.
var DefaultRankingWeights = RankingWeights{
	Similarity:     0.5,
	Recency:        0.15,
	Importance:     0.10,
	Frequency:      0.05,
	GraphRelevance: 0.20,
}

// RecallService orchestrates memory recall with vector search, tag filtering,
// graph traversal, and multi-factor ranking.
type RecallService struct {
	memories      MemoryReader
	projects      ProjectRepository
	namespaces    NamespaceRepository
	tokenUsage    TokenUsageRepository
	vectorSearch  VectorSearcher
	entityReader  EntityReader
	traverser     RelationshipTraverser
	shares        MemoryShareReader
	embedProvider func() provider.EmbeddingProvider
	weights       RankingWeights
}

// NewRecallService creates a new RecallService with the given dependencies.
func NewRecallService(
	memories MemoryReader,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	tokenUsage TokenUsageRepository,
	vectorSearch VectorSearcher,
	entityReader EntityReader,
	traverser RelationshipTraverser,
	shares MemoryShareReader,
	embedProvider func() provider.EmbeddingProvider,
) *RecallService {
	return &RecallService{
		memories:      memories,
		projects:      projects,
		namespaces:    namespaces,
		tokenUsage:    tokenUsage,
		vectorSearch:  vectorSearch,
		entityReader:  entityReader,
		traverser:     traverser,
		shares:        shares,
		embedProvider: embedProvider,
		weights:       DefaultRankingWeights,
	}
}

// SetWeights overrides the default ranking weights.
func (s *RecallService) SetWeights(w RankingWeights) {
	s.weights = w
}

// scoredMemory is an internal type used during ranking.
type scoredMemory struct {
	memory         model.Memory
	similarity     float64
	graphRelevance float64
	projectID      uuid.UUID
	projectSlug    string
	namespacePath  string
	sharedFromNs   *string // non-nil if surfaced via cross-namespace sharing (source namespace slug)
}

// Recall retrieves and ranks memories matching the given query.
func (s *RecallService) Recall(ctx context.Context, req *RecallRequest) (*RecallResponse, error) {
	start := time.Now()

	// Validate required fields.
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("query is required")
	}

	// Apply defaults.
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	threshold := req.Threshold
	graphDepth := req.GraphDepth
	if graphDepth <= 0 {
		graphDepth = 2
	}

	// Determine namespace ID.
	var namespaceID uuid.UUID
	var projectID uuid.UUID
	var projectSlug string
	var namespacePath string

	if req.NamespaceID != nil {
		namespaceID = *req.NamespaceID
		projectID = req.ProjectID
		// Resolve namespace path for the override namespace.
		if s.namespaces != nil {
			if ns, err := s.namespaces.GetByID(ctx, namespaceID); err == nil {
				namespacePath = ns.Path
			}
		}
	} else {
		if req.ProjectID == uuid.Nil {
			return nil, fmt.Errorf("project_id is required")
		}
		project, err := s.projects.GetByID(ctx, req.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("project not found: %w", err)
		}
		namespaceID = project.NamespaceID
		projectID = project.ID
		projectSlug = project.Slug
		// Resolve namespace path.
		if s.namespaces != nil {
			if ns, err := s.namespaces.GetByID(ctx, namespaceID); err == nil {
				namespacePath = ns.Path
			}
		}
	}

	candidates := []scoredMemory{}

	// Try vector search if embedding provider is available.
	var embeddingUsed bool
	if s.embedProvider != nil {
		ep := s.embedProvider()
		if ep != nil && s.vectorSearch != nil {
			dim := bestEmbeddingDimension(ep.Dimensions())

			embReq := &provider.EmbeddingRequest{
				Input:     []string{req.Query},
				Dimension: dim,
			}

			resp, err := ep.Embed(ctx, embReq)
			if err == nil && len(resp.Embeddings) > 0 {
				embeddingUsed = true

				// Use the actual returned embedding dimension for search,
				// not the requested one — some providers (e.g., Ollama)
				// ignore the dimension parameter and return their native size.
				actualDim := len(resp.Embeddings[0])

				// Record token usage.
				if s.tokenUsage != nil {
					usage := &model.TokenUsage{
						ID:           uuid.New(),
						OrgID:        req.OrgID,
						UserID:       req.UserID,
						ProjectID:    &projectID,
						NamespaceID:  namespaceID,
						Operation:    "embedding",
						Provider:     ep.Name(),
						Model:        resp.Model,
						TokensInput:  resp.Usage.PromptTokens,
						TokensOutput: resp.Usage.CompletionTokens,
						APIKeyID:     req.APIKeyID,
						CreatedAt:    time.Now(),
					}
					_ = s.tokenUsage.Record(ctx, usage)
				}

				// Over-fetch for re-ranking.
				topK := limit * 3
				if topK < 10 {
					topK = 10
				}

				// Search primary namespace.
				searchNamespaces := []uuid.UUID{namespaceID}
				// Also search the global namespace if set and different from primary.
				if req.GlobalNamespaceID != nil && *req.GlobalNamespaceID != namespaceID {
					searchNamespaces = append(searchNamespaces, *req.GlobalNamespaceID)
				}

				simMap := make(map[uuid.UUID]float64)
				for _, nsID := range searchNamespaces {
					results, err := s.vectorSearch.Search(ctx, resp.Embeddings[0], nsID, actualDim, topK)
					if err != nil {
						continue
					}
					for _, r := range results {
						// Keep the best score if a memory appears in multiple searches.
						if existing, ok := simMap[r.ID]; !ok || r.Score > existing {
							simMap[r.ID] = r.Score
						}
					}
				}

				// Fetch full memories.
				ids := make([]uuid.UUID, 0, len(simMap))
				for id := range simMap {
					ids = append(ids, id)
				}
				if len(ids) > 0 {
					memories, err := s.memories.GetBatch(ctx, ids)
					if err == nil {
						for _, mem := range memories {
							sim := simMap[mem.ID]
							candidates = append(candidates, scoredMemory{
								memory:        mem,
								similarity:    sim,
								projectID:     projectID,
								projectSlug:   projectSlug,
								namespacePath: namespacePath,
							})
						}
					}
				}
			}
		}
	}

	// Fall back to listing by namespace if no embedding was used.
	if !embeddingUsed {
		seenIDs := make(map[uuid.UUID]bool)
		memories, err := s.memories.ListByNamespace(ctx, namespaceID, limit*3, 0)
		if err == nil {
			for _, mem := range memories {
				seenIDs[mem.ID] = true
				candidates = append(candidates, scoredMemory{
					memory:        mem,
					projectID:     projectID,
					projectSlug:   projectSlug,
					namespacePath: namespacePath,
				})
			}
		}
		// Also include global namespace memories in text fallback.
		if req.GlobalNamespaceID != nil && *req.GlobalNamespaceID != namespaceID {
			globalMems, err := s.memories.ListByNamespace(ctx, *req.GlobalNamespaceID, limit*3, 0)
			if err == nil {
				for _, mem := range globalMems {
					if !seenIDs[mem.ID] {
						candidates = append(candidates, scoredMemory{
							memory:        mem,
							projectID:     projectID,
							projectSlug:   projectSlug,
							namespacePath: namespacePath,
						})
					}
				}
			}
		}
	}

	// Include memories from shared namespaces.
	if s.shares != nil && s.memories != nil {
		sharedRecords, err := s.shares.ListSharedToNamespace(ctx, namespaceID)
		if err == nil {
			for _, share := range sharedRecords {
				// Skip revoked shares.
				if share.RevokedAt != nil {
					continue
				}
				// Resolve the source namespace slug for the shared_from field.
				var sourceNsSlug string
				if s.namespaces != nil {
					if sourceNs, err := s.namespaces.GetByID(ctx, share.SourceNsID); err == nil {
						sourceNsSlug = sourceNs.Slug
					}
				}
				if sourceNsSlug == "" {
					sourceNsSlug = share.SourceNsID.String()
				}
				// Fetch memories from the source namespace.
				sharedMems, err := s.memories.ListByNamespace(ctx, share.SourceNsID, limit*3, 0)
				if err == nil {
					for _, mem := range sharedMems {
						slug := sourceNsSlug
						candidates = append(candidates, scoredMemory{
							memory:        mem,
							projectID:     projectID,
							projectSlug:   projectSlug,
							namespacePath: namespacePath,
							sharedFromNs:  &slug,
						})
					}
				}
			}
		}
	}

	// Track total candidates searched (before tag/threshold filtering).
	totalSearched := len(candidates)

	// Filter by tags (intersection: memory must have ALL requested tags).
	if len(req.Tags) > 0 {
		filtered := candidates[:0]
		for _, c := range candidates {
			if hasAllTags(c.memory.Tags, req.Tags) {
				filtered = append(filtered, c)
			}
		}
		candidates = filtered
	}

	// Graph traversal if requested.
	graphEntities := []RecallEntity{}
	graphRelationships := []RecallRelationship{}
	if req.IncludeGraph && s.entityReader != nil && s.traverser != nil {
		// Search for entities related to the query.
		foundEntities, err := s.entityReader.FindBySimilarity(ctx, namespaceID, req.Query, "", 10)
		if err == nil {
			// Build set of memory IDs connected via graph.
			graphMemoryRelevance := make(map[uuid.UUID]float64)
			// Deduplicate relationships by ID.
			seenRels := make(map[uuid.UUID]struct{})

			for _, ent := range foundEntities {
				graphEntities = append(graphEntities, RecallEntity{
					ID:         ent.ID,
					Name:       ent.Name,
					EntityType: ent.EntityType,
				})

				rels, err := s.traverser.TraverseFromEntity(ctx, ent.ID, graphDepth)
				if err == nil {
					for _, rel := range rels {
						if _, seen := seenRels[rel.ID]; !seen {
							seenRels[rel.ID] = struct{}{}
							graphRelationships = append(graphRelationships, RecallRelationship{
								ID:       rel.ID,
								SourceID: rel.SourceID,
								TargetID: rel.TargetID,
								Relation: rel.Relation,
								Weight:   rel.Weight,
							})
						}
						if rel.SourceMemory != nil {
							// Compute graph relevance: 1/(1+hops) * weight.
							// We approximate hops as 1 for directly connected relationships.
							relevance := 1.0 / 2.0 * rel.Weight
							if existing, ok := graphMemoryRelevance[*rel.SourceMemory]; !ok || relevance > existing {
								graphMemoryRelevance[*rel.SourceMemory] = relevance
							}
						}
					}
				}
			}

			// Apply graph relevance to candidates.
			for i := range candidates {
				if rel, ok := graphMemoryRelevance[candidates[i].memory.ID]; ok {
					candidates[i].graphRelevance = rel
				}
			}
		}
	}

	// Compute final scores using the ranking formula.
	now := time.Now()

	// Find max access count for frequency normalization.
	maxAccess := 0
	for _, c := range candidates {
		if c.memory.AccessCount > maxAccess {
			maxAccess = c.memory.AccessCount
		}
	}

	for i := range candidates {
		c := &candidates[i]
		mem := &c.memory

		// Recency: exp(-0.01 * hours_since_creation)
		hoursSinceCreation := now.Sub(mem.CreatedAt).Hours()
		recencyScore := math.Exp(-0.01 * hoursSinceCreation)

		// Frequency: log(1 + access_count) / log(1 + max_access_in_results)
		var frequencyScore float64
		if maxAccess > 0 {
			frequencyScore = math.Log(1+float64(mem.AccessCount)) / math.Log(1+float64(maxAccess))
		}

		// Composite score.
		c.similarity = clampScore(c.similarity)
		score := s.weights.Similarity*c.similarity +
			s.weights.Recency*recencyScore +
			s.weights.Importance*mem.Importance +
			s.weights.Frequency*frequencyScore +
			s.weights.GraphRelevance*c.graphRelevance

		// Store back — we reuse similarity field for the raw similarity, score is computed below.
		_ = score // used in sort
		candidates[i] = *c
	}

	// Sort by computed score descending.
	sort.Slice(candidates, func(i, j int) bool {
		si := computeScore(candidates[i], s.weights, now, maxAccess)
		sj := computeScore(candidates[j], s.weights, now, maxAccess)
		return si > sj
	})

	// Apply threshold filter.
	var results []RecallResult
	for _, c := range candidates {
		score := computeScore(c, s.weights, now, maxAccess)
		if score < threshold {
			continue
		}

		var sim *float64
		if embeddingUsed {
			sv := c.similarity
			sim = &sv
		}

		tags := c.memory.Tags
		if tags == nil {
			tags = []string{}
		}
		results = append(results, RecallResult{
			ID:          c.memory.ID,
			ProjectID:   c.projectID,
			ProjectSlug: c.projectSlug,
			Path:        c.namespacePath,
			Content:     c.memory.Content,
			Tags:        tags,
			Source:      c.memory.Source,
			Score:       score,
			Similarity:  sim,
			Confidence:  c.memory.Confidence,
			SharedFrom:  c.sharedFromNs,
			AccessCount: c.memory.AccessCount,
			Enriched:    c.memory.Enriched,
			Metadata:    c.memory.Metadata,
			CreatedAt:   c.memory.CreatedAt,
		})

		if len(results) >= limit {
			break
		}
	}

	if results == nil {
		results = []RecallResult{}
	}

	latency := time.Since(start).Milliseconds()

	return &RecallResponse{
		Memories: results,
		Graph: RecallGraph{
			Entities:      graphEntities,
			Relationships: graphRelationships,
		},
		TotalSearched: totalSearched,
		LatencyMs:     latency,
	}, nil
}

// computeScore calculates the composite ranking score for a candidate.
func computeScore(c scoredMemory, w RankingWeights, now time.Time, maxAccess int) float64 {
	hoursSinceCreation := now.Sub(c.memory.CreatedAt).Hours()
	recencyScore := math.Exp(-0.01 * hoursSinceCreation)

	var frequencyScore float64
	if maxAccess > 0 {
		frequencyScore = math.Log(1+float64(c.memory.AccessCount)) / math.Log(1+float64(maxAccess))
	}

	return w.Similarity*clampScore(c.similarity) +
		w.Recency*recencyScore +
		w.Importance*c.memory.Importance +
		w.Frequency*frequencyScore +
		w.GraphRelevance*c.graphRelevance
}

// clampScore ensures a score is in the [0, 1] range.
func clampScore(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// hasAllTags returns true if memTags contains every tag in required.
func hasAllTags(memTags, required []string) bool {
	tagSet := make(map[string]struct{}, len(memTags))
	for _, t := range memTags {
		tagSet[t] = struct{}{}
	}
	for _, t := range required {
		if _, ok := tagSet[t]; !ok {
			return false
		}
	}
	return true
}
