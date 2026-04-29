package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryReader provides read access to stored memories.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	ListByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error)
}

// LexicalSearcher runs a backend-native full-text query (FTS5 on SQLite,
// to_tsvector on Postgres) and returns rows in best-first order. The recall
// path uses it as a second retrieval channel that gets fused with vector
// search via Reciprocal Rank Fusion. Implementations must fail soft — a
// malformed query should yield an empty result, not an error, so recall is
// never gated on lexical input parsing.
type LexicalSearcher interface {
	SearchByText(ctx context.Context, namespaceID uuid.UUID, query string, limit int) ([]storage.MemoryRank, error)
}

// VectorSearcher provides vector similarity search.
type VectorSearcher interface {
	Search(ctx context.Context, kind storage.VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error)
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
	// IncludeLowNovelty, when true, bypasses the dream-source low_novelty
	// filter so demoted dream memories surface alongside the rest. Default
	// false preserves the standard recall behavior.
	IncludeLowNovelty bool `json:"include_low_novelty,omitempty"`
	// DiversifyByTagPrefix, when non-empty, post-processes the ranked candidate
	// set by grouping results by the first tag matching this prefix and
	// round-robin-picking across groups up to Limit. Candidates with no
	// prefix-matching tag are excluded from the diversified output. Vector
	// search and graph traversal are unchanged — this is a pure rerank step.
	DiversifyByTagPrefix string `json:"diversify_by_tag_prefix,omitempty"`
	// Caller context
	UserID   *uuid.UUID `json:"-"`
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
	UpdatedAt   time.Time       `json:"updated_at"`
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
	// CoverageGaps surfaces prefix-matching tag values that were observed in
	// the unfiltered candidate pool but produced zero memories in the returned
	// results. Populated only when DiversifyByTagPrefix is set. Each gap
	// carries a cause attributing the hole to the pipeline stage where the
	// group's last surviving candidate died: "tag_filter", "threshold", or
	// "limit".
	CoverageGaps []CoverageGap `json:"coverage_gaps,omitempty"`
}

// CoverageGap describes a prefix-group observed in the candidate pool but
// absent from the returned memories, and why.
type CoverageGap struct {
	GroupKey string `json:"group_key"`
	Cause    string `json:"cause"` // one of CoverageCause* constants
}

// Coverage-gap cause codes attribute a missing prefix-group to the pipeline
// stage where its last surviving candidate was dropped.
const (
	CoverageCauseTagFilter = "tag_filter"
	CoverageCauseThreshold = "threshold"
	CoverageCauseLimit     = "limit"
)

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
	Confidence     float64
}

// DefaultRankingWeights provides sensible defaults for ranking. Frequency is
// 0 because access_count already drives Confidence reinforcement; weighting
// both double-counts the same signal. Operators can re-enable Frequency via
// the ranking.weight.frequency setting.
var DefaultRankingWeights = RankingWeights{
	Similarity:     0.50,
	Recency:        0.15,
	Importance:     0.10,
	Frequency:      0.00,
	GraphRelevance: 0.20,
	Confidence:     0.05,
}

// FusionConfig governs candidate retrieval (parallel vector + lexical,
// fused via RRF). The fused score lands in scoredMemory.similarity, so
// RankingWeights.Similarity still controls its weight in computeScore —
// that's why this is a separate struct from RankingWeights.
type FusionConfig struct {
	Enabled       bool    // off by default; flip via /v1/admin/settings
	RRFConstant   int     // RRF k; canonical default 60
	VectorWeight  float64 // weight on each vector channel's RRF contribution
	LexicalWeight float64 // weight on each lexical channel's RRF contribution
}

// DefaultFusionConfig ships with the feature dark — operators flip
// recall.fusion.enabled in admin settings after migration + smoke test.
var DefaultFusionConfig = FusionConfig{
	Enabled:       false,
	RRFConstant:   60,
	VectorWeight:  0.70,
	LexicalWeight: 0.30,
}

// RecallService orchestrates memory recall with vector search, tag filtering,
// graph traversal, and multi-factor ranking.
type RecallService struct {
	memories      MemoryReader
	projects      ProjectRepository
	namespaces    NamespaceRepository
	vectorSearch  VectorSearcher
	lexical       LexicalSearcher
	entityReader  EntityReader
	traverser     RelationshipTraverser
	shares        MemoryShareReader
	embedProvider func() provider.EmbeddingProvider
	weights       RankingWeights
	fusion        FusionConfig
	// reinforcement is optional. When nil (the default, matching all existing
	// callers), recall has no read-path write. When wired via SetReinforcement,
	// every successful recall asynchronously bumps access_count, last_accessed,
	// and confidence on the surfaced memories — the reconsolidation hook.
	reinforcement *ReinforcementDeps
}

// NewRecallService creates a new RecallService with the given dependencies.
// token_usage recording is handled by the UsageRecordingProvider middleware
// wrapping the registry-issued providers.
func NewRecallService(
	memories MemoryReader,
	projects ProjectRepository,
	namespaces NamespaceRepository,
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
		vectorSearch:  vectorSearch,
		entityReader:  entityReader,
		traverser:     traverser,
		shares:        shares,
		embedProvider: embedProvider,
		weights:       DefaultRankingWeights,
		fusion:        DefaultFusionConfig,
	}
}

// SetWeights overrides the default ranking weights.
func (s *RecallService) SetWeights(w RankingWeights) {
	s.weights = w
}

// SetLexical wires the lexical (BM25/tsvector) searcher used by the hybrid
// recall path. Passing nil disables fusion regardless of FusionConfig.Enabled.
func (s *RecallService) SetLexical(l LexicalSearcher) {
	s.lexical = l
}

// SetFusion overrides the fusion configuration. Off by default; flip via
// /v1/admin/settings (key recall.fusion.enabled) after migrations have been
// applied and the lexical searcher is wired.
func (s *RecallService) SetFusion(cfg FusionConfig) {
	s.fusion = cfg
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

// projectAttribution carries the owning project's ID and slug for a given
// namespace, so each candidate can be stamped with its actual home project
// rather than the recall's primary target.
type projectAttribution struct {
	ProjectID   uuid.UUID
	ProjectSlug string
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

	// projectByNamespace maps each namespace this recall touches to the project
	// that owns it. Without this, every candidate gets stamped with the primary
	// project's slug — globals fetched alongside primary results would be
	// mis-attributed to the search-target project. The map covers primary,
	// global, and shared-source namespaces (seeded lazily during shared
	// resolution below). Falls back to the primary stamp when a namespace has
	// no owning project (e.g., org-level shares).
	projectByNamespace := map[uuid.UUID]projectAttribution{
		namespaceID: {ProjectID: projectID, ProjectSlug: projectSlug},
	}
	if req.GlobalNamespaceID != nil && *req.GlobalNamespaceID != namespaceID {
		if gp, err := s.projects.GetByNamespaceID(ctx, *req.GlobalNamespaceID); err == nil && gp != nil {
			projectByNamespace[*req.GlobalNamespaceID] = projectAttribution{ProjectID: gp.ID, ProjectSlug: gp.Slug}
		}
	}
	attribute := func(memNs uuid.UUID) projectAttribution {
		if attr, ok := projectByNamespace[memNs]; ok {
			return attr
		}
		return projectAttribution{ProjectID: projectID, ProjectSlug: projectSlug}
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

			// Stamp ownership/correlation context for the
			// UsageRecordingProvider middleware to attribute the embedding
			// token_usage row to the right org/user/project/namespace and
			// correlate it back to the API key.
			projectIDForCtx := projectID
			recallCtx := provider.WithUsageContext(ctx, &model.UsageContext{
				UserID:    req.UserID,
				ProjectID: &projectIDForCtx,
			})
			recallCtx = provider.WithNamespaceID(recallCtx, namespaceID)
			recallCtx = provider.WithAPIKeyID(recallCtx, req.APIKeyID)
			recallCtx = provider.WithOperation(recallCtx, provider.OperationEmbedding)

			resp, err := ep.Embed(recallCtx, embReq)
			if err == nil && len(resp.Embeddings) > 0 {
				embeddingUsed = true

				// Use the actual returned embedding dimension for search,
				// not the requested one — some providers (e.g., Ollama)
				// ignore the dimension parameter and return their native size.
				actualDim := len(resp.Embeddings[0])

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
				if s.fusion.Enabled && s.lexical != nil {
					// Hybrid path: fan out vector + lexical per namespace,
					// then fuse via RRF. The fused score (normalized to
					// [0, 1] by max) replaces raw cosine similarity in the
					// downstream computeScore — RankingWeights.Similarity
					// semantics are unchanged from the caller's view.
					simMap = s.runHybridSearch(ctx, runHybridArgs{
						Query:        req.Query,
						Embedding:    resp.Embeddings[0],
						Dim:          actualDim,
						Namespaces:   searchNamespaces,
						TopK:         topK,
						PrimaryNS:    namespaceID,
						PrimaryProj:  projectID,
					})
				} else {
					for _, nsID := range searchNamespaces {
						results, err := s.vectorSearch.Search(ctx, storage.VectorKindMemory, resp.Embeddings[0], nsID, actualDim, topK)
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
							attr := attribute(mem.NamespaceID)
							candidates = append(candidates, scoredMemory{
								memory:        mem,
								similarity:    sim,
								projectID:     attr.ProjectID,
								projectSlug:   attr.ProjectSlug,
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
				attr := attribute(mem.NamespaceID)
				candidates = append(candidates, scoredMemory{
					memory:        mem,
					projectID:     attr.ProjectID,
					projectSlug:   attr.ProjectSlug,
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
						attr := attribute(mem.NamespaceID)
						candidates = append(candidates, scoredMemory{
							memory:        mem,
							projectID:     attr.ProjectID,
							projectSlug:   attr.ProjectSlug,
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
				if _, ok := projectByNamespace[share.SourceNsID]; !ok {
					if sp, err := s.projects.GetByNamespaceID(ctx, share.SourceNsID); err == nil && sp != nil {
						projectByNamespace[share.SourceNsID] = projectAttribution{ProjectID: sp.ID, ProjectSlug: sp.Slug}
					}
				}
				// Fetch memories from the source namespace.
				sharedMems, err := s.memories.ListByNamespace(ctx, share.SourceNsID, limit*3, 0)
				if err == nil {
					for _, mem := range sharedMems {
						slug := sourceNsSlug
						attr := attribute(mem.NamespaceID)
						candidates = append(candidates, scoredMemory{
							memory:        mem,
							projectID:     attr.ProjectID,
							projectSlug:   attr.ProjectSlug,
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

	// Snapshot the pre-tag-filter prefix-group set so coverage_gaps can
	// attribute groups stripped by the intersection filter. Storing the set
	// directly (instead of copying candidates) avoids a slice allocation
	// proportional to the search pool.
	var rawGroups map[string]struct{}
	if req.DiversifyByTagPrefix != "" {
		rawGroups = prefixGroups(candidates, scoredMemoryTags, req.DiversifyByTagPrefix)
	}

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

	var postTagGroups map[string]struct{}
	if req.DiversifyByTagPrefix != "" {
		postTagGroups = prefixGroups(candidates, scoredMemoryTags, req.DiversifyByTagPrefix)
	}

	// Graph traversal if requested.
	graphEntities := []RecallEntity{}
	graphRelationships := []RecallRelationship{}
	if req.IncludeGraph && s.entityReader != nil && s.traverser != nil {
		// Search for entities related to the query using multiple strategies:
		// 1. Full query string match
		// 2. Individual significant words (3+ chars) from the query
		seenEntityIDs := make(map[uuid.UUID]bool)
		var foundEntities []model.Entity

		// Strategy 1: full query match
		if ents, err := s.entityReader.FindBySimilarity(ctx, namespaceID, req.Query, "", 10); err == nil {
			for _, e := range ents {
				if !seenEntityIDs[e.ID] {
					seenEntityIDs[e.ID] = true
					foundEntities = append(foundEntities, e)
				}
			}
		}

		// Strategy 2: search by individual words (3+ chars, skip common words)
		if len(foundEntities) < 10 {
			for _, word := range splitQueryWords(req.Query) {
				if len(foundEntities) >= 10 {
					break
				}
				ents, err := s.entityReader.FindBySimilarity(ctx, namespaceID, word, "", 5)
				if err != nil {
					continue
				}
				for _, e := range ents {
					if !seenEntityIDs[e.ID] {
						seenEntityIDs[e.ID] = true
						foundEntities = append(foundEntities, e)
					}
				}
			}
		}

		if len(foundEntities) > 0 {
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

	// Normalize raw similarity into [0, 1] so RecallResult.Similarity reflects
	// the same value computeScore feeds into the weighted sum.
	for i := range candidates {
		candidates[i].similarity = clampScore(candidates[i].similarity)
	}

	// Resolve effective weights per candidate based on its owning project.
	// Each candidate carries c.projectID (stamped during candidate building),
	// so cross-project recall — globals, shared namespaces — gets each row's
	// owner's tuning rather than the requester's. Cache lifetime is one
	// Recall call; operator changes to a project's overrides are picked up on
	// the next call.
	weightsByProject := make(map[uuid.UUID]RankingWeights, 4)
	resolveWeights := func(projID uuid.UUID) RankingWeights {
		if w, ok := weightsByProject[projID]; ok {
			return w
		}
		merged := s.weights
		if projID != uuid.Nil && s.projects != nil {
			if proj, err := s.projects.GetByID(ctx, projID); err == nil && proj != nil {
				var settings struct {
					RankingWeights json.RawMessage `json:"ranking_weights"`
				}
				if len(proj.Settings) > 0 {
					_ = json.Unmarshal(proj.Settings, &settings)
				}
				if ov, perr := ParseRankingOverride(settings.RankingWeights); perr == nil {
					merged = MergeWeights(s.weights, ov)
				}
			}
		}
		weightsByProject[projID] = merged
		return merged
	}

	// Sort by computed score descending. Each comparison resolves weights
	// from the candidate's owning project, so a single sort can score
	// candidates from different projects under different effective weights.
	sort.Slice(candidates, func(i, j int) bool {
		si := computeScore(candidates[i], resolveWeights(candidates[i].projectID), now, maxAccess)
		sj := computeScore(candidates[j], resolveWeights(candidates[j].projectID), now, maxAccess)
		return si > sj
	})

	// Apply threshold filter to build the post-threshold ranked list. Limit is
	// applied later — diversification needs the full passing set to group over.
	var passing []RecallResult
	for _, c := range candidates {
		// Confidence-zero is the explicit kill signal regardless of source. The
		// pruning phase will soft-delete the row after the 7d grace window, but
		// recall stops surfacing it immediately.
		if c.memory.Confidence == 0 {
			continue
		}
		// Superseded memories are duplicates of a newer winner. Hide them from
		// recall the moment supersede is set; the supersede-prune branch
		// soft-deletes them after 7d of zero access.
		if c.memory.SupersededBy != nil {
			continue
		}
		// isLowNovelty stays gated on dream-source because the metadata key is
		// only written by the dream novelty audit. Callers can opt into the
		// demoted set via IncludeLowNovelty for inspection/debugging.
		if !req.IncludeLowNovelty && c.memory.Source != nil && *c.memory.Source == model.DreamSource {
			if isLowNovelty(c.memory.Metadata) {
				continue
			}
		}

		score := computeScore(c, resolveWeights(c.projectID), now, maxAccess)
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
		passing = append(passing, RecallResult{
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
			UpdatedAt:   c.memory.UpdatedAt,
		})
	}

	var results []RecallResult
	var coverageGaps []CoverageGap
	if req.DiversifyByTagPrefix != "" {
		passingGroups := prefixGroups(passing, recallResultTags, req.DiversifyByTagPrefix)
		results = diversifyByTagPrefix(passing, req.DiversifyByTagPrefix, limit)
		returnedGroups := prefixGroups(results, recallResultTags, req.DiversifyByTagPrefix)
		coverageGaps = computeCoverageGaps(rawGroups, postTagGroups, passingGroups, returnedGroups)
	} else if len(passing) > limit {
		results = passing[:limit]
	} else {
		results = passing
	}

	if results == nil {
		results = []RecallResult{}
	}

	// Reconsolidation hook. Fire-and-forget goroutine that cannot panic or
	// error its way back into the recall response — this is a read-path write
	// and must never affect the caller's outcome. Gated by SetReinforcement;
	// when reinforcement is not wired, reinforce returns immediately.
	if s.reinforcement != nil && len(results) > 0 {
		ids := make([]uuid.UUID, 0, len(results))
		for _, r := range results {
			ids = append(ids, r.ID)
		}
		go func(ids []uuid.UUID) {
			defer func() { _ = recover() }()
			s.reinforce(context.Background(), ids)
		}(ids)
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
		CoverageGaps:  coverageGaps,
	}, nil
}

// runHybridArgs bundles the inputs to the hybrid search fan-out so the
// signature does not balloon when fusion grows new knobs.
type runHybridArgs struct {
	Query       string
	Embedding   []float32
	Dim         int
	Namespaces  []uuid.UUID
	TopK        int
	PrimaryNS   uuid.UUID
	PrimaryProj uuid.UUID
}

// runHybridSearch returns a simMap normalized to [0, 1] so the caller can
// drop it into scoredMemory.similarity unchanged.
//
// Both channels' errors are swallowed by design: a vector hiccup or
// unparseable lexical query must not strand a recall that the other
// channel can still serve.
func (s *RecallService) runHybridSearch(ctx context.Context, args runHybridArgs) map[uuid.UUID]float64 {
	var (
		mu          sync.Mutex
		vecRankings [][]storage.MemoryRank
		lexRankings [][]storage.MemoryRank
	)

	g, gctx := errgroup.WithContext(ctx)
	for _, nsID := range args.Namespaces {
		nsID := nsID
		g.Go(func() error {
			results, err := s.vectorSearch.Search(gctx, storage.VectorKindMemory, args.Embedding, nsID, args.Dim, args.TopK)
			if err != nil {
				return nil
			}
			ranks := make([]storage.MemoryRank, 0, len(results))
			for _, r := range results {
				ranks = append(ranks, storage.MemoryRank{ID: r.ID, Rank: r.Score})
			}
			mu.Lock()
			vecRankings = append(vecRankings, ranks)
			mu.Unlock()
			return nil
		})
		g.Go(func() error {
			ranks, _ := s.lexical.SearchByText(gctx, nsID, args.Query, args.TopK)
			mu.Lock()
			lexRankings = append(lexRankings, ranks)
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	// Compose the ranking list and per-list weights for RRF. Each
	// per-namespace list contributes independently — if a memory shows up
	// in primary's vector list and global's lexical list, both
	// contributions accumulate.
	allRankings := make([][]storage.MemoryRank, 0, len(vecRankings)+len(lexRankings))
	allWeights := make([]float64, 0, len(vecRankings)+len(lexRankings))
	var vecCount, lexCount int
	vecIDs := make(map[uuid.UUID]struct{})
	for _, r := range vecRankings {
		allRankings = append(allRankings, r)
		allWeights = append(allWeights, s.fusion.VectorWeight)
		vecCount += len(r)
		for _, m := range r {
			vecIDs[m.ID] = struct{}{}
		}
	}
	overlap := 0
	for _, r := range lexRankings {
		allRankings = append(allRankings, r)
		allWeights = append(allWeights, s.fusion.LexicalWeight)
		lexCount += len(r)
		for _, m := range r {
			if _, ok := vecIDs[m.ID]; ok {
				overlap++
			}
		}
	}

	fused := ReciprocalRankFusion(allRankings, s.fusion.RRFConstant, allWeights)

	// Normalize by max so the fused score lives in [0, 1] like the cosine
	// it replaces. clampScore in computeScore expects this range, and
	// RankingWeights default to summing to 1.0 against [0, 1] inputs.
	var maxScore float64
	for _, v := range fused {
		if v > maxScore {
			maxScore = v
		}
	}
	simMap := make(map[uuid.UUID]float64, len(fused))
	if maxScore > 0 {
		for id, v := range fused {
			simMap[id] = v / maxScore
		}
	}

	if len(fused) > 0 {
		slog.Info("recall: fusion",
			"vector_count", vecCount,
			"lexical_count", lexCount,
			"overlap", overlap,
			"fused_count", len(fused),
			"namespace_id", args.PrimaryNS,
			"project_id", args.PrimaryProj,
		)
	}

	return simMap
}

// computeScore calculates the composite ranking score for a candidate.
//
// The Confidence term is gated on c.memory.Confidence > 0 so confidence=0 rows
// score identically to "no Confidence term applied" if they ever reach this
// function. Today the kill-signal filter at the post-threshold loop drops them
// before the sort, but the gate keeps the math sound for any future caller
// that bypasses that filter.
func computeScore(c scoredMemory, w RankingWeights, now time.Time, maxAccess int) float64 {
	hoursSinceCreation := now.Sub(c.memory.CreatedAt).Hours()
	recencyScore := math.Exp(-0.01 * hoursSinceCreation)

	var frequencyScore float64
	if maxAccess > 0 {
		frequencyScore = math.Log(1+float64(c.memory.AccessCount)) / math.Log(1+float64(maxAccess))
	}

	score := w.Similarity*clampScore(c.similarity) +
		w.Recency*recencyScore +
		w.Importance*c.memory.Importance +
		w.Frequency*frequencyScore +
		w.GraphRelevance*c.graphRelevance

	if c.memory.Confidence > 0 {
		score += w.Confidence * clampScore(c.memory.Confidence)
	}
	return score
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

// splitQueryWords splits a query into individual significant words for entity matching.
// Filters out common stop words and words shorter than 3 characters.
func splitQueryWords(query string) []string {
	stopWords := map[string]bool{
		"the": true, "and": true, "for": true, "are": true, "but": true,
		"not": true, "you": true, "all": true, "can": true, "had": true,
		"her": true, "was": true, "one": true, "our": true, "out": true,
		"has": true, "his": true, "how": true, "its": true, "let": true,
		"may": true, "new": true, "now": true, "old": true, "see": true,
		"way": true, "who": true, "did": true, "get": true, "got": true,
		"him": true, "hit": true, "say": true, "she": true, "too": true,
		"use": true, "what": true, "when": true, "where": true, "which": true,
		"with": true, "this": true, "that": true, "from": true, "have": true,
		"been": true, "will": true, "about": true, "their": true, "there": true,
		"would": true, "could": true, "should": true, "does": true, "tell": true,
		"them": true, "than": true, "then": true, "some": true, "into": true,
	}

	var words []string
	for _, word := range strings.Fields(query) {
		// Strip common punctuation
		word = strings.Trim(word, ".,;:!?\"'()[]{}—–-")
		lower := strings.ToLower(word)
		if len(word) >= 3 && !stopWords[lower] {
			words = append(words, word)
		}
	}
	return words
}

// isLowNovelty reports whether a memory carries the low_novelty marker set by
// the dream novelty audit. Falsy on missing or unparseable metadata.
func isLowNovelty(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	v, ok := m["low_novelty"]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
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

// firstTagWithPrefix returns the first tag in tags that begins with prefix,
// preserving slice order. Returns the empty string when no tag matches.
func firstTagWithPrefix(tags []string, prefix string) string {
	for _, t := range tags {
		if strings.HasPrefix(t, prefix) {
			return t
		}
	}
	return ""
}

// diversifyByTagPrefix groups passing by firstTagWithPrefix(tags, prefix),
// drops candidates with no prefix-matching tag, and round-robins across groups
// in first-seen order (preserving ranking within each group) up to limit.
func diversifyByTagPrefix(passing []RecallResult, prefix string, limit int) []RecallResult {
	if limit <= 0 || len(passing) == 0 {
		return []RecallResult{}
	}
	var groupOrder []string
	groups := make(map[string][]RecallResult)
	for _, r := range passing {
		g := firstTagWithPrefix(r.Tags, prefix)
		if g == "" {
			continue
		}
		if _, seen := groups[g]; !seen {
			groupOrder = append(groupOrder, g)
		}
		groups[g] = append(groups[g], r)
	}
	out := make([]RecallResult, 0, limit)
	for len(out) < limit {
		picked := false
		for _, g := range groupOrder {
			if len(groups[g]) == 0 {
				continue
			}
			out = append(out, groups[g][0])
			groups[g] = groups[g][1:]
			picked = true
			if len(out) >= limit {
				break
			}
		}
		if !picked {
			break
		}
	}
	return out
}

// computeCoverageGaps produces the coverage-gap list for a diversified recall,
// attributing each observed-but-absent group key to the pipeline stage where
// its last surviving candidate was dropped. Output is sorted by group key for
// deterministic responses.
func computeCoverageGaps(raw, postTag, postThreshold, returned map[string]struct{}) []CoverageGap {
	if len(raw) == 0 {
		return nil
	}
	var gaps []CoverageGap
	for g := range raw {
		if _, ok := returned[g]; ok {
			continue
		}
		cause := CoverageCauseLimit
		if _, ok := postTag[g]; !ok {
			cause = CoverageCauseTagFilter
		} else if _, ok := postThreshold[g]; !ok {
			cause = CoverageCauseThreshold
		}
		gaps = append(gaps, CoverageGap{GroupKey: g, Cause: cause})
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].GroupKey < gaps[j].GroupKey })
	return gaps
}

// prefixGroups returns the set of distinct tags-with-prefix observed across
// items, keyed by the first prefix-matching tag of each item (via tags()).
// Items whose tag list contains no prefix match contribute no key.
func prefixGroups[T any](items []T, tags func(T) []string, prefix string) map[string]struct{} {
	s := make(map[string]struct{})
	for _, it := range items {
		if g := firstTagWithPrefix(tags(it), prefix); g != "" {
			s[g] = struct{}{}
		}
	}
	return s
}

func scoredMemoryTags(c scoredMemory) []string { return c.memory.Tags }
func recallResultTags(r RecallResult) []string { return r.Tags }
