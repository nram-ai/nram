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
	"github.com/nram-ai/nram/internal/observability/metrics"
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

// VectorHydrator fetches stored embeddings by ID for the recall pipeline's
// candidate-build hydration step. Lifted as a narrow interface (separate from
// VectorSearcher) so downstream consumers that only do search (the enrichment
// dedup path, for instance) are not forced to implement GetByIDs on every
// mock. In production the same concrete vector store implements both. When
// nil on a RecallService, hydration is skipped and MMR rerank falls through
// to the no-MMR pass.
type VectorHydrator interface {
	GetByIDs(ctx context.Context, kind storage.VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error)
}

// EntityReader provides entity lookup operations.
type EntityReader interface {
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
	// GetBatch hydrates entities by ID. The cross-namespace vector-channel
	// activation in recall's graph block surfaces entity IDs from the vector
	// store and needs their name/type to populate the response graph and seed
	// traversal. Missing IDs are silently dropped; order is not preserved.
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Entity, error)
}

// RelationshipTraverser provides graph traversal from entities. maxEdges <= 0
// disables the short-circuit cap.
type RelationshipTraverser interface {
	TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops, maxEdges int) (storage.TraversalResult, error)
}

// RecallRequest contains all parameters needed to recall memories.
type RecallRequest struct {
	ProjectID uuid.UUID `json:"project_id"`
	Query     string    `json:"query"`
	Limit     int       `json:"limit"`
	// Threshold filters on the composite ranking score (the weighted sum
	// of similarity, recency, importance, frequency, graph relevance,
	// confidence, and origin; frequency and origin are zero-weighted by
	// default but operator-settable). NOT a raw vector similarity floor.
	// A composite score >= Threshold passes; rows below are dropped
	// post-ranking. Use SimilarityThreshold to filter on vector evidence
	// directly.
	Threshold float64 `json:"threshold"`
	// SimilarityThreshold is the vector-evidence cutoff (must be a finite
	// value in [0, 1]; 0 disables the filter; NaN or out-of-range returns
	// 400). SimilarityThresholdMode selects which similarity value is
	// compared:
	//
	//   "raw_cosine" (default): the raw cosine returned by the vector
	//   store before RRF. Absolute scale (compared against the embedder's
	//   cosine output directly). Only vector-channel rows are filtered;
	//   lexical-only hits and list-fallback candidates bypass.
	//
	//   "fused_combined": the post-RRF max-normalized similarity. Filters
	//   every simMap entry, including lexical-only entries that surfaced
	//   via RRF on the lexical channel (their normalized score reflects
	//   combined evidence). List-fallback candidates still bypass because
	//   they never enter simMap. Rank-relative: the
	//   top result for a given query always normalizes to 1.0, so the
	//   threshold's selectivity floats with query difficulty. Requires
	//   recall.fusion.enabled=true; combining fused_combined with a
	//   non-zero threshold while fusion is disabled returns 400.
	//
	// The mode is validated whenever set, regardless of whether
	// SimilarityThreshold is zero.
	SimilarityThreshold     float64  `json:"similarity_threshold,omitempty"`
	SimilarityThresholdMode string   `json:"similarity_threshold_mode,omitempty"`
	Tags                    []string `json:"tags"`
	IncludeGraph            bool     `json:"include_graph"`
	GraphDepth              int      `json:"graph_depth"`
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
	// AboutMeNamespaceID, when set, adds the per-user about_me (persona) namespace
	// to the recall aperture alongside the primary and global namespaces, so
	// self-knowledge surfaces by association. Mirrors GlobalNamespaceID.
	AboutMeNamespaceID *uuid.UUID `json:"-"`
}

// RecallResult holds a single recalled memory with its computed score.
type RecallResult struct {
	ID          uuid.UUID          `json:"id"`
	ProjectID   uuid.UUID          `json:"project_id"`
	ProjectSlug string             `json:"project_slug"`
	Path        string             `json:"path"`
	Content     string             `json:"content"`
	Tags        []string           `json:"tags"`
	Source      *string            `json:"source,omitempty"`
	Origin      model.MemoryOrigin `json:"origin"`
	Score       float64            `json:"score"`
	Similarity  *float64           `json:"similarity"`
	Confidence  float64            `json:"confidence"`
	AccessCount int                `json:"access_count"`
	Enriched    bool               `json:"enriched"`
	Metadata    json.RawMessage    `json:"metadata,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`

	// embedding carries the candidate's hydrated embedding through to the
	// MMR rerank stage. Unexported so JSON serialization drops it. Nil for
	// candidates whose embedding was absent at hydration (e.g. backfill not
	// yet run, or recall ran without a wired VectorHydrator); MMR treats
	// these as missing-embedding and pads them after the embedded-subset
	// rerank rather than demoting them on a similarity it cannot compute.
	embedding []float32
}

// RecallGraph holds the graph entities and relationships found during graph traversal.
type RecallGraph struct {
	Entities      []RecallEntity       `json:"entities"`
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

// SimilarityThresholdMode* selects which similarity value the vector
// filter compares against. raw_cosine drops rows whose vector-store
// cosine is below the threshold before RRF, so lexical-only hits and
// non-vector candidates are unaffected. fused_combined drops rows
// whose post-RRF max-normalized similarity is below the threshold,
// which intentionally includes lexical-only entries that surfaced via
// RRF on the lexical channel (their normalized fused score reflects
// combined evidence). List-fallback candidates still bypass under both
// modes because they never enter simMap.
const (
	SimilarityThresholdModeRawCosine     = "raw_cosine"
	SimilarityThresholdModeFusedCombined = "fused_combined"
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
// Origin is a project-affinity term that lifts candidates from the recall's
// primary project above otherwise-equivalent globals. MmrLambda is the
// redundancy-aware rerank trade-off (see mmrSelect): not a linear-combination
// weight, but it shares the cascade and override surface because operators
// tune it through the same per-project ranking_weights JSON.
type RankingWeights struct {
	Similarity     float64
	Recency        float64
	Importance     float64
	Frequency      float64
	GraphRelevance float64
	Confidence     float64
	Origin         float64
	MmrLambda      float64
}

// DefaultRankingWeights provides sensible defaults for ranking. Frequency is
// 0 because access_count already drives Confidence reinforcement; weighting
// both double-counts the same signal. Origin is 0 so upgrades preserve
// pre-origin ranking output. MmrLambda 0.75 is the conservative mild-nudge
// value (literature standard 0.7-0.8): demotes near-identical siblings without
// regressing single-fact lookups where there is no sibling to demote against.
var DefaultRankingWeights = RankingWeights{
	Similarity:     0.50,
	Recency:        0.15,
	Importance:     0.10,
	Frequency:      0.00,
	GraphRelevance: 0.20,
	Confidence:     0.05,
	Origin:         0.00,
	MmrLambda:      0.75,
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
	// When true, each channel's RRF weight is divided by its length so a
	// deep corpus does not crowd out a sparse one in the fused output.
	NormalizePerChannel bool
}

// DefaultFusionConfig ships with the feature dark — operators flip
// recall.fusion.enabled in admin settings after migration + smoke test.
// VectorWeight/LexicalWeight default to 0.60/0.40 per a synthetic controlled
// experiment (internal/service/testdata/recall_contamination/results.md,
// 2026-05-22). 60/40 widened the canonical-vs-contaminant margin over the
// prior 70/30 default without sacrificing canonical@1; 50/50 dropped
// canonical@1 on lex-vulnerable queries (keyword-stuffed noise, typo
// queries). The experiment uses simulated cosines/ranks, not a live-corpus
// A/B; operators should re-validate before adopting in production.
var DefaultFusionConfig = FusionConfig{
	Enabled:             false,
	RRFConstant:         60,
	VectorWeight:        0.60,
	LexicalWeight:       0.40,
	NormalizePerChannel: false,
}

// RecallService orchestrates memory recall with vector search, tag filtering,
// graph traversal, and multi-factor ranking.
type RecallService struct {
	memories      MemoryReader
	projects      ProjectRepository
	namespaces    NamespaceRepository
	vectorSearch  VectorSearcher
	vectors       VectorHydrator
	lexical       LexicalSearcher
	entityReader  EntityReader
	traverser     RelationshipTraverser
	embedProvider func() provider.EmbeddingProvider
	weights       RankingWeights
	fusion        FusionConfig
	// settings is optional. When nil, ranking and pagination knobs fall
	// back to the registered defaults via service.GetDefault*. Wired in
	// production via SetSettings so operators can retune recall scoring
	// (recency decay, over-fetch multiplier, default limit/depth) live.
	settings *SettingsService
	// reinforcement is optional. When nil (the default, matching all existing
	// callers), recall has no read-path write. When wired via SetReinforcement,
	// every successful recall asynchronously bumps access_count, last_accessed,
	// and confidence on the surfaced memories — the reconsolidation hook.
	reinforcement *ReinforcementDeps
	// metrics is optional. When non-nil, a successful Recall increments the
	// nram_memories_recalled_total counter.
	metrics *metrics.Metrics
}

// WithMetrics attaches the Prometheus metrics sink. Returns the same service
// for chaining at construction time.
func (s *RecallService) WithMetrics(m *metrics.Metrics) *RecallService {
	s.metrics = m
	return s
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
	embedProvider func() provider.EmbeddingProvider,
) *RecallService {
	return &RecallService{
		memories:      memories,
		projects:      projects,
		namespaces:    namespaces,
		vectorSearch:  vectorSearch,
		entityReader:  entityReader,
		traverser:     traverser,
		embedProvider: embedProvider,
		weights:       DefaultRankingWeights,
		fusion:        DefaultFusionConfig,
	}
}

// SetWeights overrides the default ranking weights.
func (s *RecallService) SetWeights(w RankingWeights) {
	s.weights = w
}

// SetSettings wires the settings service so recall scoring knobs (recency
// decay, graph hop multiplier, over-fetch shape, default limit/depth)
// resolve through the registry. Optional — when nil, the registered
// defaults apply via service.GetDefault*.
func (s *RecallService) SetSettings(svc *SettingsService) {
	s.settings = svc
}

// SetLexical wires the lexical (BM25/tsvector) searcher used by the hybrid
// recall path. Passing nil disables fusion regardless of FusionConfig.Enabled.
func (s *RecallService) SetLexical(l LexicalSearcher) {
	s.lexical = l
}

// SetVectorHydrator wires the embedding-fetch capability used by the
// candidate-build hydration step. When nil (the default), hydration is
// skipped, candidate.embedding stays nil for every row, and MMR rerank
// falls through to the no-MMR pass (every candidate looks missing-embedding).
// Production wires the same concrete vector store that backs vectorSearch.
func (s *RecallService) SetVectorHydrator(h VectorHydrator) {
	s.vectors = h
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
	isPrimary      bool
	// viaVector is true if this candidate's ID actually surfaced via the
	// vector channel (non-fusion vector search, or hybrid search where the
	// vector backend returned this ID). False for list-fallback candidates
	// that never entered simMap, AND false for lexical-only RRF hits that
	// entered simMap via the lexical channel. Drives the
	// RecallResult.Similarity pointer-vs-nil distinction so consumers can
	// tell "vector said X" from "no vector evidence at all" (the latter
	// serializes as null).
	viaVector bool
	// embedding holds the candidate's embedding vector after the candidate-build
	// phase finishes. Hydrated in one batch GetByIDs call after the tag filter
	// (recall.go: between tag filter and graph block). Nil means the embedding
	// row was absent at hydration time (e.g. the dream embedding_backfill phase
	// has not yet run on a freshly-stored memory, or the row's stored dimension
	// does not match the active embedder). Downstream stages that need an
	// embedding (e.g. mmrSelect) must handle nil gracefully.
	embedding []float32
}

// projectAttribution carries the owning project's ID and slug for a given
// namespace, so each candidate can be stamped with its actual home project
// rather than the recall's primary target.
type projectAttribution struct {
	ProjectID   uuid.UUID
	ProjectSlug string
	IsPrimary   bool
}

// Recall retrieves and ranks memories matching the given query.
func (s *RecallService) Recall(ctx context.Context, req *RecallRequest) (*RecallResponse, error) {
	start := time.Now()

	// Validate required fields.
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("query is required")
	}

	// Resolve similarity-threshold mode. An entirely missing field
	// (rawMode == "") defaults to raw_cosine. A field provided but whose
	// trimmed value is empty (e.g. "   ") is treated as a caller error,
	// not as "no value"; the caller asked for a mode by sending a visible
	// non-empty field. Trimming after the empty-vs-not check is what
	// surfaces this distinction. The mode is validated whenever set,
	// regardless of whether SimilarityThreshold is zero; an unknown mode
	// is a caller bug worth surfacing.
	rawMode := req.SimilarityThresholdMode
	simMode := strings.TrimSpace(rawMode)
	if rawMode == "" {
		simMode = SimilarityThresholdModeRawCosine
	}
	switch simMode {
	case SimilarityThresholdModeRawCosine, SimilarityThresholdModeFusedCombined:
	default:
		return nil, fmt.Errorf("invalid similarity_threshold_mode %q (allowed: %q, %q)",
			rawMode,
			SimilarityThresholdModeRawCosine,
			SimilarityThresholdModeFusedCombined,
		)
	}
	simThreshold := req.SimilarityThreshold
	if math.IsNaN(simThreshold) || simThreshold < 0 || simThreshold > 1 {
		return nil, fmt.Errorf("invalid similarity_threshold %v (must be a finite value in [0, 1])", simThreshold)
	}

	// Resolve ranking weights and fusion config once at the top of the
	// recall, so admin-UI edits to ranking.weight.* / recall.fusion.* take
	// effect on the very next call without a server restart. Every read
	// of these values downstream goes through these locals; the cached
	// s.weights / s.fusion are the fallback for the test-only path where
	// no settings service is wired.
	effWeights := s.resolveWeights(ctx)
	effFusion := s.resolveFusion(ctx)

	// fused_combined compares against post-RRF max-normalized similarity,
	// which only exists when fusion is on. Without fusion the simMap holds
	// raw cosines, and applying the fusedFloor to those values would
	// silently collapse fused_combined semantics into raw_cosine. Surface
	// the misalignment to the caller as a 400 instead of a quiet behavior
	// drift.
	if simMode == SimilarityThresholdModeFusedCombined && simThreshold > 0 && !effFusion.Enabled {
		return nil, fmt.Errorf("similarity_threshold_mode=fused_combined requires recall.fusion.enabled=true")
	}

	// Apply defaults from the registry (with in-code fallback when settings
	// hasn't been wired — preserves the test-only constructor path).
	limit := req.Limit
	if limit <= 0 {
		limit = s.recallDefaultLimit(ctx)
	}
	threshold := req.Threshold
	graphDepth := req.GraphDepth
	if graphDepth <= 0 {
		graphDepth = s.recallGraphDefaultDepth(ctx)
	}
	graphHopMultiplier := s.recallGraphHopMultiplier(ctx)
	overfetchLimit := s.recallOverfetch(ctx, limit)

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
	// mis-attributed to the search-target project. The map covers primary and
	// global namespaces. Falls back to the primary stamp when a namespace has
	// no owning project.
	projectByNamespace := map[uuid.UUID]projectAttribution{
		namespaceID: {ProjectID: projectID, ProjectSlug: projectSlug, IsPrimary: true},
	}
	for _, extraNS := range []*uuid.UUID{req.GlobalNamespaceID, req.AboutMeNamespaceID} {
		if extraNS == nil || *extraNS == namespaceID {
			continue
		}
		if p, err := s.projects.GetByNamespaceID(ctx, *extraNS); err == nil && p != nil {
			projectByNamespace[*extraNS] = projectAttribution{ProjectID: p.ID, ProjectSlug: p.Slug}
		}
	}
	attribute := func(memNs uuid.UUID) projectAttribution {
		if attr, ok := projectByNamespace[memNs]; ok {
			return attr
		}
		// Defensive fallback for any candidate whose namespace was not
		// seeded above (primary + global). All known candidate-builder
		// paths stay within that set, so this branch should not fire;
		// stamping such a row with the primary project's slug but
		// IsPrimary=false keeps origin weighting treating it as
		// non-primary rather than corrupting the primary count.
		return projectAttribution{ProjectID: projectID, ProjectSlug: projectSlug}
	}

	candidates := []scoredMemory{}

	// searchNamespaces is the recall aperture: the primary project namespace
	// plus the global and about_me namespaces when set and distinct. Lifted to
	// function scope so both the memory vector-search branch and the graph
	// block's cross-namespace vector-channel entity activation share one
	// definition of the [project, global, about_me] aperture.
	searchNamespaces := []uuid.UUID{namespaceID}
	for _, extraNS := range []*uuid.UUID{req.GlobalNamespaceID, req.AboutMeNamespaceID} {
		if extraNS != nil && *extraNS != namespaceID {
			searchNamespaces = append(searchNamespaces, *extraNS)
		}
	}

	// queryEmbeddingDim is the actual embedding dimension produced for the
	// query, lifted out of the vector-search block so the post-tag-filter
	// hydration step can fetch candidate embeddings at the same dim. Stays 0
	// when the embedding path did not run (no provider, no embedding produced,
	// or list-fallback path); in that case hydration is skipped.
	var queryEmbeddingDim int
	// queryEmbedding mirrors queryEmbeddingDim's lift: mmrSelect uses it for
	// on-the-fly relevance computation when a candidate has a hydrated
	// embedding but no Similarity pointer (lexical-only fusion hits),
	// keeping every embedded candidate on the same cosine scale. Stays nil
	// when the embedding path did not run.
	var queryEmbedding []float32

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
				// not the requested one. Some providers (e.g., Ollama)
				// ignore the dimension parameter and return their native size.
				actualDim := len(resp.Embeddings[0])
				queryEmbeddingDim = actualDim
				queryEmbedding = resp.Embeddings[0]

				// Over-fetch for re-ranking. Pool size is overfetchLimit
				// resolved once at Recall entry from the registry knobs.
				topK := overfetchLimit

				// searchNamespaces (the [project, global] aperture) is now
				// lifted to function scope above so the graph block can reuse
				// it for vector-channel entity activation.

				// rawCosineFloor is the raw-cosine cutoff applied inside the
				// vector channel before RRF / simMap insertion. Active only
				// in raw_cosine mode; fused mode applies its filter later
				// against the post-RRF simMap value.
				rawCosineFloor := 0.0
				if simMode == SimilarityThresholdModeRawCosine && simThreshold > 0 {
					rawCosineFloor = simThreshold
				}

				simMap := make(map[uuid.UUID]float64)
				// vecIDs tracks which simMap entries actually surfaced via
				// the vector channel (as opposed to lexical-only RRF hits).
				// In the non-fusion branch every entry came from
				// vectorSearch.Search, so vecIDs == simMap keys. In the
				// fusion branch runHybridSearch returns the channel-specific
				// set. viaVector on each candidate is gated on membership so
				// lexical-only fusion hits do not falsely advertise vector
				// evidence.
				vecIDs := make(map[uuid.UUID]struct{})
				if effFusion.Enabled && s.lexical != nil {
					// Hybrid path: fan out vector + lexical per namespace,
					// then fuse via RRF. The fused score (normalized to
					// [0, 1] by max) replaces raw cosine similarity in the
					// downstream computeScore. RankingWeights.Similarity
					// semantics are unchanged from the caller's view.
					simMap, vecIDs = s.runHybridSearch(ctx, runHybridArgs{
						Query:          req.Query,
						Embedding:      resp.Embeddings[0],
						Dim:            actualDim,
						Namespaces:     searchNamespaces,
						TopK:           topK,
						PrimaryNS:      namespaceID,
						PrimaryProj:    projectID,
						RawCosineFloor: rawCosineFloor,
						Fusion:         effFusion,
					})
				} else {
					for _, nsID := range searchNamespaces {
						results, err := s.vectorSearch.Search(ctx, storage.VectorKindMemory, resp.Embeddings[0], nsID, actualDim, topK)
						if err != nil {
							continue
						}
						for _, r := range results {
							// raw_cosine filter site: drop low-cosine rows
							// before they enter simMap. No lexical channel
							// in this branch, so a drop here is final. The
							// !(>=) form (rather than <) drops NaN scores,
							// which would otherwise propagate through
							// clampScore and break sort.Slice ordering.
							if rawCosineFloor > 0 && !(r.Score >= rawCosineFloor) {
								continue
							}
							// Keep the best score if a memory appears in multiple searches.
							if existing, ok := simMap[r.ID]; !ok || r.Score > existing {
								simMap[r.ID] = r.Score
							}
							vecIDs[r.ID] = struct{}{}
						}
					}
				}

				// fusedFloor is the post-RRF cutoff applied to every simMap
				// entry, including lexical-only entries that surfaced via
				// RRF on the lexical channel: their normalized fused score
				// reflects combined evidence and is in scope. Active only in
				// fused_combined mode (which requires fusion.Enabled, per
				// service validation); raw_cosine already filtered inside
				// the channel above. List-fallback candidates bypass this
				// filter because they never enter simMap.
				fusedFloor := 0.0
				if simMode == SimilarityThresholdModeFusedCombined && simThreshold > 0 {
					fusedFloor = simThreshold
				}

				// Fetch full memories. Pre-filter against fusedFloor so we
				// don't pay a DB round-trip for rows we will immediately
				// discard. The per-mem check below is retained as a
				// belt-and-suspenders defense (cheap).
				ids := make([]uuid.UUID, 0, len(simMap))
				for id, sim := range simMap {
					if fusedFloor > 0 && sim < fusedFloor {
						continue
					}
					ids = append(ids, id)
				}
				if len(ids) > 0 {
					memories, err := s.memories.GetBatch(ctx, ids)
					if err == nil {
						for _, mem := range memories {
							sim := simMap[mem.ID]
							if fusedFloor > 0 && sim < fusedFloor {
								continue
							}
							_, viaVec := vecIDs[mem.ID]
							attr := attribute(mem.NamespaceID)
							candidates = append(candidates, scoredMemory{
								memory:        mem,
								similarity:    sim,
								projectID:     attr.ProjectID,
								projectSlug:   attr.ProjectSlug,
								namespacePath: namespacePath,
								isPrimary:     attr.IsPrimary,
								viaVector:     viaVec,
							})
						}
					}
				}
			}
		}
	}

	// Fall back to listing by namespace if no embedding was used.
	if !embeddingUsed {
		if simThreshold > 0 {
			slog.Debug("recall: similarity_threshold ignored (no embedder)",
				"project_id", projectID,
				"similarity_threshold", simThreshold,
				"mode", simMode,
			)
		}
		seenIDs := make(map[uuid.UUID]bool)
		memories, err := s.memories.ListByNamespace(ctx, namespaceID, overfetchLimit, 0)
		if err == nil {
			for _, mem := range memories {
				seenIDs[mem.ID] = true
				attr := attribute(mem.NamespaceID)
				candidates = append(candidates, scoredMemory{
					memory:        mem,
					projectID:     attr.ProjectID,
					projectSlug:   attr.ProjectSlug,
					namespacePath: namespacePath,
					isPrimary:     attr.IsPrimary,
				})
			}
		}
		// Also include global and about_me namespace memories in text fallback.
		for _, extraNS := range []*uuid.UUID{req.GlobalNamespaceID, req.AboutMeNamespaceID} {
			if extraNS == nil || *extraNS == namespaceID {
				continue
			}
			extraMems, err := s.memories.ListByNamespace(ctx, *extraNS, overfetchLimit, 0)
			if err != nil {
				continue
			}
			for _, mem := range extraMems {
				if !seenIDs[mem.ID] {
					seenIDs[mem.ID] = true
					attr := attribute(mem.NamespaceID)
					candidates = append(candidates, scoredMemory{
						memory:        mem,
						projectID:     attr.ProjectID,
						projectSlug:   attr.ProjectSlug,
						namespacePath: namespacePath,
						isPrimary:     attr.IsPrimary,
					})
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

	// Hydrate candidate embeddings in one batch. Runs after the tag filter so
	// we never pay a fetch for a candidate the filter will discard. Each
	// candidate gets its stored embedding stamped onto scoredMemory.embedding;
	// downstream redundancy-aware stages (mmrSelect today, future reranks)
	// rely on this single hydration pass rather than re-fetching per stage.
	// Guarded on the active embedding dim and the wired hydrator: a list-
	// fallback recall, a recall with no embedding provider, or a service
	// constructed without SetVectorHydrator all skip hydration entirely and
	// MMR falls through to the no-MMR pass. A candidate whose row is absent
	// from the returned map (no embedding stored, or stored at a different
	// dim) keeps its zero-value embedding slice.
	if queryEmbeddingDim > 0 && s.vectors != nil && len(candidates) > 0 {
		ids := make([]uuid.UUID, len(candidates))
		for i, c := range candidates {
			ids[i] = c.memory.ID
		}
		got, err := s.vectors.GetByIDs(ctx, storage.VectorKindMemory, ids, queryEmbeddingDim)
		if err != nil {
			// Hydration failure is not fatal: candidate.embedding stays nil
			// for every row, mmrSelect's len(embedded) < 2 fast path engages,
			// and the recall returns composite-only ranking. Log so a backend
			// outage that silently disables MMR rerank is observable rather
			// than hidden inside a vector-store-error log under a different
			// subsystem. Matches the slog.Debug bypass pattern used earlier
			// in this function for the list-fallback similarity_threshold
			// bypass.
			slog.Warn("recall: embedding hydration failed; MMR rerank will bypass",
				"project_id", projectID,
				"candidates", len(candidates),
				"dim", queryEmbeddingDim,
				"err", err,
			)
		} else {
			for i := range candidates {
				if vec, ok := got[candidates[i].memory.ID]; ok {
					candidates[i].embedding = vec
				}
			}
		}
	}

	// Graph traversal if requested.
	graphEntities := []RecallEntity{}
	graphRelationships := []RecallRelationship{}
	// graphRelRefs parallels graphRelationships with (id, namespace) pairs
	// for the relationship-reinforcement hook; the JSON projection drops both.
	var graphRelRefs []RelationshipRef
	if req.IncludeGraph && s.entityReader != nil && s.traverser != nil {
		// Search for entities related to the query using multiple strategies:
		// 1. Full query string match
		// 2. Individual significant words (3+ chars) from the query
		seenEntityIDs := make(map[uuid.UUID]bool)
		var foundEntities []model.Entity

		// Strategy 1: full query match
		if ents, err := s.entityReader.FindBySimilarity(ctx, namespaceID, req.Query, "", 10); err == nil {
			foundEntities = addNewEntities(seenEntityIDs, foundEntities, ents)
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
				foundEntities = addNewEntities(seenEntityIDs, foundEntities, ents)
			}
		}

		// Strategy 3 (vector channel): activate entities by vector similarity
		// across the [project, global] aperture, in addition to the lexical
		// name match scoped to the primary namespace above. This is the
		// cross-namespace association layer: the same entity embeds
		// near-identically across namespaces, so an entity surfaced here can
		// boost a connected memory in a different tier even when no lexical
		// match existed. Tracked separately from the lexical hits so the two
		// channels interleave fairly into the edge budget below. Fail-soft at
		// every step — a vector or hydration error drops back to lexical-only
		// activation and never fails the recall. seenEntityIDs dedups against
		// the lexical hits so an entity found by both channels is seeded once.
		// The enabled switch is nil-safe (test constructors that leave
		// s.settings unset fall through to the registered default of true);
		// topk is resolved at point of use inside the channel.
		var vectorEntities []model.Entity
		if s.settings.ResolveBoolWithDefault(ctx, SettingRecallGraphVectorActivationEnabled, "global") &&
			embeddingUsed && queryEmbeddingDim > 0 && s.vectorSearch != nil {
			vecActTopK := s.settings.ResolveIntWithDefault(ctx, SettingRecallGraphVectorActivationTopK, "global")
			for _, nsID := range searchNamespaces {
				res, err := s.vectorSearch.Search(ctx, storage.VectorKindEntity, queryEmbedding, nsID, queryEmbeddingDim, vecActTopK)
				if err != nil {
					continue // fail-soft: skip this namespace, keep lexical hits
				}
				ids := make([]uuid.UUID, 0, len(res))
				for _, r := range res {
					if !seenEntityIDs[r.ID] {
						ids = append(ids, r.ID)
					}
				}
				if len(ids) == 0 {
					continue
				}
				ents, err := s.entityReader.GetBatch(ctx, ids)
				if err != nil {
					continue // fail-soft: skip this namespace, keep lexical hits
				}
				vectorEntities = addNewEntities(seenEntityIDs, vectorEntities, ents)
			}
		}

		// Interleave lexical and vector hits (both already deduped via
		// seenEntityIDs) so neither channel monopolizes the per-seed edge
		// budget: seed order alternates lexical, vector, lexical, vector, ...
		foundEntities = interleaveEntities(foundEntities, vectorEntities)

		if len(foundEntities) > 0 {
			// Build set of memory IDs connected via graph.
			graphMemoryRelevance := make(map[uuid.UUID]float64)
			// Dedup; also the per-recall throttle for relationship reinforcement.
			seenRels := make(map[uuid.UUID]struct{})

			// Per-recall traversal edge budget, decoupled from graph.max_edges
			// (the visualization-endpoint cap). Resolved here at point of use;
			// nil-safe (test constructors that leave s.settings unset fall
			// through to the registered default). Split into per-seed fair
			// shares so a hot first seed cannot drain the whole budget before
			// later seeds (e.g. a cross-tier entity surfaced by the vector
			// channel) get to traverse — the starvation the all-remaining cap
			// allowed. The aggregate break below is kept as the belt-and-
			// suspenders ceiling so a single hot anchor still cannot blow past
			// recallMaxEdges in total.
			recallMaxEdges := s.settings.ResolveIntWithDefault(ctx, SettingRecallGraphMaxEdges, "global")
			perSeed := recallMaxEdges
			if recallMaxEdges > 0 {
				perSeed = max(1, ceilDiv(recallMaxEdges, len(foundEntities)))
			}

		recallSeeds:
			for _, ent := range foundEntities {
				graphEntities = append(graphEntities, RecallEntity{
					ID:         ent.ID,
					Name:       ent.Name,
					EntityType: ent.EntityType,
				})

				tr, err := s.traverser.TraverseFromEntity(ctx, ent.ID, graphDepth, perSeed)
				if err == nil {
					for _, rel := range tr.Relationships {
						if _, seen := seenRels[rel.ID]; !seen {
							seenRels[rel.ID] = struct{}{}
							graphRelationships = append(graphRelationships, RecallRelationship{
								ID:       rel.ID,
								SourceID: rel.SourceID,
								TargetID: rel.TargetID,
								Relation: rel.Relation,
								Weight:   rel.Weight,
							})
							graphRelRefs = append(graphRelRefs, RelationshipRef{
								ID:          rel.ID,
								NamespaceID: rel.NamespaceID,
							})
						}
						if rel.SourceMemory != nil {
							// Compute graph relevance: hop_multiplier * weight.
							// hop_multiplier defaults to 0.5 (the historical
							// 1.0/2.0 = "approximate hops as 1") and is
							// operator-tunable via ranking.graph.hop_multiplier.
							relevance := graphHopMultiplier * rel.Weight
							if existing, ok := graphMemoryRelevance[*rel.SourceMemory]; !ok || relevance > existing {
								graphMemoryRelevance[*rel.SourceMemory] = relevance
							}
						}
						if recallMaxEdges > 0 && len(graphRelationships) >= recallMaxEdges {
							break recallSeeds
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
	recencyDecayPerHour := s.recallRecencyDecay(ctx)

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
	// so cross-project recall (globals) gets each row's owner's tuning rather
	// than the requester's. The global baseline
	// (effWeights) was resolved once at the top of Recall, so admin-UI
	// changes to ranking.weight.* apply to this same call; project overrides
	// continue to merge on top per the existing precedent. Cache lifetime is
	// one Recall call.
	weightsByProject := make(map[uuid.UUID]RankingWeights, 4)
	weightsForProject := func(projID uuid.UUID) RankingWeights {
		if w, ok := weightsByProject[projID]; ok {
			return w
		}
		merged := effWeights
		if projID != uuid.Nil && s.projects != nil {
			if proj, err := s.projects.GetByID(ctx, projID); err == nil && proj != nil {
				var settings struct {
					RankingWeights json.RawMessage `json:"ranking_weights"`
				}
				if len(proj.Settings) > 0 {
					_ = json.Unmarshal(proj.Settings, &settings)
				}
				if ov, perr := ParseRankingOverride(settings.RankingWeights); perr == nil {
					merged = MergeWeights(effWeights, ov)
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
		si := computeScore(candidates[i], weightsForProject(candidates[i].projectID), now, maxAccess, recencyDecayPerHour)
		sj := computeScore(candidates[j], weightsForProject(candidates[j].projectID), now, maxAccess, recencyDecayPerHour)
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
		// isLowNovelty stays gated on dream origin because the metadata key is
		// only written by the dream novelty audit. Callers can opt into the
		// demoted set via IncludeLowNovelty for inspection/debugging.
		if !req.IncludeLowNovelty && c.memory.IsDream() {
			if isLowNovelty(c.memory.Metadata) {
				continue
			}
		}

		score := computeScore(c, weightsForProject(c.projectID), now, maxAccess, recencyDecayPerHour)
		if score < threshold {
			continue
		}

		// Gate the Similarity pointer on the candidate's actual vector
		// provenance (c.viaVector), not on the global embeddingUsed flag.
		// A list-fallback row appearing in the same recall as a
		// vector-channel row would otherwise report similarity=0.0 and be
		// indistinguishable from a vector row whose cosine genuinely was
		// zero.
		var sim *float64
		if c.viaVector {
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
			Origin:      c.memory.Origin,
			Score:       score,
			Similarity:  sim,
			Confidence:  c.memory.Confidence,
			AccessCount: c.memory.AccessCount,
			Enriched:    c.memory.Enriched,
			Metadata:    c.memory.Metadata,
			CreatedAt:   c.memory.CreatedAt,
			UpdatedAt:   c.memory.UpdatedAt,
			embedding:   c.embedding,
		})
	}

	// MMR redundancy-aware rerank between threshold filtering and final-select.
	// Reorders passing without truncating: final-select (tag-prefix round-
	// robin or plain slice) is the truncation stage and needs every candidate
	// available for its own logic. Missing-embedding rows stay anchored to
	// their composite-rank position inside mmrSelect, so a high-composite
	// lexical-only or unbackfilled hit is not demoted; only the embedded
	// subset gets reordered. Because no candidate is dropped, the set of
	// tag-prefix groups is preserved across this stage — no post-MMR
	// coverage_gaps attribution is required. Fast paths bypass when lambda is
	// at the disabling edges (>= 1.0 or <= 0.0) or fewer than two embedded
	// candidates exist; see mmrSelect. Lambda is taken from the primary
	// project's effective ranking weights so cross-project recalls apply one
	// trade-off across the whole result set rather than mixing per-candidate
	// weights.
	mmrLambda := weightsForProject(projectID).MmrLambda
	passing = mmrSelect(passing, queryEmbedding, mmrLambda, len(passing))

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

	// seenRels above guarantees graphRelRefs holds one entry per edge
	// surfaced in this call — the per-relationship throttle.
	if s.reinforcement != nil && len(graphRelRefs) > 0 {
		refs := make([]RelationshipRef, len(graphRelRefs))
		copy(refs, graphRelRefs)
		go func(refs []RelationshipRef) {
			defer func() { _ = recover() }()
			s.reinforceRels(context.Background(), refs)
		}(refs)
	}

	latency := time.Since(start).Milliseconds()

	if s.metrics != nil {
		s.metrics.IncMemoriesRecalled()
	}

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
	// RawCosineFloor, when > 0, drops vector-channel rows whose raw cosine
	// is below the floor before they enter RRF. Lexical-channel rows are
	// not touched. Zero disables the filter.
	RawCosineFloor float64
	// Fusion carries the resolved FusionConfig for this recall — passed in
	// rather than read from s.fusion inside runHybridSearch so the caller
	// can resolve once per Recall and feed the same value into every
	// downstream consumer. Live admin-UI edits to recall.fusion.* therefore
	// take effect on the next recall without a server restart.
	Fusion FusionConfig
}

// runHybridSearch returns a simMap normalized to [0, 1] (so the caller can
// drop it into scoredMemory.similarity unchanged) plus the set of IDs that
// surfaced via the vector channel. The vecIDs set lets the caller tell true
// vector-evidence rows from lexical-only RRF entries when populating
// scoredMemory.viaVector.
//
// Both channels' errors are swallowed by design: a vector hiccup or
// unparseable lexical query must not strand a recall that the other
// channel can still serve.
func (s *RecallService) runHybridSearch(ctx context.Context, args runHybridArgs) (map[uuid.UUID]float64, map[uuid.UUID]struct{}) {
	// channelResult pairs the (possibly filtered) ranks with the channel's
	// pre-filter length, so NormalizePerChannel can divide by the channel's
	// natural depth rather than the post-RawCosineFloor survivor count.
	// Without preLen the filter would shrink the divisor and inflate every
	// surviving row's per-row weight.
	type channelResult struct {
		ranks  []storage.MemoryRank
		preLen int
	}

	var (
		mu          sync.Mutex
		vecRankings []channelResult
		lexRankings []channelResult
	)

	g, gctx := errgroup.WithContext(ctx)
	for _, nsID := range args.Namespaces {
		g.Go(func() error {
			results, err := s.vectorSearch.Search(gctx, storage.VectorKindMemory, args.Embedding, nsID, args.Dim, args.TopK)
			if err != nil {
				return nil
			}
			preLen := len(results)
			ranks := make([]storage.MemoryRank, 0, preLen)
			for _, r := range results {
				// raw_cosine filter site for the fusion path: drop rows
				// below the floor before they enter RRF. A dropped row can
				// still appear via the lexical channel; that's the whole
				// point of filtering at this site rather than downstream.
				// The !(>=) form drops NaN scores so they cannot propagate
				// into RRF or sort.Slice.
				if args.RawCosineFloor > 0 && !(r.Score >= args.RawCosineFloor) {
					continue
				}
				ranks = append(ranks, storage.MemoryRank{ID: r.ID, Rank: r.Score})
			}
			mu.Lock()
			vecRankings = append(vecRankings, channelResult{ranks: ranks, preLen: preLen})
			mu.Unlock()
			return nil
		})
		g.Go(func() error {
			ranks, _ := s.lexical.SearchByText(gctx, nsID, args.Query, args.TopK)
			mu.Lock()
			lexRankings = append(lexRankings, channelResult{ranks: ranks, preLen: len(ranks)})
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	// Compose the ranking list and per-list weights for RRF. Each
	// per-namespace list contributes independently: if a memory shows up
	// in primary's vector list and global's lexical list, both
	// contributions accumulate.
	allRankings := make([][]storage.MemoryRank, 0, len(vecRankings)+len(lexRankings))
	allWeights := make([]float64, 0, len(vecRankings)+len(lexRankings))
	var vecCount, lexCount int
	vecIDs := make(map[uuid.UUID]struct{})
	for _, cr := range vecRankings {
		w := args.Fusion.VectorWeight
		if args.Fusion.NormalizePerChannel && cr.preLen > 0 {
			w = w / float64(cr.preLen)
		}
		allRankings = append(allRankings, cr.ranks)
		allWeights = append(allWeights, w)
		vecCount += len(cr.ranks)
		for _, m := range cr.ranks {
			vecIDs[m.ID] = struct{}{}
		}
	}
	overlap := 0
	for _, cr := range lexRankings {
		w := args.Fusion.LexicalWeight
		if args.Fusion.NormalizePerChannel && cr.preLen > 0 {
			w = w / float64(cr.preLen)
		}
		allRankings = append(allRankings, cr.ranks)
		allWeights = append(allWeights, w)
		lexCount += len(cr.ranks)
		for _, m := range cr.ranks {
			if _, ok := vecIDs[m.ID]; ok {
				overlap++
			}
		}
	}

	fused := ReciprocalRankFusion(allRankings, args.Fusion.RRFConstant, allWeights)

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
	// Loud failure path for misconfigured fusion: when fused has entries
	// but maxScore is zero, every RRF score collapsed to zero (typically
	// both weights are zero, or RRF degenerated). The simMap is empty,
	// embeddingUsed is still true, so the caller would silently return zero
	// candidates with no operator signal.
	if len(fused) > 0 && maxScore <= 0 {
		slog.Warn("recall: fusion produced zero max score; check fusion weights",
			"fused_count", len(fused),
			"vector_weight", args.Fusion.VectorWeight,
			"lexical_weight", args.Fusion.LexicalWeight,
			"namespace_id", args.PrimaryNS,
			"project_id", args.PrimaryProj,
		)
	}

	return simMap, vecIDs
}

// addNewEntities appends entities from src to dst, skipping any whose ID is
// already in seen (and marking newly-added IDs in seen). Shared by the three
// recall entity-discovery channels (full-query lexical, per-word lexical, and
// the cross-namespace vector channel) so dedup semantics live in one place.
func addNewEntities(seen map[uuid.UUID]bool, dst, src []model.Entity) []model.Entity {
	for _, e := range src {
		if !seen[e.ID] {
			seen[e.ID] = true
			dst = append(dst, e)
		}
	}
	return dst
}

// ceilDiv returns ceil(a/b) for positive integers. Used to split the recall
// edge budget into per-seed fair shares; b is guarded > 0 by the caller.
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}

// interleaveEntities alternates two already-deduped entity slices (lexical
// hits first at each round) so neither channel monopolizes the per-seed edge
// budget. Order within each slice is preserved. Either slice may be empty.
func interleaveEntities(lexical, vector []model.Entity) []model.Entity {
	if len(vector) == 0 {
		return lexical
	}
	if len(lexical) == 0 {
		return vector
	}
	out := make([]model.Entity, 0, len(lexical)+len(vector))
	for i := 0; i < len(lexical) || i < len(vector); i++ {
		if i < len(lexical) {
			out = append(out, lexical[i])
		}
		if i < len(vector) {
			out = append(out, vector[i])
		}
	}
	return out
}

// computeScore calculates the composite ranking score for a candidate.
// recencyDecayPerHour drives the exp(-rate * hours_since_creation) term;
// the registered default is 0.01 (~69h half-life).
func computeScore(c scoredMemory, w RankingWeights, now time.Time, maxAccess int, recencyDecayPerHour float64) float64 {
	hoursSinceCreation := now.Sub(c.memory.CreatedAt).Hours()
	recencyScore := math.Exp(-recencyDecayPerHour * hoursSinceCreation)

	var frequencyScore float64
	if maxAccess > 0 {
		frequencyScore = math.Log(1+float64(c.memory.AccessCount)) / math.Log(1+float64(maxAccess))
	}

	var originScore float64
	if c.isPrimary {
		originScore = 1.0
	}

	return w.Similarity*clampScore(c.similarity) +
		w.Recency*recencyScore +
		w.Importance*c.memory.Importance +
		w.Frequency*frequencyScore +
		w.GraphRelevance*c.graphRelevance +
		w.Confidence*clampScore(c.memory.Confidence) +
		w.Origin*originScore
}

// recallDefaultLimit returns the default page size when the caller passes
// limit <= 0. Falls back to the registered default when settings is nil.
func (s *RecallService) recallDefaultLimit(ctx context.Context) int {
	if s.settings == nil {
		return GetDefaultInt(SettingRecallDefaultLimit)
	}
	return s.settings.ResolveIntWithDefault(ctx, SettingRecallDefaultLimit, "global")
}

// recallGraphDefaultDepth returns the default graph traversal depth when
// the caller passes graph_depth <= 0.
func (s *RecallService) recallGraphDefaultDepth(ctx context.Context) int {
	if s.settings == nil {
		return GetDefaultInt(SettingRecallGraphDefaultDepth)
	}
	return s.settings.ResolveIntWithDefault(ctx, SettingRecallGraphDefaultDepth, "global")
}

// recallRecencyDecay returns the per-hour decay rate for the recency term.
func (s *RecallService) recallRecencyDecay(ctx context.Context) float64 {
	if s.settings == nil {
		return GetDefaultFloat(SettingRankingRecencyDecayPerHour)
	}
	return s.settings.ResolveFloatWithDefault(ctx, SettingRankingRecencyDecayPerHour, "global")
}

// recallGraphHopMultiplier returns the per-hop multiplier applied to the
// graph-traversal contribution. Default 0.5 matches the historical 1.0/2.0
// approximation when hops≈1.
func (s *RecallService) recallGraphHopMultiplier(ctx context.Context) float64 {
	if s.settings == nil {
		return GetDefaultFloat(SettingRankingGraphHopMultiplier)
	}
	return s.settings.ResolveFloatWithDefault(ctx, SettingRankingGraphHopMultiplier, "global")
}

// recallOverfetch sizes the candidate pool the score-and-rerank pass
// selects from: limit * overfetch_multiplier, floored at overfetch_min.
func (s *RecallService) recallOverfetch(ctx context.Context, limit int) int {
	mul := GetDefaultFloat(SettingRecallOverfetchMultiplier)
	floor := GetDefaultInt(SettingRecallOverfetchMin)
	if s.settings != nil {
		mul = s.settings.ResolveFloatWithDefault(ctx, SettingRecallOverfetchMultiplier, "global")
		floor = s.settings.ResolveIntWithDefault(ctx, SettingRecallOverfetchMin, "global")
	}
	out := max(int(math.Round(float64(limit)*mul)), floor)
	return out
}

// resolveWeights returns the global baseline RankingWeights, reading each
// ranking.weight.* key live from the settings registry so admin-UI edits
// take effect on the next recall. Falls back to s.weights (the pinned
// default or test-supplied override) when the settings service is not
// wired (test-only constructor path) or when a specific key is missing
// or out of range.
func (s *RecallService) resolveWeights(ctx context.Context) RankingWeights {
	if s.settings == nil {
		return s.weights
	}
	d := s.weights
	return RankingWeights{
		Similarity:     s.settings.ResolveFloatInRange(ctx, SettingRankWeightSim, "global", 0, 1, d.Similarity),
		Recency:        s.settings.ResolveFloatInRange(ctx, SettingRankWeightRec, "global", 0, 1, d.Recency),
		Importance:     s.settings.ResolveFloatInRange(ctx, SettingRankWeightImp, "global", 0, 1, d.Importance),
		Frequency:      s.settings.ResolveFloatInRange(ctx, SettingRankWeightFreq, "global", 0, 1, d.Frequency),
		GraphRelevance: s.settings.ResolveFloatInRange(ctx, SettingRankWeightGraph, "global", 0, 1, d.GraphRelevance),
		Confidence:     s.settings.ResolveFloatInRange(ctx, SettingRankWeightConf, "global", 0, 1, d.Confidence),
		Origin:         s.settings.ResolveFloatInRange(ctx, SettingRankWeightOrigin, "global", 0, 1, d.Origin),
		MmrLambda:      s.settings.ResolveFloatInRange(ctx, SettingRankWeightMmr, "global", 0, 1, d.MmrLambda),
	}
}

// resolveFusion returns the FusionConfig used by the current recall, reading
// each recall.fusion.* key live from the settings registry. Falls back to
// s.fusion (the pinned default or test-supplied override) when the settings
// service is not wired. Mirrors loadFusionConfig's per-key guard rails:
// negative weights and non-positive RRF constants are ignored.
func (s *RecallService) resolveFusion(ctx context.Context) FusionConfig {
	if s.settings == nil {
		return s.fusion
	}
	cfg := s.fusion
	cfg.Enabled = s.settings.ResolveBool(ctx, SettingRecallFusionEnabled, "global")
	if k, err := s.settings.ResolveInt(ctx, SettingRecallFusionK, "global"); err == nil && k > 0 {
		cfg.RRFConstant = k
	}
	if w, err := s.settings.ResolveFloat(ctx, SettingRecallFusionVecW, "global"); err == nil && w >= 0 {
		cfg.VectorWeight = w
	}
	if w, err := s.settings.ResolveFloat(ctx, SettingRecallFusionLexW, "global"); err == nil && w >= 0 {
		cfg.LexicalWeight = w
	}
	cfg.NormalizePerChannel = s.settings.ResolveBool(ctx, SettingRecallFusionNormalizePerChan, "global")
	return cfg
}

// clampScore ensures a score is in the [0, 1] range. NaN inputs collapse to
// 0 so downstream sort.Slice and composite arithmetic stay well-defined even
// if a future caller forgets to filter NaN at its source.
func clampScore(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
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
	for word := range strings.FieldsSeq(query) {
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
	var m map[string]any
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
// its last surviving candidate was dropped. Stages, in pipeline order: tag
// filter, threshold filter, final-limit truncation. A group present in `raw`
// but missing from `returned` is attributed to the earliest stage that drops
// it. MMR is not represented because it reorders without dropping; groups
// surviving the threshold stage and missing from `returned` are bucketed
// under the final-limit truncation. Output is sorted by group key for
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
