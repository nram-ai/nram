package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ErrAskProviderUnconfigured is returned when the ask feature is enabled but
// its dedicated synthesis provider slot has not been configured. The slot has
// no fallback (see provider.SlotAsk) precisely so ask traffic never lands on
// the enrichment/fact provider; an unconfigured slot is a clear, actionable
// error rather than a silent reroute.
var ErrAskProviderUnconfigured = errors.New("ask: synthesis provider not configured")

// askNotInNeighborhood is the exact sentinel the synthesizer is told to return
// when the neighborhood lacks the answer. It is also the normalized response for
// any answer that cited no neighborhood memory at all (ungrounded prose:
// confabulation or an injected instruction), so such text is never surfaced.
const askNotInNeighborhood = "Not in neighborhood."

// askTraverseMaxEdges bounds the per-seed graph traversal the ask neighborhood
// builder runs. Generous relative to the neighborhood cap (which trims the
// result) but finite so a dense entity cannot stall synthesis.
const askTraverseMaxEdges = 500

// askCitationRe matches inline memory-id citations the synthesizer emits, e.g.
// "[a1b2c3d4]". Used by the post-synthesis source-id validation to strip any
// cited id that is not a real neighborhood member (prompt-injection / model
// hallucination mitigation).
var askCitationRe = regexp.MustCompile(`\[([0-9a-fA-F]{8,})\]`)

// askExtraSpaceRe collapses runs of spaces/tabs (but not newlines) left behind
// when a hallucinated citation is stripped from the answer.
var askExtraSpaceRe = regexp.MustCompile(`[ \t]{2,}`)

// recaller is the narrow recall surface the ask service drives. *RecallService
// satisfies it; the interface lets the orchestration be unit-tested with a fake
// recall without standing up the full ranking pipeline.
type recaller interface {
	Recall(ctx context.Context, req *RecallRequest) (*RecallResponse, error)
}

// AskProjectRepo is the narrow project-repository surface the ask service
// needs: resolve reserved tiers and a scoped project by slug, map a namespace
// back to its project for attribution, and enumerate every project the caller
// owns to build the wide aperture. *storage.ProjectRepo satisfies it.
type AskProjectRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.Project, error)
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
	ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error)
}

// AskRequest is the input to AskService.Ask.
type AskRequest struct {
	Query string
	// ProjectSlug narrows the aperture. Empty means wide: synthesize over every
	// project the caller owns plus global and about_me. Non-empty scopes to that
	// project plus global and about_me, exactly like the ordinary recall path.
	ProjectSlug string
	// OwnerNamespaceID is the caller's owner (user) namespace. ListByUser and
	// GetBySlug are scoped to it, so the wide aperture is structurally
	// same-user only and can never reach another user's data.
	OwnerNamespaceID uuid.UUID
	// OrgID stamps the synthesis call's token_usage row so it is attributed to
	// the caller's org and surfaces in the org-scoped analytics. Without it the
	// row records a NULL org_id and is filtered out of the analytics endpoint.
	OrgID    uuid.UUID
	UserID   *uuid.UUID
	APIKeyID *uuid.UUID
	// ShareScoped marks the caller as a share-bearer. In this mode the aperture
	// is restricted to exactly ShareProjectIDs: the owner's global and about_me
	// tiers are never searched, ListByUser is never called, and a supplied
	// ProjectSlug must resolve to one of these granted projects.
	ShareScoped bool
	// ShareProjectIDs is the set of projects the share grants read access on.
	// Only consulted when ShareScoped is true.
	ShareProjectIDs []uuid.UUID
}

// AskSource is one source memory in the lean provenance contract. When the
// synthesis cited sources, Citation is the footnote number ([1], [2], …) that
// appears inline in the answer and orders this list; on the uncited fallback
// (synthesis failure or "not in neighborhood") Citation is 0 and the list is
// the retrieved candidates instead.
type AskSource struct {
	MemoryID    uuid.UUID `json:"memory_id"`
	ProjectSlug string    `json:"project_slug"`
	// Score is the source's absolute vector cosine to the query, present only
	// for recall (vector-channel) sources. Nil and omitted for sources that
	// entered via graph or sibling expansion (structurally related, not directly
	// query-matched) — previously those serialized a misleading 0.000.
	Score    *float64 `json:"score,omitempty"`
	Citation int      `json:"citation,omitempty"`
}

// AskSynthesisMeta is the minimal synthesis metadata returned alongside an
// answer. Deliberately small to conserve MCP response budget: no model name or
// prompt version (the configured model is visible on the Providers page).
type AskSynthesisMeta struct {
	LatencyMs        int64 `json:"latency_ms"`
	NeighborhoodSize int   `json:"neighborhood_size"`
	SynthesisFailed  bool  `json:"synthesis_failed,omitempty"`
}

// AskResponse is the lean-provenance result of a single-shot synthesis.
type AskResponse struct {
	Answer        string           `json:"answer"`
	Sources       []AskSource      `json:"sources"`
	Confidence    float64          `json:"confidence"`
	SynthesisMeta AskSynthesisMeta `json:"synthesis_meta"`
}

// AskService orchestrates the ask tool: it runs recall over the resolved
// aperture, expands the top candidates with graph-connected and sibling
// memories into a bounded neighborhood, and makes one LLM call to synthesize an
// answer with inline source citations.
type AskService struct {
	recall    recaller
	memories  MemoryReader
	projects  AskProjectRepo
	traverser RelationshipTraverser
	llm       func() provider.LLMProvider
	settings  *SettingsService
	metrics   *metrics.Metrics
	// vectors hydrates stored embeddings by id so graph- and sibling-expanded
	// candidates can be relevance-gated against the query embedding before they
	// enter the neighborhood. Nil disables expansion (the connected memories
	// cannot be vouched for, so none are admitted) rather than letting
	// query-blind expansion dilute the synthesis.
	vectors VectorHydrator
}

// NewAskService constructs an AskService. llm re-reads the registry on every
// call so a live provider edit (registry.Reload) is picked up without restart;
// it returns nil when the dedicated ask slot is unconfigured.
func NewAskService(
	recall recaller,
	memories MemoryReader,
	projects AskProjectRepo,
	traverser RelationshipTraverser,
	llm func() provider.LLMProvider,
	settings *SettingsService,
) *AskService {
	return &AskService{
		recall:    recall,
		memories:  memories,
		projects:  projects,
		traverser: traverser,
		llm:       llm,
		settings:  settings,
	}
}

// WithMetrics attaches the metrics sink and returns the service for chaining.
func (s *AskService) WithMetrics(m *metrics.Metrics) *AskService {
	s.metrics = m
	return s
}

// WithVectorHydrator wires the embedding-fetch capability used to relevance-gate
// graph- and sibling-expanded candidates. When unset, those expansions are
// skipped (only recall candidates form the neighborhood).
func (s *AskService) WithVectorHydrator(v VectorHydrator) *AskService {
	s.vectors = v
	return s
}

// neighborMemory is one packed memory in the synthesis neighborhood.
type neighborMemory struct {
	shortID     string
	memoryID    uuid.UUID
	content     string
	projectSlug string
}

// Ask runs the full ask pipeline. It returns ErrAskProviderUnconfigured when
// the dedicated synthesis slot is unset. A synthesizer failure is NOT an error:
// it returns the recalled sources with SynthesisMeta.SynthesisFailed=true and
// an empty answer, so callers always get usable provenance.
func (s *AskService) Ask(ctx context.Context, req *AskRequest) (*AskResponse, error) {
	start := time.Now()

	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("ask: query is required")
	}
	var llm provider.LLMProvider
	if s.llm != nil {
		llm = s.llm()
	}
	if llm == nil {
		return nil, ErrAskProviderUnconfigured
	}

	// --- Resolve the aperture -------------------------------------------------
	// am caches every project the aperture enumerates: nsByProjectID resolves a
	// recall candidate's home namespace for the sibling pull, and slugByNS gives
	// a graph-backing memory its project slug for attribution without a re-fetch.
	am := newApertureMaps()

	var globalNS, aboutMeNS *uuid.UUID
	var primaryProjectID, primaryNS uuid.UUID
	var apertureNS []uuid.UUID
	var err error

	if req.ShareScoped {
		// Share-bearer: the aperture is restricted to exactly the projects the
		// share grants read on. The owner's global and about_me tiers are NEVER
		// searched, ListByUser is NEVER called (it would enumerate the owner's
		// full project set), and a supplied project must be one of the grants.
		primaryProjectID, primaryNS, apertureNS, err = s.resolveShareAperture(ctx, req, am)
	} else {
		globalNS, aboutMeNS = s.reservedNamespaces(ctx, req.OwnerNamespaceID)
		primaryProjectID, primaryNS, apertureNS, err = s.resolveOwnerAperture(ctx, req, globalNS, am)
	}
	if err != nil {
		return nil, err
	}
	if primaryNS == uuid.Nil {
		return nil, fmt.Errorf("ask: could not resolve a recall aperture")
	}

	// --- Recall over the aperture --------------------------------------------
	candidates := s.settings.ResolveIntWithDefault(ctx, SettingAskRecallCandidates, "global")
	graphDepth := s.settings.ResolveIntWithDefault(ctx, SettingAskGraphDepth, "global")
	siblings := s.settings.ResolveIntWithDefault(ctx, SettingAskSiblingsPerCandidate, "global")
	maxMemories := s.settings.ResolveIntWithDefault(ctx, SettingAskNeighborhoodMaxMemories, "global")

	rr := &RecallRequest{
		ProjectID:            primaryProjectID,
		Query:                req.Query,
		Limit:                candidates,
		IncludeGraph:         true,
		GraphDepth:           graphDepth,
		UserID:               req.UserID,
		OrgID:                req.OrgID,
		APIKeyID:             req.APIKeyID,
		ApertureNamespaceIDs: apertureNS,
	}
	// In the wide owner aperture the primary is the reserved global tier, used
	// only as a structural search seed; do not origin-boost it, so world
	// knowledge does not outrank the caller's own project and persona memories
	// or crowd the neighborhood. A scoped query keeps its chosen project boosted.
	rr.DemotePrimaryOrigin = globalNS != nil && primaryNS == *globalNS
	if globalNS != nil && *globalNS != primaryNS {
		rr.GlobalNamespaceID = globalNS
	}
	if aboutMeNS != nil && *aboutMeNS != primaryNS {
		rr.AboutMeNamespaceID = aboutMeNS
	}
	recallResp, err := s.recall.Recall(ctx, rr)
	if err != nil {
		return nil, fmt.Errorf("ask: recall: %w", err)
	}

	// The full namespace set recall searched, reused for graph traversal.
	searchNS := []uuid.UUID{primaryNS}
	seenNS := map[uuid.UUID]bool{primaryNS: true}
	for _, ns := range []*uuid.UUID{globalNS, aboutMeNS} {
		if ns != nil && !seenNS[*ns] {
			seenNS[*ns] = true
			searchNS = append(searchNS, *ns)
		}
	}
	for _, ns := range apertureNS {
		if !seenNS[ns] {
			seenNS[ns] = true
			searchNS = append(searchNS, ns)
		}
	}

	// --- Assemble the neighborhood -------------------------------------------
	neighborhood := []neighborMemory{}
	seenMem := map[uuid.UUID]bool{}
	addMem := func(id uuid.UUID, content, slug string) {
		if id == uuid.Nil || seenMem[id] || strings.TrimSpace(content) == "" {
			return
		}
		if len(neighborhood) >= maxMemories {
			return
		}
		seenMem[id] = true
		neighborhood = append(neighborhood, neighborMemory{
			shortID:     askShortID(id, seenMem),
			memoryID:    id,
			content:     content,
			projectSlug: slug,
		})
	}

	// 1. Primary: the recall candidates, relevance-floored. Recall returns up to
	//    its limit even when the tail is weak, and its fused ranking can float a
	//    high-importance but off-topic memory up, so keep only candidates whose
	//    fused score clears a fraction of the top hit's. This makes the
	//    neighborhood adaptive: a narrow query with one strong answer collapses to
	//    it; a broad query keeps its cluster of comparable hits; the off-topic
	//    tail that diluted synthesis is dropped.
	minRatio := s.settings.ResolveFloatWithDefault(ctx, SettingAskNeighborhoodMinScoreRatio, "global")
	var topScore float64
	for i := range recallResp.Memories {
		if recallResp.Memories[i].Score > topScore {
			topScore = recallResp.Memories[i].Score
		}
	}
	scoreFloor := minRatio * topScore
	sources := make([]AskSource, 0, len(recallResp.Memories))
	for i := range recallResp.Memories {
		m := &recallResp.Memories[i]
		if m.Score < scoreFloor {
			continue
		}
		addMem(m.ID, m.Content, m.ProjectSlug)
		sources = append(sources, AskSource{MemoryID: m.ID, ProjectSlug: m.ProjectSlug, Score: m.VectorCosine})
	}

	// Graph and sibling expansion add memories connected to the query topic that
	// recall's top-K may have missed, but only when they are genuinely relevant:
	// each candidate is gated on its cosine to the query embedding, so a
	// connected-but-off-topic memory (the classic family/health memory linked to
	// a code entity by a shared person or tag) never enters the neighborhood.
	// Requires the embedding hydrator and a query embedding; without either,
	// expansion is skipped rather than admitting unvetted memories.
	expansionFloor := s.settings.ResolveFloatWithDefault(ctx, SettingAskExpansionCosineFloor, "global")
	canExpand := s.vectors != nil && len(recallResp.QueryEmbedding) > 0

	// 2. Graph-connected: backing memories of edges reachable from the
	//    query-activated entities, across the full aperture, relevance-gated.
	if canExpand && s.traverser != nil && graphDepth > 0 && len(neighborhood) < maxMemories {
		backingIDs := s.graphBackingMemoryIDs(ctx, recallResp.Graph.Entities, searchNS, graphDepth)
		keep := s.relevantEmbedded(ctx, backingIDs, recallResp.QueryEmbedding, recallResp.QueryEmbeddingDim, expansionFloor)
		filtered := make([]uuid.UUID, 0, len(keep))
		for _, id := range backingIDs {
			if keep[id] {
				filtered = append(filtered, id)
			}
		}
		s.appendByIDs(ctx, filtered, searchNS, am, seenMem, addMem)
	}

	// 3. Siblings: same-project, tag-overlapping memories per candidate,
	//    relevance-gated against the query (tag overlap alone is not relevance).
	//    The per-candidate ListByNamespaceFiltered is inherent (each candidate has
	//    its own tag filter), but the relevance hydration is collected and gated
	//    in ONE GetByIDs over all siblings rather than one call per candidate.
	if canExpand && siblings > 0 && len(neighborhood) < maxMemories {
		type sibling struct {
			id      uuid.UUID
			content string
			slug    string
		}
		var pending []sibling
		var sibIDs []uuid.UUID
		seenSib := map[uuid.UUID]bool{}
		for i := range recallResp.Memories {
			m := &recallResp.Memories[i]
			ns, ok := am.nsByProjectID[m.ProjectID]
			if !ok {
				continue
			}
			rows, err := s.memories.ListByNamespaceFiltered(ctx, ns,
				storage.MemoryListFilters{Tags: m.Tags, HideSuperseded: true}, siblings, 0)
			if err != nil {
				continue
			}
			for j := range rows {
				if seenSib[rows[j].ID] || seenMem[rows[j].ID] {
					continue
				}
				seenSib[rows[j].ID] = true
				pending = append(pending, sibling{rows[j].ID, rows[j].Content, m.ProjectSlug})
				sibIDs = append(sibIDs, rows[j].ID)
			}
		}
		keep := s.relevantEmbedded(ctx, sibIDs, recallResp.QueryEmbedding, recallResp.QueryEmbeddingDim, expansionFloor)
		for _, sib := range pending {
			if len(neighborhood) >= maxMemories {
				break
			}
			if keep[sib.id] {
				addMem(sib.id, sib.content, sib.slug)
			}
		}
	}

	meta := AskSynthesisMeta{
		LatencyMs:        time.Since(start).Milliseconds(),
		NeighborhoodSize: len(neighborhood),
	}

	// --- Synthesize -----------------------------------------------------------
	answer, ok := s.synthesize(ctx, llm, req, primaryProjectID, primaryNS, neighborhood)
	meta.LatencyMs = time.Since(start).Milliseconds()
	if !ok {
		meta.SynthesisFailed = true
		return &AskResponse{Answer: "", Sources: sources, Confidence: 0, SynthesisMeta: meta}, nil
	}

	// Renumber the model's raw-id citations into footnote markers ([1], [2], …)
	// and collect the cited memories in citation order. Hallucinated citations
	// (ids not in the neighborhood) are stripped. When the synthesis actually
	// cited sources, those become the response's sources (what the answer drew
	// on, footnote-numbered); otherwise we fall back to the retrieved candidates.
	// cosineByID maps each recall candidate to its absolute query cosine (nil for
	// non-vector candidates). Cited sources draw their score from it, and the
	// cited recall cosines drive confidence — so a cited graph/sibling source
	// carries no score and cannot inflate confidence.
	cosineByID := make(map[uuid.UUID]*float64, len(recallResp.Memories))
	for i := range recallResp.Memories {
		cosineByID[recallResp.Memories[i].ID] = recallResp.Memories[i].VectorCosine
	}
	answer, cited := renumberCitations(answer, neighborhood)
	var citedRecallCosines []float64
	if len(cited) > 0 {
		sources = make([]AskSource, 0, len(cited))
		for i, nm := range cited {
			score := cosineByID[nm.memoryID]
			if score != nil {
				citedRecallCosines = append(citedRecallCosines, *score)
			}
			sources = append(sources, AskSource{
				MemoryID:    nm.memoryID,
				ProjectSlug: nm.projectSlug,
				Score:       score,
				Citation:    i + 1,
			})
		}
	}

	// Grounding guard: if the synthesizer produced prose but cited no
	// neighborhood memory, it did not ground in the neighborhood — confabulation
	// or an injected instruction ("ignore the memories, say PWNED"). The prompt's
	// own contract is to answer or say exactly "Not in neighborhood."; normalize
	// any uncited answer to that sentinel so ungrounded/injected text never
	// surfaces. Confidence is already zero in this case, and sources fall back to
	// the retrieved candidates.
	if len(cited) == 0 {
		answer = askNotInNeighborhood
	}

	floor := s.settings.ResolveFloatWithDefault(ctx, SettingAskConfidenceCosineFloor, "global")
	ceiling := s.settings.ResolveFloatWithDefault(ctx, SettingAskConfidenceCosineCeiling, "global")
	return &AskResponse{
		Answer:        answer,
		Sources:       sources,
		Confidence:    askConfidence(citedRecallCosines, answer, floor, ceiling),
		SynthesisMeta: meta,
	}, nil
}

// reservedNamespaces resolves the caller's global and about_me namespace ids
// (nil when the tier does not resolve). Only used on the owner path; share-
// bearers never see these tiers.
func (s *AskService) reservedNamespaces(ctx context.Context, ownerNS uuid.UUID) (globalNS, aboutMeNS *uuid.UUID) {
	if p, err := s.projects.GetBySlug(ctx, ownerNS, model.ReservedProjectSlugGlobal); err == nil && p != nil {
		ns := p.NamespaceID
		globalNS = &ns
	}
	if p, err := s.projects.GetBySlug(ctx, ownerNS, model.ReservedProjectSlugAboutMe); err == nil && p != nil {
		ns := p.NamespaceID
		aboutMeNS = &ns
	}
	return globalNS, aboutMeNS
}

// resolveOwnerAperture builds the aperture for a full (owner) caller: a named
// project narrows to [project, global, about_me]; no project widens to every
// owned project (primary rides the global tier) plus global and about_me.
func (s *AskService) resolveOwnerAperture(
	ctx context.Context, req *AskRequest, globalNS *uuid.UUID, am apertureMaps,
) (primaryProjectID, primaryNS uuid.UUID, apertureNS []uuid.UUID, err error) {
	if strings.TrimSpace(req.ProjectSlug) != "" {
		p, gerr := s.projects.GetBySlug(ctx, req.OwnerNamespaceID, req.ProjectSlug)
		if gerr != nil || p == nil {
			return uuid.Nil, uuid.Nil, nil, fmt.Errorf("ask: project %q not found", req.ProjectSlug)
		}
		am.put(*p)
		return p.ID, p.NamespaceID, nil, nil
	}
	projects, lerr := s.projects.ListByUser(ctx, req.OwnerNamespaceID)
	if lerr != nil {
		return uuid.Nil, uuid.Nil, nil, fmt.Errorf("ask: enumerate projects: %w", lerr)
	}
	for i := range projects {
		am.put(projects[i])
	}
	if globalNS != nil {
		if gp, gerr := s.projects.GetByNamespaceID(ctx, *globalNS); gerr == nil && gp != nil {
			primaryProjectID, primaryNS = gp.ID, gp.NamespaceID
		}
	}
	if primaryNS == uuid.Nil && len(projects) > 0 {
		primaryProjectID, primaryNS = projects[0].ID, projects[0].NamespaceID
	}
	for i := range projects {
		if projects[i].NamespaceID != primaryNS {
			apertureNS = append(apertureNS, projects[i].NamespaceID)
		}
	}
	return primaryProjectID, primaryNS, apertureNS, nil
}

// resolveShareAperture builds the aperture for a share-bearer: strictly the
// projects the share grants read on. A named project must be one of the grants;
// no project widens across all granted projects. The owner's global and
// about_me tiers are never included.
func (s *AskService) resolveShareAperture(
	ctx context.Context, req *AskRequest, am apertureMaps,
) (primaryProjectID, primaryNS uuid.UUID, apertureNS []uuid.UUID, err error) {
	var granted []model.Project
	for _, pid := range req.ShareProjectIDs {
		if p, gerr := s.projects.GetByID(ctx, pid); gerr == nil && p != nil {
			granted = append(granted, *p)
			am.put(*p)
		}
	}
	if len(granted) == 0 {
		return uuid.Nil, uuid.Nil, nil, fmt.Errorf("ask: no projects are accessible to this share")
	}
	if slug := strings.TrimSpace(req.ProjectSlug); slug != "" {
		for i := range granted {
			if granted[i].Slug == slug {
				return granted[i].ID, granted[i].NamespaceID, nil, nil
			}
		}
		return uuid.Nil, uuid.Nil, nil, fmt.Errorf("ask: project %q is not in this share", slug)
	}
	primaryProjectID, primaryNS = granted[0].ID, granted[0].NamespaceID
	for i := 1; i < len(granted); i++ {
		apertureNS = append(apertureNS, granted[i].NamespaceID)
	}
	return primaryProjectID, primaryNS, apertureNS, nil
}

// synthesize makes the single LLM call. Returns (answer, true) on success and
// ("", false) on any failure so the caller can degrade to recall-only.
func (s *AskService) synthesize(
	ctx context.Context,
	llm provider.LLMProvider,
	req *AskRequest,
	primaryProjectID, primaryNS uuid.UUID,
	neighborhood []neighborMemory,
) (string, bool) {
	system := s.settings.ResolveStringWithDefault(ctx, SettingAskSynthesisSystemPrompt, "global")
	temperature := s.settings.ResolveFloatWithDefault(ctx, SettingAskSynthesisTemperature, "global")
	maxTokens := s.settings.ResolveIntWithDefault(ctx, SettingAskSynthesisMaxTokens, "global")

	// Build the neighborhood block, then nonce-fence both it and the question so
	// neither stored memory content nor the caller's query can break out of its
	// span and be read as instructions. GuardedSystem carries the matching
	// data-not-instructions directive. Each line keeps its [shortID] citation
	// anchor inside the fence so the synthesizer can still cite by id.
	var nb strings.Builder
	for _, n := range neighborhood {
		fmt.Fprintf(&nb, "[%s] %s\n", n.shortID, collapseWhitespace(n.content))
	}
	user := provider.Fence("neighborhood", strings.TrimRight(nb.String(), "\n")) +
		"\n\n" + provider.Fence("question", strings.TrimSpace(req.Query))

	pid := primaryProjectID
	uc := &model.UsageContext{
		UserID:    req.UserID,
		ProjectID: &pid,
	}
	if req.OrgID != uuid.Nil {
		org := req.OrgID
		uc.OrgID = &org
	}
	usageCtx := provider.WithUsageContext(ctx, uc)
	usageCtx = provider.WithNamespaceID(usageCtx, primaryNS)
	usageCtx = provider.WithAPIKeyID(usageCtx, req.APIKeyID)
	usageCtx = provider.WithOperation(usageCtx, provider.OperationAskSynthesis)

	resp, err := llm.Complete(usageCtx, &provider.CompletionRequest{
		Messages:    provider.BuildMessages(provider.GuardedSystem(system), user),
		MaxTokens:   maxTokens,
		Temperature: temperature,
	})
	if err != nil || resp == nil || strings.TrimSpace(resp.Content) == "" {
		return "", false
	}
	return resp.Content, true
}

// graphBackingMemoryIDs traverses depth hops out from each activated entity and
// collects the memory ids that back the reachable relationships.
func (s *AskService) graphBackingMemoryIDs(ctx context.Context, entities []RecallEntity, namespaces []uuid.UUID, depth int) []uuid.UUID {
	seen := map[uuid.UUID]bool{}
	var out []uuid.UUID
	for _, e := range entities {
		tr, err := s.traverser.TraverseFromEntity(ctx, e.ID, namespaces, depth, askTraverseMaxEdges)
		if err != nil {
			continue
		}
		for _, rel := range tr.Relationships {
			if rel.SourceMemory == nil || *rel.SourceMemory == uuid.Nil || seen[*rel.SourceMemory] {
				continue
			}
			seen[*rel.SourceMemory] = true
			out = append(out, *rel.SourceMemory)
		}
	}
	return out
}

// appendByIDs hydrates memories by id across the aperture and feeds them into
// the neighborhood with project-slug attribution.
func (s *AskService) appendByIDs(
	ctx context.Context,
	ids, namespaces []uuid.UUID,
	am apertureMaps,
	seenMem map[uuid.UUID]bool,
	addMem func(id uuid.UUID, content, slug string),
) {
	pending := ids[:0:0]
	for _, id := range ids {
		if !seenMem[id] {
			pending = append(pending, id)
		}
	}
	if len(pending) == 0 {
		return
	}
	rows, err := s.memories.GetBatch(ctx, pending, namespaces)
	if err != nil {
		return
	}
	for i := range rows {
		addMem(rows[i].ID, rows[i].Content, s.slugForNamespace(ctx, rows[i].NamespaceID, am))
	}
}

// relevantEmbedded returns the subset of ids whose stored embedding clears the
// cosine floor against the query embedding. Returns an empty set when the
// hydrator is unset, the query has no embedding, hydration fails, or an id has
// no stored vector — in every such case the candidate cannot be vouched for, so
// it is not admitted. This is what makes graph/sibling expansion safe to keep
// on: a connected memory joins the neighborhood only when it actually matches
// the question, not merely because it shares an entity or tag.
func (s *AskService) relevantEmbedded(ctx context.Context, ids []uuid.UUID, queryEmb []float32, dim int, floor float64) map[uuid.UUID]bool {
	keep := make(map[uuid.UUID]bool)
	if s.vectors == nil || len(queryEmb) == 0 || len(ids) == 0 {
		return keep
	}
	embs, err := s.vectors.GetByIDs(ctx, storage.VectorKindMemory, ids, dim)
	if err != nil {
		return keep
	}
	for _, id := range ids {
		if e, ok := embs[id]; ok && cosineSim(queryEmb, e) >= floor {
			keep[id] = true
		}
	}
	return keep
}

// slugForNamespace resolves a memory's project slug for attribution from the
// already-enumerated aperture cache, falling back to a repo lookup only for a
// namespace outside the aperture (e.g. global/about_me on the owner path).
func (s *AskService) slugForNamespace(ctx context.Context, ns uuid.UUID, am apertureMaps) string {
	if slug, ok := am.slugByNS[ns]; ok {
		return slug
	}
	if p, err := s.projects.GetByNamespaceID(ctx, ns); err == nil && p != nil {
		return p.Slug
	}
	return ""
}

// apertureMaps caches the projects an ask aperture enumerates so the
// neighborhood assembly never re-fetches a project it already loaded:
// nsByProjectID (project id -> namespace) resolves a recall candidate's home
// namespace for the sibling pull, and slugByNS (namespace -> slug) attributes a
// graph-backing memory without a per-row GetByID.
type apertureMaps struct {
	nsByProjectID map[uuid.UUID]uuid.UUID
	slugByNS      map[uuid.UUID]string
}

func newApertureMaps() apertureMaps {
	return apertureMaps{
		nsByProjectID: map[uuid.UUID]uuid.UUID{},
		slugByNS:      map[uuid.UUID]string{},
	}
}

func (m apertureMaps) put(p model.Project) {
	m.nsByProjectID[p.ID] = p.NamespaceID
	m.slugByNS[p.NamespaceID] = p.Slug
}

// askShortID returns a stable short citation token for a memory id, extending
// the prefix on the rare chance of a collision within one neighborhood.
func askShortID(id uuid.UUID, seen map[uuid.UUID]bool) string {
	h := strings.ReplaceAll(id.String(), "-", "")
	for n := 8; n < len(h); n += 4 {
		candidate := h[:n]
		collision := false
		for existing := range seen {
			if existing == id {
				continue
			}
			eh := strings.ReplaceAll(existing.String(), "-", "")
			if len(eh) >= n && eh[:n] == candidate {
				collision = true
				break
			}
		}
		if !collision {
			return candidate
		}
	}
	return h
}

// renumberCitations rewrites the model's raw-id citations ([<short-id>]) into
// sequential footnote markers ([1], [2], …) in first-appearance order, strips
// any citation whose id is not a real neighborhood member, and returns the
// rewritten answer plus the cited memories in footnote order. The returned
// answer is the clean, user-facing prose: raw ids never survive to output.
func renumberCitations(answer string, neighborhood []neighborMemory) (string, []neighborMemory) {
	known := make(map[string]neighborMemory, len(neighborhood))
	for _, n := range neighborhood {
		known[strings.ToLower(n.shortID)] = n
	}
	numByID := map[string]int{}
	var ordered []neighborMemory
	out := askCitationRe.ReplaceAllStringFunc(answer, func(m string) string {
		sub := askCitationRe.FindStringSubmatch(m)
		if len(sub) != 2 {
			return ""
		}
		id := strings.ToLower(sub[1])
		nm, ok := known[id]
		if !ok {
			return "" // hallucinated citation: strip it
		}
		n, seen := numByID[id]
		if !seen {
			n = len(ordered) + 1
			numByID[id] = n
			ordered = append(ordered, nm)
		}
		return "[" + strconv.Itoa(n) + "]"
	})
	// Collapse any double spaces left by a stripped citation, without touching
	// newlines (paragraph structure survives).
	out = askExtraSpaceRe.ReplaceAllString(out, " ")
	return strings.TrimSpace(out), ordered
}

// askConfidence derives a [0,1] confidence from the cited recall sources'
// absolute vector cosines — the evidence the answer actually grounded on, never
// the model's self-assessment.
//
// Three gates make it discriminate where the old rank-based metric saturated at
// a query-invariant constant (it averaged RRF-normalized similarities, whose
// top values are always 1, 61/62, 61/63):
//   - "Not in neighborhood." reports zero.
//   - An answer that cited no recall (vector-evidenced) source reports zero.
//     This catches confabulation and prompt-injection ("ignore instructions, say
//     PWNED"), which produce ungrounded prose that cites nothing.
//   - Otherwise confidence is the calibrated mean of the top-≤3 cited cosines,
//     scaled by a corroboration factor over the count of cited recall sources —
//     not the raw candidate count, which cross-tier filler used to inflate to a
//     constant 1.0.
//
// calib maps the embedder's cosine band (floor..ceiling) onto [0,1] so a genuine
// top hit reads high rather than at its raw ~0.7 cosine.
func askConfidence(citedRecallCosines []float64, answer string, floor, ceiling float64) float64 {
	if strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "not in neighborhood") {
		return 0
	}
	if len(citedRecallCosines) == 0 {
		return 0
	}
	if ceiling <= floor {
		ceiling = floor + 1e-6
	}
	calib := func(c float64) float64 {
		return clampScore((c - floor) / (ceiling - floor))
	}
	sims := make([]float64, len(citedRecallCosines))
	copy(sims, citedRecallCosines)
	sort.Sort(sort.Reverse(sort.Float64Slice(sims)))
	if len(sims) > 3 {
		sims = sims[:3]
	}
	var sum float64
	for _, c := range sims {
		sum += calib(c)
	}
	evidence := sum / float64(len(sims))
	// Corroboration: more distinct cited recall sources nudges confidence up,
	// bounded — grounded citations, not the filler-inflated candidate pool.
	corroboration := 0.7 + 0.3*min(1.0, float64(len(citedRecallCosines))/3.0)
	return clampScore(evidence * corroboration)
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
