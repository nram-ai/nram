package mcp

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// graphResponse is the JSON envelope returned by the memory_graph tool.
// Argument echoes (query, depth, include_history) are deliberately omitted —
// the caller already has them. Truncated carries an edge-cap signal when the
// traverser short-circuited at graph.max_edges; the byte-budget reducer in
// result_limit.go writes its own truncationInfo into a map payload when it
// fires, replacing this field with its own envelope.
type graphResponse struct {
	Entities      []graphEntity       `json:"entities"`
	Relationships []graphRelationship `json:"relationships"`
	Truncated     *truncationInfo     `json:"_truncated,omitempty"`
}

// graphEntity is a minimal entity representation for graph results. canonical
// is dropped (redundant with name in the common case) and properties is
// dropped (raw JSON, can be large; callers can fetch the entity directly via
// the REST endpoint if they need the full record).
type graphEntity struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Type         string    `json:"type"`
	MentionCount int       `json:"mention_count"`
}

// graphRelationship is a minimal relationship representation for graph
// results. The per-edge id is dropped (callers have no use for it). valid_from
// is dropped because it usually equals the relationship's creation time and
// callers asking for history can use the REST API. valid_until and
// source_memory are kept (omitempty) because they are only set when meaningful
// AND because source_memory is a resolvable lineage pointer the caller can
// fetch via memory_get.
type graphRelationship struct {
	SourceID     uuid.UUID  `json:"source_id"`
	TargetID     uuid.UUID  `json:"target_id"`
	Relation     string     `json:"relation"`
	Weight       float64    `json:"weight"`
	ValidUntil   *time.Time `json:"valid_until,omitempty"`
	SourceMemory *uuid.UUID `json:"source_memory,omitempty"`
}

// projectItem is the JSON representation of a project in the memory_projects response.
type projectItem struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Slug        string    `json:"slug"`
	Description string    `json:"description"`
}

// listProjectsResponse wraps the projectItem slice with pagination metadata
// (mirrors listMemoryResponse). The object root is required because mcp-go's
// outputSchema must declare type=object; a bare slice would mis-advertise.
//
// Truncated is RESERVED for newListProjectsReducer (result_limit.go) — same
// invariant as listMemoryResponse.Truncated; handlers MUST NOT set it.
type listProjectsResponse struct {
	Projects   []projectItem    `json:"projects"`
	Pagination model.Pagination `json:"pagination"`
	Truncated  *truncationInfo  `json:"_truncated,omitempty"`
}

// RegisterGraphProjectsTools registers graph and list_projects. The export
// tool that previously rounded out this trio was withdrawn 2026-05-27: its
// payload travelled inline through the MCP transport, which truncates
// anything beyond the configured byte budget, so the only exports it could
// return were toy-sized. The REST + UI pipeline at /v1/me/exports is the
// only export surface now.
func RegisterGraphProjectsTools(s *Server) {
	registerMemoryGraph(s)
	registerMemoryProjects(s)
}

func registerMemoryGraph(s *Server) {
	tool := mcp.NewTool("graph",
		mcp.WithTitleAnnotation("Explore Knowledge Graph"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[graphResponse]()),
		mcp.WithDescription("Explore entity relationships in the knowledge graph. Use to discover how people, technologies, and concepts connect — especially when recall alone does not surface enough context."),
		mcp.WithString("entity", mcp.Required(), mcp.Description("Entity name or search query")),
		mcp.WithString("project", mcp.Description("Project slug to scope the search")),
		mcp.WithNumber("depth", mcp.Description("Graph traversal depth (default recall.graph.default_depth=2, server-capped at recall.graph.max_depth, default 5).")),
		mcp.WithNumber("min_weight", mcp.Description("Minimum relationship weight to include (default 0.1). Set to 0 to include all.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryGraph(ctx, s, request)
	})
}

func registerMemoryProjects(s *Server) {
	tool := mcp.NewTool("list_projects",
		mcp.WithTitleAnnotation("List Projects"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[listProjectsResponse]()),
		mcp.WithDescription("List all available projects with slugs and descriptions, paginated (default limit 50, max 200). ALWAYS call this before store to check for an existing project — an unknown slug on store auto-creates a new project. The reserved projects 'global' (world-knowledge) and 'about_me' (the user's self-knowledge) are auto-created for every user, carry nram-managed descriptions, and cannot be deleted."),
		mcp.WithNumber("limit", mcp.Description("Maximum number of projects to return (default 50, max 200)")),
		mcp.WithNumber("offset", mcp.Description("Number of projects to skip for pagination (default 0)")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryProjects(ctx, s, request)
	})
}

func handleMemoryGraph(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	entityQuery, ok := args["entity"].(string)
	if !ok || strings.TrimSpace(entityQuery) == "" {
		return mcp.NewToolResultError("entity is required"), nil
	}

	deps := s.Deps()

	depth := 2
	if v, ok := args["depth"].(float64); ok && v > 0 {
		depth = int(v)
		maxDepth := resolvePositiveCapInt(ctx, deps.Settings, service.SettingRecallGraphMaxDepth)
		if depth > maxDepth {
			depth = maxDepth
		}
	}

	minWeight := 0.1
	if v, ok := args["min_weight"].(float64); ok && v >= 0 {
		minWeight = v
	}

	includeHistory := false
	includeSuperseded := false

	// Resolve namespace: project-scoped or user-scoped.
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)

	isShareBearer := ac.ShareTokenID != nil
	if isShareBearer && projectSlug == "" {
		return mcp.NewToolResultError("share-bearer requests must specify project; the global fan-out is not available"), nil
	}

	// Collect namespaces to search: project-scoped + global (consistent with
	// memory_recall). Share-bearer callers stay project-only — the owner's
	// global namespace is never part of an implicit share.
	var namespaces []uuid.UUID
	if projectSlug != "" {
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}
		if denied := requireShareProject(ac, "graph", projectSlug, project.ID); denied != nil {
			return denied, nil
		}
		namespaces = append(namespaces, project.NamespaceID)
		if !isShareBearer && projectSlug != "global" {
			if gp, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, "global"); err == nil && gp != nil {
				namespaces = append(namespaces, gp.NamespaceID)
			}
		}
	} else {
		namespaces = append(namespaces, user.NamespaceID)
	}

	// Find matching entities across all namespaces. SearchEntities is the
	// agent-facing matcher: tokenizes on whitespace and ORs LIKE clauses
	// against name AND alias. The literal-substring FindBySimilarity is
	// reserved for canonical/programmatic callers (enrichment dedup,
	// dreaming) — using it here would mean multi-word agent queries like
	// "OAuth client" only match entities literally named that phrase.
	var entities []model.Entity
	for _, nsID := range namespaces {
		found, err := deps.EntityReader.SearchEntities(ctx, nsID, entityQuery, "", 10)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("entity search failed: %v", err)), nil
		}
		entities = append(entities, found...)
	}

	// No-match diagnostic for multi-token queries. SearchEntities tokenizes
	// via strings.Fields (any Unicode whitespace), so the gate here must use
	// the same rule — checking for ASCII space/tab only would let a query
	// like "OAuth\nclient" take the multi-token matcher path and then fall
	// through to a silently-empty response if every token missed. Surface
	// the miss explicitly so the agent has a signal to retry rather than
	// concluding the graph is empty.
	if len(entities) == 0 && len(strings.Fields(entityQuery)) > 1 {
		empty := []graphEntity{}
		emptyRels := []graphRelationship{}
		return wrapToolResult(s.deps.Metrics, "graph", mcpBudgetBytes(ctx, s.deps.Settings), &graphResponse{
			Entities:      empty,
			Relationships: emptyRels,
			Truncated: &truncationInfo{
				Reason: "no_match",
				Hint:   "entity name match is substring-based; try a single token (e.g. 'OAuth' instead of the full phrase) or a known canonical name",
			},
		}, nil)
	}

	// Cap pre-filter relationships so the BFS short-circuits before it
	// pulls (and the handler then marshals) edges the client cannot
	// consume. ResolveIntWithDefault is nil-safe; if Settings was never
	// wired, the registered default in settingDefaults applies. The cap
	// is applied both per-seed (inside TraverseFromEntity) AND
	// cumulatively across seeds in the loop below — without the
	// cumulative check, N seeds each returning < cap edges to disjoint
	// neighborhoods could still produce an N×cap deduped union and force
	// the post-traversal filter / orphan-resolve / marshal pipeline to
	// run on data the client cannot consume.
	maxEdges := deps.Settings.ResolveIntWithDefault(ctx, service.SettingGraphMaxEdges, "global")

	// Carry the model.Relationship slice (not the JSON projection) through
	// the filter passes so the reinforcement hook at the bottom retains
	// id and namespace without re-resolving them.
	seenEntities := make(map[uuid.UUID]struct{})
	var graphEntities []graphEntity
	var rels []model.Relationship
	seenRels := make(map[uuid.UUID]struct{})
	truncatedByCap := false

seeds:
	for _, ent := range entities {
		if _, ok := seenEntities[ent.ID]; ok {
			continue
		}
		seenEntities[ent.ID] = struct{}{}
		graphEntities = append(graphEntities, graphEntity{
			ID:           ent.ID,
			Name:         ent.Name,
			Type:         ent.EntityType,
			MentionCount: ent.MentionCount,
		})

		// Tighten the cap on each subsequent seed to whatever budget
		// remains, so a single very large neighborhood cannot starve the
		// later seeds and so the per-seed BFS short-circuits as soon as
		// the cumulative budget is hit.
		seedCap := maxEdges
		if maxEdges > 0 {
			seedCap = maxEdges - len(rels)
			if seedCap <= 0 {
				truncatedByCap = true
				break
			}
		}

		tr, err := deps.Traverser.TraverseFromEntity(ctx, ent.ID, depth, seedCap)
		if err != nil {
			continue
		}
		if tr.Truncated {
			truncatedByCap = true
		}
		for _, rel := range tr.Relationships {
			if _, ok := seenRels[rel.ID]; ok {
				continue
			}
			seenRels[rel.ID] = struct{}{}
			rels = append(rels, rel)
			if maxEdges > 0 && len(rels) >= maxEdges {
				truncatedByCap = true
				break seeds
			}
		}
	}

	// Filter relationships by expiry and minimum weight.
	{
		now := time.Now()
		filtered := rels[:0]
		for _, rel := range rels {
			// Skip expired unless include_history is set.
			if !includeHistory && rel.ValidUntil != nil && !rel.ValidUntil.After(now) {
				continue
			}
			// Skip relationships below the minimum weight threshold.
			if rel.Weight < minWeight {
				continue
			}
			filtered = append(filtered, rel)
		}
		rels = filtered
	}

	// Drop relationships whose provenance is gone. A NULL source_memory means
	// the sourcing memory was hard-deleted (the FK ON DELETE SET NULL nulled
	// the pointer) — it is permanently gone, so these edges are ALWAYS dropped,
	// even under includeSuperseded (there is no "include deleted"). The
	// lifecycle sweep reaps them from the store; this keeps the graph
	// consistent in the meantime. Additionally, unless includeSuperseded is
	// set, drop edges whose source memory is now soft-deleted or superseded so
	// the graph stays consistent with memory_list/memory_recall.
	if len(rels) > 0 {
		alive := make(map[uuid.UUID]struct{})
		if !includeSuperseded {
			idSet := make(map[uuid.UUID]struct{})
			for _, rel := range rels {
				if rel.SourceMemory != nil {
					idSet[*rel.SourceMemory] = struct{}{}
				}
			}
			if len(idSet) > 0 {
				ids := make([]uuid.UUID, 0, len(idSet))
				for id := range idSet {
					ids = append(ids, id)
				}
				if mems, err := deps.MemoryLister.GetBatch(ctx, ids); err == nil {
					for _, m := range mems {
						if m.IsLiveProvenance() {
							alive[m.ID] = struct{}{}
						}
					}
				}
			}
		}
		filtered := rels[:0]
		for _, rel := range rels {
			// Lost-provenance edge (source memory hard-deleted): always drop.
			if rel.SourceMemory == nil {
				continue
			}
			// When not including superseded, require the source memory to be
			// present, not soft-deleted, and not superseded.
			if !includeSuperseded {
				if _, ok := alive[*rel.SourceMemory]; !ok {
					continue
				}
			}
			filtered = append(filtered, rel)
		}
		rels = filtered
	}

	// Project to JSON shape; orphan resolution runs on the projection so the
	// reinforcement hook below only writes back edges the user actually saw.
	graphRels := make([]graphRelationship, 0, len(rels))
	for _, rel := range rels {
		graphRels = append(graphRels, graphRelationship{
			SourceID:     rel.SourceID,
			TargetID:     rel.TargetID,
			Relation:     rel.Relation,
			Weight:       rel.Weight,
			ValidUntil:   rel.ValidUntil,
			SourceMemory: rel.SourceMemory,
		})
	}
	// graphEntities holds exactly the seed entities here; resolveGraphOrphans
	// appends edge-endpoint orphans next. Capture the seed set first so
	// rankGraphSlice can rank by hop distance from the query's seeds.
	seedIDs := graphEntityIDSet(graphEntities)
	graphEntities, graphRels = resolveGraphOrphans(ctx, deps.EntityReader, graphEntities, graphRels, namespaces)

	// Orphan resolver may have pruned edges; intersect surviving (s,t,rel)
	// triples with rels to recover (id, namespace_id) for each visible edge.
	survived := make(map[graphEdgeKey]struct{}, len(graphRels))
	for _, gr := range graphRels {
		survived[graphEdgeKey{src: gr.SourceID, tgt: gr.TargetID, rel: gr.Relation}] = struct{}{}
	}
	var refs []service.RelationshipRef
	for _, rel := range rels {
		if _, ok := survived[graphEdgeKey{src: rel.SourceID, tgt: rel.TargetID, rel: rel.Relation}]; !ok {
			continue
		}
		refs = append(refs, service.RelationshipRef{
			ID:          rel.ID,
			NamespaceID: rel.NamespaceID,
		})
	}
	if deps.Recall != nil {
		deps.Recall.ReinforceGraphEdgesAsync(refs)
	}

	if graphEntities == nil {
		graphEntities = []graphEntity{}
	}
	if graphRels == nil {
		graphRels = []graphRelationship{}
	}

	// Collapse relation-string variants (legacy / pre-backfill rows) before
	// ranking so duplicate edges never consume slice budget. Runs AFTER the
	// reinforcement refs above, which key on the raw relations, so every
	// traversed edge is still reinforced by id.
	graphRels = dedupGraphRelationships(graphRels)

	// Rank by proximity to the seed entities (hop distance, then seed-connection
	// strength, with global salience only as a tiebreak) and diversify each hop
	// tier across distinct source nodes, so the byte-budget reducer's prefix trim
	// keeps the seed-relevant, source-diverse edges rather than namespace hubs.
	rankGraphSlice(seedIDs, graphEntities, graphRels)

	resp := &graphResponse{
		Entities:      graphEntities,
		Relationships: graphRels,
	}
	if truncatedByCap {
		resp.Truncated = &truncationInfo{
			Reason:        "edge_cap",
			ReturnedCount: len(graphRels),
			Hint:          fmt.Sprintf("traversal stopped at graph.max_edges=%d; raise the setting or narrow the entity query/depth", maxEdges),
		}
	}

	return wrapToolResult(s.deps.Metrics, "graph", mcpBudgetBytes(ctx, s.deps.Settings), resp, newGraphReducer(resp))
}

// graphEdgeKey identifies a graph edge by its endpoints and relation type.
// Used to intersect the post-orphan-resolution projection back with the
// pre-projection model slice for the reinforcement hook.
type graphEdgeKey struct {
	src, tgt uuid.UUID
	rel      string
}

// namespaceSet builds a set of the allowed namespace IDs for O(1) membership
// tests. Used by every graph-entity path that must drop rows the caller cannot
// read (resolveGraphOrphans, backfillMentionCounts).
func namespaceSet(allowedNamespaces []uuid.UUID) map[uuid.UUID]struct{} {
	allowed := make(map[uuid.UUID]struct{}, len(allowedNamespaces))
	for _, ns := range allowedNamespaces {
		allowed[ns] = struct{}{}
	}
	return allowed
}

// graphEntityIDSet builds a set of the entities' IDs. Both graph-slice surfaces
// use it to capture the seed set (the pre-orphan-resolution entities) for
// proximity ranking.
func graphEntityIDSet(ents []graphEntity) map[uuid.UUID]struct{} {
	ids := make(map[uuid.UUID]struct{}, len(ents))
	for _, e := range ents {
		ids[e.ID] = struct{}{}
	}
	return ids
}

// resolveGraphOrphans guarantees that every relationship's endpoints appear
// in entities[]. Missing endpoints are batch-fetched and merged in (filtered
// to allowedNamespaces); anything still unresolved gets the relationship
// pruned, so a GetBatch failure can never produce a dangling-endpoint emit.
func resolveGraphOrphans(
	ctx context.Context,
	entityReader EntityReader,
	entities []graphEntity,
	rels []graphRelationship,
	allowedNamespaces []uuid.UUID,
) ([]graphEntity, []graphRelationship) {
	known := make(map[uuid.UUID]struct{}, len(entities))
	for _, e := range entities {
		known[e.ID] = struct{}{}
	}

	missing := make(map[uuid.UUID]struct{})
	for _, rel := range rels {
		if _, ok := known[rel.SourceID]; !ok {
			missing[rel.SourceID] = struct{}{}
		}
		if _, ok := known[rel.TargetID]; !ok {
			missing[rel.TargetID] = struct{}{}
		}
	}

	if len(missing) > 0 && entityReader != nil {
		ids := make([]uuid.UUID, 0, len(missing))
		for id := range missing {
			ids = append(ids, id)
		}
		if fetched, err := entityReader.GetBatch(ctx, ids); err == nil {
			allowed := namespaceSet(allowedNamespaces)
			for _, ent := range fetched {
				if _, ok := allowed[ent.NamespaceID]; !ok {
					continue
				}
				if _, ok := known[ent.ID]; ok {
					continue
				}
				known[ent.ID] = struct{}{}
				entities = append(entities, graphEntity{
					ID:           ent.ID,
					Name:         ent.Name,
					Type:         ent.EntityType,
					MentionCount: ent.MentionCount,
				})
			}
		}
	}

	pruned := rels[:0]
	for _, rel := range rels {
		if _, ok := known[rel.SourceID]; !ok {
			continue
		}
		if _, ok := known[rel.TargetID]; !ok {
			continue
		}
		pruned = append(pruned, rel)
	}
	return entities, pruned
}

// backfillMentionCounts populates MentionCount on entities in place by batch-
// fetching the entity records, filtered to allowedNamespaces (the same scope
// guard resolveGraphOrphans applies, so a caller cannot pull mention signal
// from a namespace it lacks read access to). Entities not returned by the batch
// (or outside the allowed namespaces) keep their existing count. A GetBatch
// error leaves all counts unchanged — mention signal is best-effort ranking
// input, never a correctness gate.
func backfillMentionCounts(ctx context.Context, entityReader EntityReader, entities []graphEntity, allowedNamespaces []uuid.UUID) {
	if entityReader == nil || len(entities) == 0 {
		return
	}
	ids := make([]uuid.UUID, 0, len(entities))
	for _, e := range entities {
		ids = append(ids, e.ID)
	}
	fetched, err := entityReader.GetBatch(ctx, ids)
	if err != nil {
		return
	}
	allowed := namespaceSet(allowedNamespaces)
	counts := make(map[uuid.UUID]int, len(fetched))
	for _, ent := range fetched {
		if _, ok := allowed[ent.NamespaceID]; !ok {
			continue
		}
		counts[ent.ID] = ent.MentionCount
	}
	for i := range entities {
		if c, ok := counts[entities[i].ID]; ok {
			entities[i].MentionCount = c
		}
	}
}

func handleMemoryProjects(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	limit := parseIntArg(args, "limit", listDefaultLimit, 1, listMaxLimit)
	offset := parseIntArg(args, "offset", 0, 0, math.MaxInt32)

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	projects, err := deps.ProjectRepo.ListByUser(ctx, user.NamespaceID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list projects: %v", err)), nil
	}

	// Share-bearer callers see only the projects in their allowlist. Filter
	// the full list before pagination so offset/limit operate on the visible
	// subset.
	if ac.ShareTokenID != nil {
		filtered := projects[:0]
		for _, p := range projects {
			if shareTokenAllowsProjectID(ac, p.ID) {
				filtered = append(filtered, p)
			}
		}
		projects = filtered
	}

	total := len(projects)

	// Apply offset + limit slice. Bounds-safe: offset >= total returns empty;
	// offset+limit > total clamps to total.
	start := min(offset, total)
	end := min(start+limit, total)
	page := projects[start:end]

	items := make([]projectItem, 0, len(page))
	for _, p := range page {
		items = append(items, projectItem{
			ID:          p.ID,
			Name:        p.Name,
			Slug:        p.Slug,
			Description: p.Description,
		})
	}

	resp := &listProjectsResponse{
		Projects: items,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	}
	return wrapToolResult(s.deps.Metrics, "list_projects", mcpBudgetBytes(ctx, s.deps.Settings), resp, newListProjectsReducer(resp))
}
