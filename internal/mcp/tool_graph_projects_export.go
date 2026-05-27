package mcp

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
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

// mcpExportResponse wraps service.ExportData with the byte-budget reducer
// envelope. The service type stays untouched so REST consumers don't see the
// MCP-specific Truncated field.
type mcpExportResponse struct {
	service.ExportData
	Truncated *truncationInfo `json:"_truncated,omitempty"`
}

// RegisterGraphProjectsExportTools registers graph, list_projects, and export.
func RegisterGraphProjectsExportTools(s *Server) {
	registerMemoryGraph(s)
	registerMemoryProjects(s)
	registerMemoryExport(s)
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
		mcp.WithDescription("List all available projects with slugs and descriptions, paginated (default limit 50, max 200). ALWAYS call this before store to check for an existing project — an unknown slug on store auto-creates a new project."),
		mcp.WithNumber("limit", mcp.Description("Maximum number of projects to return (default 50, max 200)")),
		mcp.WithNumber("offset", mcp.Description("Number of projects to skip for pagination (default 0)")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryProjects(ctx, s, request)
	})
}

func registerMemoryExport(s *Server) {
	tool := mcp.NewTool("export",
		mcp.WithTitleAnnotation("Export Project"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		// No outputSchema: the tool has two output shapes — format=json
		// returns a structured mcpExportResponse, format=ndjson returns
		// a line-delimited text body. A single JSON Schema cannot honestly
		// describe both, so the tool advertises none.
		mcp.WithDescription("Export all memories from a project for backup, migration, or analysis. Project must already exist."),
		mcp.WithString("project", mcp.Description("Project slug to export (default: 'global')")),
		mcp.WithString("format", mcp.Description("Export format: \"json\" or \"ndjson\" (default \"json\")")),
		mcp.WithBoolean(includeSupersededArg, mcp.Description(includeSupersededDesc)),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryExport(ctx, s, request)
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

	// Collect namespaces to search: project-scoped + global (consistent with memory_recall).
	var namespaces []uuid.UUID
	if projectSlug != "" {
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}
		namespaces = append(namespaces, project.NamespaceID)
		if projectSlug != "global" {
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

	// Drop relationships extracted from a memory that has since been
	// superseded so the graph stays consistent with memory_list/memory_recall.
	// One GetBatch over the distinct source-memory IDs; superseded rows are
	// dropped in Go.
	if !includeSuperseded && len(rels) > 0 {
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
			alive := make(map[uuid.UUID]struct{})
			if mems, err := deps.MemoryLister.GetBatch(ctx, ids); err == nil {
				for _, m := range mems {
					if m.SupersededBy == nil && m.DeletedAt == nil {
						alive[m.ID] = struct{}{}
					}
				}
			}
			filtered := rels[:0]
			for _, rel := range rels {
				if rel.SourceMemory == nil {
					filtered = append(filtered, rel)
					continue
				}
				if _, ok := alive[*rel.SourceMemory]; ok {
					filtered = append(filtered, rel)
				}
			}
			rels = filtered
		}
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

	// Sort both axes by signal-strength descending so the byte-budget
	// reducer in result_limit.go (which trims via prefix slice) preserves
	// the most informative items first when the response exceeds budget.
	//
	// Tiebreak chains produce a total order so the prefix is deterministic
	// across calls. Equal-weight edges are common (many extractors emit
	// Weight=1.0) and BFS traversal order depends on `seenRels` map
	// iteration (Go-randomized), so without a tiebreak chain two identical
	// graph() calls could return different surviving prefixes when
	// truncation fires. UUIDs are stringified for lex ordering — bytes
	// would compare equally but UUID has no comparable receiver and
	// String() is O(36).
	sort.Slice(graphRels, func(i, j int) bool {
		a, b := graphRels[i], graphRels[j]
		if a.Weight != b.Weight {
			return a.Weight > b.Weight
		}
		if a.SourceID != b.SourceID {
			return a.SourceID.String() < b.SourceID.String()
		}
		if a.TargetID != b.TargetID {
			return a.TargetID.String() < b.TargetID.String()
		}
		return a.Relation < b.Relation
	})
	sort.Slice(graphEntities, func(i, j int) bool {
		a, b := graphEntities[i], graphEntities[j]
		if a.MentionCount != b.MentionCount {
			return a.MentionCount > b.MentionCount
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		// Two distinct entities can share a display name (different
		// sources, same canonical) — final tiebreak on ID guarantees a
		// total order.
		return a.ID.String() < b.ID.String()
	})

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
			allowed := make(map[uuid.UUID]struct{}, len(allowedNamespaces))
			for _, ns := range allowedNamespaces {
				allowed[ns] = struct{}{}
			}
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

func handleMemoryExport(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)
	if projectSlug == "" {
		projectSlug = "global"
	}

	format := "json"
	if v, ok := args["format"].(string); ok && strings.TrimSpace(v) != "" {
		format = strings.TrimSpace(strings.ToLower(v))
	}
	if format != "json" && format != "ndjson" {
		return mcp.NewToolResultError(fmt.Sprintf("unsupported format %q; use \"json\" or \"ndjson\"", format)), nil
	}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	includeSuperseded := argBool(args, includeSupersededArg, false)

	exportSvc := deps.Export

	if format == "ndjson" {
		req := &service.ExportRequest{
			ProjectID:         project.ID,
			Format:            service.ExportFormatNDJSON,
			IncludeSuperseded: includeSuperseded,
		}
		var buf bytes.Buffer
		if err := exportSvc.ExportNDJSON(ctx, req, &buf); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("export failed: %v", err)), nil
		}
		return wrapToolResultText(s.deps.Metrics, "export", mcpBudgetBytes(ctx, s.deps.Settings), buf.String())
	}

	req := &service.ExportRequest{
		ProjectID:         project.ID,
		Format:            service.ExportFormatJSON,
		IncludeSuperseded: includeSuperseded,
	}
	data, err := exportSvc.Export(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("export failed: %v", err)), nil
	}

	// Export is exempt from outputSchema (the tool has two output shapes —
	// json structured and ndjson text — that a single schema cannot honestly
	// describe). Without a declared schema, structuredContent is half-honored
	// per the MCP spec (clients MAY ignore it), so emit text only and use
	// the full byte budget rather than the halved structured budget.
	resp := &mcpExportResponse{ExportData: *data}
	return wrapToolResultJSONText(s.deps.Metrics, "export", mcpBudgetBytes(ctx, s.deps.Settings), resp, newExportReducer(resp))
}
