package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterRecallTool registers the recall MCP tool on the given server.
func RegisterRecallTool(s *Server) {
	tool := mcp.NewTool("recall",
		mcp.WithTitleAnnotation("Recall Memories"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpRecallResponse]()),
		mcp.WithDescription("Search persistent memory. ALWAYS recall at the start of a new task to load context. Recall before making assumptions and before storing to avoid duplicates. Use natural language queries. Specifying a project searches that project plus global; omitting searches global only. Graph entities and relationships are always included when the knowledge graph is populated."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query")),
		mcp.WithString("project", mcp.Description("Project slug. Searches this project + global. Omit to search only the global project")),
		mcp.WithNumber("limit", mcp.Description("Maximum results to return (default 10, server-capped at recall.max_limit, default 50). For larger result sets use the list tool.")),
		mcp.WithArray("tags", mcp.Description("Filter by tags (intersection: memory must have ALL)")),
		mcp.WithNumber("graph_depth", mcp.Description("Graph traversal depth (default recall.graph.default_depth=2, server-capped at recall.graph.max_depth, default 5).")),
		mcp.WithString("diversify_by_tag_prefix", mcp.Description("Group results by the first tag matching this prefix (e.g. \"category-\") and round-robin across groups up to limit. Use only when you specifically want spread across a tag axis; otherwise omit.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryRecall(ctx, s, request)
	})
}

func handleMemoryRecall(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	query, ok := args["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return mcp.NewToolResultError("query is required"), nil
	}

	// Extract optional parameters.
	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)

	deps := s.Deps()

	// Leave limit zero when the caller omits it so the recall service applies
	// recall.default_limit. When provided, clamp against the MCP-scoped
	// recall.max_limit cap so a runaway caller cannot pull arbitrarily large
	// pages through the LLM tool surface.
	var limit int
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
		maxLimit := resolvePositiveCapInt(ctx, deps.Settings, service.SettingRecallMaxLimit)
		if limit > maxLimit {
			limit = maxLimit
		}
	}

	tags := extractStringSlice(args["tags"])

	// Same shape as limit: zero means "let the service apply the default";
	// when provided, clamp against the MCP-scoped max depth.
	var graphDepth int
	if v, ok := args["graph_depth"].(float64); ok && v > 0 {
		graphDepth = int(v)
		maxDepth := resolvePositiveCapInt(ctx, deps.Settings, service.SettingRecallGraphMaxDepth)
		if graphDepth > maxDepth {
			graphDepth = maxDepth
		}
	}

	diversifyPrefix, _ := args["diversify_by_tag_prefix"].(string)

	uid := ac.UserID

	req := &service.RecallRequest{
		Query:                query,
		Limit:                limit,
		Tags:                 tags,
		IncludeGraph:         true,
		GraphDepth:           graphDepth,
		DiversifyByTagPrefix: diversifyPrefix,
		UserID:               &uid,
		APIKeyID:             ac.APIKeyID,
	}

	// Resolve the user's global project namespace for inclusion in all recalls.
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	// Look up the global project to get its namespace.
	var globalNsID *uuid.UUID
	var globalProject *model.Project
	if gp, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, "global"); err == nil && gp != nil {
		globalProject = gp
		nsID := gp.NamespaceID
		globalNsID = &nsID
	}

	// allowedNS bounds orphan resolution to namespaces the caller is already
	// permitted to read from — without it, GetBatch could surface entities
	// from another user's scope.
	var allowedNS []uuid.UUID

	// Share-bearer callers MUST scope to a project they hold a grant on, and
	// MUST NOT pick up the owner's global namespace via the fan-out below.
	// The omitted-project case is rejected before any namespace decisions.
	isShareBearer := ac.ShareTokenID != nil

	if projectSlug != "" {
		// Project-scoped recall: search this project + global.
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}

		if denied := requireShareProject(ac, "recall", projectSlug, project.ID); denied != nil {
			return denied, nil
		}

		req.ProjectID = project.ID
		allowedNS = append(allowedNS, project.NamespaceID)
		// Include global memories alongside project-specific results — but
		// only for non-share-bearer callers. The share grant is per-project
		// by design; the owner's global namespace is not implicitly shared.
		if !isShareBearer && projectSlug != "global" {
			req.GlobalNamespaceID = globalNsID
			if globalNsID != nil {
				allowedNS = append(allowedNS, *globalNsID)
			}
		}
	} else {
		if isShareBearer {
			return mcp.NewToolResultError("share-bearer requests must specify project; the global fan-out is not available"), nil
		}
		// No project specified: search only the global project.
		if globalProject != nil {
			req.ProjectID = globalProject.ID
			allowedNS = append(allowedNS, globalProject.NamespaceID)
		} else {
			// Fallback: no global project exists, search all user projects.
			req.NamespaceID = &user.NamespaceID
			allowedNS = append(allowedNS, user.NamespaceID)
		}
	}

	resp, err := deps.Recall.Recall(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("recall failed: %v", err)), nil
	}

	mcpResp := buildMCPRecallResponse(ctx, deps.EntityReader, resp, allowedNS, projectionOpts{})

	// Pre-cap the graph to a reserved slice of the byte budget BEFORE the
	// memory-focused reducer runs. Without this the reducer's last-resort
	// stage would drop the whole graph on any over-budget response, blinding
	// the caller to graph context. The pack keeps a balanced, signal-sorted
	// subset (entities AND relationships) so recall always surfaces some graph.
	budget := mcpBudgetBytes(ctx, s.deps.Settings)
	reserveBytes := int(float64(budget) * recallGraphReserveFraction(ctx, s.deps.Settings))
	keptE, keptR, graphSentinels := packGraphToByteBudget(mcpResp.Graph.Entities, mcpResp.Graph.Relationships, reserveBytes)
	mcpResp.Graph.Entities, mcpResp.Graph.Relationships = keptE, keptR
	graphPreTrimmed := len(graphSentinels) > 0
	if graphPreTrimmed {
		// The pre-cap is itself a truncation; stamp it so the envelope is
		// present even when the response fits without the reducer firing (the
		// reducer is the only other writer of Truncated — see projection_recall.go).
		mcpResp.Truncated = &truncationInfo{
			Reason:  "response_too_large",
			Dropped: graphSentinels,
			Hint:    recallGraphTrimHint,
		}
	}

	return wrapToolResult(s.deps.Metrics, "recall", budget, mcpResp, newRecallReducer(mcpResp, graphPreTrimmed))
}

// recallGraphReserveFraction resolves recall.graph.reserve_fraction through the
// SettingsService cascade, falling back to the registered default when Settings
// is nil (matching the mcpBudgetBytes nil-safe pattern).
func recallGraphReserveFraction(ctx context.Context, s *service.SettingsService) float64 {
	if s == nil {
		return service.GetDefaultFloat(service.SettingRecallGraphReserveFraction)
	}
	return s.ResolveFloatWithDefault(ctx, service.SettingRecallGraphReserveFraction, "global")
}
