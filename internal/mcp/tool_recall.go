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

	if projectSlug != "" {
		// Project-scoped recall: search this project + global.
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}

		req.ProjectID = project.ID
		allowedNS = append(allowedNS, project.NamespaceID)
		// Include global memories alongside project-specific results.
		if projectSlug != "global" {
			req.GlobalNamespaceID = globalNsID
			if globalNsID != nil {
				allowedNS = append(allowedNS, *globalNsID)
			}
		}
	} else {
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
	return wrapToolResult(mcpResp, newRecallReducer(mcpResp))
}
