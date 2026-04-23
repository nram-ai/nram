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

// RegisterRecallTool registers the memory_recall MCP tool on the given server.
func RegisterRecallTool(s *Server) {
	opts := []mcp.ToolOption{
		mcp.WithDescription("Search persistent memory. ALWAYS recall at the start of a new task to load context. Recall before making assumptions and before storing to avoid duplicates. Use natural language queries. Specifying a project searches that project plus global; omitting searches global only."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query")),
		mcp.WithString("project", mcp.Description("Project slug. Searches this project + global. Omit to search only the global project")),
		mcp.WithNumber("limit", mcp.Description("Maximum results to return (default 10)")),
		mcp.WithArray("tags", mcp.Description("Filter by tags (intersection: memory must have ALL)")),
		mcp.WithString("diversify_by_tag_prefix", mcp.Description("Post-rerank the ranked candidates to spread results across a tag axis. When set (e.g. \"category-\"), groups candidates by the first tag matching this prefix and round-robins across groups up to limit. Candidates with no prefix-matching tag are excluded. Response includes coverage_gaps listing groups that dropped out due to tag filtering, threshold, or limit.")),
	}
	opts = append(opts,
		mcp.WithBoolean("include_graph", mcp.Description("Include graph entities in results (default true)")),
		mcp.WithNumber("graph_depth", mcp.Description("Graph traversal depth (default 2)")),
	)

	tool := mcp.NewTool("memory_recall", opts...)

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

	limit := 10
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}

	tags := extractStringSlice(args["tags"])

	includeGraph := true
	if v, ok := args["include_graph"].(bool); ok {
		includeGraph = v
	}

	graphDepth := 2
	if v, ok := args["graph_depth"].(float64); ok && v > 0 {
		graphDepth = int(v)
	}

	diversifyPrefix, _ := args["diversify_by_tag_prefix"].(string)

	deps := s.Deps()
	uid := ac.UserID

	req := &service.RecallRequest{
		Query:                query,
		Limit:                limit,
		Tags:                 tags,
		IncludeGraph:         includeGraph,
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

	if projectSlug != "" {
		// Project-scoped recall: search this project + global.
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}

		req.ProjectID = project.ID
		// Include global memories alongside project-specific results.
		if projectSlug != "global" {
			req.GlobalNamespaceID = globalNsID
		}
	} else {
		// No project specified: search only the global project.
		if globalProject != nil {
			req.ProjectID = globalProject.ID
		} else {
			// Fallback: no global project exists, search all user projects.
			req.NamespaceID = &user.NamespaceID
		}
	}

	resp, err := deps.Recall.Recall(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("recall failed: %v", err)), nil
	}

	return wrapToolResult(resp, newRecallReducer(resp))
}
