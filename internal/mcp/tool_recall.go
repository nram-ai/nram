package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// RegisterRecallTool registers the memory_recall MCP tool on the given server.
// On SQLite, the include_graph and graph_depth parameters are omitted since
// vector/graph features are not available.
func RegisterRecallTool(s *Server) {
	opts := []mcp.ToolOption{
		mcp.WithDescription("Recall memories matching a natural language query. Omit project to search all user projects."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query")),
		mcp.WithString("project", mcp.Description("Project slug. Omit to search all user projects")),
		mcp.WithString("org", mcp.Description("Org slug. Search across entire org")),
		mcp.WithNumber("limit", mcp.Description("Maximum results to return (default 10)")),
		mcp.WithArray("tags", mcp.Description("Filter by tags (intersection: memory must have ALL)")),
	}
	if s.Backend() == storage.BackendPostgres {
		opts = append(opts,
			mcp.WithBoolean("include_graph", mcp.Description("Include graph entities in results (default true)")),
			mcp.WithNumber("graph_depth", mcp.Description("Graph traversal depth (default 2)")),
		)
	}

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

	orgSlug, _ := args["org"].(string)
	orgSlug = strings.TrimSpace(orgSlug)

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

	deps := s.Deps()
	uid := ac.UserID

	req := &service.RecallRequest{
		Query:        query,
		Limit:        limit,
		Tags:         tags,
		IncludeGraph: includeGraph,
		GraphDepth:   graphDepth,
		UserID:       &uid,
		APIKeyID:     ac.APIKeyID,
	}

	if projectSlug != "" {
		// Project-scoped recall: resolve project by slug.
		user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
		}

		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}

		req.ProjectID = project.ID
	} else if orgSlug != "" {
		// Org-scoped recall: resolve org and use its namespace.
		if deps.OrgRepo == nil {
			return mcp.NewToolResultError("org lookup not available"), nil
		}

		org, err := deps.OrgRepo.GetBySlug(ctx, orgSlug)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("org not found: %v", err)), nil
		}

		req.NamespaceID = &org.NamespaceID
	} else {
		// User-scoped recall: search all user projects via user namespace.
		user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
		}

		req.NamespaceID = &user.NamespaceID
	}

	resp, err := deps.Recall.Recall(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("recall failed: %v", err)), nil
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}
