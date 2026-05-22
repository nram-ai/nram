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
		mcp.WithBoolean("include_low_novelty", mcp.Description("When true, surface dream-source memories the novelty audit demoted (low_novelty=true) and include the low_novelty / low_novelty_reason markers in their metadata so the caller sees why they were demoted. Default false hides them.")),
		mcp.WithNumber("similarity_threshold", mcp.Description("Vector-evidence cutoff (must be in [0, 1]; 0 disables, out-of-range returns 400). Filters candidates by raw cosine from the vector store (raw_cosine mode) or by post-RRF max-normalized similarity (fused_combined mode). List-fallback and shared-namespace candidates ALWAYS pass through unfiltered regardless of mode. Distinct from `threshold`, which filters the composite ranking score (weighted sum of similarity, recency, importance, frequency, graph relevance, confidence, origin). Pass `similarity_threshold` for a vector-evidence cutoff; pass `threshold` for a composite floor; pass both to combine.")),
		mcp.WithString("similarity_threshold_mode", mcp.Description("Which similarity value `similarity_threshold` compares against. `raw_cosine` (default) compares the raw cosine returned by the vector store before RRF on an absolute scale; only vector-channel rows are filtered, so lexical-only hits and non-vector candidates pass through. `fused_combined` compares the post-RRF max-normalized similarity and filters every simMap entry (including lexical-only entries whose normalized score reflects combined evidence); list-fallback and shared-namespace candidates still bypass. fused_combined is rank-relative: post-RRF scores are normalized so the top result for a given query is always 1.0, so the threshold's selectivity floats with query difficulty. fused_combined requires recall.fusion.enabled=true; combining it with a non-zero threshold while fusion is disabled returns 400.")),
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

	// Leave limit and graphDepth zero when the caller omits them so the
	// recall service applies recall.default_limit / recall.graph.default_depth.
	var limit int
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}

	tags := extractStringSlice(args["tags"])

	includeGraph := true
	if v, ok := args["include_graph"].(bool); ok {
		includeGraph = v
	}

	var graphDepth int
	if v, ok := args["graph_depth"].(float64); ok && v > 0 {
		graphDepth = int(v)
	}

	diversifyPrefix, _ := args["diversify_by_tag_prefix"].(string)

	includeLowNovelty := false
	if v, ok := args["include_low_novelty"].(bool); ok {
		includeLowNovelty = v
	}

	// Pass through whatever JSON gave us; the service layer applies the
	// [0, 1] range check (and the NaN guard). Silently zeroing negatives
	// here would make MCP and REST disagree for the same input.
	var similarityThreshold float64
	if v, ok := args["similarity_threshold"].(float64); ok {
		similarityThreshold = v
	}
	similarityThresholdMode, _ := args["similarity_threshold_mode"].(string)
	similarityThresholdMode = strings.TrimSpace(similarityThresholdMode)

	deps := s.Deps()
	uid := ac.UserID

	req := &service.RecallRequest{
		Query:                   query,
		Limit:                   limit,
		SimilarityThreshold:     similarityThreshold,
		SimilarityThresholdMode: similarityThresholdMode,
		Tags:                    tags,
		IncludeGraph:            includeGraph,
		GraphDepth:              graphDepth,
		IncludeLowNovelty:       includeLowNovelty,
		DiversifyByTagPrefix:    diversifyPrefix,
		UserID:                  &uid,
		APIKeyID:                ac.APIKeyID,
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

	mcpResp := buildMCPRecallResponse(ctx, deps.EntityReader, resp, allowedNS, projectionOpts{IncludeLowNovelty: includeLowNovelty})
	return wrapToolResult(mcpResp, newRecallReducer(mcpResp))
}
