package mcp

import (
	"context"
	"fmt"
	"math"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// aboutMeDefaultLimit is the framing-fetch default page size — larger than the
// generic list default because "load who the user is" wants the whole persona,
// still capped at listMaxLimit.
const aboutMeDefaultLimit = 100

// RegisterAboutMeTool registers the about_me framing-fetch tool: a per-user
// read that returns the persona tier (the reserved about_me project) ordered
// most-defining first. Like the procedural tools, it is intentionally NOT in
// shareToolPolicy, so share-bearer connections can neither see nor call it —
// self-knowledge is never exposed through a per-project share grant.
func RegisterAboutMeTool(s *Server) {
	tool := mcp.NewTool("about_me",
		mcp.WithTitleAnnotation("Who The User Is"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[listMemoryResponse]()),
		mcp.WithDescription("Return what nram knows about who the user is — their persona / self-knowledge (identity, background, preferences, relationships, ongoing personal context) — ordered most-defining first (by how central each fact's entities are, then how often it has surfaced, then recency). Call this on demand when you need to understand the user — before personalizing work, making assumptions about them, or when the task hinges on their preferences or background. You do NOT need to load it every session: ordinary recall already surfaces relevant about_me facts by association."),
		mcp.WithNumber("limit", mcp.Description("Maximum number of entries to return (default 100, max 200).")),
		mcp.WithNumber("offset", mcp.Description("Number of entries to skip for pagination (default 0).")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleAboutMe(ctx, s, request)
	})
}

func handleAboutMe(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}
	if ac.ShareTokenID != nil {
		return mcp.NewToolResultError("forbidden: about_me is not accessible to share-token callers"), nil
	}

	deps := s.Deps()
	args := request.GetArguments()
	limit := parseIntArg(args, "limit", aboutMeDefaultLimit, 1, listMaxLimit)
	offset := parseIntArg(args, "offset", 0, 0, math.MaxInt32)

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, model.ReservedProjectSlugAboutMe)
	if err != nil || project == nil {
		return mcp.NewToolResultError("about_me project not found"), nil
	}

	// HideSuperseded matches the framing query, which excludes superseded rows.
	total, err := deps.MemoryLister.CountByNamespaceFiltered(ctx, project.NamespaceID, storage.MemoryListFilters{HideSuperseded: true})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("about_me count failed: %v", err)), nil
	}

	memories, err := deps.MemoryLister.ListByNamespaceFramingOrder(ctx, project.NamespaceID, limit, offset)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("about_me fetch failed: %v", err)), nil
	}

	items := buildListMemoryItems(memories, func(model.Memory) string { return model.ReservedProjectSlugAboutMe })

	resp := &listMemoryResponse{
		Data: items,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	}
	return wrapToolResult(s.deps.Metrics, "about_me", mcpBudgetBytes(ctx, s.deps.Settings), resp, newListReducer(resp))
}
