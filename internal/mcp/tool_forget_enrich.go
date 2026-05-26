package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterForgetTool registers the forget MCP tool on the given server.
func RegisterForgetTool(s *Server) {
	tool := mcp.NewTool("forget",
		mcp.WithTitleAnnotation("Forget Memories"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[service.ForgetResponse]()),
		mcp.WithDescription("Delete memories that are outdated, incorrect, or superseded. Soft-deletes by default. Project must already exist."),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global')")),
		mcp.WithArray("ids", mcp.Required(), mcp.Description("Memory IDs to forget")),
		mcp.WithBoolean("hard", mcp.Description("Permanent hard delete that bypasses the tombstone and cannot be reversed. Default false performs a soft delete that the lifecycle sweeper later purges per the purge_after policy.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryForget(ctx, s, request)
	})
}

func handleMemoryForget(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if err := checkWriteAccess(ctx); err != nil {
		return err, nil
	}
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

	rawIDs, ok := args["ids"].([]interface{})
	if !ok || len(rawIDs) == 0 {
		return mcp.NewToolResultError("ids is required and must be a non-empty array"), nil
	}

	parsedIDs := make([]uuid.UUID, 0, len(rawIDs))
	for i, v := range rawIDs {
		str, ok := v.(string)
		if !ok {
			return mcp.NewToolResultError(fmt.Sprintf("ids[%d] is not a string", i)), nil
		}
		parsed, err := uuid.Parse(str)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("ids[%d] is not a valid UUID: %v", i, err)), nil
		}
		parsedIDs = append(parsedIDs, parsed)
	}

	var hard bool
	if v, ok := args["hard"].(bool); ok {
		hard = v
	}

	// Resolve project (no auto-create for forget).
	deps := s.Deps()
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	uid := ac.UserID
	req := &service.ForgetRequest{
		ProjectID:  project.ID,
		MemoryIDs:  parsedIDs,
		HardDelete: hard,
		UserID:     &uid,
	}

	resp, err := deps.Forget.Forget(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("forget failed: %v", err)), nil
	}

	events.Emit(ctx, deps.EventBus, events.MemoryDeleted, "project:"+project.ID.String(), map[string]string{
		"project_id": project.ID.String(),
		"deleted":    fmt.Sprintf("%d", resp.Deleted),
	})

	return wrapToolResult(s.deps.Metrics, "forget", mcpBudgetBytes(ctx, s.deps.Settings), resp, nil)
}
