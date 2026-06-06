package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterProjectDeleteTool registers the memory_delete_project MCP tool.
func RegisterProjectDeleteTool(s *Server) {
	if s.Deps().ProjectDelete == nil {
		return
	}
	registerProjectDelete(s)
}

func registerProjectDelete(s *Server) {
	tool := mcp.NewTool("delete_project",
		mcp.WithTitleAnnotation("Delete Project"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[service.ProjectDeleteResponse]()),
		mcp.WithDescription("Permanently delete a project and all its memories, entities, and relationships. Only works on projects you own. The 'global' project cannot be deleted."),
		mcp.WithString("project", mcp.Required(), mcp.Description("Project slug to delete")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProjectDelete(ctx, s, request)
	})
}

func handleProjectDelete(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if ac.ShareTokenID != nil {
		return mcp.NewToolResultError("share-bearer is not authorized to delete projects"), nil
	}

	args := request.GetArguments()

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)
	if projectSlug == "" {
		return mcp.NewToolResultError("project slug is required"), nil
	}
	if model.IsReservedProjectSlug(projectSlug) {
		return mcp.NewToolResultError(fmt.Sprintf("the %s project is reserved and cannot be deleted", projectSlug)), nil
	}

	deps := s.Deps()
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	// Resolve project by slug (no auto-create).
	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	// Verify ownership.
	if project.OwnerNamespaceID != user.NamespaceID {
		return mcp.NewToolResultError("you can only delete your own projects"), nil
	}

	resp, err := deps.ProjectDelete.Delete(ctx, &service.ProjectDeleteRequest{
		ProjectID: project.ID,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("delete failed: %v", err)), nil
	}

	return wrapToolResult(s.deps.Metrics, "delete_project", mcpBudgetBytes(ctx, s.deps.Settings), resp, nil)
}
