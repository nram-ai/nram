package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// ProjectUpdater provides project update operations for MCP tool handlers.
type ProjectUpdater interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	Update(ctx context.Context, project *model.Project) error
}

// RegisterProjectUpdateTool registers the memory_update_project MCP tool.
func RegisterProjectUpdateTool(s *Server) {
	tool := mcp.NewTool("memory_update_project",
		mcp.WithDescription("Update a project's name, description, or default tags. Only works on projects you own."),
		mcp.WithString("project", mcp.Required(), mcp.Description("Project slug to update")),
		mcp.WithString("name", mcp.Description("New project name")),
		mcp.WithString("description", mcp.Description("New project description")),
		mcp.WithArray("default_tags", mcp.Description("Replace default tags for the project")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProjectUpdate(ctx, s, request)
	})
}

func handleProjectUpdate(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
		return mcp.NewToolResultError("project slug is required"), nil
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

	if project.OwnerNamespaceID != user.NamespaceID {
		return mcp.NewToolResultError("you can only update your own projects"), nil
	}

	changed := false

	if name, ok := args["name"].(string); ok && strings.TrimSpace(name) != "" {
		project.Name = strings.TrimSpace(name)
		changed = true
	}

	if desc, ok := args["description"].(string); ok {
		project.Description = desc
		changed = true
	}

	if rawTags := extractStringSlice(args["default_tags"]); rawTags != nil {
		project.DefaultTags = rawTags
		changed = true
	}

	if !changed {
		return mcp.NewToolResultError("at least one of name, description, or default_tags must be provided"), nil
	}

	if deps.ProjectUpdater == nil {
		return mcp.NewToolResultError("project update not available"), nil
	}

	if err := deps.ProjectUpdater.Update(ctx, project); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("update failed: %v", err)), nil
	}

	out, err := json.Marshal(map[string]interface{}{
		"project":     project.Slug,
		"name":        project.Name,
		"description": project.Description,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}
