package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterForgetEnrichTools registers the memory_forget and memory_enrich MCP
// tools on the given server.
func RegisterForgetEnrichTools(s *Server) {
	registerMemoryForget(s)
	registerMemoryEnrich(s)
}

func registerMemoryForget(s *Server) {
	tool := mcp.NewTool("memory_forget",
		mcp.WithDescription("Forget (delete) one or more memories from a project."),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global')")),
		mcp.WithArray("ids", mcp.Required(), mcp.Description("Memory IDs to forget")),
		mcp.WithBoolean("hard", mcp.Description("Hard delete vs soft delete (default false)")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryForget(ctx, s, request)
	})
}

func registerMemoryEnrich(s *Server) {
	tool := mcp.NewTool("memory_enrich",
		mcp.WithDescription("Queue enrichment for memories in a project."),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global')")),
		mcp.WithArray("ids", mcp.Description("Specific memory IDs to enrich; omit to enrich all un-enriched")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryEnrich(ctx, s, request)
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

	out, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}

func handleMemoryEnrich(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	// Resolve project (no auto-create for enrich).
	deps := s.Deps()
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	// Parse optional ids array.
	var parsedIDs []uuid.UUID
	if rawIDs, ok := args["ids"].([]interface{}); ok && len(rawIDs) > 0 {
		parsedIDs = make([]uuid.UUID, 0, len(rawIDs))
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
	}

	req := &service.EnrichRequest{
		ProjectID: project.ID,
	}
	if len(parsedIDs) > 0 {
		req.MemoryIDs = parsedIDs
	} else {
		req.All = true
	}

	resp, err := deps.Enrich.Enrich(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("enrich failed: %v", err)), nil
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}
