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

// RegisterUpdateGetTools registers the memory_update and memory_get MCP tools
// on the given server.
func RegisterUpdateGetTools(s *Server) {
	registerMemoryUpdate(s)
	registerMemoryGet(s)
}

func registerMemoryUpdate(s *Server) {
	tool := mcp.NewTool("memory_update",
		mcp.WithDescription("Update an existing memory by ID. Use when information has changed or needs correction rather than storing a duplicate. Project must already exist."),
		mcp.WithString("id", mcp.Required(), mcp.Description("Memory ID to update")),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global')")),
		mcp.WithString("content", mcp.Description("New content (triggers re-embedding on Postgres)")),
		mcp.WithArray("tags", mcp.Description("Replace tags")),
		mcp.WithObject("metadata", mcp.Description("Replace metadata")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryUpdate(ctx, s, request)
	})
}

func registerMemoryGet(s *Server) {
	tool := mcp.NewTool("memory_get",
		mcp.WithDescription("Retrieve specific memories by ID when you need the full content from a previous recall result. Project must already exist."),
		mcp.WithArray("ids", mcp.Required(), mcp.Description("Memory IDs to retrieve")),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global')")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryGet(ctx, s, request)
	})
}

func handleMemoryUpdate(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	idStr, ok := args["id"].(string)
	if !ok || strings.TrimSpace(idStr) == "" {
		return mcp.NewToolResultError("id is required"), nil
	}

	memoryID, err := uuid.Parse(idStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid memory id: %v", err)), nil
	}

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)
	if projectSlug == "" {
		projectSlug = "global"
	}

	// Build optional fields — only set pointers when args are present.
	var contentPtr *string
	if v, ok := args["content"].(string); ok {
		contentPtr = &v
	}

	var tagsPtr *[]string
	if _, ok := args["tags"]; ok {
		tags := extractStringSlice(args["tags"])
		if tags == nil {
			tags = []string{}
		}
		tagsPtr = &tags
	}

	var metadataPtr *json.RawMessage
	if _, ok := args["metadata"]; ok {
		raw := extractRawJSON(args["metadata"])
		if raw == nil {
			empty := json.RawMessage(`{}`)
			metadataPtr = &empty
		} else {
			metadataPtr = &raw
		}
	}

	if contentPtr == nil && tagsPtr == nil && metadataPtr == nil {
		return mcp.NewToolResultError("at least one of content, tags, or metadata must be provided"), nil
	}

	// Resolve project (no auto-create for update).
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
	req := &service.UpdateRequest{
		ProjectID: project.ID,
		MemoryID:  memoryID,
		Content:   contentPtr,
		Tags:      tagsPtr,
		Metadata:  metadataPtr,
		UserID:    &uid,
		APIKeyID:  ac.APIKeyID,
	}

	resp, err := deps.Update.Update(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("update failed: %v", err)), nil
	}

	events.Emit(ctx, deps.EventBus, events.MemoryUpdated, "project:"+project.ID.String(), map[string]string{
		"memory_id":  resp.ID.String(),
		"project_id": project.ID.String(),
	})

	return wrapToolResult(resp, nil)
}

func handleMemoryGet(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	ids := make([]uuid.UUID, 0, len(rawIDs))
	for i, v := range rawIDs {
		s, ok := v.(string)
		if !ok {
			return mcp.NewToolResultError(fmt.Sprintf("ids[%d] is not a string", i)), nil
		}
		parsed, err := uuid.Parse(s)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("ids[%d] is not a valid UUID: %v", i, err)), nil
		}
		ids = append(ids, parsed)
	}

	// Resolve project (no auto-create for get).
	deps := s.Deps()
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	req := &service.BatchGetRequest{
		ProjectID: project.ID,
		IDs:       ids,
	}

	resp, err := deps.BatchGet.BatchGet(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("batch get failed: %v", err)), nil
	}

	return wrapToolResult(resp, nil)
}
