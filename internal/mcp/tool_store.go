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
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterStoreTools registers the memory_store and memory_store_batch MCP tools
// on the given server.
func RegisterStoreTools(s *Server) {
	registerMemoryStore(s)
	registerMemoryStoreBatch(s)
}

func registerMemoryStore(s *Server) {
	opts := []mcp.ToolOption{
		mcp.WithDescription("Store important context to persistent memory. Use proactively: store preferences, decisions, corrections, architecture choices, bugs, workarounds, and task summaries without being asked. ALWAYS call memory_projects first and use an existing project — only auto-create when no existing project fits. Tag consistently for easy recall. Identical content in the same project returns the existing memory's ID without creating a duplicate; tags and metadata on the new request are ignored on a dedup hit."),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global'). Prefer an existing project — call memory_projects first. Auto-creates if missing, but treat this as a last resort.")),
		mcp.WithString("project_description", mcp.Description("Description for the project (sets on create, or updates if currently empty)")),
		mcp.WithString("content", mcp.Required(), mcp.Description("Content to store")),
		mcp.WithString("source", mcp.Description("Origin identifier")),
		mcp.WithArray("tags", mcp.Description("Labels for filtering")),
		mcp.WithObject("metadata", mcp.Description("Arbitrary key-value metadata")),
		mcp.WithString("ttl", mcp.Description("Time-to-live duration (e.g. '24h', '7d', '30m'). Memory expires after this duration.")),
	}
	opts = append(opts, mcp.WithBoolean("enrich", mcp.Description("Queue async enrichment (default false)")))

	tool := mcp.NewTool("memory_store", opts...)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryStore(ctx, s, request)
	})
}

func registerMemoryStoreBatch(s *Server) {
	opts := []mcp.ToolOption{
		mcp.WithDescription("Store multiple memories at once. Use when you have several related facts, decisions, or observations to persist. Each item needs its own content; they share the same project and TTL. ALWAYS call memory_projects first and use an existing project — only auto-create when no existing project fits. Items whose content matches an existing memory in the same project return that memory's ID instead of creating a duplicate."),
		mcp.WithString("project", mcp.Description("Project slug (default: 'global'). Prefer an existing project — call memory_projects first. Auto-creates if missing, but treat this as a last resort.")),
		mcp.WithString("project_description", mcp.Description("Description for the project (sets on create, or updates if currently empty)")),
		mcp.WithArray("items", mcp.Required(), mcp.Description("Array of objects with content (required), source, tags, metadata")),
		mcp.WithString("ttl", mcp.Description("Time-to-live duration (e.g. '24h', '7d', '30m'). All items expire after this duration.")),
	}
	opts = append(opts, mcp.WithBoolean("enrich", mcp.Description("Queue enrichment for all items (default false)")))

	tool := mcp.NewTool("memory_store_batch", opts...)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryStoreBatch(ctx, s, request)
	})
}

func handleMemoryStore(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	projectDesc, _ := args["project_description"].(string)
	projectDesc = strings.TrimSpace(projectDesc)

	content, ok := args["content"].(string)
	if !ok || strings.TrimSpace(content) == "" {
		return mcp.NewToolResultError("content is required"), nil
	}

	source, _ := args["source"].(string)
	tags := extractStringSlice(args["tags"])
	metadata := extractRawJSON(args["metadata"])

	var enrich bool
	if v, ok := args["enrich"].(bool); ok {
		enrich = v
	}

	ttl, _ := args["ttl"].(string)

	project, err := resolveOrCreateProject(ctx, s.Deps(), ac.UserID, projectSlug, projectDesc)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to resolve project: %v", err)), nil
	}

	uid := ac.UserID
	req := &service.StoreRequest{
		ProjectID: project.ID,
		Content:   content,
		Source:    source,
		Tags:      tags,
		Metadata:  metadata,
		Options: service.StoreOptions{
			Enrich: enrich,
			TTL:    ttl,
		},
		UserID:   &uid,
		APIKeyID: ac.APIKeyID,
	}
	if ac.OrgID != uuid.Nil {
		oid := ac.OrgID
		req.OrgID = &oid
	}

	resp, err := s.Deps().Store.Store(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("store failed: %v", err)), nil
	}

	events.Emit(ctx, s.Deps().EventBus, events.MemoryCreated, "project:"+project.ID.String(), map[string]string{
		"memory_id":  resp.ID.String(),
		"project_id": project.ID.String(),
	})

	return wrapToolResult(buildMCPStoreResponse(resp), nil)
}

func handleMemoryStoreBatch(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	projectDesc, _ := args["project_description"].(string)
	projectDesc = strings.TrimSpace(projectDesc)

	rawItems, ok := args["items"].([]interface{})
	if !ok || len(rawItems) == 0 {
		return mcp.NewToolResultError("items is required and must be a non-empty array"), nil
	}

	items, err := extractBatchItems(rawItems)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid items: %v", err)), nil
	}

	var enrich bool
	if v, ok := args["enrich"].(bool); ok {
		enrich = v
	}

	ttl, _ := args["ttl"].(string)

	project, err := resolveOrCreateProject(ctx, s.Deps(), ac.UserID, projectSlug, projectDesc)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to resolve project: %v", err)), nil
	}

	uid := ac.UserID
	req := &service.BatchStoreRequest{
		ProjectID: project.ID,
		Items:     items,
		Options: service.StoreOptions{
			Enrich: enrich,
			TTL:    ttl,
		},
		UserID:   &uid,
		APIKeyID: ac.APIKeyID,
	}
	if ac.OrgID != uuid.Nil {
		oid := ac.OrgID
		req.OrgID = &oid
	}

	resp, err := s.Deps().BatchStore.BatchStore(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("batch store failed: %v", err)), nil
	}

	scope := "project:" + project.ID.String()
	for i := 0; i < resp.MemoriesCreated; i++ {
		events.Emit(ctx, s.Deps().EventBus, events.MemoryCreated, scope, map[string]string{
			"project_id": project.ID.String(),
		})
	}

	return wrapToolResult(buildMCPBatchStoreResponse(resp), nil)
}

// resolveOrCreateProject looks up an existing project by slug under the user's
// namespace. If not found, it auto-creates a child namespace and project.
// If description is non-empty and the project's description is empty, it is updated.
func resolveOrCreateProject(ctx context.Context, deps Dependencies, userID uuid.UUID, slug, description string) (*model.Project, error) {
	user, err := deps.UserRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}

	// Try existing project first.
	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, slug)
	if err == nil {
		// Update description if the caller provided one and the project has none.
		if description != "" && project.Description == "" {
			_ = deps.ProjectRepo.UpdateDescription(ctx, project.ID, description)
			project.Description = description
		}
		return project, nil
	}

	// Auto-create: look up the user namespace, create a child namespace, create the project.
	userNS, err := deps.NamespaceRepo.GetByID(ctx, user.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("user namespace not found: %w", err)
	}

	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     slug,
		Slug:     slug,
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     userNS.Path + "/" + slug,
		Depth:    userNS.Depth + 1,
	}
	if err := deps.NamespaceRepo.Create(ctx, projectNS); err != nil {
		return nil, fmt.Errorf("create project namespace: %w", err)
	}

	project = &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             slug,
		Slug:             slug,
		Description:      description,
		DefaultTags:      []string{},
		Settings:         json.RawMessage(`{}`),
	}
	if err := deps.ProjectRepo.Create(ctx, project); err != nil {
		return nil, fmt.Errorf("create project: %w", err)
	}

	return project, nil
}

// extractStringSlice converts an interface{} (expected []interface{} of strings)
// into a []string. Returns nil if the input is nil or not a slice.
func extractStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

// extractRawJSON converts an interface{} (expected map[string]interface{}) back
// into json.RawMessage. Returns nil if the input is nil or marshalling fails.
func extractRawJSON(v interface{}) json.RawMessage {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return raw
}

// extractBatchItems converts a []interface{} of map items into service.BatchStoreItem slice.
func extractBatchItems(raw []interface{}) ([]service.BatchStoreItem, error) {
	items := make([]service.BatchStoreItem, 0, len(raw))
	for i, v := range raw {
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("item at index %d is not an object", i)
		}

		content, _ := m["content"].(string)
		if strings.TrimSpace(content) == "" {
			return nil, fmt.Errorf("item at index %d: content is required", i)
		}

		item := service.BatchStoreItem{
			Content:  content,
			Source:   stringFromMap(m, "source"),
			Tags:     extractStringSlice(m["tags"]),
			Metadata: extractRawJSON(m["metadata"]),
		}
		items = append(items, item)
	}
	return items, nil
}

// stringFromMap extracts a string value from a map by key, returning "" if absent.
func stringFromMap(m map[string]interface{}, key string) string {
	v, _ := m[key].(string)
	return v
}
