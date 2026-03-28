package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

// graphResponse is the JSON envelope returned by the memory_graph tool.
type graphResponse struct {
	Entities       []graphEntity       `json:"entities"`
	Relationships  []graphRelationship `json:"relationships"`
	Query          string              `json:"query"`
	Depth          int                 `json:"depth"`
	IncludeHistory bool                `json:"include_history"`
}

// graphEntity is a minimal entity representation for graph results.
type graphEntity struct {
	ID           uuid.UUID       `json:"id"`
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Canonical    string          `json:"canonical"`
	Properties   json.RawMessage `json:"properties,omitempty"`
	MentionCount int             `json:"mention_count"`
}

// graphRelationship is a minimal relationship representation for graph results.
type graphRelationship struct {
	ID         uuid.UUID  `json:"id"`
	SourceID   uuid.UUID  `json:"source_id"`
	TargetID   uuid.UUID  `json:"target_id"`
	Relation   string     `json:"relation"`
	Weight     float64    `json:"weight"`
	ValidFrom  time.Time  `json:"valid_from"`
	ValidUntil *time.Time `json:"valid_until,omitempty"`
}

// projectItem is the JSON representation of a project in the memory_projects response.
type projectItem struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Slug        string    `json:"slug"`
	Description string    `json:"description"`
}

// RegisterGraphProjectsExportTools registers memory_graph, memory_projects, and memory_export.
func RegisterGraphProjectsExportTools(s *Server) {
	registerMemoryGraph(s)
	registerMemoryProjects(s)
	registerMemoryExport(s)
}

func registerMemoryGraph(s *Server) {
	tool := mcp.NewTool("memory_graph",
		mcp.WithDescription("Explore entity relationships in the knowledge graph. Use to discover how people, technologies, and concepts connect — especially when recall alone does not surface enough context."),
		mcp.WithString("entity", mcp.Required(), mcp.Description("Entity name or search query")),
		mcp.WithString("project", mcp.Description("Project slug to scope the search")),
		mcp.WithNumber("depth", mcp.Description("Graph traversal depth (default 2)")),
		mcp.WithBoolean("include_history", mcp.Description("Include expired/past relationships (default false)")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryGraph(ctx, s, request)
	})
}

func registerMemoryProjects(s *Server) {
	tool := mcp.NewTool("memory_projects",
		mcp.WithDescription("List all available projects with slugs and descriptions. ALWAYS call this before memory_store to check for an existing project — do not create new projects unnecessarily."),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryProjects(ctx, s, request)
	})
}

func registerMemoryExport(s *Server) {
	tool := mcp.NewTool("memory_export",
		mcp.WithDescription("Export all memories from a project for backup, migration, or analysis. Project must already exist."),
		mcp.WithString("project", mcp.Description("Project slug to export (default: 'global')")),
		mcp.WithString("format", mcp.Description("Export format: \"json\" or \"ndjson\" (default \"json\")")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryExport(ctx, s, request)
	})
}

func handleMemoryGraph(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	entityQuery, ok := args["entity"].(string)
	if !ok || strings.TrimSpace(entityQuery) == "" {
		return mcp.NewToolResultError("entity is required"), nil
	}

	depth := 2
	if v, ok := args["depth"].(float64); ok && v > 0 {
		depth = int(v)
	}

	includeHistory := false
	if v, ok := args["include_history"].(bool); ok {
		includeHistory = v
	}

	deps := s.Deps()

	// Resolve namespace: project-scoped or user-scoped.
	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	namespaceID := user.NamespaceID

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)
	if projectSlug != "" {
		project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil {
			return mcp.NewToolResultError("project not found"), nil
		}
		namespaceID = project.NamespaceID
	}

	// Find matching entities.
	entities, err := deps.EntityReader.FindBySimilarity(ctx, namespaceID, entityQuery, "", 10)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entity search failed: %v", err)), nil
	}

	// Collect entities and traverse relationships.
	seenEntities := make(map[uuid.UUID]struct{})
	var graphEntities []graphEntity
	var graphRels []graphRelationship
	seenRels := make(map[uuid.UUID]struct{})

	for _, ent := range entities {
		if _, ok := seenEntities[ent.ID]; ok {
			continue
		}
		seenEntities[ent.ID] = struct{}{}
		graphEntities = append(graphEntities, graphEntity{
			ID:           ent.ID,
			Name:         ent.Name,
			Type:         ent.EntityType,
			Canonical:    ent.Canonical,
			Properties:   ent.Properties,
			MentionCount: ent.MentionCount,
		})

		rels, err := deps.Traverser.TraverseFromEntity(ctx, ent.ID, depth)
		if err != nil {
			continue
		}
		for _, rel := range rels {
			if _, ok := seenRels[rel.ID]; ok {
				continue
			}
			seenRels[rel.ID] = struct{}{}
			graphRels = append(graphRels, graphRelationship{
				ID:         rel.ID,
				SourceID:   rel.SourceID,
				TargetID:   rel.TargetID,
				Relation:   rel.Relation,
				Weight:     rel.Weight,
				ValidFrom:  rel.ValidFrom,
				ValidUntil: rel.ValidUntil,
			})
		}
	}

	// Filter out expired relationships when include_history is false.
	if !includeHistory {
		now := time.Now()
		filtered := graphRels[:0]
		for _, rel := range graphRels {
			if rel.ValidUntil == nil || rel.ValidUntil.After(now) {
				filtered = append(filtered, rel)
			}
		}
		graphRels = filtered
	}

	if graphEntities == nil {
		graphEntities = []graphEntity{}
	}
	if graphRels == nil {
		graphRels = []graphRelationship{}
	}

	resp := graphResponse{
		Entities:       graphEntities,
		Relationships:  graphRels,
		Query:          entityQuery,
		Depth:          depth,
		IncludeHistory: includeHistory,
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}

func handleMemoryProjects(ctx context.Context, s *Server, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	projects, err := deps.ProjectRepo.ListByUser(ctx, user.NamespaceID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list projects: %v", err)), nil
	}

	items := make([]projectItem, 0, len(projects))
	for _, p := range projects {
		items = append(items, projectItem{
			ID:          p.ID,
			Name:        p.Name,
			Slug:        p.Slug,
			Description: p.Description,
		})
	}

	out, err := json.Marshal(items)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}

func handleMemoryExport(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	format := "json"
	if v, ok := args["format"].(string); ok && strings.TrimSpace(v) != "" {
		format = strings.TrimSpace(strings.ToLower(v))
	}
	if format != "json" && format != "ndjson" {
		return mcp.NewToolResultError(fmt.Sprintf("unsupported format %q; use \"json\" or \"ndjson\"", format)), nil
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

	exportSvc := deps.Export

	if format == "ndjson" {
		req := &service.ExportRequest{
			ProjectID: project.ID,
			Format:    service.ExportFormatNDJSON,
		}
		var buf bytes.Buffer
		if err := exportSvc.ExportNDJSON(ctx, req, &buf); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("export failed: %v", err)), nil
		}
		return mcp.NewToolResultText(buf.String()), nil
	}

	req := &service.ExportRequest{
		ProjectID: project.ID,
		Format:    service.ExportFormatJSON,
	}
	data, err := exportSvc.Export(ctx, req)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("export failed: %v", err)), nil
	}

	out, err := json.Marshal(data)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal response: %v", err)), nil
	}

	return mcp.NewToolResultText(string(out)), nil
}
