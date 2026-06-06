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
	"github.com/nram-ai/nram/internal/service"
)

// RegisterResources registers all MCP resources on the given server.
func RegisterResources(s *Server) {
	registerProjectsResource(s)
	registerProjectEntitiesResource(s)
	registerProjectGraphResource(s)
}

// extractSlugFromURI parses a project slug from a resource URI.
// URI format: nram://projects/{slug}/{suffix}
func extractSlugFromURI(uri string, suffix string) string {
	const prefix = "nram://projects/"
	s := strings.TrimPrefix(uri, prefix)
	s = strings.TrimSuffix(s, "/"+suffix)
	return s
}

// registerProjectsResource registers the nram://projects static resource.
func registerProjectsResource(s *Server) {
	resource := mcp.NewResource(
		"nram://projects",
		"My Projects",
		mcp.WithResourceDescription("List all projects for the authenticated user"),
		mcp.WithMIMEType("application/json"),
		mcp.WithResourceIcons(iconAnnotation()),
	)

	s.MCPServer().AddResource(resource, func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return handleProjectsResource(ctx, s, request)
	})
}

func handleProjectsResource(ctx context.Context, s *Server, _ mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return nil, fmt.Errorf("no HTTP request in context")
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return nil, fmt.Errorf("authentication required")
	}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}

	projects, err := deps.ProjectRepo.ListByUser(ctx, user.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}

	items := make([]projectItem, 0, len(projects))
	for _, p := range projects {
		// Share-bearers may only see projects covered by their grant set.
		// shareTokenAllowsProjectID is a no-op for non-share callers.
		if !shareTokenAllowsProjectID(ac, p.ID) {
			continue
		}
		items = append(items, projectItem{
			ID:          p.ID,
			Name:        p.Name,
			Slug:        p.Slug,
			Description: p.Description,
		})
	}

	// Wire shape matches the list_projects tool's outputSchema-conforming
	// envelope so the resource and the tool serialize the same data the same
	// way. The resource is unpaginated, so populate Pagination honestly:
	// Total/Limit equal the full item count and Offset is 0; a client that
	// reads pagination.total to render "showing X of Y" sees an accurate
	// "showing N of N" rather than the misleading "showing N of 0" the
	// zero-value Pagination would produce.
	out, err := json.Marshal(&listProjectsResponse{
		Projects: items,
		Pagination: model.Pagination{
			Total:  len(items),
			Limit:  len(items),
			Offset: 0,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "nram://projects",
			MIMEType: "application/json",
			Text:     string(out),
		},
	}, nil
}

// registerProjectEntitiesResource registers the nram://projects/{slug}/entities resource template.
func registerProjectEntitiesResource(s *Server) {
	template := mcp.NewResourceTemplate(
		"nram://projects/{slug}/entities",
		"Project Entities",
		mcp.WithTemplateDescription("All entities in a given project"),
		mcp.WithTemplateMIMEType("application/json"),
		mcp.WithTemplateIcons(iconAnnotation()),
	)

	s.MCPServer().AddResourceTemplate(template, func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return handleProjectEntitiesResource(ctx, s, request)
	})
}

// resourceEntity is the JSON representation of an entity in the entities resource.
type resourceEntity struct {
	ID           uuid.UUID       `json:"id"`
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Canonical    string          `json:"canonical"`
	Properties   json.RawMessage `json:"properties,omitempty"`
	MentionCount int             `json:"mention_count"`
}

func handleProjectEntitiesResource(ctx context.Context, s *Server, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return nil, fmt.Errorf("no HTTP request in context")
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return nil, fmt.Errorf("authentication required")
	}

	uri := request.Params.URI
	slug := extractSlugFromURI(uri, "entities")
	if slug == "" {
		return nil, fmt.Errorf("project slug is required")
	}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, slug)
	if err != nil {
		return nil, fmt.Errorf("project not found")
	}

	// Share-bearers may only read entities for projects in their grant set.
	if !shareTokenAllowsProjectID(ac, project.ID) {
		return nil, fmt.Errorf("project not found")
	}

	entities, err := deps.EntityReader.ListByNamespace(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list entities: %w", err)
	}

	items := make([]resourceEntity, 0, len(entities))
	for _, e := range entities {
		items = append(items, resourceEntity{
			ID:           e.ID,
			Name:         e.Name,
			Type:         e.EntityType,
			Canonical:    e.Canonical,
			Properties:   e.Properties,
			MentionCount: e.MentionCount,
		})
	}

	out, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      uri,
			MIMEType: "application/json",
			Text:     string(out),
		},
	}, nil
}

// registerProjectGraphResource registers the nram://projects/{slug}/graph resource template.
func registerProjectGraphResource(s *Server) {
	template := mcp.NewResourceTemplate(
		"nram://projects/{slug}/graph",
		"Project Graph",
		mcp.WithTemplateDescription("Entity relationship graph for a project"),
		mcp.WithTemplateMIMEType("application/json"),
		mcp.WithTemplateIcons(iconAnnotation()),
	)

	s.MCPServer().AddResourceTemplate(template, func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return handleProjectGraphResource(ctx, s, request)
	})
}

// resourceGraph is the JSON envelope for the graph resource. Truncated
// carries the edge_cap signal when traversal short-circuited at
// graph.max_edges; consumers must inspect it to detect partial graphs
// since the resource has no other mechanism to flag a partial result.
type resourceGraph struct {
	Entities      []graphEntity       `json:"entities"`
	Relationships []graphRelationship `json:"relationships"`
	Truncated     *truncationInfo     `json:"_truncated,omitempty"`
}

func handleProjectGraphResource(ctx context.Context, s *Server, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return nil, fmt.Errorf("no HTTP request in context")
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return nil, fmt.Errorf("authentication required")
	}

	uri := request.Params.URI
	slug := extractSlugFromURI(uri, "graph")
	if slug == "" {
		return nil, fmt.Errorf("project slug is required")
	}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, slug)
	if err != nil {
		return nil, fmt.Errorf("project not found")
	}

	// Share-bearers may only read the graph for projects in their grant set.
	if !shareTokenAllowsProjectID(ac, project.ID) {
		return nil, fmt.Errorf("project not found")
	}

	entities, err := deps.EntityReader.ListByNamespace(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list entities: %w", err)
	}

	// Same edge cap as the memory_graph tool; this resource is the
	// project-scoped quick-view backed by the same traverser, so it shares
	// the graph.max_edges knob to short-circuit traversal on large
	// namespaces. The cap is applied per-seed AND cumulatively across
	// seeds so a project with many lightly-connected entities cannot
	// silently produce an N×cap union. ResolveIntWithDefault is nil-safe.
	maxEdges := deps.Settings.ResolveIntWithDefault(ctx, service.SettingGraphMaxEdges, "global")

	seenEntities := make(map[uuid.UUID]struct{})
	var graphEntities []graphEntity
	var graphRels []graphRelationship
	seenRels := make(map[uuid.UUID]struct{})
	truncatedByCap := false

seeds:
	for _, ent := range entities {
		if _, ok := seenEntities[ent.ID]; ok {
			continue
		}
		seenEntities[ent.ID] = struct{}{}
		graphEntities = append(graphEntities, graphEntity{
			ID:           ent.ID,
			Name:         ent.Name,
			Type:         ent.EntityType,
			MentionCount: ent.MentionCount,
		})

		seedCap := maxEdges
		if maxEdges > 0 {
			seedCap = maxEdges - len(graphRels)
			if seedCap <= 0 {
				truncatedByCap = true
				break
			}
		}

		tr, tErr := deps.Traverser.TraverseFromEntity(ctx, ent.ID, 1, seedCap)
		if tErr != nil {
			continue
		}
		if tr.Truncated {
			truncatedByCap = true
		}
		for _, rel := range tr.Relationships {
			if _, ok := seenRels[rel.ID]; ok {
				continue
			}
			seenRels[rel.ID] = struct{}{}
			graphRels = append(graphRels, graphRelationship{
				SourceID:   rel.SourceID,
				TargetID:   rel.TargetID,
				Relation:   rel.Relation,
				Weight:     rel.Weight,
				ValidUntil: rel.ValidUntil,
			})
			if maxEdges > 0 && len(graphRels) >= maxEdges {
				truncatedByCap = true
				break seeds
			}
		}
	}

	if graphEntities == nil {
		graphEntities = []graphEntity{}
	}
	if graphRels == nil {
		graphRels = []graphRelationship{}
	}

	resp := resourceGraph{
		Entities:      graphEntities,
		Relationships: graphRels,
	}
	if truncatedByCap {
		resp.Truncated = &truncationInfo{
			Reason:        "edge_cap",
			ReturnedCount: len(graphRels),
			Hint:          fmt.Sprintf("traversal stopped at graph.max_edges=%d; raise the setting or fetch a narrower view", maxEdges),
		}
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      uri,
			MIMEType: "application/json",
			Text:     string(out),
		},
	}, nil
}
