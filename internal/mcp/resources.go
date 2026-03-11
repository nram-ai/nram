package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
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
		items = append(items, projectItem{
			ID:          p.ID,
			Name:        p.Name,
			Slug:        p.Slug,
			Description: p.Description,
		})
	}

	out, err := json.Marshal(items)
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
	)

	s.MCPServer().AddResourceTemplate(template, func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return handleProjectGraphResource(ctx, s, request)
	})
}

// resourceGraph is the JSON envelope for the graph resource.
type resourceGraph struct {
	Entities      []graphEntity       `json:"entities"`
	Relationships []graphRelationship `json:"relationships"`
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

	entities, err := deps.EntityReader.ListByNamespace(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list entities: %w", err)
	}

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

		rels, tErr := deps.Traverser.TraverseFromEntity(ctx, ent.ID, 1)
		if tErr != nil {
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
