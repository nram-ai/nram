package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- extractSlugFromURI tests ---

func TestExtractSlugFromURI_Entities(t *testing.T) {
	slug := extractSlugFromURI("nram://projects/my-project/entities", "entities")
	if slug != "my-project" {
		t.Errorf("expected %q, got %q", "my-project", slug)
	}
}

func TestExtractSlugFromURI_Graph(t *testing.T) {
	slug := extractSlugFromURI("nram://projects/another-project/graph", "graph")
	if slug != "another-project" {
		t.Errorf("expected %q, got %q", "another-project", slug)
	}
}

func TestExtractSlugFromURI_Empty(t *testing.T) {
	slug := extractSlugFromURI("nram://projects//entities", "entities")
	if slug != "" {
		t.Errorf("expected empty slug, got %q", slug)
	}
}

func TestExtractSlugFromURI_NoPrefix(t *testing.T) {
	slug := extractSlugFromURI("http://other/path", "entities")
	// Should return the input minus /entities suffix attempt
	if slug == "" {
		t.Error("expected non-empty result for non-matching URI")
	}
}

// --- nram://projects resource tests ---

func TestProjectsResource_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	_ = srv // resources registered

	req := mcp.ReadResourceRequest{}
	_, err := handleProjectsResource(context.Background(), srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "no HTTP request in context") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectsResource_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	ctx := buildNoAuthCtx()
	_, err := handleProjectsResource(ctx, srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "authentication required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectsResource_ListSuccess(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	p1ID := uuid.New()
	p2ID := uuid.New()
	projects := []model.Project{
		{ID: p1ID, Name: "Project One", Slug: "project-one", Description: "First"},
		{ID: p2ID, Name: "Project Two", Slug: "project-two", Description: "Second"},
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{
			listResult: projects,
		},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectsResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.URI != "nram://projects" {
		t.Errorf("expected URI %q, got %q", "nram://projects", tc.URI)
	}
	if tc.MIMEType != "application/json" {
		t.Errorf("expected MIME type %q, got %q", "application/json", tc.MIMEType)
	}

	var items []projectItem
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(items))
	}
	if items[0].Slug != "project-one" {
		t.Errorf("expected slug %q, got %q", "project-one", items[0].Slug)
	}
}

func TestProjectsResource_EmptyList(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{
			listResult: []model.Project{},
		},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectsResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tc := contents[0].(mcp.TextResourceContents)
	var items []projectItem
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 projects, got %d", len(items))
	}
}

// --- nram://projects/{slug}/entities resource tests ---

func TestProjectEntitiesResource_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test/entities"
	_, err := handleProjectEntitiesResource(context.Background(), srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "no HTTP request in context") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectEntitiesResource_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test/entities"
	ctx := buildNoAuthCtx()
	_, err := handleProjectEntitiesResource(ctx, srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "authentication required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectEntitiesResource_ListSuccess(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNsID,
		OwnerNamespaceID: nsID,
		Slug:             "test-project",
	}

	entityID := uuid.New()
	entities := []model.Entity{
		{
			ID:           entityID,
			NamespaceID:  projectNsID,
			Name:         "Alice",
			EntityType:   "person",
			Canonical:    "alice",
			MentionCount: 5,
		},
	}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: &mockEntityReader{entities: entities},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test-project/entities"
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectEntitiesResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.URI != "nram://projects/test-project/entities" {
		t.Errorf("expected URI %q, got %q", "nram://projects/test-project/entities", tc.URI)
	}

	var items []resourceEntity
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(items))
	}
	if items[0].Name != "Alice" {
		t.Errorf("expected name %q, got %q", "Alice", items[0].Name)
	}
	if items[0].Type != "person" {
		t.Errorf("expected type %q, got %q", "person", items[0].Type)
	}
	if items[0].MentionCount != 5 {
		t.Errorf("expected mention_count 5, got %d", items[0].MentionCount)
	}
}

func TestProjectEntitiesResource_EmptyEntities(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNsID,
		OwnerNamespaceID: nsID,
		Slug:             "empty-project",
	}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: &mockEntityReader{entities: []model.Entity{}},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/empty-project/entities"
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectEntitiesResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tc := contents[0].(mcp.TextResourceContents)
	var items []resourceEntity
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 entities, got %d", len(items))
	}
}

func TestProjectEntitiesResource_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: fmt.Errorf("not found")},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/nonexistent/entities"
	ctx := buildAuthCtx(userID)
	_, err := handleProjectEntitiesResource(ctx, srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "project not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- nram://projects/{slug}/graph resource tests ---

func TestProjectGraphResource_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test/graph"
	_, err := handleProjectGraphResource(context.Background(), srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "no HTTP request in context") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectGraphResource_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test/graph"
	ctx := buildNoAuthCtx()
	_, err := handleProjectGraphResource(ctx, srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "authentication required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectGraphResource_WithEntitiesAndRelationships(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNsID,
		OwnerNamespaceID: nsID,
		Slug:             "test-project",
	}

	entity1ID := uuid.New()
	entity2ID := uuid.New()
	relID := uuid.New()

	entities := []model.Entity{
		{ID: entity1ID, NamespaceID: projectNsID, Name: "Alice", EntityType: "person", Canonical: "alice"},
		{ID: entity2ID, NamespaceID: projectNsID, Name: "Bob", EntityType: "person", Canonical: "bob"},
	}
	rels := []model.Relationship{
		{
			ID:        relID,
			SourceID:  entity1ID,
			TargetID:  entity2ID,
			Relation:  "knows",
			Weight:    1.0,
			ValidFrom: time.Now(),
		},
	}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    &mockTraverser{rels: rels},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/test-project/graph"
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectGraphResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.URI != "nram://projects/test-project/graph" {
		t.Errorf("expected URI %q, got %q", "nram://projects/test-project/graph", tc.URI)
	}

	var graph resourceGraph
	if err := json.Unmarshal([]byte(tc.Text), &graph); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(graph.Entities) != 2 {
		t.Errorf("expected 2 entities, got %d", len(graph.Entities))
	}
	if len(graph.Relationships) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(graph.Relationships))
	}
	if graph.Relationships[0].Relation != "knows" {
		t.Errorf("expected relation %q, got %q", "knows", graph.Relationships[0].Relation)
	}
}

func TestProjectGraphResource_EmptyGraph(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNsID,
		OwnerNamespaceID: nsID,
		Slug:             "empty-project",
	}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: &mockEntityReader{entities: []model.Entity{}},
		Traverser:    &mockTraverser{},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/empty-project/graph"
	ctx := buildAuthCtx(userID)
	contents, err := handleProjectGraphResource(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tc := contents[0].(mcp.TextResourceContents)
	var graph resourceGraph
	if err := json.Unmarshal([]byte(tc.Text), &graph); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(graph.Entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(graph.Entities))
	}
	if len(graph.Relationships) != 0 {
		t.Errorf("expected 0 relationships, got %d", len(graph.Relationships))
	}
}

func TestProjectGraphResource_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: fmt.Errorf("not found")},
	}
	srv := NewServer(deps)

	req := mcp.ReadResourceRequest{}
	req.Params.URI = "nram://projects/nonexistent/graph"
	ctx := buildAuthCtx(userID)
	_, err := handleProjectGraphResource(ctx, srv, req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "project not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- resource registration tests ---

func TestResourcesRegistered_NoPanic(t *testing.T) {
	// Verify that NewServer registers resources without panicking.
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestResourcesRegistered_Postgres_NoPanic(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}
