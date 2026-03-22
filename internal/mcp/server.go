package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// ProjectRepo defines the project lookup operations needed by MCP tool handlers.
type ProjectRepo interface {
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
	ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error)
	Create(ctx context.Context, project *model.Project) error
}

// UserRepo defines the user lookup operations needed by MCP tool handlers.
type UserRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// NamespaceRepo defines the namespace lookup operations needed by MCP tool handlers.
type NamespaceRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
	Create(ctx context.Context, ns *model.Namespace) error
}

// OrgRepo defines the organization lookup operations needed by MCP tool handlers.
type OrgRepo interface {
	GetBySlug(ctx context.Context, slug string) (*model.Organization, error)
}

// EntityReader provides entity lookup operations for MCP tool handlers.
type EntityReader interface {
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
}

// RelationshipTraverser provides graph traversal for MCP tool handlers.
type RelationshipTraverser interface {
	TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops int) ([]model.Relationship, error)
}

// Dependencies holds all service and repository references that MCP tool handlers require.
type Dependencies struct {
	Backend       string
	Store         *service.StoreService
	Recall        *service.RecallService
	Forget        *service.ForgetService
	Update        *service.UpdateService
	BatchGet      *service.BatchGetService
	BatchStore    *service.BatchStoreService
	Enrich        *service.EnrichService
	Export        *service.ExportService
	ProjectRepo   ProjectRepo
	UserRepo      UserRepo
	NamespaceRepo NamespaceRepo
	OrgRepo       OrgRepo
	EntityReader  EntityReader
	Traverser     RelationshipTraverser
	EventBus events.EventBus
	// ProviderStatus returns the current provider availability at call time.
	// This is called per-connection to build dynamic MCP instructions.
	ProviderStatus func() (hasEmbedding, hasEnrichment bool)
}

// Server wraps an MCP server with its Streamable HTTP transport and dependency context.
type Server struct {
	mcpServer   *server.MCPServer
	httpHandler http.Handler
	deps        Dependencies
}

// ctxKey is the context key type for storing the originating HTTP request.
type ctxKey int

const httpRequestKey ctxKey = 0

// HTTPRequestFromContext retrieves the originating *http.Request stored during
// the Streamable HTTP context injection. Returns nil if no request is present.
func HTTPRequestFromContext(ctx context.Context) *http.Request {
	r, _ := ctx.Value(httpRequestKey).(*http.Request)
	return r
}

// buildInstructions returns the MCP server instructions string, conditioned on
// which providers are configured. Without an embedding provider, semantic search
// is unavailable; without enrichment providers, memory_enrich and memory_graph
// are non-functional.
func buildInstructions(hasEmbedding, hasEnrichment bool) string {
	var b strings.Builder

	b.WriteString(`You have access to nram, a persistent memory layer for AI agents. Use it to store and recall information across conversations.

Key concepts:
- Memories are organized into projects (identified by slug). Projects are auto-created on first use.
- Every user has a "global" project that serves as the default scope. When you omit the project parameter, memories are stored in and recalled from the global project.
- Each memory has content (the main text), optional tags (for filtering), and optional metadata (key-value pairs).
- Memories support TTL (time-to-live) for automatic expiration.

Recommended workflow:
1. Use memory_store to save important context, decisions, user preferences, or facts worth remembering.`)

	if hasEmbedding {
		b.WriteString(`
2. Use memory_recall to search for relevant memories using natural language queries. Semantic search is enabled — describe what you're looking for rather than using exact keywords.`)
	} else {
		b.WriteString(`
2. Use memory_recall to search for relevant memories. Note: no embedding provider is configured, so recall uses tag filtering and text matching rather than semantic search. Configure an embedding provider in the admin UI to enable natural language recall.`)
	}

	b.WriteString(`
3. Use memory_store_batch when you have multiple related memories to store at once.
4. Use memory_update to modify existing memories (e.g., to correct or enrich them).
5. Use memory_get to retrieve specific memories by their IDs when you already know which ones you need.
6. Use memory_forget to remove memories that are no longer relevant.
7. Use memory_projects to list available projects and their slugs.
8. Use memory_export to export all memories from a project for backup or migration.`)

	if hasEnrichment {
		b.WriteString(`
9. Use memory_enrich to trigger entity extraction and enrichment on stored memories, enhancing future recall and graph traversal.
10. Use memory_graph to explore the knowledge graph — find entities and their relationships starting from a name or search query.`)
	}

	b.WriteString(`

Tips:
- Be specific with tags — they enable precise filtering during recall.`)

	if hasEmbedding {
		b.WriteString(`
- When recalling, provide a natural language query describing what you're looking for rather than exact keywords.`)
	} else {
		b.WriteString(`
- Use tags consistently when storing and recalling, since semantic search is not currently available.`)
	}

	b.WriteString(`
- Store memories proactively: if a user shares preferences, project context, or important decisions, store them immediately.
- Check for existing memories before storing duplicates — use memory_recall first.
- When recalling without a project, only the global project is searched. To find project-specific memories, specify the project slug.
- When recalling with a project, both the project's memories and global memories are included in the results.`)

	return b.String()
}

// NewServer creates the MCP server foundation with Streamable HTTP transport.
// Tool registration is deferred to later initialization steps; this function
// only sets up the server skeleton and HTTP handler.
func NewServer(deps Dependencies) *Server {
	// Build initial instructions from current provider state.
	hasEmbed, hasEnrich := false, false
	if deps.ProviderStatus != nil {
		hasEmbed, hasEnrich = deps.ProviderStatus()
	}

	// Use a hook to rebuild instructions at connection time so they reflect
	// the current provider configuration, not a boot-time snapshot.
	hooks := &server.Hooks{}
	hooks.AddAfterInitialize(func(_ context.Context, _ any, _ *mcp.InitializeRequest, result *mcp.InitializeResult) {
		he, hr := false, false
		if deps.ProviderStatus != nil {
			he, hr = deps.ProviderStatus()
		}
		result.Instructions = buildInstructions(he, hr)
	})

	mcpSrv := server.NewMCPServer(
		"nram",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true), // subscribe=false, listChanged=true
		server.WithRecovery(),                        // recover from panics in tool handlers
		server.WithInstructions(buildInstructions(hasEmbed, hasEnrich)),
		server.WithHooks(hooks),
	)

	httpSrv := server.NewStreamableHTTPServer(
		mcpSrv,
		server.WithHTTPContextFunc(func(ctx context.Context, r *http.Request) context.Context {
			return context.WithValue(ctx, httpRequestKey, r)
		}),
	)

	s := &Server{
		mcpServer:   mcpSrv,
		httpHandler: httpSrv,
		deps:        deps,
	}

	RegisterStoreTools(s)
	RegisterUpdateGetTools(s)
	RegisterRecallTool(s)
	RegisterForgetEnrichTools(s)
	RegisterGraphProjectsExportTools(s)
	RegisterResources(s)

	return s
}

// Handler returns the http.Handler that serves the MCP Streamable HTTP
// protocol. Mount this on the application router at /mcp.
// It wraps the SDK handler with Origin header validation per the MCP spec:
// "Servers MUST validate the Origin header on all incoming connections to
// prevent DNS rebinding attacks."
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			// Skip origin check for authenticated requests — the OAuth token
			// validates the client's legitimacy. Only enforce strict same-origin
			// for unauthenticated requests (DNS rebinding protection).
			if r.Header.Get("Authorization") == "" && !isAllowedOrigin(origin, r.Host) {
				http.Error(w, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"forbidden: invalid origin"}}`, http.StatusForbidden)
				return
			}
		}
		s.httpHandler.ServeHTTP(w, r)
	})
}

// isAllowedOrigin checks whether the Origin header matches the server's Host.
// This prevents DNS rebinding attacks per the MCP spec security requirements.
func isAllowedOrigin(origin, host string) bool {
	// Strip scheme from origin to compare against Host header.
	// Origin is like "http://localhost:8674" or "https://nram.example.com".
	stripped := origin
	for _, prefix := range []string{"https://", "http://"} {
		if len(stripped) > len(prefix) && stripped[:len(prefix)] == prefix {
			stripped = stripped[len(prefix):]
			break
		}
	}
	return stripped == host
}

// checkWriteAccess verifies that the authenticated user is not readonly.
// Returns a tool error result if the user's role is readonly, nil otherwise.
// Call this at the top of every MCP write tool handler (store, batch store,
// update, forget, enrich, import).
func checkWriteAccess(ctx context.Context) *mcp.CallToolResult {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return nil // auth check handled separately
	}
	if ac.Role == auth.RoleReadonly {
		return mcp.NewToolResultError("forbidden: readonly users cannot perform write operations")
	}
	return nil
}

// Backend returns the storage backend identifier ("sqlite" or "postgres")
// configured for this server instance.
func (s *Server) Backend() string {
	return s.deps.Backend
}

// MCPServer returns the underlying MCPServer for tool/resource registration.
func (s *Server) MCPServer() *server.MCPServer {
	return s.mcpServer
}

// Deps returns the dependency bag so tool registrars can access services.
func (s *Server) Deps() Dependencies {
	return s.deps
}

// ExportServicer is a convenience interface satisfied by *service.ExportService.
// It is used by tool handlers that need both Export and ExportNDJSON.
type ExportServicer interface {
	Export(ctx context.Context, req *service.ExportRequest) (*service.ExportData, error)
	ExportNDJSON(ctx context.Context, req *service.ExportRequest, w io.Writer) error
}
