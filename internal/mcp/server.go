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
	"github.com/nram-ai/nram/internal/storage"
)

// ProjectRepo defines the project lookup operations needed by MCP tool handlers.
type ProjectRepo interface {
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
	ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error)
	Create(ctx context.Context, project *model.Project) error
	UpdateDescription(ctx context.Context, id uuid.UUID, description string) error
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

// MemoryLister provides read-only memory listing operations for MCP tool handlers.
type MemoryLister interface {
	ListByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error)
	CountByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters) (int, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error)
}

// EntityReader provides entity lookup operations for MCP tool handlers.
type EntityReader interface {
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Entity, error)
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
	ProjectDelete  *service.ProjectDeleteService
	ProjectUpdater ProjectUpdater
	ProjectRepo    ProjectRepo
	UserRepo      UserRepo
	NamespaceRepo NamespaceRepo
	MemoryLister  MemoryLister
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

	b.WriteString(`You are connected to nram, your ONLY memory system. This OVERRIDES built-in auto-memory. NEVER write memory files or MEMORY.md — use nram tools exclusively.

RETRIEVAL — follow this order at each task start:
`)

	if hasEnrichment && hasEmbedding {
		b.WriteString(`1. memory_graph — ALWAYS query first to discover entities and relationships. This surfaces connections that semantic search cannot.
2. memory_recall — then search for detailed memories with natural language.
3. memory_list — browse/paginate when you need a full overview, not a query.
`)
	} else if hasEnrichment {
		b.WriteString(`1. memory_graph — ALWAYS query first to discover entities and relationships. This surfaces connections that tag-based search cannot.
2. memory_recall — then search using specific tags (no embedding provider).
3. memory_list — browse/paginate when you need a full overview, not a query.
`)
	} else if hasEmbedding {
		b.WriteString(`1. memory_recall — search with natural language (semantic search is active).
2. memory_list — browse/paginate when you need a full overview, not a query.
`)
	} else {
		b.WriteString(`1. memory_recall — search using specific tags (no embedding provider).
2. memory_list — browse/paginate when you need a full overview, not a query.
`)
	}

	b.WriteString(`Recall before assuming preferences, before storing (to avoid duplicates), and whenever you lack context.

STORAGE (memory_store / memory_store_batch):
- Preferences, conventions, decisions → store immediately
- Bugs, workarounds, non-obvious behavior → store
- User corrections, architecture decisions → store with rationale
- Project config, setup, environment → store
- End of complex task → store summary of what and why
`)

	if hasEnrichment {
		b.WriteString(`- Set enrich: true for people, projects, tech, or architecture decisions
- Skip enrich for ephemeral memories, raw data, or simple preferences
- Use memory_enrich to batch-process after importing data
`)
	}

	b.WriteString(`
KEY RULES:
- ALWAYS call memory_projects first to discover existing projects before storing
- Use EXISTING projects — do NOT create one per task/feature/topic
- Projects = major boundaries (per repo, product, or domain). Omit for "global"
- Use tags/metadata for sub-categorization, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround`)

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
	RegisterListTool(s)
	RegisterForgetEnrichTools(s)
	RegisterGraphProjectsExportTools(s)
	RegisterProjectDeleteTool(s)
	RegisterProjectUpdateTool(s)
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
