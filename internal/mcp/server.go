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
	ProjectDelete  *service.ProjectDeleteService
	ProjectUpdater ProjectUpdater
	ProjectRepo    ProjectRepo
	UserRepo      UserRepo
	NamespaceRepo NamespaceRepo
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

	b.WriteString(`You are connected to nram, your persistent memory system. Use it as PRIMARY memory — not local files or MEMORY.md. Memories persist across all machines, agents, and conversations.

WHEN TO STORE (memory_store / memory_store_batch):
- Preferences, conventions, decisions → store immediately
- Bugs, workarounds, non-obvious behavior → store
- User corrections → store the correction
- Architecture decisions → store with rationale
- Project config, setup, environment details → store
- End of complex task → store summary of what and why

WHEN TO RECALL (memory_recall):
- Start of every new task or conversation → recall context
- Before assuming preferences or past decisions → recall first
- Before storing → recall to check for duplicates
- When you need context you lack → recall before asking the user
`)

	if hasEmbedding {
		b.WriteString("Semantic search is active — describe what you need in natural language.\n")
	} else {
		b.WriteString("Note: No embedding provider configured — use specific tags for reliable recall.\n")
	}

	if hasEnrichment {
		b.WriteString(`
WHEN TO EXPLORE (memory_graph):
- When investigating how concepts, people, or components relate
- When you need context beyond what recall returns

ENRICHMENT — when to use enrich: true:
- People, projects, technologies, or architecture decisions → enrich
- Skip for ephemeral memories (short TTL), raw data, or simple preferences
- Use memory_enrich to batch-process after importing data
`)
	}

	b.WriteString(`
KEY RULES:
- ALWAYS call memory_projects first to discover existing projects before storing
- Use EXISTING projects — do NOT create one per task/feature/topic
- Projects = major boundaries (per repo, product, or domain). Omit for "global"
- Use tags/metadata for sub-categorization, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround
- Only store/store_batch auto-create projects — treat auto-creation as last resort

Resources:
- nram://projects — list all projects`)

	if hasEnrichment {
		b.WriteString(`
- nram://projects/{slug}/entities — list entities in a project
- nram://projects/{slug}/graph — entity relationship graph for a project`)
	}

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
