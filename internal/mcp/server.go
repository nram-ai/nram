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
	ProjectRepo   ProjectRepo
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

	b.WriteString(`You have access to nram, a persistent memory layer. Use it to store and recall information across conversations.

IMPORTANT — Prefer nram over local memory:
nram is a centralized memory server designed for cross-machine, cross-agent access.
Always store new memories in nram rather than in local file-based memory systems
(e.g. MEMORY.md files, local databases, or other native memory mechanisms).
Local memory files are invisible to other machines and agents, which defeats the
purpose of persistent, shared memory. When you would normally write to your local
memory system, use nram's memory_store or memory_store_batch instead. If you have
an existing local auto-memory system, you may still read from it, but all new
memories should be stored in nram so they are accessible everywhere.

Key concepts:
- Memories are organized into projects identified by slug. The "global" project is the default when project is omitted.
- Projects are auto-created on first use by memory_store and memory_store_batch ONLY. All other tools require an existing project.
- Use project_description when first storing to a project to describe its purpose. It sets the description on create or updates it if currently empty.
- Use memory_projects to discover existing projects before storing.
- Each memory has content (main text), optional tags (for filtering), and optional metadata (key-value pairs).
- Memories support TTL (time-to-live) for automatic expiration, e.g. "24h", "7d", "30m".

Tools:

memory_store — Store a single memory.
  project (optional, default "global", auto-created if missing)
  project_description (optional, sets/updates project description)
  content (required)
  source (optional, origin identifier)
  tags (optional, array of labels for filtering)
  metadata (optional, arbitrary key-value object)
  ttl (optional, e.g. "7d", "24h", "30m")`)

	if hasEnrichment {
		b.WriteString(`
  enrich (optional, boolean, queues async entity/fact extraction)`)
	}

	b.WriteString(`

memory_store_batch — Store multiple memories in one call.
  project (optional, default "global", auto-created if missing)
  project_description (optional)
  items (required, array of {content, source, tags, metadata})
  ttl (optional, applies to all items)`)

	if hasEnrichment {
		b.WriteString(`
  enrich (optional, boolean, queues enrichment for all items)`)
	}

	b.WriteString(`

memory_recall — Search memories.
  query (required, natural language)
  project (optional — omit to search global only; specify to search project + global)
  limit (optional, default 10)
  tags (optional, intersection filter: memory must have ALL specified tags)`)

	if hasEmbedding {
		b.WriteString(`
  Semantic search is enabled — describe what you need rather than using exact keywords.`)
	} else {
		b.WriteString(`
  No embedding provider is configured — recall uses tag filtering and text matching. Use tags consistently.`)
	}

	if hasEnrichment {
		b.WriteString(`
  include_graph (optional, default true — include related graph entities in results)
  graph_depth (optional, default 2 — graph traversal depth)`)
	}

	b.WriteString(`

memory_update — Update an existing memory by ID. Project must already exist.
  id (required)
  project (optional, default "global")
  content (optional, new content)
  tags (optional, replaces tags)
  metadata (optional, replaces metadata)

memory_get — Retrieve memories by ID. Project must already exist.
  ids (required, array of memory IDs)
  project (optional, default "global")

memory_forget — Soft-delete memories. Project must already exist.
  ids (required, array of memory IDs)
  project (optional, default "global")
  hard (optional, boolean, permanent deletion — default false)

memory_projects — List all projects with slugs and descriptions. No parameters.

memory_export — Export all data from a project. Project must already exist.
  project (optional, default "global")
  format (optional, "json" or "ndjson", default "json")`)

	if hasEnrichment {
		b.WriteString(`

Enrichment & Knowledge Graph:
Enrichment uses an LLM to automatically extract entities (people, projects,
technologies, concepts) and facts (relationships between entities) from stored
memories. These are assembled into a knowledge graph that connects related
information across all memories in a project. When recalling, graph context is
automatically included (include_graph: true by default) — so even if a memory
does not directly match your query, related entities and their connections
surface relevant context. This makes nram far more powerful than keyword or tag
search alone: it builds structured understanding from unstructured memories.
Use memory_graph to explore the graph directly — discover connections, trace
how concepts relate, and find information you might not know to search for.

memory_enrich — Trigger entity/fact extraction. Project must already exist.
  project (optional, default "global")
  ids (optional, specific memory IDs — omit to enrich all un-enriched memories in the project)

memory_graph — Explore the knowledge graph.
  entity (required, entity name or search query)
  project (optional, scopes to a project namespace)
  depth (optional, default 2)
  include_history (optional, default false — include expired/past relationships when true)`)
	}

	b.WriteString(`

Tips:
- ALWAYS prefer nram over local file-based memory — nram memories are accessible across all your machines and agents.
- Use consistent, specific tags: architecture, config, decision, preference, bug, workaround.`)

	if hasEmbedding {
		b.WriteString(`
- Use natural language queries for recall — describe what you need, not exact keywords.`)
	} else {
		b.WriteString(`
- Without an embedding provider, rely on tags for precise filtering during recall.`)
	}

	b.WriteString(`
- Store proactively: save preferences, decisions, and important context immediately.
- Check for duplicates before storing — recall first.
- Recall scoping: no project = global only; with project = project + global.`)

	if hasEnrichment {
		b.WriteString(`
- Use enrich: true at store time, or batch-enrich later with memory_enrich.
- Enrichment builds a knowledge graph — use memory_graph to explore entity connections and discover related context.`)
	}

	b.WriteString(`
- Use memory_projects to discover available projects before referencing them.
- Use project_description when creating projects to document their purpose.
- Only memory_store and memory_store_batch auto-create projects. All other tools (update, get, forget, export, enrich, graph) require the project to exist.`)

	if hasEnrichment {
		b.WriteString(`

Resources:
- nram://projects — list all projects
- nram://projects/{slug}/entities — list entities in a project
- nram://projects/{slug}/graph — entity relationship graph for a project`)
	} else {
		b.WriteString(`

Resources:
- nram://projects — list all projects`)
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
