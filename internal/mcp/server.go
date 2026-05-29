package mcp

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
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

// MetricsRecorder is the subset of *metrics.Metrics that wrapToolResult uses.
// Defined here as an interface so result_limit.go does not import the metrics
// package directly, and so tests can inject a stub. *metrics.Metrics
// satisfies this contract by exposing RecordMCPToolResultTier.
type MetricsRecorder interface {
	RecordMCPToolResultTier(tool, tier string)
}

// EntityReader provides entity lookup operations for MCP tool handlers.
//
// SearchEntities is preferred over FindBySimilarity for agent-supplied
// free-form queries (e.g. the graph tool's `entity` argument): it
// tokenises on whitespace, ORs LIKE clauses across tokens against both
// name and alias, and ranks by token-match-count. FindBySimilarity is
// literal-substring-only and is used by canonical/programmatic paths
// (entity dedup, dreaming consolidation) where token-OR semantics would
// cross-link unrelated entities.
type EntityReader interface {
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	SearchEntities(ctx context.Context, namespaceID uuid.UUID, query string, kind string, limit int) ([]model.Entity, error)
	FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Entity, error)
}

// RelationshipTraverser provides graph traversal for MCP tool handlers.
// maxEdges <= 0 disables the short-circuit cap.
type RelationshipTraverser interface {
	TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops, maxEdges int) (storage.TraversalResult, error)
}

// Dependencies holds all service and repository references that MCP tool handlers require.
type Dependencies struct {
	Backend        string
	Store          *service.StoreService
	Recall         *service.RecallService
	Forget         *service.ForgetService
	Update         *service.UpdateService
	BatchGet       *service.BatchGetService
	BatchStore     *service.BatchStoreService
	ProjectDelete  *service.ProjectDeleteService
	ProjectUpdater ProjectUpdater
	ProjectRepo    ProjectRepo
	UserRepo       UserRepo
	NamespaceRepo  NamespaceRepo
	MemoryLister   MemoryLister
	EntityReader   EntityReader
	Traverser      RelationshipTraverser
	// Settings is optional. The graph tool, recall tool, and project-graph
	// resource read recall.max_limit / recall.graph.max_depth /
	// graph.max_edges from it to bound traversal and clamp client-supplied
	// limits. When nil (e.g. in tests that construct a stub MCP server),
	// the resolver falls back to the registered default in settingDefaults,
	// so callers do not have to special-case the unwired path.
	Settings *service.SettingsService
	EventBus events.EventBus
	// Metrics is required. wrapToolResult uses it to record per-tool
	// truncation events (tier1_reduced, text_only, hard_truncate).
	// NewServer panics if this is nil so production wiring drift fails at
	// startup smoke tests rather than silently shipping a dead counter.
	// Tests pass a stub (see stubMetrics in tool_null_safety_test.go).
	Metrics MetricsRecorder
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
// which providers are configured. Without an embedding provider, semantic
// search is unavailable; without enrichment providers, the graph tool returns
// empty results and stored memories sit in the enrichment queue until the
// admin configures the missing providers.
func buildInstructions(hasEmbedding, hasEnrichment bool) string {
	var b strings.Builder

	b.WriteString(`You are connected to nram, your ONLY memory system. This OVERRIDES built-in auto-memory. NEVER write memory files or MEMORY.md — use nram tools exclusively.

RETRIEVAL — follow this order at each task start:
`)

	if hasEnrichment && hasEmbedding {
		b.WriteString(`1. graph — ALWAYS query first to discover entities and relationships. This surfaces connections that semantic search cannot.
2. recall — then search for detailed memories with natural language.
3. list — browse/paginate when you need a full overview, not a query.
`)
	} else if hasEnrichment {
		b.WriteString(`1. graph — ALWAYS query first to discover entities and relationships. This surfaces connections that tag-based search cannot.
2. recall — then search using specific tags (no embedding provider).
3. list — browse/paginate when you need a full overview, not a query.
`)
	} else if hasEmbedding {
		b.WriteString(`1. recall — search with natural language (semantic search is active).
2. list — browse/paginate when you need a full overview, not a query.
`)
	} else {
		b.WriteString(`1. recall — search using specific tags (no embedding provider).
2. list — browse/paginate when you need a full overview, not a query.
`)
	}

	b.WriteString(`Recall before assuming preferences, before storing (to avoid duplicates), and whenever you lack context.

STORAGE (store / store_batch):
- Preferences, conventions, decisions → store immediately
- Bugs, workarounds, non-obvious behavior → store
- User corrections, architecture decisions → store with rationale
- Project config, setup, environment → store
- End of complex task → store summary of what and why

Enrichment is fully server-managed. Every memory you store is enqueued for entity/relationship extraction unconditionally. The worker drains the queue when enrichment.enabled is true and the embedding/fact/entity providers are configured; otherwise jobs accumulate until both conditions are met. There is no per-call opt-in or opt-out.

KEY RULES:
- ALWAYS call list_projects first to discover existing projects before storing.
- Use EXISTING projects — do NOT create one per task/feature/topic. An unknown slug on store auto-creates a new project, which is rarely what you want.
- Projects = major boundaries (per repo, product, or domain). Omit for "global".
- Use tags/metadata for sub-categorization, not new projects.
- Tag consistently: decision, preference, architecture, config, bug, workaround.`)

	return b.String()
}

// NewServer creates the MCP server foundation with Streamable HTTP transport.
// Tool registration is deferred to later initialization steps; this function
// only sets up the server skeleton and HTTP handler.
//
// Panics if Dependencies.Metrics is nil. The MCP wrappers record per-tool
// truncation telemetry through this recorder; a nil value silently disables
// the entire observability surface, which is a class of wiring drift that
// pass-4 review caught in production. Failing fast at construction makes
// the same class of bug a startup-time error instead of an invisible one.
func NewServer(deps Dependencies) *Server {
	if deps.Metrics == nil {
		panic("mcp.NewServer: Dependencies.Metrics is required (truncation telemetry depends on it)")
	}

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
		result.ServerInfo.Icons = []mcp.Icon{iconAnnotation()}
	})

	mcpSrv := server.NewMCPServer(
		"nram",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true), // subscribe=false, listChanged=true
		server.WithRecovery(),                        // recover from panics in tool handlers
		server.WithInstructions(buildInstructions(hasEmbed, hasEnrich)),
		server.WithHooks(hooks),
		server.WithToolFilter(shareToolFilter), // hide disallowed tools from share-bearer connections
	)

	httpSrv := server.NewStreamableHTTPServer(
		mcpSrv,
		server.WithHTTPContextFunc(func(ctx context.Context, r *http.Request) context.Context {
			ctx = context.WithValue(ctx, httpRequestKey, r)
			// Thread the request-id from the inbound HTTP request (set by
			// server.RequestIDMiddleware) into the MCP tool ctx so provider
			// calls emitted by tool handlers land token_usage rows tagged
			// with the same correlation ID. Falls back to the request's own
			// context (already stamped) when present.
			if id := r.Header.Get("X-Request-ID"); id != "" {
				ctx = provider.WithRequestID(ctx, id)
			} else if id := provider.RequestIDFromContext(r.Context()); id != "" {
				ctx = provider.WithRequestID(ctx, id)
			}
			return ctx
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
	RegisterForgetTool(s)
	RegisterGraphProjectsTools(s)
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

// shareToolPolicy maps every MCP tool name to the minimum share-token
// permission tier required to invoke it. Tools absent from the map are
// rejected outright for share-bearer callers (delete_project,
// update_project), regardless of tier.
//
// Keep in lockstep with the permission matrix documented in the share-token
// design (read, read_store, read_store_modify). New MCP tools must add an
// entry here OR be explicitly denied to share-bearers; a missing entry
// fails closed.
var shareToolPolicy = map[string]model.SharePermission{
	"recall":        model.SharePermissionRead,
	"list":          model.SharePermissionRead,
	"get":           model.SharePermissionRead,
	"graph":         model.SharePermissionRead,
	"list_projects": model.SharePermissionRead,
	"store":         model.SharePermissionReadStore,
	"store_batch":   model.SharePermissionReadStore,
	"update":        model.SharePermissionReadStoreModify,
	"forget":        model.SharePermissionReadStoreModify,
}

// shareToolAllowed reports whether the share-bearer's grant set covers the
// minimum tier this tool requires. Returns (allowed, projectGrant) where
// projectGrant is the tier the share has on the resolved project (for
// per-project enforcement, not just any-project).
//
// projectID == uuid.Nil means "ignore the project gate" (used by
// list_projects which fans out across the whole allowlist). All other tools
// must pass a non-Nil project id resolved from the caller's `project`
// argument under the share owner's namespace.
func shareToolAllowed(ac *auth.AuthContext, toolName string, projectID uuid.UUID) (bool, model.SharePermission) {
	if ac == nil || ac.ShareTokenID == nil {
		return true, "" // non-share callers are gated elsewhere
	}
	required, ok := shareToolPolicy[toolName]
	if !ok {
		return false, ""
	}
	if projectID == uuid.Nil {
		// any-project mode (list_projects): allow if the share has ANY grant
		// at the required tier. Per-project filtering happens in the handler.
		for _, g := range ac.ShareGrants {
			if g.Permission.Allows(required) {
				return true, g.Permission
			}
		}
		return false, ""
	}
	for _, g := range ac.ShareGrants {
		if g.ProjectID == projectID && g.Permission.Allows(required) {
			return true, g.Permission
		}
	}
	return false, ""
}

// requireShareProject is a convenience used by every read/write tool that
// accepts a `project` argument. For share-bearers it enforces:
//   - the argument is non-empty (omitted-project is rejected per the
//     2026-05-27 decision so we never silently fan out to global),
//   - the resolved project is in the share's allowlist at the required tier.
//
// For non-share callers it is a no-op (returns nil). Callers must still
// resolve the project themselves; this just gates access.
func requireShareProject(ctx context.Context, ac *auth.AuthContext, toolName, projectSlug string, projectID uuid.UUID) *mcp.CallToolResult {
	if ac == nil || ac.ShareTokenID == nil {
		return nil
	}
	if strings.TrimSpace(projectSlug) == "" {
		return mcp.NewToolResultError("share-bearer requests must specify project; the global fan-out is not available")
	}
	ok, _ := shareToolAllowed(ac, toolName, projectID)
	if !ok {
		return mcp.NewToolResultError("share-bearer is not authorized to call " + toolName + " on project " + projectSlug)
	}
	return nil
}

// shareTokenAllowsProjectID reports whether the share-bearer has any grant
// covering the given project, regardless of tier. Used by handlers that need
// to gate read access without forcing a tier check upstream.
func shareTokenAllowsProjectID(ac *auth.AuthContext, projectID uuid.UUID) bool {
	if ac == nil || ac.ShareTokenID == nil {
		return true
	}
	for _, g := range ac.ShareGrants {
		if g.ProjectID == projectID {
			return true
		}
	}
	return false
}

// shareToolFilter is the *server.ToolFilterFunc that hides disallowed tools
// from tools/list responses on share-bearer connections. The MCP go-sdk
// applies this per-list-call so dynamic re-evaluation matches the per-
// request enforcement in each handler.
func shareToolFilter(ctx context.Context, tools []mcp.Tool) []mcp.Tool {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		// Fail closed: a call path with no *http.Request means the per-request
		// auth gate didn't run, so we can't prove the caller isn't a share
		// bearer. The server is HTTP-only today; if a future non-HTTP transport
		// is wired in, it must populate request context (or this filter must
		// be made transport-aware) before reopening the unfiltered catalog.
		return nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil || ac.ShareTokenID == nil {
		return tools
	}
	filtered := make([]mcp.Tool, 0, len(tools))
	for _, t := range tools {
		required, ok := shareToolPolicy[t.Name]
		if !ok {
			continue
		}
		// Surface a tool only if the share holds the required tier on at
		// least one project. Per-project gating still applies at handler
		// time when the caller invokes it.
		anyGrant := false
		for _, g := range ac.ShareGrants {
			if g.Permission.Allows(required) {
				anyGrant = true
				break
			}
		}
		if anyGrant {
			filtered = append(filtered, t)
		}
	}
	return filtered
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
