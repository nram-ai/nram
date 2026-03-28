package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
)

// RouterConfig holds the dependencies needed to build the HTTP router.
type RouterConfig struct {
	AuthMiddleware *auth.AuthMiddleware
	RateLimiter    *auth.RateLimiter
	Metrics        *api.Metrics
	// SetupGuard is middleware that returns 503 until initial setup is complete.
	// If nil, no setup guard is applied.
	SetupGuard func(http.Handler) http.Handler
	// ProjectAccess is middleware that enforces project-level ownership checks.
	// If nil, no ownership check is applied (useful in tests).
	ProjectAccess func(http.Handler) http.Handler
}

// Handlers holds all handler instances. Nil handlers are replaced with a
// 501 Not Implemented response at router construction time.
type Handlers struct {
	// Project-scoped memory handlers
	Store      http.HandlerFunc
	List       http.HandlerFunc
	Detail     http.HandlerFunc
	Update     http.HandlerFunc
	Delete     http.HandlerFunc
	BatchStore http.HandlerFunc
	BatchGet   http.HandlerFunc
	Recall     http.HandlerFunc
	BulkForget http.HandlerFunc
	Enrich     http.HandlerFunc
	Export     http.HandlerFunc
	Import     http.HandlerFunc

	// User-scoped handlers
	MeRecall            http.HandlerFunc
	MeProjects          http.HandlerFunc // GET + POST
	MeProjectItem       http.HandlerFunc // GET + PUT /v1/me/projects/{id}
	MeProjectDelete     http.HandlerFunc // DELETE /v1/me/projects/{id}
	MeAPIKeys           http.HandlerFunc // GET + POST
	MeAPIKeyRevoke      http.HandlerFunc
	MeOAuthClients      http.HandlerFunc
	MeOAuthClientRevoke http.HandlerFunc
	MeChangePassword    http.HandlerFunc

	// Org-scoped handlers
	OrgUsers http.HandlerFunc
	OrgIdP   http.HandlerFunc

	// SSE events
	Events http.HandlerFunc

	// MCP Streamable HTTP server
	MCP http.Handler

	// Embedded admin UI
	UI http.Handler

	// Health
	Health http.HandlerFunc

	// Auth handlers (semi-public: setup guard, no auth)
	AuthLogin  http.HandlerFunc
	AuthLookup http.HandlerFunc

	// OAuth handlers
	OAuthAuthorize         http.HandlerFunc
	OAuthToken             http.HandlerFunc
	OAuthRegister          http.HandlerFunc
	OAuthUserInfo          http.HandlerFunc
	OAuthMetadata          http.HandlerFunc
	OAuthProtectedResource http.HandlerFunc

	// Admin handlers
	AdminSetupStatus http.HandlerFunc
	AdminSetup       http.HandlerFunc
	AdminDashboard   http.HandlerFunc
	AdminActivity    http.HandlerFunc
	AdminOrgs        http.HandlerFunc
	AdminUsers       http.HandlerFunc
	AdminProjects    http.HandlerFunc
	AdminProviders   http.HandlerFunc
	AdminSettings    http.HandlerFunc
	AdminEnrichment  http.HandlerFunc
	AdminOAuth       http.HandlerFunc
	AdminWebhooks    http.HandlerFunc
	AdminAnalytics   http.HandlerFunc
	AdminUsage       http.HandlerFunc
	AdminNamespaces  http.HandlerFunc
	AdminDatabase    http.HandlerFunc
	AdminGraph       http.HandlerFunc
}

// notImplemented returns a handler that responds with 501 Not Implemented.
func notImplemented(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusNotImplemented)
	json.NewEncoder(w).Encode(map[string]string{
		"error": "not implemented",
	})
}

// handler returns the given HandlerFunc if non-nil, otherwise returns notImplemented.
func handler(h http.HandlerFunc) http.HandlerFunc {
	if h == nil {
		return notImplemented
	}
	return h
}

// NewRouter constructs the chi router with all middleware and route groups.
func NewRouter(config RouterConfig, handlers Handlers) *chi.Mux {
	r := chi.NewRouter()

	// Global middleware applied to all routes: panic recovery and metrics.
	r.Use(api.ErrorMiddleware)
	if config.Metrics != nil {
		r.Use(api.MetricsMiddleware(config.Metrics))
	}

	// Public routes (no auth required).
	if config.Metrics != nil {
		r.Handle("/metrics", api.MetricsHandler(config.Metrics))
	}
	r.Get("/v1/health", handler(handlers.Health))

	// Setup endpoints are public — must be accessible before first user exists.
	r.Get("/v1/admin/setup/status", handler(handlers.AdminSetupStatus))
	r.Post("/v1/admin/setup", handler(handlers.AdminSetup))

	// OAuth discovery and flow endpoints (public — no auth, no setup guard).
	// Paths follow MCP spec fallback defaults: /authorize, /token, /register.
	// CORS middleware is applied so browser-based MCP clients can reach these.
	// Routes use HandleFunc (all methods) so OPTIONS preflight reaches the
	// CORS middleware instead of being rejected by chi's method routing.
	r.Group(func(r chi.Router) {
		r.Use(CORSMiddleware)
		r.HandleFunc("/.well-known/oauth-authorization-server", handler(handlers.OAuthMetadata))
		r.HandleFunc("/.well-known/oauth-protected-resource", handler(handlers.OAuthProtectedResource))
		r.HandleFunc("/authorize", handler(handlers.OAuthAuthorize))
		r.HandleFunc("/token", handler(handlers.OAuthToken))
		r.HandleFunc("/register", handler(handlers.OAuthRegister))
	})

	// Semi-public routes: setup guard required but no auth (login flow).
	r.Group(func(r chi.Router) {
		if config.SetupGuard != nil {
			r.Use(config.SetupGuard)
		}
		r.Post("/v1/auth/login", handler(handlers.AuthLogin))
		r.Post("/v1/auth/lookup", handler(handlers.AuthLookup))
	})

	// Authenticated routes.
	r.Group(func(r chi.Router) {
		if config.SetupGuard != nil {
			r.Use(config.SetupGuard)
		}
		if config.AuthMiddleware != nil {
			r.Use(config.AuthMiddleware.Handler)
		}
		if config.RateLimiter != nil {
			r.Use(config.RateLimiter.Handler)
		}

		// OAuth userinfo and MCP endpoints need CORS for browser-based clients.
		r.Group(func(r chi.Router) {
			r.Use(CORSMiddleware)
			r.HandleFunc("/userinfo", handler(handlers.OAuthUserInfo))
			if handlers.MCP != nil {
				r.Handle("/mcp", handlers.MCP)
				r.Handle("/mcp/*", handlers.MCP)
			}
		})

		// SSE events endpoint.
		r.Get("/v1/events", handler(handlers.Events))

		// Project-scoped memory routes.
		r.Route("/v1/projects/{project_id}/memories", func(r chi.Router) {
			if config.ProjectAccess != nil {
				r.Use(config.ProjectAccess)
			}

			// Read operations — accessible to all authenticated roles including readonly.
			r.Get("/", handler(handlers.List))
			r.Get("/{id}", handler(handlers.Detail))
			r.Post("/get", handler(handlers.BatchGet))
			r.Post("/recall", handler(handlers.Recall))
			r.Get("/export", handler(handlers.Export))

			// Write operations — blocked for readonly users.
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireWriteAccess())
				r.Post("/", handler(handlers.Store))
				r.Put("/{id}", handler(handlers.Update))
				r.Delete("/{id}", handler(handlers.Delete))
				r.Post("/batch", handler(handlers.BatchStore))
				r.Post("/forget", handler(handlers.BulkForget))
				r.Post("/enrich", handler(handlers.Enrich))
				r.Post("/import", handler(handlers.Import))
			})
		})

		// User-scoped routes.
		r.Route("/v1/me", func(r chi.Router) {
			r.Post("/memories/recall", handler(handlers.MeRecall))
			r.HandleFunc("/projects", handler(handlers.MeProjects))
			r.Get("/projects/{id}", handler(handlers.MeProjectItem))
			r.Put("/projects/{id}", handler(handlers.MeProjectItem))
			r.Delete("/projects/{id}", handler(handlers.MeProjectDelete))
			r.HandleFunc("/api-keys", handler(handlers.MeAPIKeys))
			r.Delete("/api-keys/{id}", handler(handlers.MeAPIKeyRevoke))
			r.HandleFunc("/oauth-clients", handler(handlers.MeOAuthClients))
			r.Delete("/oauth-clients/{id}", handler(handlers.MeOAuthClientRevoke))
			r.Post("/password", handler(handlers.MeChangePassword))
		})

		// Scoped data-viewing routes (all authenticated users — scope auto-applied).
		r.Get("/v1/dashboard", handler(handlers.AdminDashboard))
		r.Get("/v1/activity", handler(handlers.AdminActivity))
		r.Get("/v1/analytics", handler(handlers.AdminAnalytics))
		r.Get("/v1/usage", handler(handlers.AdminUsage))
		r.Get("/v1/graph", handler(handlers.AdminGraph))
		r.Get("/v1/namespaces/tree", handler(handlers.AdminNamespaces))
		r.HandleFunc("/v1/enrichment", handler(handlers.AdminEnrichment))
		r.HandleFunc("/v1/enrichment/*", handler(handlers.AdminEnrichment))

		// Org-scoped routes.
		r.Route("/v1/orgs/{org_id}", func(r chi.Router) {
			r.Use(api.OrgAccessMiddleware())

			// Data viewing (member+ in org).
			r.Get("/analytics", handler(handlers.AdminAnalytics))
			r.Get("/usage", handler(handlers.AdminUsage))

			// Management (org_owner+).
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireRole(auth.RoleOrgOwner))
				r.HandleFunc("/users", handler(handlers.OrgUsers))
				r.HandleFunc("/users/*", handler(handlers.OrgUsers))
				r.HandleFunc("/idp", handler(handlers.OrgIdP))
				r.HandleFunc("/idp/*", handler(handlers.OrgIdP))
			})

			})

		// Admin routes (require administrator role).
		r.Route("/v1/admin", func(r chi.Router) {
			r.Use(auth.RequireRole(auth.RoleAdministrator))
			r.HandleFunc("/orgs", handler(handlers.AdminOrgs))
			r.HandleFunc("/orgs/*", handler(handlers.AdminOrgs))
			r.HandleFunc("/users", handler(handlers.AdminUsers))
			r.HandleFunc("/users/*", handler(handlers.AdminUsers))
			r.HandleFunc("/projects", handler(handlers.AdminProjects))
			r.HandleFunc("/projects/*", handler(handlers.AdminProjects))
			r.HandleFunc("/providers", handler(handlers.AdminProviders))
			r.HandleFunc("/providers/*", handler(handlers.AdminProviders))
			r.HandleFunc("/settings", handler(handlers.AdminSettings))
			r.HandleFunc("/oauth", handler(handlers.AdminOAuth))
			r.HandleFunc("/oauth/*", handler(handlers.AdminOAuth))
			r.HandleFunc("/webhooks", handler(handlers.AdminWebhooks))
			r.HandleFunc("/webhooks/*", handler(handlers.AdminWebhooks))
			r.HandleFunc("/database", handler(handlers.AdminDatabase))
			r.HandleFunc("/database/*", handler(handlers.AdminDatabase))
		})
	})

	// Serve embedded UI for all other paths.
	if handlers.UI != nil {
		r.NotFound(handlers.UI.ServeHTTP)
	}

	return r
}
