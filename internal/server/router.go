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
	MeAPIKeys           http.HandlerFunc // GET + POST
	MeAPIKeyRevoke      http.HandlerFunc
	MeOAuthClients      http.HandlerFunc
	MeOAuthClientRevoke http.HandlerFunc

	// Org-scoped handlers
	OrgRecall http.HandlerFunc

	// Health
	Health http.HandlerFunc

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
	AdminWebhooks    http.HandlerFunc
	AdminAnalytics   http.HandlerFunc
	AdminUsage       http.HandlerFunc
	AdminNamespaces  http.HandlerFunc
	AdminDatabase    http.HandlerFunc
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

	// Authenticated routes.
	r.Group(func(r chi.Router) {
		if config.AuthMiddleware != nil {
			r.Use(config.AuthMiddleware.Handler)
		}
		if config.RateLimiter != nil {
			r.Use(config.RateLimiter.Handler)
		}

		// Project-scoped memory routes.
		r.Route("/v1/projects/{project_id}/memories", func(r chi.Router) {
			r.Post("/", handler(handlers.Store))
			r.Get("/", handler(handlers.List))
			r.Get("/{id}", handler(handlers.Detail))
			r.Put("/{id}", handler(handlers.Update))
			r.Delete("/{id}", handler(handlers.Delete))
			r.Post("/batch", handler(handlers.BatchStore))
			r.Post("/get", handler(handlers.BatchGet))
			r.Post("/recall", handler(handlers.Recall))
			r.Post("/forget", handler(handlers.BulkForget))
			r.Post("/enrich", handler(handlers.Enrich))
			r.Get("/export", handler(handlers.Export))
			r.Post("/import", handler(handlers.Import))
		})

		// User-scoped routes.
		r.Route("/v1/me", func(r chi.Router) {
			r.Post("/memories/recall", handler(handlers.MeRecall))
			r.HandleFunc("/projects", handler(handlers.MeProjects))
			r.HandleFunc("/api-keys", handler(handlers.MeAPIKeys))
			r.Delete("/api-keys/{id}", handler(handlers.MeAPIKeyRevoke))
			r.HandleFunc("/oauth-clients", handler(handlers.MeOAuthClients))
			r.Delete("/oauth-clients/{id}", handler(handlers.MeOAuthClientRevoke))
		})

		// Org-scoped routes.
		r.Post("/v1/orgs/{org_id}/memories/recall", handler(handlers.OrgRecall))

		// Admin routes (require administrator role).
		r.Route("/v1/admin", func(r chi.Router) {
			r.Use(auth.RequireRole(auth.RoleAdministrator))

			r.Get("/setup/status", handler(handlers.AdminSetupStatus))
			r.Post("/setup", handler(handlers.AdminSetup))
			r.Get("/dashboard", handler(handlers.AdminDashboard))
			r.Get("/activity", handler(handlers.AdminActivity))
			r.HandleFunc("/orgs", handler(handlers.AdminOrgs))
			r.HandleFunc("/orgs/*", handler(handlers.AdminOrgs))
			r.HandleFunc("/users", handler(handlers.AdminUsers))
			r.HandleFunc("/users/*", handler(handlers.AdminUsers))
			r.HandleFunc("/projects", handler(handlers.AdminProjects))
			r.HandleFunc("/projects/*", handler(handlers.AdminProjects))
			r.HandleFunc("/providers", handler(handlers.AdminProviders))
			r.HandleFunc("/providers/*", handler(handlers.AdminProviders))
			r.HandleFunc("/settings", handler(handlers.AdminSettings))
			r.HandleFunc("/enrichment", handler(handlers.AdminEnrichment))
			r.HandleFunc("/enrichment/*", handler(handlers.AdminEnrichment))
			r.HandleFunc("/webhooks", handler(handlers.AdminWebhooks))
			r.HandleFunc("/webhooks/*", handler(handlers.AdminWebhooks))
			r.Get("/analytics", handler(handlers.AdminAnalytics))
			r.Get("/usage", handler(handlers.AdminUsage))
			r.Get("/namespaces/tree", handler(handlers.AdminNamespaces))
			r.HandleFunc("/database", handler(handlers.AdminDatabase))
			r.HandleFunc("/database/*", handler(handlers.AdminDatabase))
		})
	})

	return r
}
