package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/observability/metrics"
)

// RouterConfig holds the dependencies needed to build the HTTP router.
type RouterConfig struct {
	AuthMiddleware *auth.AuthMiddleware
	RateLimiter    *auth.RateLimiter
	Metrics        *metrics.Metrics
	// SetupGuard is middleware that returns 503 until initial setup is complete.
	// If nil, no setup guard is applied.
	SetupGuard func(http.Handler) http.Handler
	// ProjectAccess is middleware that enforces project-level ownership checks.
	// If nil, no ownership check is applied (useful in tests).
	ProjectAccess func(http.Handler) http.Handler
	// EnrichmentGate is middleware that returns 503 when the enrichment +
	// dreaming gate is closed (any of embedding/fact/entity unconfigured).
	// Wraps memory enrich, admin enrichment, and admin dreaming routes.
	// If nil, no gate is applied (useful in tests that don't exercise it).
	EnrichmentGate func(http.Handler) http.Handler
}

// Handlers holds all handler instances. Nil handlers are replaced with a
// 501 Not Implemented response at router construction time.
type Handlers struct {
	// Project-scoped memory handlers
	Store      http.HandlerFunc
	List       http.HandlerFunc
	ListIDs    http.HandlerFunc
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
	// PreviewAugment runs the query-augmentation phase against a single memory
	// without persisting; used by the memory-detail Preview button.
	PreviewAugment http.HandlerFunc

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
	MeProfile           http.HandlerFunc
	MeProfilePatch      http.HandlerFunc
	// Self-tier system-pipeline observability — added 2026-04-30 so project
	// owners can see their own dream cycles + enrichment queue items after
	// the cross-tenant /v1/dreaming and /v1/enrichment routes moved under
	// /v1/admin/.
	MeDreaming   http.HandlerFunc
	MeEnrichment http.HandlerFunc
	// Self-tier capability flags. Drives sidebar nav visibility for the
	// Enrichment Queue / Dreaming entries without requiring non-admins to
	// probe the admin-only /v1/admin/providers endpoint.
	MeCapabilities http.HandlerFunc
	// Self-tier read of the eight ranking.weight.* schema entries plus
	// their effective global-scope values. Drives the Ranking Weights
	// placeholders on the per-project edit panel for non-admin owners
	// who cannot read /v1/admin/settings.
	MeRankingWeightsDefaults http.HandlerFunc

	// Self-tier export job pipeline. Replaced the truncation-bound MCP
	// export tool — large multi-project exports run asynchronously, the
	// artifact lands on disk, and the caller downloads it through these
	// handlers. See internal/service/export_job.go.
	MeExports        http.HandlerFunc // GET (list) + POST (create) /v1/me/exports
	MeExportItem     http.HandlerFunc // GET + DELETE /v1/me/exports/{job_id}
	MeExportDownload http.HandlerFunc // GET /v1/me/exports/{job_id}/download

	// Self-tier share-token management. Capability-bearer credentials for
	// granting external recipients scoped access to curated projects without
	// the recipient having an nram account.
	MeShares     http.HandlerFunc // GET (list) + POST (create) /v1/me/shares
	MeShareItem  http.HandlerFunc // GET + PATCH + DELETE /v1/me/shares/{id}

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

	// User-scoped passkey management
	MePasskeysList          http.HandlerFunc
	MePasskeyRegisterBegin  http.HandlerFunc
	MePasskeyRegisterFinish http.HandlerFunc
	MePasskeyDelete         http.HandlerFunc

	// Auth handlers (semi-public: setup guard, no auth)
	AuthLogin        http.HandlerFunc
	AuthLookup       http.HandlerFunc
	AuthPasskeyBegin  http.HandlerFunc
	AuthPasskeyFinish http.HandlerFunc

	// OAuth handlers
	OAuthAuthorize         http.HandlerFunc
	OAuthToken             http.HandlerFunc
	OAuthRegister          http.HandlerFunc
	OAuthUserInfo          http.HandlerFunc
	OAuthMetadata          http.HandlerFunc
	OAuthProtectedResource http.HandlerFunc
	// OAuthAuthorizeContext serves GET /v1/oauth/authorize/context. The
	// React consent page at /authorize calls this on mount to validate
	// the OAuth request and learn its rendering context.
	OAuthAuthorizeContext http.HandlerFunc
	// OAuthSharePreview serves POST /v1/oauth/share/preview. Validates a
	// pasted share secret and returns the share's grants without
	// consuming it, so the React consent page can show the recipient
	// what they are about to authorize before the final approve POST.
	OAuthSharePreview http.HandlerFunc
	// ShareAccept serves GET /v1/share/accept?token=<secret>. The React
	// landing page at /share/accept calls this to render the share's
	// grants and the MCP server URL to configure in the recipient's
	// client.
	ShareAccept http.HandlerFunc

	// IdP SSO handlers (public — no auth required)
	IdPLogin    http.HandlerFunc
	IdPCallback http.HandlerFunc

	// Admin handlers — tier-A "self" for AdminDashboard / AdminActivity /
	// AdminAnalytics / AdminUsage / AdminGraph / AdminNamespaces.
	// Mounted at /v1/dashboard, /v1/activity, /v1/analytics, /v1/usage,
	// /v1/graph, /v1/namespaces/tree. Self-scoped to caller (post-2026-04-30
	// leak fix); admin sees own data only on these surfaces.
	AdminSetupStatus http.HandlerFunc
	AdminSetup       http.HandlerFunc
	AdminDashboard   http.HandlerFunc
	AdminActivity    http.HandlerFunc
	AdminOrgs        http.HandlerFunc
	AdminUsers       http.HandlerFunc
	AdminProjects    http.HandlerFunc
	AdminProviders   http.HandlerFunc
	AdminSettings    http.HandlerFunc
	AdminSettingsReset http.HandlerFunc
	AdminEnrichment  http.HandlerFunc
	AdminOAuth       http.HandlerFunc
	AdminWebhooks    http.HandlerFunc
	AdminAnalytics   http.HandlerFunc
	AdminUsage       http.HandlerFunc
	UsageCostRates   http.HandlerFunc
	AdminNamespaces  http.HandlerFunc
	AdminDatabase    http.HandlerFunc
	AdminGraph       http.HandlerFunc
	AdminDreaming    http.HandlerFunc

	// Tier-B (org-aggregate) handlers at /v1/orgs/{org_id}/{dashboard,
	// activity,analytics,usage}. Caller must be RoleOrgOwner+ of the org.
	// Aggregate counts + distributions only; no row-level user/memory data,
	// no content.
	OrgDashboard   http.HandlerFunc
	OrgActivity    http.HandlerFunc
	OrgAnalytics   http.HandlerFunc
	OrgUsage       http.HandlerFunc
	OrgDreaming    http.HandlerFunc
	OrgEnrichment  http.HandlerFunc

	// Tier-C (system-aggregate) handlers at /v1/admin/system/{dashboard,
	// activity,analytics,usage}. RoleAdministrator only. System totals +
	// per-org breakdown rows; no per-user, no per-memory, no content.
	SystemDashboard http.HandlerFunc
	SystemActivity  http.HandlerFunc
	SystemAnalytics http.HandlerFunc
	SystemUsage     http.HandlerFunc
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

// uiHandler returns h.ServeHTTP if h is non-nil, otherwise notImplemented.
// Used where the chi route must be registered unconditionally (e.g. GET
// /authorize wires explicit method routing so chi does not return 405)
// but tests construct Handlers without populating the UI field.
func uiHandler(h http.Handler) http.HandlerFunc {
	if h == nil {
		return notImplemented
	}
	return h.ServeHTTP
}

// uiHandlerNoStore wraps uiHandler with Cache-Control: no-store. Used for
// the SPA shell on sensitive pre-auth surfaces (OAuth consent at
// /authorize, share-accept landing at /share/accept) where browser cache
// or bfcache reuse could surface stale state alongside fresh OAuth
// params.
func uiHandlerNoStore(h http.Handler) http.HandlerFunc {
	inner := uiHandler(h)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		inner(w, r)
	}
}

// NewRouter constructs the chi router with all middleware and route groups.
func NewRouter(config RouterConfig, handlers Handlers) *chi.Mux {
	r := chi.NewRouter()

	// Global middleware applied to all routes: panic recovery, request-ID
	// correlation, and metrics. Request-ID runs before metrics so the ID is
	// available in any downstream observability hook.
	r.Use(api.ErrorMiddleware)
	r.Use(RequestIDMiddleware)
	if config.Metrics != nil {
		r.Use(metrics.Middleware(config.Metrics))
	}

	// Public routes (no auth required).
	if config.Metrics != nil {
		r.Handle("/metrics", metrics.Handler(config.Metrics))
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
		// /authorize: GET serves the React consent SPA shell, POST
		// receives the user's decision and 302-redirects to the OAuth
		// client. Chi's method routing returns 405 for unmatched
		// methods on a registered path (it does not fall through to
		// NotFound), so GET is wired explicitly to the UI handler
		// rather than relying on the SPA fallback. no-store on the
		// shell response prevents browser cache / bfcache reuse on a
		// sensitive consent surface.
		r.Get("/authorize", uiHandlerNoStore(handlers.UI))
		r.Post("/authorize", handler(handlers.OAuthAuthorize))
		// /share/accept also serves the SPA shell with no-store, for
		// the same reason. The data side lives at /v1/share/accept.
		r.Get("/share/accept", uiHandlerNoStore(handlers.UI))
		// JSON endpoints driven by the React consent and share-accept
		// pages. The React /authorize page calls authorize/context on
		// mount and share/preview when the recipient pastes a secret;
		// /v1/share/accept backs the magic-link landing at
		// /share/accept.
		r.Get("/v1/oauth/authorize/context", handler(handlers.OAuthAuthorizeContext))
		r.Post("/v1/oauth/share/preview", handler(handlers.OAuthSharePreview))
		r.Get("/v1/share/accept", handler(handlers.ShareAccept))
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

		// Passkey login flow (public — user is not yet authenticated).
		r.Post("/v1/auth/passkey/begin", handler(handlers.AuthPasskeyBegin))
		r.Post("/v1/auth/passkey/finish", handler(handlers.AuthPasskeyFinish))

		// IdP SSO flow (public — user is not yet authenticated).
		r.Get("/auth/idp/login", handler(handlers.IdPLogin))
		r.Get("/auth/idp/callback", handler(handlers.IdPCallback))
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
		// Share-token credentials are scoped to /mcp + /userinfo only.
		// Mount the guard after AuthMiddleware so AuthContext is populated;
		// the guard checks the path and short-circuits MCP-allowed routes.
		r.Use(api.RejectShareTokenMiddleware)

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
			r.Get("/ids", handler(handlers.ListIDs))
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
				r.Post("/import", handler(handlers.Import))
				// Preview-augmentation runs an LLM call — cost-incurring, so
				// gated to write-tier users even though it does not persist.
				// Otherwise any readonly API key could rack up an LLM bill by
				// spamming the preview surface.
				r.Post("/{id}/preview-augmentation", handler(handlers.PreviewAugment))

				// /enrich is gated behind the enrichment-available signal —
				// returns 503 unless all three provider slots are configured.
				r.Group(func(r chi.Router) {
					if config.EnrichmentGate != nil {
						r.Use(config.EnrichmentGate)
					}
					r.Post("/enrich", handler(handlers.Enrich))
				})
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
			r.Get("/profile", handler(handlers.MeProfile))
			r.Patch("/profile", handler(handlers.MeProfilePatch))
			r.Get("/passkeys", handler(handlers.MePasskeysList))
			r.Post("/passkeys/register/begin", handler(handlers.MePasskeyRegisterBegin))
			r.Post("/passkeys/register/finish", handler(handlers.MePasskeyRegisterFinish))
			r.Delete("/passkeys/{id}", handler(handlers.MePasskeyDelete))

			// Self-tier dream + enrichment observability. Read-only;
			// write operations remain admin-only at /v1/admin/dreaming
			// and /v1/admin/enrichment. Wrapped in EnrichmentGate.
			r.Group(func(r chi.Router) {
				if config.EnrichmentGate != nil {
					r.Use(config.EnrichmentGate)
				}
				r.HandleFunc("/dreaming", handler(handlers.MeDreaming))
				r.HandleFunc("/dreaming/*", handler(handlers.MeDreaming))
				r.HandleFunc("/enrichment", handler(handlers.MeEnrichment))
				r.HandleFunc("/enrichment/*", handler(handlers.MeEnrichment))
			})

			// Self-tier capability flags. Two booleans — no provider config,
			// no slot details, no secrets. Sidebar nav reads this to decide
			// whether to show Enrichment Queue / Dreaming entries.
			r.Get("/capabilities", handler(handlers.MeCapabilities))

			// Self-tier ranking-weight schema + effective global defaults.
			// Returns only the eight ranking.weight.* keys consumed by the
			// per-project Ranking Weights editor. Non-admins cannot read
			// /v1/admin/settings; this is the narrow read surface that lets
			// them populate placeholders without an admin gate.
			r.Get("/ranking-weights/defaults", handler(handlers.MeRankingWeightsDefaults))

			// Self-tier export jobs. List/create at the root; per-job
			// status and delete at {job_id}; artifact download under
			// /download. No admin equivalent — the codebase's privacy
			// invariant deliberately keeps memory content off admin
			// surfaces, so an admin cannot trigger an export against
			// another user's data.
			r.HandleFunc("/exports", handler(handlers.MeExports))
			r.HandleFunc("/exports/{job_id}", handler(handlers.MeExportItem))
			r.Get("/exports/{job_id}/download", handler(handlers.MeExportDownload))

			// Share-token management. Owner-only by virtue of running under
			// the /v1/me/* route group; each handler also re-checks owner_user_id
			// before mutating to prevent reads/edits on another user's share.
			r.HandleFunc("/shares", handler(handlers.MeShares))
			r.HandleFunc("/shares/{id}", handler(handlers.MeShareItem))
		})

		// Scoped data-viewing routes (all authenticated users — scope auto-applied).
		// Tier-A self-data routes. Each handler self-scopes via SelfScope —
		// admin sees admin's own data here, not system-wide. Cross-tenant
		// drill-down moved to /v1/admin/system/* and /v1/orgs/{id}/* (the
		// per-tier handler split is staged for follow-up; today these still
		// use the legacy single-handler instances pinned to self-scope).
		r.Get("/v1/dashboard", handler(handlers.AdminDashboard))
		r.Get("/v1/activity", handler(handlers.AdminActivity))
		r.Get("/v1/analytics", handler(handlers.AdminAnalytics))
		r.Get("/v1/usage", handler(handlers.AdminUsage))
		r.Get("/v1/usage/cost_rates", handler(handlers.UsageCostRates))
		r.Get("/v1/graph", handler(handlers.AdminGraph))
		r.Get("/v1/namespaces/tree", handler(handlers.AdminNamespaces))

		// Org-scoped routes.
		r.Route("/v1/orgs/{org_id}", func(r chi.Router) {
			r.Use(api.OrgAccessMiddleware())

			// Tier-B aggregate data viewing — handlers gate on
			// requireOrgOwner internally; admin passes via the
			// OrgAccessMiddleware admin short-circuit. Each handler
			// returns aggregate counts + distributions; no row-level
			// user/memory data, no content fields.
			r.Get("/dashboard", handler(handlers.OrgDashboard))
			r.Get("/activity", handler(handlers.OrgActivity))
			r.Get("/analytics", handler(handlers.OrgAnalytics))
			r.Get("/usage", handler(handlers.OrgUsage))

			// Org-tier dream + enrichment surfaces. Org owners get
			// retry/abandon/rollback within their org via these handlers;
			// the global enable/disable + pause/resume controls remain
			// admin-only on /v1/admin/*. Wrapped in EnrichmentGate so the
			// routes return 503 until provider slots are configured.
			r.Group(func(r chi.Router) {
				if config.EnrichmentGate != nil {
					r.Use(config.EnrichmentGate)
				}
				r.HandleFunc("/dreaming", handler(handlers.OrgDreaming))
				r.HandleFunc("/dreaming/*", handler(handlers.OrgDreaming))
				r.HandleFunc("/enrichment", handler(handlers.OrgEnrichment))
				r.HandleFunc("/enrichment/*", handler(handlers.OrgEnrichment))
			})

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
		//
		// 2026-04-30 leak fix:
		//   - /v1/admin/projects deleted: admins use the self-tier
		//     /v1/me/projects like every other role. Cross-tenant project
		//     listing exposed user-authored project names + descriptions
		//     and was a privacy leak.
		//   - /v1/admin/oauth retained because the same handler also serves
		//     the IdP-config admin sub-paths (/admin/oauth/idp/*); only the
		//     /admin/oauth/clients/* sub-path is the cross-tenant OAuth-client
		//     listing leak. Frontend now calls /me/oauth-clients for that;
		//     a follow-up will split the handler so the /clients sub-path
		//     can be removed cleanly.
		//   - /v1/dreaming + /v1/enrichment moved here from the authenticated-
		//     public group. Today they were callable by any authenticated
		//     user and returned system-wide cycle/queue data — admin-gated
		//     now, since these are system-ops surfaces (admin sees full
		//     pipeline visibility for debugging, no other role does).
		r.Route("/v1/admin", func(r chi.Router) {
			r.Use(auth.RequireRole(auth.RoleAdministrator))
			r.HandleFunc("/orgs", handler(handlers.AdminOrgs))
			r.HandleFunc("/orgs/*", handler(handlers.AdminOrgs))
			r.HandleFunc("/users", handler(handlers.AdminUsers))
			r.HandleFunc("/users/*", handler(handlers.AdminUsers))
			r.HandleFunc("/providers", handler(handlers.AdminProviders))
			r.HandleFunc("/providers/*", handler(handlers.AdminProviders))
			r.HandleFunc("/settings", handler(handlers.AdminSettings))
			r.HandleFunc("/settings/reset", handler(handlers.AdminSettingsReset))
			r.HandleFunc("/oauth", handler(handlers.AdminOAuth))
			r.HandleFunc("/oauth/*", handler(handlers.AdminOAuth))
			r.HandleFunc("/webhooks", handler(handlers.AdminWebhooks))
			r.HandleFunc("/webhooks/*", handler(handlers.AdminWebhooks))
			r.HandleFunc("/database", handler(handlers.AdminDatabase))
			r.HandleFunc("/database/*", handler(handlers.AdminDatabase))

			// Tier-C (system-aggregate) data views — admin-only by virtue
			// of being inside this /v1/admin route group. System totals +
			// per-org breakdown rows; no per-user, no per-memory, no
			// content.
			r.Get("/system/dashboard", handler(handlers.SystemDashboard))
			r.Get("/system/activity", handler(handlers.SystemActivity))
			r.Get("/system/analytics", handler(handlers.SystemAnalytics))
			r.Get("/system/usage", handler(handlers.SystemUsage))

			// System-ops pipelines (admin-only, full cross-tenant
			// observability for debugging). Wrapped in EnrichmentGate so
			// the routes return 503 until all provider slots are configured.
			r.Group(func(r chi.Router) {
				if config.EnrichmentGate != nil {
					r.Use(config.EnrichmentGate)
				}
				r.HandleFunc("/enrichment", handler(handlers.AdminEnrichment))
				r.HandleFunc("/enrichment/*", handler(handlers.AdminEnrichment))
				r.HandleFunc("/dreaming", handler(handlers.AdminDreaming))
				r.HandleFunc("/dreaming/*", handler(handlers.AdminDreaming))
			})
		})
	})

	// Serve embedded UI for all other paths.
	if handlers.UI != nil {
		r.NotFound(handlers.UI.ServeHTTP)
	}

	return r
}
