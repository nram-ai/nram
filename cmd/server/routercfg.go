package main

import (
	"context"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/server"
	"github.com/nram-ai/nram/internal/service"
)

// routerDeps are the already-constructed pieces buildRouterConfig assembles
// into a server.RouterConfig.
type routerDeps struct {
	Metrics        *metrics.Metrics
	AuthMiddleware *auth.AuthMiddleware
	RateLimiter    *auth.RateLimiter
	// SetupComplete reports whether initial setup has finished; routes behind
	// the setup guard serve 503 until it does.
	SetupComplete func(context.Context) bool
	// ProjectAccess configures org-membership enforcement on the
	// /v1/projects/{project_id}/memories/* routes.
	ProjectAccess api.ProjectAccessConfig
	// EnrichmentAvailable reports whether the embedding/fact/entity providers
	// are configured; enrichment and dreaming routes serve 503 when they are not.
	EnrichmentAvailable func() bool
	// Settings backs the two gates resolved live per request, so an admin can
	// toggle them without a restart.
	Settings *service.SettingsService
}

// buildRouterConfig assembles the production router wiring. It is a function
// rather than a literal inside main() so TestBuildRouterConfigWiresEverything
// can construct the real thing and assert no field was left nil; see that test
// for why that matters.
func buildRouterConfig(d routerDeps) server.RouterConfig {
	// Bound once rather than inside each closure, so the two long-lived
	// middlewares hold only the settings service instead of a copy of every
	// dependency in d.
	settings := d.Settings

	return server.RouterConfig{
		Metrics:        d.Metrics,
		AuthMiddleware: d.AuthMiddleware,
		RateLimiter:    d.RateLimiter,
		SetupGuard:     api.SetupGuardMiddleware(d.SetupComplete),
		ProjectAccess:  api.ProjectAccessMiddleware(d.ProjectAccess),
		EnrichmentGate: api.EnrichmentGateMiddleware(d.EnrichmentAvailable),
		// Resolved live per request so toggling ask.enabled in the admin UI
		// surfaces or hides the ask endpoints without a restart.
		AskGate: api.AskGateMiddleware(func(ctx context.Context) bool {
			return settings.ResolveBoolWithDefault(ctx, service.SettingAskEnabled, "global")
		}),
		// Resolved live per request for the same reason.
		HostGuard: server.HostGuardMiddleware(func(ctx context.Context) bool {
			return settings.ResolveBoolWithDefault(ctx, service.SettingServerHostRebindingProtection, "global")
		}),
	}
}
