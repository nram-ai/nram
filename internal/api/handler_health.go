package api

import (
	"context"
	"net/http"
	"time"

	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/version"
)

// DatabasePinger abstracts database connectivity checking.
type DatabasePinger interface {
	Backend() string
	Ping(ctx context.Context) error
}

// ProviderRegistry abstracts access to provider slots. GetLLM resolves any LLM
// slot by name and applies the slot's fallback (e.g. query_augment falls back to
// fact), so the health surface reports a slot as operational whenever it can
// actually serve.
type ProviderRegistry interface {
	GetEmbedding() provider.EmbeddingProvider
	GetLLM(name string) provider.LLMProvider
}

// QueueStatter abstracts enrichment queue statistics retrieval.
type QueueStatter interface {
	CountByStatus(ctx context.Context) (*storage.QueueStats, error)
}

// HealthConfig holds the dependencies for the health handler.
type HealthConfig struct {
	DB        DatabasePinger
	Providers ProviderRegistry // may be nil (no registry configured)
	Queue     QueueStatter     // may be nil (SQLite mode)
	Version   string
	Build     version.BuildInfo
	StartTime time.Time
}

type healthResponse struct {
	Status          string                          `json:"status"`
	Version         string                          `json:"version"`
	Build           healthBuild                     `json:"build"`
	Backend         string                          `json:"backend"`
	Database        healthDatabase                  `json:"database"`
	Providers       map[string]healthProviderStatus `json:"providers"`
	EnrichmentQueue *healthEnrichmentQueue          `json:"enrichment_queue,omitempty"`
	UptimeSeconds   int64                           `json:"uptime_seconds"`
}

// healthBuild reports the VCS build identity of the running binary. The
// semantic version lives in the top-level Version field; this object carries
// only the build-time provenance.
type healthBuild struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
	Time   string `json:"time"`
	Go     string `json:"go"`
}

type healthDatabase struct {
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
}

type healthProviderStatus struct {
	Status    string `json:"status"`
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model,omitempty"`
	LatencyMs *int64 `json:"latency_ms,omitempty"`
}

type healthEnrichmentQueue struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Failed     int `json:"failed"`
}

// NewHealthHandler returns an http.HandlerFunc that reports system health
// including database connectivity, provider status, enrichment queue depth,
// and uptime.
func NewHealthHandler(cfg HealthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		backend := cfg.DB.Backend()

		// Check database connectivity.
		dbStatus := healthDatabase{Status: "ok"}
		overallStatus := "ok"

		start := time.Now()
		if err := cfg.DB.Ping(ctx); err != nil {
			dbStatus.Status = "error"
			overallStatus = "degraded"
		}
		dbStatus.LatencyMs = time.Since(start).Milliseconds()

		// Check provider health.
		providers := buildProviderHealth(ctx, cfg.Providers)

		// Build response.
		resp := healthResponse{
			Status:  overallStatus,
			Version: cfg.Version,
			Build: healthBuild{
				Commit: cfg.Build.Commit,
				Dirty:  cfg.Build.Dirty,
				Time:   cfg.Build.Time,
				Go:     cfg.Build.Go,
			},
			Backend:       backend,
			Database:      dbStatus,
			Providers:     providers,
			UptimeSeconds: int64(time.Since(cfg.StartTime).Seconds()),
		}

		// Enrichment queue stats.
		if cfg.Queue != nil {
			stats, err := cfg.Queue.CountByStatus(ctx)
			if err == nil {
				resp.EnrichmentQueue = &healthEnrichmentQueue{
					Pending:    stats.Pending,
					Processing: stats.Processing,
					Failed:     stats.Failed,
				}
			}
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// buildProviderHealth checks every provider slot and returns health statuses
// keyed by canonical slot name. It iterates provider.Slots, the single source of
// truth, so a newly added slot is reported without touching this function.
//
// LLM status is cached by the underlying provider instance: when a provider is
// shared across slots (fact also serves query_augment and ingestion_decision via
// fallback) it is pinged once per health check, not once per slot.
func buildProviderHealth(ctx context.Context, reg ProviderRegistry) map[string]healthProviderStatus {
	out := make(map[string]healthProviderStatus, len(provider.Slots))
	seen := make(map[provider.LLMProvider]healthProviderStatus)
	for _, slot := range provider.Slots {
		if slot.Kind == provider.KindEmbedding {
			out[slot.Name] = checkEmbeddingProvider(ctx, reg)
			continue
		}
		out[slot.Name] = checkLLMProvider(ctx, reg, slot.Name, seen)
	}
	return out
}

// checkEmbeddingProvider checks the embedding provider slot.
func checkEmbeddingProvider(ctx context.Context, reg ProviderRegistry) healthProviderStatus {
	if reg == nil {
		return healthProviderStatus{Status: "not_configured"}
	}

	ep := reg.GetEmbedding()
	if ep == nil {
		return healthProviderStatus{Status: "not_configured"}
	}

	status := healthProviderStatus{
		Status:   "ok",
		Provider: ep.Name(),
	}

	// Check if provider implements ProviderHealth for ping.
	if ph, ok := ep.(provider.ProviderHealth); ok {
		start := time.Now()
		if err := ph.Ping(ctx); err != nil {
			status.Status = "error"
		}
		latency := time.Since(start).Milliseconds()
		status.LatencyMs = &latency
	}

	return status
}

// checkLLMProvider checks an LLM provider slot by name. Resolution is
// fallback-aware (via reg.GetLLM), so an optional slot backed only by its
// fallback still reports as operational with the serving provider. The seen map
// memoizes status per underlying provider so a shared provider is pinged once.
func checkLLMProvider(ctx context.Context, reg ProviderRegistry, slot string, seen map[provider.LLMProvider]healthProviderStatus) healthProviderStatus {
	if reg == nil {
		return healthProviderStatus{Status: "not_configured"}
	}

	lp := reg.GetLLM(slot)
	if lp == nil {
		return healthProviderStatus{Status: "not_configured"}
	}

	if st, ok := seen[lp]; ok {
		return st
	}

	status := healthProviderStatus{
		Status:   "ok",
		Provider: lp.Name(),
	}

	models := lp.Models()
	if len(models) > 0 {
		status.Model = models[0]
	}

	// Check if provider implements ProviderHealth for ping.
	if ph, ok := lp.(provider.ProviderHealth); ok {
		start := time.Now()
		if err := ph.Ping(ctx); err != nil {
			status.Status = "error"
		}
		latency := time.Since(start).Milliseconds()
		status.LatencyMs = &latency
	}

	seen[lp] = status
	return status
}
