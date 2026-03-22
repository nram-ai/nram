package api

import (
	"context"
	"net/http"
	"time"

	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// DatabasePinger abstracts database connectivity checking.
type DatabasePinger interface {
	Backend() string
	Ping(ctx context.Context) error
}

// ProviderRegistry abstracts access to provider slots.
type ProviderRegistry interface {
	GetEmbedding() provider.EmbeddingProvider
	GetFact() provider.LLMProvider
	GetEntity() provider.LLMProvider
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
	StartTime time.Time
}

type healthResponse struct {
	Status          string                 `json:"status"`
	Version         string                 `json:"version"`
	Backend         string                 `json:"backend"`
	Database        healthDatabase         `json:"database"`
	Providers       healthProviders        `json:"providers"`
	EnrichmentQueue *healthEnrichmentQueue `json:"enrichment_queue,omitempty"`
	UptimeSeconds   int64                  `json:"uptime_seconds"`
}

type healthDatabase struct {
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
}

type healthProviders struct {
	Embedding        healthProviderStatus `json:"embedding"`
	FactExtraction   healthProviderStatus `json:"fact_extraction"`
	EntityExtraction healthProviderStatus `json:"entity_extraction"`
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
		providers := buildProviderHealth(ctx, backend, cfg.Providers)

		// Build response.
		resp := healthResponse{
			Status:    overallStatus,
			Version:   cfg.Version,
			Backend:   backend,
			Database:  dbStatus,
			Providers: providers,
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

// buildProviderHealth checks each provider slot and returns health statuses.
func buildProviderHealth(ctx context.Context, _ string, reg ProviderRegistry) healthProviders {
	return healthProviders{
		Embedding:        checkEmbeddingProvider(ctx, reg),
		FactExtraction:   checkLLMProvider(ctx, reg, "fact"),
		EntityExtraction: checkLLMProvider(ctx, reg, "entity"),
	}
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

	dims := ep.Dimensions()
	if len(dims) > 0 {
		// Use the first dimension as a representative value — not shown as "model" for embedding.
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

// checkLLMProvider checks a fact or entity extraction provider slot.
func checkLLMProvider(ctx context.Context, reg ProviderRegistry, slot string) healthProviderStatus {
	if reg == nil {
		return healthProviderStatus{Status: "not_configured"}
	}

	var lp provider.LLMProvider
	switch slot {
	case "fact":
		lp = reg.GetFact()
	case "entity":
		lp = reg.GetEntity()
	}

	if lp == nil {
		return healthProviderStatus{Status: "not_configured"}
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

	return status
}
