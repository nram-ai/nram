// Package metrics owns the Prometheus instrumentation surface for nram:
// the metric definitions, the chi middleware that records HTTP timings,
// and the /metrics handler. Keeping it in its own package avoids an
// import cycle when service-layer code (which is already imported by
// internal/api) needs to record business metrics.
package metrics

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the nram server.
type Metrics struct {
	Registry *prometheus.Registry

	// HTTP metrics
	HTTPRequestsTotal    *prometheus.CounterVec
	HTTPRequestDuration  *prometheus.HistogramVec
	HTTPRequestsInFlight prometheus.Gauge

	// MCPToolResultTruncations counts how often a tool response did not
	// arrive naturally at Tier 1. Tier values:
	//   "tier1_reduced"  — fit the structured budget but only after the
	//                      reducer discarded data. Structurally Tier 1,
	//                      but the caller received less than they asked
	//                      for. Operators watch this to spot tools whose
	//                      callers consistently exceed budget.
	//   "text_only"      — fit the full text budget but not the halved
	//                      structured budget. Data is complete; only the
	//                      structured wire copy was dropped.
	//   "hard_truncate"  — even the reduced text exceeded budget; body
	//                      byte-cut with a sentinel suffix. Alert on this.
	// Natural Tier 1 (no reducer iteration) is the no-truncation baseline
	// and does not increment the counter.
	MCPToolResultTruncations *prometheus.CounterVec

	// Business metrics
	MemoriesTotal        prometheus.Counter
	MemoriesRecalled     prometheus.Counter
	MemoriesForgotten    prometheus.Counter
	EnrichmentsTotal     *prometheus.CounterVec
	EmbeddingsTotal      *prometheus.CounterVec
	EmbeddingDuration    prometheus.Histogram
	TokensUsedTotal      *prometheus.CounterVec
	VectorSearchDuration prometheus.Histogram

	// Deprecated aliases. Earlier nram releases exposed
	// nram_memories_recalled and nram_memories_forgotten without the
	// _total suffix. The suffix was added to comply with Prometheus
	// naming convention; these aliases continue to expose the old series
	// so external dashboards and alerting rules keep working through one
	// release. Remove no earlier than the second release after this one.
	memoriesRecalledLegacy  prometheus.Counter
	memoriesForgottenLegacy prometheus.Counter
}

// New creates and registers all Prometheus metrics in a custom registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,

		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed.",
		}, []string{"method", "path", "status"}),

		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),

		HTTPRequestsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed.",
		}),

		MemoriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_total",
			Help: "Total number of memories stored.",
		}),

		MemoriesRecalled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_recalled_total",
			Help: "Total number of recall operations.",
		}),

		MemoriesForgotten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_forgotten_total",
			Help: "Total number of individual memories forgotten (one increment per deleted row, not per forget request — a bulk-forget request deleting N memories increments by N).",
		}),

		EnrichmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nram_enrichments_total",
			Help: "Total number of enrichment operations.",
		}, []string{"status"}),

		EmbeddingsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nram_embeddings_total",
			Help: "Total number of embedding operations, labeled by outcome (success|failure).",
		}, []string{"status"}),

		EmbeddingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "nram_embedding_duration_seconds",
			Help:    "Duration of embedding operations in seconds.",
			Buckets: prometheus.DefBuckets,
		}),

		TokensUsedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nram_tokens_used_total",
			Help: "Total number of tokens consumed.",
		}, []string{"provider", "operation"}),

		VectorSearchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "nram_vector_search_duration_seconds",
			Help:    "Duration of vector search operations in seconds.",
			Buckets: prometheus.DefBuckets,
		}),

		MCPToolResultTruncations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nram_mcp_tool_result_truncation_total",
			Help: "MCP tool responses that did not arrive at natural Tier 1. tier=tier1_reduced|text_only|hard_truncate. tier1_reduced: the reducer ran at least one iteration and the result still fit the structured tier. text_only: result fits the text budget but not the halved structured budget — the response body MAY carry a populated `_truncated` envelope (i.e. the reducer also dropped data); inspect the response to distinguish 'fit naturally as text' from 'fit only after reducer'. hard_truncate: even the reduced payload exceeded the text budget and was byte-cut with the sentinel suffix. Operators alert on hard_truncate; tier1_reduced is the early signal that a tool's callers exceed budget.",
		}, []string{"tool", "tier"}),

		memoriesRecalledLegacy: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_recalled",
			Help: "DEPRECATED: use nram_memories_recalled_total. Kept for one release to avoid breaking existing scrapers and dashboards.",
		}),
		memoriesForgottenLegacy: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_forgotten",
			Help: "DEPRECATED: use nram_memories_forgotten_total. Kept for one release to avoid breaking existing scrapers and dashboards.",
		}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.HTTPRequestsInFlight,
		m.MemoriesTotal,
		m.MemoriesRecalled,
		m.MemoriesForgotten,
		m.EnrichmentsTotal,
		m.EmbeddingsTotal,
		m.EmbeddingDuration,
		m.TokensUsedTotal,
		m.VectorSearchDuration,
		m.MCPToolResultTruncations,
		m.memoriesRecalledLegacy,
		m.memoriesForgottenLegacy,
	)

	return m
}

// IncMemoriesRecalled increments both the suffixed counter and the
// deprecated alias so scrapers on the old name keep seeing data through
// the rename window. Call this from service code instead of touching
// MemoriesRecalled directly.
func (m *Metrics) IncMemoriesRecalled() {
	m.MemoriesRecalled.Inc()
	m.memoriesRecalledLegacy.Inc()
}

// AddMemoriesForgotten increments both the suffixed counter and the
// deprecated alias. n is the number of memories deleted in this
// operation. See IncMemoriesRecalled for the alias rationale.
func (m *Metrics) AddMemoriesForgotten(n float64) {
	if n <= 0 {
		return
	}
	m.MemoriesForgotten.Add(n)
	m.memoriesForgottenLegacy.Add(n)
}

// RecordMCPToolResultTier increments the MCP tool-result truncation
// counter. tool is the wire-level tool name ("list", "recall", etc.);
// tier is "text_only" (Tier 2) or "hard_truncate" (Tier 3). Tier 1
// (schema-conforming) responses do not increment — only degradations
// are counted. Nil-safe for tests that wire a zero-value Metrics.
func (m *Metrics) RecordMCPToolResultTier(tool, tier string) {
	if m == nil || m.MCPToolResultTruncations == nil {
		return
	}
	m.MCPToolResultTruncations.WithLabelValues(tool, tier).Inc()
}

// statusRecorder wraps http.ResponseWriter to capture the status code.
// Flush/Hijack/Unwrap delegate so streaming handlers (SSE, MCP) keep
// working through this middleware — without them, w.(http.Flusher) fails
// and the endpoint 500s.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := sr.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (sr *statusRecorder) Unwrap() http.ResponseWriter { return sr.ResponseWriter }

// Middleware returns HTTP middleware that records request count,
// duration, and in-flight gauge using the provided Metrics instance.
//
// The path label is the chi route pattern (e.g. /v1/projects/{projectID}/memories)
// rather than the raw URL, so dynamic IDs collapse to a single time series and
// cardinality stays bounded. Requests that do not match any route fall through
// to an empty-string pattern, which also collapses cleanly.
func Middleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			m.HTTPRequestsInFlight.Inc()
			defer m.HTTPRequestsInFlight.Dec()

			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r)

			duration := time.Since(start).Seconds()
			status := strconv.Itoa(rec.statusCode)

			pattern := ""
			if rc := chi.RouteContext(r.Context()); rc != nil {
				pattern = rc.RoutePattern()
			}

			m.HTTPRequestsTotal.WithLabelValues(r.Method, pattern, status).Inc()
			m.HTTPRequestDuration.WithLabelValues(r.Method, pattern).Observe(duration)
		})
	}
}

// Handler returns an http.Handler that serves Prometheus metrics
// from the custom registry.
func Handler(m *Metrics) http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
