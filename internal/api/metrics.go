package api

import (
	"net/http"
	"strconv"
	"time"

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

	// Business metrics
	MemoriesTotal            prometheus.Counter
	MemoriesRecalled         prometheus.Counter
	MemoriesForgotten        prometheus.Counter
	EnrichmentsTotal         *prometheus.CounterVec
	EmbeddingsTotal          prometheus.Counter
	EmbeddingDuration        prometheus.Histogram
	TokensUsedTotal          *prometheus.CounterVec
	VectorSearchDuration     prometheus.Histogram
}

// NewMetrics creates and registers all Prometheus metrics in a custom registry.
func NewMetrics() *Metrics {
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
			Name: "nram_memories_recalled",
			Help: "Total number of recall operations.",
		}),

		MemoriesForgotten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_memories_forgotten",
			Help: "Total number of forget operations.",
		}),

		EnrichmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nram_enrichments_total",
			Help: "Total number of enrichment operations.",
		}, []string{"status"}),

		EmbeddingsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "nram_embeddings_total",
			Help: "Total number of embedding operations.",
		}),

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
	)

	return m
}

// statusRecorder wraps http.ResponseWriter to capture the status code.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

// MetricsMiddleware returns HTTP middleware that records request count,
// duration, and in-flight gauge using the provided Metrics instance.
func MetricsMiddleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			m.HTTPRequestsInFlight.Inc()
			defer m.HTTPRequestsInFlight.Dec()

			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r)

			duration := time.Since(start).Seconds()
			status := strconv.Itoa(rec.statusCode)

			m.HTTPRequestsTotal.WithLabelValues(r.Method, r.URL.Path, status).Inc()
			m.HTTPRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
		})
	}
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics
// from the custom registry.
func MetricsHandler(m *Metrics) http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
