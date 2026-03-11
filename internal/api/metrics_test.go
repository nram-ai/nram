package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func TestMetricsMiddleware_RequestCountAndDuration(t *testing.T) {
	m := NewMetrics()
	handler := MetricsMiddleware(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// Verify request count
	counter, err := m.HTTPRequestsTotal.GetMetricWithLabelValues("GET", "/v1/health", "200")
	if err != nil {
		t.Fatalf("failed to get metric: %v", err)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.GetCounter().GetValue(); got != 1 {
		t.Errorf("expected request count 1, got %v", got)
	}

	// Verify duration was observed via the metrics endpoint
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	MetricsHandler(m).ServeHTTP(metricsRR, metricsReq)

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metricsRR.Body.String()))
	if err != nil {
		t.Fatalf("failed to parse prometheus output: %v", err)
	}
	dFam, ok := families["http_request_duration_seconds"]
	if !ok {
		t.Fatal("http_request_duration_seconds not found in output")
	}
	if got := dFam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("expected 1 duration observation, got %v", got)
	}
}

func TestMetricsMiddleware_StatusCodes(t *testing.T) {
	m := NewMetrics()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"200 OK", http.StatusOK},
		{"201 Created", http.StatusCreated},
		{"400 Bad Request", http.StatusBadRequest},
		{"404 Not Found", http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := MetricsMiddleware(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))

			req := httptest.NewRequest(http.MethodPost, "/v1/memories", nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			_ = rr

			statusLabel := strconv.Itoa(tt.statusCode)
			c, err := m.HTTPRequestsTotal.GetMetricWithLabelValues("POST", "/v1/memories", statusLabel)
			if err != nil {
				t.Fatalf("failed to get metric for status %d: %v", tt.statusCode, err)
			}
			var metric dto.Metric
			if err := c.Write(&metric); err != nil {
				t.Fatalf("failed to write metric: %v", err)
			}
			if got := metric.GetCounter().GetValue(); got < 1 {
				t.Errorf("expected at least 1 request with status %d, got %v", tt.statusCode, got)
			}
		})
	}
}

func TestMetricsHandler_PrometheusFormat(t *testing.T) {
	m := NewMetrics()

	// Make one request through the middleware to generate some data
	handler := MetricsMiddleware(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// Now hit the metrics handler
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	MetricsHandler(m).ServeHTTP(metricsRR, metricsReq)

	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", metricsRR.Code)
	}

	// Parse the prometheus text format
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metricsRR.Body.String()))
	if err != nil {
		t.Fatalf("failed to parse prometheus output: %v", err)
	}

	expectedMetrics := []string{
		"http_requests_total",
		"http_request_duration_seconds",
	}
	for _, name := range expectedMetrics {
		if _, ok := families[name]; !ok {
			t.Errorf("expected metric %q in prometheus output", name)
		}
	}
}

func TestMetrics_BusinessCounters(t *testing.T) {
	m := NewMetrics()

	// Increment each business counter
	m.MemoriesTotal.Inc()
	m.MemoriesTotal.Inc()
	m.MemoriesRecalled.Inc()
	m.MemoriesForgotten.Inc()
	m.EnrichmentsTotal.WithLabelValues("completed").Inc()
	m.EnrichmentsTotal.WithLabelValues("failed").Inc()
	m.EnrichmentsTotal.WithLabelValues("failed").Inc()
	m.EmbeddingsTotal.Inc()
	m.EmbeddingDuration.Observe(0.123)
	m.TokensUsedTotal.WithLabelValues("openai", "embedding").Add(500)
	m.VectorSearchDuration.Observe(0.045)

	// Verify via the metrics handler output
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	MetricsHandler(m).ServeHTTP(metricsRR, metricsReq)

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metricsRR.Body.String()))
	if err != nil {
		t.Fatalf("failed to parse prometheus output: %v", err)
	}

	// Check nram_memories_total
	fam, ok := families["nram_memories_total"]
	if !ok {
		t.Fatal("nram_memories_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 2 {
		t.Errorf("nram_memories_total: expected 2, got %v", got)
	}

	// Check nram_memories_recalled
	fam, ok = families["nram_memories_recalled"]
	if !ok {
		t.Fatal("nram_memories_recalled not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("nram_memories_recalled: expected 1, got %v", got)
	}

	// Check nram_memories_forgotten
	fam, ok = families["nram_memories_forgotten"]
	if !ok {
		t.Fatal("nram_memories_forgotten not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("nram_memories_forgotten: expected 1, got %v", got)
	}

	// Check nram_enrichments_total by status label
	fam, ok = families["nram_enrichments_total"]
	if !ok {
		t.Fatal("nram_enrichments_total not found")
	}
	enrichmentsByStatus := map[string]float64{}
	for _, met := range fam.GetMetric() {
		for _, lp := range met.GetLabel() {
			if lp.GetName() == "status" {
				enrichmentsByStatus[lp.GetValue()] = met.GetCounter().GetValue()
			}
		}
	}
	if enrichmentsByStatus["completed"] != 1 {
		t.Errorf("enrichments completed: expected 1, got %v", enrichmentsByStatus["completed"])
	}
	if enrichmentsByStatus["failed"] != 2 {
		t.Errorf("enrichments failed: expected 2, got %v", enrichmentsByStatus["failed"])
	}

	// Check nram_embeddings_total
	fam, ok = families["nram_embeddings_total"]
	if !ok {
		t.Fatal("nram_embeddings_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("nram_embeddings_total: expected 1, got %v", got)
	}

	// Check nram_embedding_duration_seconds
	fam, ok = families["nram_embedding_duration_seconds"]
	if !ok {
		t.Fatal("nram_embedding_duration_seconds not found")
	}
	if got := fam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("nram_embedding_duration_seconds: expected 1 sample, got %v", got)
	}

	// Check nram_tokens_used_total
	fam, ok = families["nram_tokens_used_total"]
	if !ok {
		t.Fatal("nram_tokens_used_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 500 {
		t.Errorf("nram_tokens_used_total: expected 500, got %v", got)
	}

	// Check nram_vector_search_duration_seconds
	fam, ok = families["nram_vector_search_duration_seconds"]
	if !ok {
		t.Fatal("nram_vector_search_duration_seconds not found")
	}
	if got := fam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("nram_vector_search_duration_seconds: expected 1 sample, got %v", got)
	}
}
