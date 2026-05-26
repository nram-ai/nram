package metrics

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// wrapWithRoute exercises Middleware behind a chi router so the
// RoutePattern() label is populated the same way it is in production.
func wrapWithRoute(m *Metrics, method, pattern string, h http.HandlerFunc) http.Handler {
	r := chi.NewRouter()
	r.Use(Middleware(m))
	r.Method(method, pattern, h)
	return r
}

func TestMiddleware_RequestCountAndDuration(t *testing.T) {
	m := New()
	handler := wrapWithRoute(m, http.MethodGet, "/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

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

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	Handler(m).ServeHTTP(metricsRR, metricsReq)

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

func TestMiddleware_StatusCodes(t *testing.T) {
	m := New()

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
			handler := wrapWithRoute(m, http.MethodPost, "/v1/memories", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			req := httptest.NewRequest(http.MethodPost, "/v1/memories", nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

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

func TestHandler_PrometheusFormat(t *testing.T) {
	m := New()

	handler := wrapWithRoute(m, http.MethodGet, "/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	Handler(m).ServeHTTP(metricsRR, metricsReq)

	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", metricsRR.Code)
	}

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

// TestMiddleware_RoutePatternCardinality is the regression guard for the
// 2026-05-25 cardinality fix: hitting the same route with different path
// parameters must collapse to a single time series under the route pattern,
// not produce one series per concrete URL.
func TestMiddleware_RoutePatternCardinality(t *testing.T) {
	m := New()
	handler := wrapWithRoute(m, http.MethodGet, "/v1/projects/{projectID}/memories", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for _, id := range []string{"alpha", "bravo", "charlie", "delta"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+id+"/memories", nil)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}

	counter, err := m.HTTPRequestsTotal.GetMetricWithLabelValues("GET", "/v1/projects/{projectID}/memories", "200")
	if err != nil {
		t.Fatalf("failed to get metric: %v", err)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.GetCounter().GetValue(); got != 4 {
		t.Errorf("expected 4 requests collapsed under route pattern, got %v", got)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	Handler(m).ServeHTTP(metricsRR, metricsReq)
	body := metricsRR.Body.String()
	for _, id := range []string{"alpha", "bravo", "charlie", "delta"} {
		if strings.Contains(body, "/v1/projects/"+id+"/memories") {
			t.Errorf("metrics exposition contains raw path with id %q — cardinality fix regressed", id)
		}
	}
}

func TestBusinessCounters(t *testing.T) {
	m := New()

	m.MemoriesTotal.Inc()
	m.MemoriesTotal.Inc()
	m.IncMemoriesRecalled()
	m.AddMemoriesForgotten(1)
	m.EnrichmentsTotal.WithLabelValues("completed").Inc()
	m.EnrichmentsTotal.WithLabelValues("failed").Inc()
	m.EnrichmentsTotal.WithLabelValues("failed").Inc()
	m.EmbeddingsTotal.WithLabelValues("success").Inc()
	m.EmbeddingDuration.Observe(0.123)
	m.TokensUsedTotal.WithLabelValues("openai", "embedding").Add(500)
	m.VectorSearchDuration.Observe(0.045)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	Handler(m).ServeHTTP(metricsRR, metricsReq)

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metricsRR.Body.String()))
	if err != nil {
		t.Fatalf("failed to parse prometheus output: %v", err)
	}

	fam, ok := families["nram_memories_total"]
	if !ok {
		t.Fatal("nram_memories_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 2 {
		t.Errorf("nram_memories_total: expected 2, got %v", got)
	}

	fam, ok = families["nram_memories_recalled_total"]
	if !ok {
		t.Fatal("nram_memories_recalled_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("nram_memories_recalled_total: expected 1, got %v", got)
	}

	fam, ok = families["nram_memories_forgotten_total"]
	if !ok {
		t.Fatal("nram_memories_forgotten_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("nram_memories_forgotten_total: expected 1, got %v", got)
	}

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

	fam, ok = families["nram_embeddings_total"]
	if !ok {
		t.Fatal("nram_embeddings_total not found")
	}
	successCount := 0.0
	for _, met := range fam.GetMetric() {
		for _, lp := range met.GetLabel() {
			if lp.GetName() == "status" && lp.GetValue() == "success" {
				successCount = met.GetCounter().GetValue()
			}
		}
	}
	if successCount != 1 {
		t.Errorf("nram_embeddings_total{status=\"success\"}: expected 1, got %v", successCount)
	}

	fam, ok = families["nram_embedding_duration_seconds"]
	if !ok {
		t.Fatal("nram_embedding_duration_seconds not found")
	}
	if got := fam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("nram_embedding_duration_seconds: expected 1 sample, got %v", got)
	}

	fam, ok = families["nram_tokens_used_total"]
	if !ok {
		t.Fatal("nram_tokens_used_total not found")
	}
	if got := fam.GetMetric()[0].GetCounter().GetValue(); got != 500 {
		t.Errorf("nram_tokens_used_total: expected 500, got %v", got)
	}

	fam, ok = families["nram_vector_search_duration_seconds"]
	if !ok {
		t.Fatal("nram_vector_search_duration_seconds not found")
	}
	if got := fam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("nram_vector_search_duration_seconds: expected 1 sample, got %v", got)
	}
}
