package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/nram-ai/nram/internal/auth"
)

// TestMetricsScrape_EndToEnd boots the production router exactly the way
// cmd/server/main.go does, drives a handful of requests through it, then
// scrapes /metrics and validates:
//  1. /metrics returns 200 with parseable Prometheus text format.
//  2. The expected metric families are present.
//  3. Path-parameterized routes collapse to a single time series per route
//     pattern (regression guard for the cardinality fix).
//
// This is the missing companion to TestMetricsEndpointNoAuth, which only
// checked status code on a router that had never served traffic.
func TestMetricsScrape_EndToEnd(t *testing.T) {
	storeCalled := 0
	handlers := Handlers{
		Store: func(w http.ResponseWriter, r *http.Request) {
			storeCalled++
			w.WriteHeader(http.StatusCreated)
		},
		Health: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	}
	r := newTestRouter(t, handlers)

	// One health hit (no path params).
	healthReq := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	r.ServeHTTP(httptest.NewRecorder(), healthReq)

	// Three store hits across three different project IDs. With the route
	// pattern fix, these collapse to a single time series labeled
	// /v1/projects/{projectID}/memories. Without it (the old behavior),
	// each unique UUID produced a separate series.
	projectIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, pid := range projectIDs {
		userID := uuid.New()
		token := generateTestJWT(t, userID, auth.RoleMember)
		req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+pid.String()+"/memories", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		r.ServeHTTP(httptest.NewRecorder(), req)
	}
	if storeCalled != 3 {
		t.Fatalf("expected store handler to be called 3 times, got %d", storeCalled)
	}

	// One 404 to confirm unmatched routes do not balloon cardinality.
	missReq := httptest.NewRequest(http.MethodGet, "/does/not/exist", nil)
	r.ServeHTTP(httptest.NewRecorder(), missReq)

	// Scrape /metrics through the same router.
	scrape := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	scrapeRec := httptest.NewRecorder()
	r.ServeHTTP(scrapeRec, scrape)
	if scrapeRec.Code != http.StatusOK {
		t.Fatalf("/metrics returned status %d; body: %s", scrapeRec.Code, scrapeRec.Body.String())
	}

	body := scrapeRec.Body.String()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		t.Fatalf("/metrics body is not valid prometheus text format: %v", err)
	}

	// CounterVecs with no recorded label combinations
	// (nram_enrichments_total, nram_tokens_used_total, nram_embeddings_total)
	// intentionally do NOT appear in the exposition until the first
	// .WithLabelValues(...).Inc() — that is the documented Prometheus
	// client behavior and matches how Grafana absent() detection reads
	// them. The live-server verification step in the plan exercises all
	// three, so absence in this no-traffic test is correct.
	for _, name := range []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"http_requests_in_flight",
		"nram_memories_total",
		"nram_memories_recalled_total",
		"nram_memories_forgotten_total",
		"nram_embedding_duration_seconds",
		"nram_vector_search_duration_seconds",
	} {
		if _, ok := families[name]; !ok {
			t.Errorf("expected metric family %q in /metrics output", name)
		}
	}

	// Cardinality guard: the three random project UUIDs must not appear in
	// the exposition. The route pattern /v1/projects/{projectID}/memories
	// should appear exactly once for the three POSTs.
	for _, pid := range projectIDs {
		if strings.Contains(body, pid.String()) {
			t.Errorf("metrics exposition leaked raw UUID %q — cardinality fix regressed", pid.String())
		}
	}

	// chi formats the route pattern with the literal {param_name} placeholder
	// from the route registration. router.go registers the project memory
	// route with the {project_id} URL parameter, so that is what lands in
	// the path label.
	patternHits := strings.Count(body, `path="/v1/projects/{project_id}/memories"`)
	if patternHits == 0 {
		t.Errorf("expected at least one series labeled with route pattern /v1/projects/{project_id}/memories; body:\n%s", body)
	}

	// The 404 path "/does/not/exist" was unmatched. It should collapse to
	// an empty-string pattern (a single series), not contribute a unique
	// series per random unmatched URL.
	if strings.Contains(body, "/does/not/exist") {
		t.Errorf("metrics exposition leaked unmatched raw path /does/not/exist — cardinality fix regressed")
	}
}
