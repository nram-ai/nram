package admin

import (
	"context"
	"testing"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

func ptr[T any](v T) *T { return &v }

// TestUsageStoreAggregation_SuccessErrorLatency exercises the new aggregate
// columns (success_count, error_count, avg_latency_ms) on the storage path
// to ensure each is computed correctly across mixed rows.
func TestUsageStoreAggregation_SuccessErrorLatency(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	nsID := insertTestNamespace(t, db, ctx)

	repo := storage.NewTokenUsageRepo(db)
	store := NewUsageStore(db)

	rows := []model.TokenUsage{
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 100, TokensOutput: 50, LatencyMs: ptr(100), Success: true},
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 100, TokensOutput: 50, LatencyMs: ptr(200), Success: true},
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 100, TokensOutput: 50, LatencyMs: ptr(300), Success: true},
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 0, TokensOutput: 0, LatencyMs: ptr(50), Success: false, ErrorCode: ptr("circuit_open")},
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 0, TokensOutput: 0, LatencyMs: ptr(50), Success: false, ErrorCode: ptr("timeout")},
		{NamespaceID: nsID, Operation: "recall", Provider: "openai", Model: "gpt-4", TokensInput: 500, TokensOutput: 200, LatencyMs: ptr(1000), Success: true},
		// AVG(latency_ms) must ignore NULL so the per-group average reflects
		// only middleware-written rows; this row is here to catch a regression
		// to COALESCE(latency_ms, 0) which would deflate the average.
		{NamespaceID: nsID, Operation: "extract", Provider: "openai", Model: "gpt-4", TokensInput: 10, TokensOutput: 0, LatencyMs: nil, Success: true},
	}
	for i := range rows {
		if err := repo.Record(ctx, &rows[i]); err != nil {
			t.Fatalf("record row %d: %v", i, err)
		}
	}

	report, err := store.QueryUsage(ctx, api.UsageFilter{GroupBy: "operation"})
	if err != nil {
		t.Fatalf("QueryUsage: %v", err)
	}

	byKey := map[string]api.UsageGroup{}
	for _, g := range report.Groups {
		byKey[g.Key] = g
	}

	extract, ok := byKey["extract"]
	if !ok {
		t.Fatalf("expected extract group, got groups: %+v", report.Groups)
	}
	if extract.CallCount != 6 {
		t.Errorf("extract.call_count = %d, want 6", extract.CallCount)
	}
	if extract.SuccessCount != 4 {
		t.Errorf("extract.success_count = %d, want 4", extract.SuccessCount)
	}
	if extract.ErrorCount != 2 {
		t.Errorf("extract.error_count = %d, want 2", extract.ErrorCount)
	}
	// AVG over the five non-NULL latencies (100, 200, 300, 50, 50) = 140.
	// The sixth (NULL) row must not be counted.
	if extract.AvgLatencyMs != 140 {
		t.Errorf("extract.avg_latency_ms = %v, want 140", extract.AvgLatencyMs)
	}

	recall, ok := byKey["recall"]
	if !ok {
		t.Fatalf("expected recall group")
	}
	if recall.SuccessCount != 1 || recall.ErrorCount != 0 {
		t.Errorf("recall split = (success=%d, error=%d), want (1, 0)", recall.SuccessCount, recall.ErrorCount)
	}
	if recall.AvgLatencyMs != 1000 {
		t.Errorf("recall.avg_latency_ms = %v, want 1000", recall.AvgLatencyMs)
	}
}

// TestUsageStoreSuccessOnlyFilter verifies the success_only filter restricts
// the aggregation to successful rows only.
func TestUsageStoreSuccessOnlyFilter(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	nsID := insertTestNamespace(t, db, ctx)

	repo := storage.NewTokenUsageRepo(db)
	store := NewUsageStore(db)

	rows := []model.TokenUsage{
		{NamespaceID: nsID, Operation: "extract", TokensInput: 100, TokensOutput: 0, LatencyMs: ptr(100), Success: true},
		{NamespaceID: nsID, Operation: "extract", TokensInput: 100, TokensOutput: 0, LatencyMs: ptr(100), Success: true},
		{NamespaceID: nsID, Operation: "extract", TokensInput: 0, TokensOutput: 0, LatencyMs: ptr(50), Success: false, ErrorCode: ptr("provider_error")},
	}
	for i := range rows {
		if err := repo.Record(ctx, &rows[i]); err != nil {
			t.Fatalf("record row %d: %v", i, err)
		}
	}

	successOnly := true
	report, err := store.QueryUsage(ctx, api.UsageFilter{GroupBy: "operation", SuccessOnly: &successOnly})
	if err != nil {
		t.Fatalf("QueryUsage: %v", err)
	}
	if len(report.Groups) != 1 || report.Groups[0].Key != "extract" {
		t.Fatalf("expected one extract group, got %+v", report.Groups)
	}
	g := report.Groups[0]
	if g.CallCount != 2 || g.ErrorCount != 0 || g.SuccessCount != 2 {
		t.Errorf("success-only group = (calls=%d, success=%d, error=%d), want (2, 2, 0)",
			g.CallCount, g.SuccessCount, g.ErrorCount)
	}
}

// TestUsageStoreGroupByErrorCode verifies error_code is accepted as a group_by
// dimension and that the error-only buckets aggregate correctly.
func TestUsageStoreGroupByErrorCode(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	nsID := insertTestNamespace(t, db, ctx)

	repo := storage.NewTokenUsageRepo(db)
	store := NewUsageStore(db)

	rows := []model.TokenUsage{
		{NamespaceID: nsID, Operation: "extract", TokensInput: 0, LatencyMs: ptr(10), Success: false, ErrorCode: ptr("circuit_open")},
		{NamespaceID: nsID, Operation: "extract", TokensInput: 0, LatencyMs: ptr(10), Success: false, ErrorCode: ptr("circuit_open")},
		{NamespaceID: nsID, Operation: "extract", TokensInput: 0, LatencyMs: ptr(20), Success: false, ErrorCode: ptr("timeout")},
		{NamespaceID: nsID, Operation: "extract", TokensInput: 100, LatencyMs: ptr(30), Success: true},
	}
	for i := range rows {
		if err := repo.Record(ctx, &rows[i]); err != nil {
			t.Fatalf("record row %d: %v", i, err)
		}
	}

	report, err := store.QueryUsage(ctx, api.UsageFilter{GroupBy: "error_code"})
	if err != nil {
		t.Fatalf("QueryUsage: %v", err)
	}

	byKey := map[string]api.UsageGroup{}
	for _, g := range report.Groups {
		byKey[g.Key] = g
	}
	// Successful rows have NULL error_code which COALESCEs to '' (empty key).
	if g, ok := byKey["circuit_open"]; !ok || g.CallCount != 2 {
		t.Errorf("circuit_open group = %+v, want call_count 2", g)
	}
	if g, ok := byKey["timeout"]; !ok || g.CallCount != 1 {
		t.Errorf("timeout group = %+v, want call_count 1", g)
	}
}
