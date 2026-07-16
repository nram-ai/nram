package enrichment

import (
	"context"
	"testing"

	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// countingUnparseableLLM returns unparseable content with a fixed finish reason
// and increments *count on every Complete call, so a test can assert whether
// the deterministic retry was issued.
func countingUnparseableLLM(finishReason string, count *int) *mockLLMProvider {
	return &mockLLMProvider{
		name: "ingest",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			*count++
			return &provider.CompletionResponse{
				Content:      "this is not json at all",
				Model:        "ingest-model",
				FinishReason: finishReason,
				Usage:        provider.TokenUsage{TotalTokens: 10},
			}, nil
		},
	}
}

func runIngestionFallbackCase(t *testing.T, temperature, finishReason string, callCount *int) {
	t.Helper()
	target := testMemory()
	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.95, NamespaceID: target.NamespaceID},
	}

	overrides := map[string]string{
		service.SettingIngestionDecisionEnabled:               "true",
		service.SettingIngestionDecisionShadow:                "false",
		service.SettingEnrichmentIngestionDecisionTemperature: temperature,
	}
	h := newIngestionHarness(
		overrides,
		dedupResults,
		minimalFactLLM(),
		minimalEntityLLM(),
		countingUnparseableLLM(finishReason, callCount),
		constEmbedder(),
	)

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Every branch must converge on ADD-FALLBACK (unparseable both ways).
	parentMark := findEnrichedMark(h.updater.enrichedMarks, newMem.ID)
	if parentMark == nil {
		t.Fatal("expected a MarkEnriched on the new memory")
	}
	md := decodeMetadata(t, parentMark.metadata)
	if got, _ := md["ingestion_decision"].(string); got != IngestionOpAddFallback {
		t.Errorf("ingestion_decision = %q, want ADD-FALLBACK", got)
	}
}

// At temperature 0 a truncated first response means the retry is deterministic,
// so it is skipped: exactly one ingestion completion, decision ADD-FALLBACK.
func TestIngestion_Truncated_Temp0_SkipsRetry(t *testing.T) {
	calls := 0
	runIngestionFallbackCase(t, "0", "length", &calls)
	if calls != 1 {
		t.Fatalf("ingestion completions = %d, want 1 (deterministic retry skipped)", calls)
	}
}

// At temperature > 0 the re-send can sample differently, so the retry still
// fires even when the first response was truncated.
func TestIngestion_Truncated_TempNonZero_StillRetries(t *testing.T) {
	calls := 0
	runIngestionFallbackCase(t, "0.7", "length", &calls)
	if calls != 2 {
		t.Fatalf("ingestion completions = %d, want 2 (retry preserved at temp>0)", calls)
	}
}

// An unknown finish reason at temperature 0 is not provably deterministic
// truncation, so today's retry behavior is preserved.
func TestIngestion_UnknownFinishReason_Temp0_StillRetries(t *testing.T) {
	calls := 0
	runIngestionFallbackCase(t, "0", "", &calls)
	if calls != 2 {
		t.Fatalf("ingestion completions = %d, want 2 (retry preserved on unknown finish reason)", calls)
	}
}

// TestIngestion_Temp0_SentAsNonNilPointer verifies the ingestion-decision call
// carries an explicit (non-nil) temperature of 0, so the model decodes greedily
// as the "0 keeps the add/update/delete/skip choice consistent" setting
// promises, rather than the 0 being dropped and sampled at the server default.
func TestIngestion_Temp0_SentAsNonNilPointer(t *testing.T) {
	target := testMemory()
	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.95, NamespaceID: target.NamespaceID},
	}
	var captured *provider.CompletionRequest
	capturingLLM := &mockLLMProvider{
		name: "ingest",
		respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			captured = req
			return &provider.CompletionResponse{
				Content:      "this is not json at all",
				Model:        "ingest-model",
				FinishReason: "length",
				Usage:        provider.TokenUsage{TotalTokens: 10},
			}, nil
		},
	}
	overrides := map[string]string{
		service.SettingIngestionDecisionEnabled:               "true",
		service.SettingIngestionDecisionShadow:                "false",
		service.SettingEnrichmentIngestionDecisionTemperature: "0",
	}
	h := newIngestionHarness(overrides, dedupResults, minimalFactLLM(), minimalEntityLLM(), capturingLLM, constEmbedder())

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}
	if captured == nil {
		t.Fatal("ingestion-decision LLM was never called")
	}
	if captured.Temperature == nil {
		t.Fatal("ingestion-decision temperature was nil (dropped); want an explicit 0 so the call is greedy")
	}
	if *captured.Temperature != 0 {
		t.Errorf("ingestion-decision temperature = %v, want 0", *captured.Temperature)
	}
}
