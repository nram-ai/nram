package model

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Enrichment step names persisted into EnrichmentJob.StepsCompleted by the
// worker as it finishes each phase. Read by runPreEmbed to short-circuit
// retries of jobs that already advanced past a phase before failing.
const (
	StepFactExtraction               = "fact_extraction"
	StepEntityExtraction             = "entity_extraction"
	StepQueryAugmentation            = "query_augmentation"
	StepEmbedding                    = "embedding"
	StepExtractedFactParaphraseGuard = "extracted_fact_paraphrase_guard"
	// StepMultiVectorFacets marks a multi-vector facet backfill sweep as
	// finished on the job so a re-claim after a partial failure is a no-op,
	// mirroring StepExtractedFactParaphraseGuard's role for the paraphrase
	// sweep.
	StepMultiVectorFacets = "multi_vector_facets"
)

// JobMarkerOnlyParaphraseGuard is a sentinel value placed in
// EnrichmentJob.StepsCompleted by the BackfillExtractedFactParaphrase
// service method when it enqueues a per-parent sweep job. The worker
// recognizes the sentinel and routes only to the paraphrase-guard sweep
// handler, skipping fact/entity extraction, augmentation, and embed steps.
// Storing the marker in StepsCompleted avoids a schema change while
// preserving the existing per-step idempotency contract.
const JobMarkerOnlyParaphraseGuard = "__only_paraphrase_guard__"

// JobMarkerOnlyMultiVector is a sentinel placed in EnrichmentJob.StepsCompleted
// by BackfillMultiVector when it enqueues a per-memory facet backfill job. The
// worker recognizes it and routes ONLY to the lean facet sweep handler, which
// reuses the memory's stored facet-0 vector and runs only the per-topic
// sentence embeds: no ingestion-decision, no query-augmentation LLM call, and
// no whole-memory re-embed. Same StepsCompleted-as-marker mechanism as
// JobMarkerOnlyParaphraseGuard, so it needs no schema change.
const JobMarkerOnlyMultiVector = "__only_multi_vector__"

// Reasons the query-augmentation phase did not land in the persisted vector.
// Written into enrichment_queue.query_augment_skip_reason when the step is
// absent from StepsCompleted on a completed job so the UI can label the
// skipped row with a specific cause instead of bare "skipped".
const (
	QueryAugmentSkipDisabled            = "disabled"
	QueryAugmentSkipContentEmpty        = "content_empty"
	QueryAugmentSkipProviderUnavailable = "provider_unavailable"
	QueryAugmentSkipLLMError            = "llm_error"
	QueryAugmentSkipParseError          = "parse_error"
	// QueryAugmentSkipDeleted marks a job whose ingestion-decision phase chose
	// DELETE: the memory is being discarded, so augmenting it would burn an LLM
	// call on a row that is about to be soft-deleted.
	QueryAugmentSkipDeleted = "deleted"
	// QueryAugmentSkipAlreadyDone marks a re-enqueued job whose memory already
	// carries a finalized augmented vector (embedding_dim set AND
	// augmented_embedding_at stamped AND augmented_queries present). The phase is
	// skipped and the stored vector reused rather than recomputed.
	QueryAugmentSkipAlreadyDone = "already_augmented"
)

// EnrichmentJob.Status values. Mirrors the schema's set; using these
// constants instead of raw strings catches typos at compile time.
const (
	EnrichmentStatusPending    = "pending"
	EnrichmentStatusProcessing = "processing"
	EnrichmentStatusCompleted  = "completed"
	EnrichmentStatusFailed     = "failed"
)

type EnrichmentJob struct {
	ID                     uuid.UUID       `json:"id"`
	MemoryID               uuid.UUID       `json:"memory_id"`
	NamespaceID            uuid.UUID       `json:"namespace_id"`
	Status                 string          `json:"status"`
	Priority               int             `json:"priority"`
	ClaimedAt              *time.Time      `json:"claimed_at"`
	ClaimedBy              *string         `json:"claimed_by"`
	HeartbeatAt            *time.Time      `json:"heartbeat_at"`
	Attempts               int             `json:"attempts"`
	MaxAttempts            int             `json:"max_attempts"`
	LastError              json.RawMessage `json:"last_error"`
	LastRequeueReason      *string         `json:"last_requeue_reason"`
	StepsCompleted         json.RawMessage `json:"steps_completed"`
	QueryAugmentSkipReason *string         `json:"query_augment_skip_reason"`
	CompletedAt            *time.Time      `json:"completed_at"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}

// EnrichmentRunKey is the per-run correlation key for a single enrichment job
// attempt. The worker stamps it onto the provider context (via
// provider.WithRequestID) before each phase's LLM/embedding call so every
// token_usage row that run records carries it in request_id; the enrichment
// monitor's read side reconstructs the same key from the queue item's id and
// attempts to join phase metrics to the exact run instead of a timestamp window.
//
// (job.ID, attempts) is a stable run identity because a claim does not bump
// attempts (only Fail/RequeueStale do), so the value is fixed for the lifetime
// of one attempt on both the write and read sides. The "enrich:" prefix keeps
// the key from colliding with HTTP X-Request-ID UUIDs that also land in
// token_usage.request_id.
func EnrichmentRunKey(jobID uuid.UUID, attempts int) string {
	return "enrich:" + jobID.String() + ":" + strconv.Itoa(attempts)
}
