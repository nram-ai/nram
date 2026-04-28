package provider

import (
	"context"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// Operation is a typed enumeration of token-consuming call sites. Using a
// distinct type prevents typo drift in the operation column of token_usage
// rows. Convert to string with string(op) when persisting.
type Operation string

const (
	OperationFactExtraction         Operation = "fact_extraction"
	OperationEntityExtraction       Operation = "entity_extraction"
	OperationRelationshipExtraction Operation = "relationship_extraction"
	OperationEmbedding              Operation = "embedding"
	OperationConsolidation          Operation = "consolidation"
	OperationConsolidationAlignment Operation = "consolidation_alignment"
	OperationConsolidationAudit     Operation = "consolidation_audit"
	OperationContradictionCheck     Operation = "contradiction_check"
	OperationIngestionDecision      Operation = "ingestion_decision"
	OperationProbe                  Operation = "system_probe"
	OperationUnknown                Operation = "unknown"

	// Dream-cycle operations. Kept distinct from the consolidation/
	// contradiction-check generics so analytics can drill into the specific
	// stage of the dream pipeline that consumed tokens.
	OperationDreamSynthesis            Operation = "dream_synthesis"
	OperationDreamAlignmentScoring     Operation = "dream_alignment_scoring"
	OperationDreamContradiction        Operation = "dream_contradiction"
	OperationDreamContradictionEmbed   Operation = "dream_contradiction_embedding"
	OperationDreamNoveltyEmbedding     Operation = "dream_novelty_embedding"
	OperationDreamNoveltyAudit         Operation = "dream_novelty_audit"
	OperationDreamNoveltyBackfill      Operation = "dream_novelty_backfill"
)

type ctxKey int

const (
	ctxKeyOperation ctxKey = iota
	ctxKeyRequestID
	ctxKeyUsageContext
	ctxKeyMemoryID
	ctxKeyNamespaceID
	ctxKeyAPIKeyID
)

// WithOperation stamps the call's operation kind into ctx so the
// UsageRecordingProvider middleware can attribute the token_usage row.
func WithOperation(ctx context.Context, op Operation) context.Context {
	return context.WithValue(ctx, ctxKeyOperation, op)
}

// OperationFromContext returns the operation stamped on ctx and whether one
// was present. Callers should treat absence as a misconfiguration.
func OperationFromContext(ctx context.Context) (Operation, bool) {
	op, ok := ctx.Value(ctxKeyOperation).(Operation)
	return op, ok
}

// WithRequestID stamps an inbound request correlation ID. The HTTP and MCP
// middleware install this for every request so multiple provider calls
// emitted by one request can be tied together in token_usage.request_id.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// RequestIDFromContext returns the request ID stamped on ctx, or "".
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// WithUsageContext stamps an already-resolved UsageContext so the middleware
// can populate org_id/user_id/project_id without a per-call DB lookup.
func WithUsageContext(ctx context.Context, uc *model.UsageContext) context.Context {
	if uc == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyUsageContext, uc)
}

// UsageContextFromContext returns the UsageContext stamped on ctx, or nil.
func UsageContextFromContext(ctx context.Context) *model.UsageContext {
	if v, ok := ctx.Value(ctxKeyUsageContext).(*model.UsageContext); ok {
		return v
	}
	return nil
}

// WithMemoryID stamps the memory the provider call relates to (if any).
func WithMemoryID(ctx context.Context, id uuid.UUID) context.Context {
	if id == uuid.Nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyMemoryID, id)
}

// MemoryIDFromContext returns the memory ID stamped on ctx, or nil.
func MemoryIDFromContext(ctx context.Context) *uuid.UUID {
	if v, ok := ctx.Value(ctxKeyMemoryID).(uuid.UUID); ok && v != uuid.Nil {
		out := v
		return &out
	}
	return nil
}

// WithNamespaceID stamps the namespace the provider call relates to. The
// middleware uses this when UsageContext is absent to fall back to a
// per-namespace resolver lookup.
func WithNamespaceID(ctx context.Context, id uuid.UUID) context.Context {
	if id == uuid.Nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyNamespaceID, id)
}

// NamespaceIDFromContext returns the namespace ID stamped on ctx, or
// uuid.Nil if none.
func NamespaceIDFromContext(ctx context.Context) uuid.UUID {
	if v, ok := ctx.Value(ctxKeyNamespaceID).(uuid.UUID); ok {
		return v
	}
	return uuid.Nil
}

// WithAPIKeyID stamps the API key whose request triggered the provider
// call. The middleware writes it to token_usage.api_key_id so analytics
// can attribute usage to specific keys.
func WithAPIKeyID(ctx context.Context, id *uuid.UUID) context.Context {
	if id == nil || *id == uuid.Nil {
		return ctx
	}
	v := *id
	return context.WithValue(ctx, ctxKeyAPIKeyID, v)
}

// APIKeyIDFromContext returns the API key ID stamped on ctx, or nil.
func APIKeyIDFromContext(ctx context.Context) *uuid.UUID {
	if v, ok := ctx.Value(ctxKeyAPIKeyID).(uuid.UUID); ok && v != uuid.Nil {
		out := v
		return &out
	}
	return nil
}
