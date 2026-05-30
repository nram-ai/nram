package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/recallview"
	"github.com/nram-ai/nram/internal/service"
)

// recallResponseBody is the REST recall wire shape. Per-memory it serializes
// recallview.Memory, the same struct the MCP recall tool emits, so a recalled
// memory is byte-identical across both transports. The envelope keeps REST's
// total_searched and its native graph shape (per-memory symmetry, not whole-
// envelope symmetry).
type recallResponseBody struct {
	Memories      []recallview.Memory   `json:"memories"`
	Graph         service.RecallGraph   `json:"graph"`
	TotalSearched int                   `json:"total_searched"`
	LatencyMs     int64                 `json:"latency_ms"`
	CoverageGaps  []service.CoverageGap `json:"coverage_gaps,omitempty"`
}

// buildRecallResponseBody projects the internal service result into the slim
// REST shape, stripping per-row audit bookkeeping and hoisting the decision
// signals. opts carries the include_low_novelty request flag through to the
// projection (it controls whether low_novelty_reason survives in residual
// metadata); the low_novelty bool itself is always emitted.
func buildRecallResponseBody(resp *service.RecallResponse, opts recallview.Options) recallResponseBody {
	memories := make([]recallview.Memory, 0, len(resp.Memories))
	for _, m := range resp.Memories {
		memories = append(memories, recallview.Project(m, opts))
	}
	return recallResponseBody{
		Memories:      memories,
		Graph:         resp.Graph,
		TotalSearched: resp.TotalSearched,
		LatencyMs:     resp.LatencyMs,
		CoverageGaps:  resp.CoverageGaps,
	}
}

// UserReader provides read access to user records for user-scoped handlers.
type UserReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// recallRequestBody represents the JSON body for recall endpoints.
type recallRequestBody struct {
	Query                   string   `json:"query"`
	Limit                   int      `json:"limit"`
	Threshold               float64  `json:"threshold"`
	SimilarityThreshold     float64  `json:"similarity_threshold"`
	SimilarityThresholdMode string   `json:"similarity_threshold_mode"`
	Tags                    []string `json:"tags"`
	IncludeGraph            bool     `json:"include_graph"`
	GraphDepth              int      `json:"graph_depth"`
	IncludeLowNovelty       bool     `json:"include_low_novelty"`
	DiversifyByTagPrefix    string   `json:"diversify_by_tag_prefix"`
}

// RecallServicer defines the interface for recall operations, allowing mocking in tests.
type RecallServicer interface {
	Recall(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error)
}

// mapRecallError converts a service error to an appropriate API error response.
func mapRecallError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "query is required"),
		strings.Contains(msg, "project_id is required"),
		strings.Contains(msg, "invalid similarity_threshold_mode"),
		strings.Contains(msg, "invalid similarity_threshold "),
		strings.Contains(msg, "requires recall.fusion.enabled"):
		WriteError(w, ErrBadRequest(msg))
	case strings.Contains(msg, "not found"):
		WriteError(w, ErrNotFound(msg))
	default:
		WriteError(w, ErrInternal(msg))
	}
}

// NewRecallHandler returns an http.HandlerFunc for project-scoped memory recall.
// It expects project_id as a URL parameter.
func NewRecallHandler(svc RecallServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		var body recallRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		if strings.TrimSpace(body.Query) == "" {
			WriteError(w, ErrBadRequest("query is required"))
			return
		}

		req := &service.RecallRequest{
			ProjectID:               projectID,
			Query:                   body.Query,
			Limit:                   body.Limit,
			Threshold:               body.Threshold,
			SimilarityThreshold:     body.SimilarityThreshold,
			SimilarityThresholdMode: body.SimilarityThresholdMode,
			Tags:                    body.Tags,
			IncludeGraph:            body.IncludeGraph,
			GraphDepth:              body.GraphDepth,
			IncludeLowNovelty:       body.IncludeLowNovelty,
			DiversifyByTagPrefix:    body.DiversifyByTagPrefix,
		}

		if ac := auth.FromContext(r.Context()); ac != nil {
			uid := ac.UserID
			req.UserID = &uid
			req.APIKeyID = ac.APIKeyID
		}

		resp, err := svc.Recall(r.Context(), req)
		if err != nil {
			mapRecallError(w, err)
			return
		}

		writeJSON(w, http.StatusOK, buildRecallResponseBody(resp, recallview.Options{IncludeLowNovelty: body.IncludeLowNovelty}))
	}
}

// NewMeRecallHandler returns an http.HandlerFunc for user-scoped memory recall.
// It looks up the authenticated user's namespace and searches across all projects.
func NewMeRecallHandler(svc RecallServicer, users UserReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body recallRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		if strings.TrimSpace(body.Query) == "" {
			WriteError(w, ErrBadRequest("query is required"))
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrNotFound("user not found"))
			return
		}

		req := &service.RecallRequest{
			Query:                   body.Query,
			Limit:                   body.Limit,
			Threshold:               body.Threshold,
			SimilarityThreshold:     body.SimilarityThreshold,
			SimilarityThresholdMode: body.SimilarityThresholdMode,
			Tags:                    body.Tags,
			IncludeGraph:            body.IncludeGraph,
			GraphDepth:              body.GraphDepth,
			IncludeLowNovelty:       body.IncludeLowNovelty,
			DiversifyByTagPrefix:    body.DiversifyByTagPrefix,
			NamespaceID:             &user.NamespaceID,
		}

		uid := ac.UserID
		req.UserID = &uid
		req.APIKeyID = ac.APIKeyID

		resp, err := svc.Recall(r.Context(), req)
		if err != nil {
			mapRecallError(w, err)
			return
		}

		writeJSON(w, http.StatusOK, buildRecallResponseBody(resp, recallview.Options{IncludeLowNovelty: body.IncludeLowNovelty}))
	}
}
