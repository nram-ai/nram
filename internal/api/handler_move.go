package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// MoveServicer defines the move operation, allowing the service to be mocked in
// tests.
type MoveServicer interface {
	Move(ctx context.Context, req *service.MoveRequest) (*service.MoveResponse, error)
}

// moveRequestBody is the JSON body for both move endpoints. The single-memory
// endpoint takes its id from the URL and ignores `ids`; the bulk endpoint reads
// `ids` from the body.
type moveRequestBody struct {
	IDs             []uuid.UUID `json:"ids"`
	TargetProjectID string      `json:"target_project_id"`
}

// mapMoveError converts a move service error to an appropriate API response.
func mapMoveError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "is required"),
		strings.Contains(msg, "must differ"):
		WriteError(w, ErrBadRequest(msg))
	case strings.Contains(msg, "not found"):
		WriteError(w, ErrNotFound(msg))
	default:
		WriteError(w, ErrInternal(msg))
	}
}

// emitMoveEvents emits a MemoryDeleted (source) and MemoryCreated (destination)
// event for each relocated memory so subscribers see both sides of the move.
func emitMoveEvents(ctx context.Context, bus events.EventBus, sourceProjectID, targetProjectID uuid.UUID, resp *service.MoveResponse) {
	for _, res := range resp.Results {
		events.Emit(ctx, bus, events.MemoryDeleted, "project:"+sourceProjectID.String(), map[string]string{
			"memory_id":  res.OldID.String(),
			"project_id": sourceProjectID.String(),
		})
		events.Emit(ctx, bus, events.MemoryCreated, "project:"+targetProjectID.String(), map[string]string{
			"memory_id":  res.NewID.String(),
			"project_id": targetProjectID.String(),
			"origin":     string(model.OriginUser),
		})
	}
}

// NewMoveHandler returns an http.HandlerFunc for moving a single memory to
// another project owned by the caller.
//
// POST /v1/projects/{project_id}/memories/{id}/move  body: {"target_project_id": "<uuid>"}
//
// The source project_id in the URL is already authorized by ProjectAccessMiddleware.
// The destination project comes from the body and the middleware never sees it, so
// this handler authorizes it explicitly via CheckProjectOrgAccess with the same
// org-ownership rule.
func NewMoveHandler(svc MoveServicer, access ProjectAccessConfig, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sourceID, ok := parseProjectID(w, r)
		if !ok {
			return
		}

		memoryIDStr := chi.URLParam(r, "id")
		memoryID, err := uuid.Parse(memoryIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid memory id: must be a valid UUID"))
			return
		}

		var body moveRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		targetID, ac, ok := authorizeMoveTarget(w, r, access, body.TargetProjectID)
		if !ok {
			return
		}

		executeMove(w, r, svc, bus, sourceID, targetID, ac, []uuid.UUID{memoryID})
	}
}

// NewBulkMoveHandler returns an http.HandlerFunc for moving multiple selected
// memories to another project owned by the caller.
//
// POST /v1/projects/{project_id}/memories/move  body: {"ids": [...], "target_project_id": "<uuid>"}
func NewBulkMoveHandler(svc MoveServicer, access ProjectAccessConfig, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sourceID, ok := parseProjectID(w, r)
		if !ok {
			return
		}

		var body moveRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}
		if len(body.IDs) == 0 {
			WriteError(w, ErrBadRequest("at least one memory id is required"))
			return
		}

		targetID, ac, ok := authorizeMoveTarget(w, r, access, body.TargetProjectID)
		if !ok {
			return
		}

		executeMove(w, r, svc, bus, sourceID, targetID, ac, body.IDs)
	}
}

// executeMove runs the move service for the given memory ids and writes the
// response. Shared by the single and bulk handlers, which differ only in how
// they obtain the ids and target project.
func executeMove(w http.ResponseWriter, r *http.Request, svc MoveServicer, bus events.EventBus, sourceID, targetID uuid.UUID, ac *auth.AuthContext, ids []uuid.UUID) {
	req := &service.MoveRequest{
		SourceProjectID: sourceID,
		TargetProjectID: targetID,
		MemoryIDs:       ids,
	}
	applyMoveCallerIdentity(req, ac)

	resp, err := svc.Move(r.Context(), req)
	if err != nil {
		mapMoveError(w, err)
		return
	}

	emitMoveEvents(r.Context(), bus, sourceID, targetID, resp)
	writeJSON(w, http.StatusOK, resp)
}

// parseProjectID parses the {project_id} URL parameter, writing a 400 and
// returning ok=false on failure.
func parseProjectID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	projectID, err := uuid.Parse(chi.URLParam(r, "project_id"))
	if err != nil {
		WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
		return uuid.Nil, false
	}
	return projectID, true
}

// authorizeMoveTarget parses and authorizes the destination project from the
// request body. It returns the parsed target ID and the caller's auth context.
// On any failure it writes the response and returns ok=false.
func authorizeMoveTarget(w http.ResponseWriter, r *http.Request, access ProjectAccessConfig, targetProjectIDStr string) (uuid.UUID, *auth.AuthContext, bool) {
	targetID, err := uuid.Parse(strings.TrimSpace(targetProjectIDStr))
	if err != nil {
		WriteError(w, ErrBadRequest("invalid target_project_id: must be a valid UUID"))
		return uuid.Nil, nil, false
	}

	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrForbidden("unauthorized"))
		return uuid.Nil, nil, false
	}

	if apiErr := CheckProjectOrgAccess(r.Context(), access, ac, targetID); apiErr != nil {
		WriteError(w, apiErr)
		return uuid.Nil, nil, false
	}

	return targetID, ac, true
}

// applyMoveCallerIdentity copies the caller identity from the auth context onto
// the move request, mirroring the store handler.
func applyMoveCallerIdentity(req *service.MoveRequest, ac *auth.AuthContext) {
	if ac == nil {
		return
	}
	uid := ac.UserID
	req.UserID = &uid
	if ac.OrgID != uuid.Nil {
		oid := ac.OrgID
		req.OrgID = &oid
	}
	req.APIKeyID = ac.APIKeyID
}
