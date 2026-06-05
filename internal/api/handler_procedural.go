package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// ProceduralServicer is the procedural tier surface needed by the self-service
// REST handlers. *service.ProceduralService satisfies it.
type ProceduralServicer interface {
	List(ctx context.Context, namespaceID uuid.UUID) ([]model.ProceduralEntry, error)
	Get(ctx context.Context, id, namespaceID uuid.UUID) (*model.ProceduralEntry, error)
	Create(ctx context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error)
	Update(ctx context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error)
	Delete(ctx context.Context, id, namespaceID uuid.UUID) error
}

// createProceduralRequest is the JSON body for POST /v1/me/procedural.
type createProceduralRequest struct {
	Content  string   `json:"content"`
	Title    string   `json:"title"`
	Category string   `json:"category"`
	Tags     []string `json:"tags"`
	Priority int      `json:"priority"`
	Enabled  *bool    `json:"enabled"`
}

// updateProceduralRequest is the JSON body for PUT /v1/me/procedural/{id}.
// Pointer fields distinguish "absent" from "set to zero value" so updates are
// partial. Tags nil means unchanged; an empty array clears them.
type updateProceduralRequest struct {
	Content  *string  `json:"content"`
	Title    *string  `json:"title"`
	Category *string  `json:"category"`
	Tags     []string `json:"tags"`
	Priority *int     `json:"priority"`
	Enabled  *bool    `json:"enabled"`
}

// NewMeProceduralHandler handles GET (list) and POST (create) on
// /v1/me/procedural.
func NewMeProceduralHandler(proc ProceduralServicer, users UserGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListProcedural(w, r, proc, users)
		case http.MethodPost:
			handleCreateProcedural(w, r, proc, users)
		default:
			w.Header().Set("Allow", "GET, POST")
			WriteError(w, &APIError{Code: "method_not_allowed", Message: "method not allowed", Status: http.StatusMethodNotAllowed})
		}
	}
}

func handleListProcedural(w http.ResponseWriter, r *http.Request, proc ProceduralServicer, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}
	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	limit := parseIntParam(r, "limit", 200)
	offset := parseIntParam(r, "offset", 0)
	if limit < 0 {
		limit = 200
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	all, err := proc.List(r.Context(), user.NamespaceID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list procedural entries"))
		return
	}
	total := len(all)
	start := min(offset, total)
	end := min(start+limit, total)
	page := all[start:end]

	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.ProceduralEntry]{
		Data: page,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
}

func handleCreateProcedural(w http.ResponseWriter, r *http.Request, proc ProceduralServicer, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}
	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	var body createProceduralRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}
	if strings.TrimSpace(body.Content) == "" {
		WriteError(w, ErrBadRequest("content is required"))
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}

	entry := &model.ProceduralEntry{
		NamespaceID: user.NamespaceID,
		Content:     body.Content,
		Title:       body.Title,
		Category:    body.Category,
		Tags:        body.Tags,
		Priority:    body.Priority,
		Enabled:     enabled,
		Origin:      string(model.OriginUser),
	}
	saved, err := proc.Create(r.Context(), entry)
	if err != nil {
		WriteError(w, ErrInternal("failed to create procedural entry"))
		return
	}
	writeJSON(w, http.StatusCreated, saved)
}

// NewMeProceduralItemHandler handles GET and PUT on /v1/me/procedural/{id}.
func NewMeProceduralItemHandler(proc ProceduralServicer, users UserGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetProcedural(w, r, proc, users)
		case http.MethodPut:
			handleUpdateProcedural(w, r, proc, users)
		default:
			w.Header().Set("Allow", "GET, PUT")
			WriteError(w, &APIError{Code: "method_not_allowed", Message: "method not allowed", Status: http.StatusMethodNotAllowed})
		}
	}
}

func proceduralPathID(r *http.Request) (uuid.UUID, error) {
	idStr := r.PathValue("id")
	if idStr == "" {
		idStr = chi.URLParam(r, "id")
	}
	return uuid.Parse(idStr)
}

func handleGetProcedural(w http.ResponseWriter, r *http.Request, proc ProceduralServicer, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}
	id, err := proceduralPathID(r)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid entry id"))
		return
	}
	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}
	entry, err := proc.Get(r.Context(), id, user.NamespaceID)
	if err != nil {
		WriteError(w, ErrNotFound("procedural entry not found"))
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func handleUpdateProcedural(w http.ResponseWriter, r *http.Request, proc ProceduralServicer, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}
	id, err := proceduralPathID(r)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid entry id"))
		return
	}
	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}
	entry, err := proc.Get(r.Context(), id, user.NamespaceID)
	if err != nil {
		WriteError(w, ErrNotFound("procedural entry not found"))
		return
	}

	var body updateProceduralRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}
	if body.Content != nil {
		if strings.TrimSpace(*body.Content) == "" {
			WriteError(w, ErrBadRequest("content cannot be empty"))
			return
		}
		entry.Content = *body.Content
	}
	if body.Title != nil {
		entry.Title = *body.Title
	}
	if body.Category != nil {
		entry.Category = *body.Category
	}
	if body.Tags != nil {
		entry.Tags = body.Tags
	}
	if body.Priority != nil {
		entry.Priority = *body.Priority
	}
	if body.Enabled != nil {
		entry.Enabled = *body.Enabled
	}

	saved, err := proc.Update(r.Context(), entry)
	if err != nil {
		WriteError(w, ErrInternal("failed to update procedural entry"))
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

// NewMeProceduralDeleteHandler handles DELETE /v1/me/procedural/{id}.
func NewMeProceduralDeleteHandler(proc ProceduralServicer, users UserGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.Header().Set("Allow", "DELETE")
			WriteError(w, &APIError{Code: "method_not_allowed", Message: "method not allowed", Status: http.StatusMethodNotAllowed})
			return
		}
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}
		id, err := proceduralPathID(r)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid entry id"))
			return
		}
		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to resolve user"))
			return
		}
		if err := proc.Delete(r.Context(), id, user.NamespaceID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("procedural entry not found"))
				return
			}
			WriteError(w, ErrInternal("failed to delete procedural entry"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
