package service

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ErrEmptyContent is returned when a procedural entry is created or updated
// with blank content.
var ErrEmptyContent = errors.New("procedural content is required")

// ProceduralRepository is the persistence contract for the procedural tier.
// It deliberately exposes no embedding, enrichment, or consolidation hooks:
// that absence is what guarantees procedural content stays verbatim.
type ProceduralRepository interface {
	Create(ctx context.Context, e *model.ProceduralEntry) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.ProceduralEntry, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.ProceduralEntry, error)
	Update(ctx context.Context, e *model.ProceduralEntry) error
	Delete(ctx context.Context, id, namespaceID uuid.UUID) error
}

// ProceduralService is thin CRUD over the procedural tier plus the no-query
// fetch. It holds NO enrichment queue, embedder, or dream dependency by design.
type ProceduralService struct {
	repo ProceduralRepository
}

// NewProceduralService constructs a ProceduralService.
func NewProceduralService(repo ProceduralRepository) *ProceduralService {
	return &ProceduralService{repo: repo}
}

// List returns every live entry in the namespace, ordered by priority then
// recency. Used by the management surfaces (REST, UI): includes disabled
// entries so they remain manageable.
func (s *ProceduralService) List(ctx context.Context, namespaceID uuid.UUID) ([]model.ProceduralEntry, error) {
	return s.repo.ListByNamespace(ctx, namespaceID)
}

// FetchActive returns only the enabled entries, ordered. This is the no-query
// fetch backing procedural_fetch: disabled entries are parked and omitted from
// the result.
func (s *ProceduralService) FetchActive(ctx context.Context, namespaceID uuid.UUID) ([]model.ProceduralEntry, error) {
	all, err := s.repo.ListByNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	active := make([]model.ProceduralEntry, 0, len(all))
	for _, e := range all {
		if e.Enabled {
			active = append(active, e)
		}
	}
	return active, nil
}

// Get returns a single entry scoped to the namespace. A foreign or missing
// entry reads as sql.ErrNoRows (existence is not leaked across namespaces).
func (s *ProceduralService) Get(ctx context.Context, id, namespaceID uuid.UUID) (*model.ProceduralEntry, error) {
	e, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if e.NamespaceID != namespaceID {
		return nil, sql.ErrNoRows
	}
	return e, nil
}

// Create persists a new entry. NamespaceID must already be set to the owning
// user's root namespace by the caller. Origin defaults to user.
func (s *ProceduralService) Create(ctx context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error) {
	if strings.TrimSpace(e.Content) == "" {
		return nil, ErrEmptyContent
	}
	if e.Origin == "" {
		e.Origin = string(model.OriginUser)
	}
	if err := s.repo.Create(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

// Update mutates an existing entry. The entry's ID and NamespaceID identify the
// row; the repo update is scoped by both, so a caller cannot touch another
// namespace's entry.
func (s *ProceduralService) Update(ctx context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error) {
	if strings.TrimSpace(e.Content) == "" {
		return nil, ErrEmptyContent
	}
	if err := s.repo.Update(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

// Delete soft-deletes an entry scoped to the namespace.
func (s *ProceduralService) Delete(ctx context.Context, id, namespaceID uuid.UUID) error {
	return s.repo.Delete(ctx, id, namespaceID)
}
