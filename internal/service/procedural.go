package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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

// proceduralExportVersion is the schema version stamped on exported payloads.
const proceduralExportVersion = "1.0"

// ProceduralExportEntry is one entry in an export/import payload. It carries the
// originating id so a user re-importing their own export updates in place; an id
// that does not belong to the importing namespace is treated as a new entry (see
// Import).
type ProceduralExportEntry struct {
	ID        uuid.UUID       `json:"id"`
	Content   string          `json:"content"`
	Title     string          `json:"title"`
	Category  string          `json:"category"`
	Tags      []string        `json:"tags"`
	Priority  int             `json:"priority"`
	Enabled   bool            `json:"enabled"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// ProceduralExportData is the JSON envelope returned by Export and accepted by
// Import. Shaped after service.ExportData for consistency with the memory
// export.
type ProceduralExportData struct {
	Version    string                  `json:"version"`
	ExportedAt time.Time               `json:"exported_at"`
	Entries    []ProceduralExportEntry `json:"entries"`
	Stats      ProceduralExportStats   `json:"stats"`
}

// ProceduralExportStats holds aggregate counts for an export.
type ProceduralExportStats struct {
	Count int `json:"count"`
}

// ProceduralImportResult summarizes the outcome of an import.
type ProceduralImportResult struct {
	Imported int                   `json:"imported"` // new rows created
	Updated  int                   `json:"updated"`  // own rows updated in place
	Skipped  int                   `json:"skipped"`
	Errors   []ProceduralImportErr `json:"errors"`
}

// ProceduralImportErr describes a per-entry failure during import.
type ProceduralImportErr struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

// Export returns every live entry in the namespace (enabled and disabled),
// ordered by priority then recency, wrapped in a versioned envelope.
func (s *ProceduralService) Export(ctx context.Context, namespaceID uuid.UUID) (*ProceduralExportData, error) {
	all, err := s.repo.ListByNamespace(ctx, namespaceID)
	if err != nil {
		return nil, fmt.Errorf("procedural export: %w", err)
	}
	entries := make([]ProceduralExportEntry, 0, len(all))
	for _, e := range all {
		tags := e.Tags
		if tags == nil {
			tags = []string{}
		}
		entries = append(entries, ProceduralExportEntry{
			ID:        e.ID,
			Content:   e.Content,
			Title:     e.Title,
			Category:  e.Category,
			Tags:      tags,
			Priority:  e.Priority,
			Enabled:   e.Enabled,
			Metadata:  e.Metadata,
			CreatedAt: e.CreatedAt,
		})
	}
	return &ProceduralExportData{
		Version:    proceduralExportVersion,
		ExportedAt: time.Now(),
		Entries:    entries,
		Stats:      ProceduralExportStats{Count: len(entries)},
	}, nil
}

// Import upserts entries into the namespace, keyed on ownership rather than the
// global id. An incoming id that resolves to a live row in this namespace is
// updated in place; any other id (foreign, soft-deleted, or absent) becomes a
// new row with a server-generated id. This is required because
// procedural_entries.id is a global primary key: reusing an incoming id across
// namespaces would collide. Per-entry failures are recorded and skipped; the
// import never aborts on a single bad entry.
func (s *ProceduralService) Import(ctx context.Context, namespaceID uuid.UUID, entries []ProceduralExportEntry) (*ProceduralImportResult, error) {
	result := &ProceduralImportResult{Errors: []ProceduralImportErr{}}
	for i, in := range entries {
		if strings.TrimSpace(in.Content) == "" {
			result.Skipped++
			result.Errors = append(result.Errors, ProceduralImportErr{Index: i, Message: "content is required"})
			continue
		}

		// Update in place only when the id belongs to this namespace.
		if in.ID != uuid.Nil {
			existing, err := s.Get(ctx, in.ID, namespaceID)
			switch {
			case err == nil:
				existing.Content = in.Content
				existing.Title = in.Title
				existing.Category = in.Category
				existing.Tags = in.Tags
				existing.Priority = in.Priority
				existing.Enabled = in.Enabled
				if in.Metadata != nil {
					existing.Metadata = in.Metadata
				}
				if _, uerr := s.Update(ctx, existing); uerr != nil {
					result.Skipped++
					result.Errors = append(result.Errors, ProceduralImportErr{Index: i, Message: uerr.Error()})
					continue
				}
				result.Updated++
				continue
			case errors.Is(err, sql.ErrNoRows):
				// Not ours: fall through to create with a fresh id.
			default:
				result.Skipped++
				result.Errors = append(result.Errors, ProceduralImportErr{Index: i, Message: err.Error()})
				continue
			}
		}

		entry := &model.ProceduralEntry{
			NamespaceID: namespaceID,
			Content:     in.Content,
			Title:       in.Title,
			Category:    in.Category,
			Tags:        in.Tags,
			Priority:    in.Priority,
			Enabled:     in.Enabled,
			Origin:      string(model.OriginImport),
			Metadata:    in.Metadata,
		}
		if _, cerr := s.Create(ctx, entry); cerr != nil {
			result.Skipped++
			result.Errors = append(result.Errors, ProceduralImportErr{Index: i, Message: cerr.Error()})
			continue
		}
		result.Imported++
	}
	return result, nil
}
