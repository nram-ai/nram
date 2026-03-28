package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryLineageRepo provides CRUD operations for the memory_lineage table.
type MemoryLineageRepo struct {
	db DB
}

// NewMemoryLineageRepo creates a new MemoryLineageRepo backed by the given DB.
func NewMemoryLineageRepo(db DB) *MemoryLineageRepo {
	return &MemoryLineageRepo{db: db}
}

// Create inserts a new memory lineage record. ID is generated if zero-valued.
// Context defaults to `{}` if nil.
func (r *MemoryLineageRepo) Create(ctx context.Context, lineage *model.MemoryLineage) error {
	if lineage.ID == uuid.Nil {
		lineage.ID = uuid.New()
	}
	if lineage.Context == nil {
		lineage.Context = json.RawMessage(`{}`)
	}

	var parentID interface{}
	if lineage.ParentID != nil {
		parentID = lineage.ParentID.String()
	}

	query := `INSERT INTO memory_lineage (id, memory_id, parent_id, relation, context)
		VALUES (?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memory_lineage (id, memory_id, parent_id, relation, context)
			VALUES ($1, $2, $3, $4, $5)`
	}

	_, err := r.db.Exec(ctx, query,
		lineage.ID.String(), lineage.MemoryID.String(), parentID,
		lineage.Relation, string(lineage.Context),
	)
	if err != nil {
		return fmt.Errorf("memory lineage create: %w", err)
	}

	return r.reload(ctx, lineage)
}

// GetByID returns a memory lineage record by its UUID.
func (r *MemoryLineageRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.MemoryLineage, error) {
	query := selectLineageColumns + ` FROM memory_lineage WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectLineageColumns + ` FROM memory_lineage WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanLineage(row)
}

// ListByMemory returns all lineage records for a memory, both as parent and
// child, ordered by created_at DESC.
func (r *MemoryLineageRepo) ListByMemory(ctx context.Context, memoryID uuid.UUID) ([]model.MemoryLineage, error) {
	query := selectLineageColumns + ` FROM memory_lineage
		WHERE memory_id = ? OR parent_id = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectLineageColumns + ` FROM memory_lineage
			WHERE memory_id = $1 OR parent_id = $2
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, memoryID.String(), memoryID.String())
	if err != nil {
		return nil, fmt.Errorf("memory lineage list by memory: %w", err)
	}
	defer rows.Close()

	return r.scanLineages(rows)
}

// FindConflicts returns lineage records with relation_type = 'contradicts'
// involving the given memory (as either memory_id or parent_id).
func (r *MemoryLineageRepo) FindConflicts(ctx context.Context, memoryID uuid.UUID) ([]model.MemoryLineage, error) {
	query := selectLineageColumns + ` FROM memory_lineage
		WHERE relation = 'contradicts' AND (memory_id = ? OR parent_id = ?)
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectLineageColumns + ` FROM memory_lineage
			WHERE relation = 'contradicts' AND (memory_id = $1 OR parent_id = $2)
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, memoryID.String(), memoryID.String())
	if err != nil {
		return nil, fmt.Errorf("memory lineage find conflicts: %w", err)
	}
	defer rows.Close()

	return r.scanLineages(rows)
}

// FindChildIDs returns the memory IDs of all direct children of the given
// parent memory.
func (r *MemoryLineageRepo) FindChildIDs(ctx context.Context, parentID uuid.UUID) ([]uuid.UUID, error) {
	query := `SELECT memory_id FROM memory_lineage WHERE parent_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT memory_id FROM memory_lineage WHERE parent_id = $1`
	}
	rows, err := r.db.Query(ctx, query, parentID.String())
	if err != nil {
		return nil, fmt.Errorf("lineage find child ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("lineage find child ids scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("lineage find child ids parse: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lineage find child ids iteration: %w", err)
	}
	return ids, nil
}

// FindParentIDs returns a map of memory_id → parent_id for all given memory IDs
// that have a parent in the lineage table. Uses a single batch query.
func (r *MemoryLineageRepo) FindParentIDs(ctx context.Context, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	if len(memoryIDs) == 0 {
		return nil, nil
	}

	// Build IN clause with appropriate placeholders.
	placeholders := make([]string, len(memoryIDs))
	args := make([]any, len(memoryIDs))
	for i, id := range memoryIDs {
		args[i] = id.String()
		if r.db.Backend() == BackendPostgres {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}

	query := fmt.Sprintf(
		`SELECT memory_id, parent_id FROM memory_lineage WHERE memory_id IN (%s) AND parent_id IS NOT NULL`,
		strings.Join(placeholders, ", "),
	)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("lineage find parent ids: %w", err)
	}
	defer rows.Close()

	result := make(map[uuid.UUID]uuid.UUID)
	for rows.Next() {
		var memIDStr, parentIDStr string
		if err := rows.Scan(&memIDStr, &parentIDStr); err != nil {
			return nil, fmt.Errorf("lineage find parent ids scan: %w", err)
		}
		memID, err := uuid.Parse(memIDStr)
		if err != nil {
			return nil, fmt.Errorf("lineage find parent ids parse memory_id: %w", err)
		}
		parentID, err := uuid.Parse(parentIDStr)
		if err != nil {
			return nil, fmt.Errorf("lineage find parent ids parse parent_id: %w", err)
		}
		result[memID] = parentID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lineage find parent ids iteration: %w", err)
	}

	return result, nil
}

// DeleteByMemoryID removes all lineage records where the given memory is either
// the child (memory_id) or the parent (parent_id).
func (r *MemoryLineageRepo) DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error {
	query := `DELETE FROM memory_lineage WHERE memory_id = ? OR parent_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memory_lineage WHERE memory_id = $1 OR parent_id = $2`
	}
	_, err := r.db.Exec(ctx, query, memoryID.String(), memoryID.String())
	if err != nil {
		return fmt.Errorf("lineage delete by memory: %w", err)
	}
	return nil
}

// reload fetches the lineage by ID and populates the struct in place.
func (r *MemoryLineageRepo) reload(ctx context.Context, lineage *model.MemoryLineage) error {
	fetched, err := r.GetByID(ctx, lineage.ID)
	if err != nil {
		return fmt.Errorf("memory lineage reload: %w", err)
	}
	*lineage = *fetched
	return nil
}

const selectLineageColumns = `SELECT id, memory_id, parent_id, relation, context, created_at`

func (r *MemoryLineageRepo) scanLineage(row *sql.Row) (*model.MemoryLineage, error) {
	var lineage model.MemoryLineage
	var idStr, memoryIDStr string
	var parentIDStr sql.NullString
	var contextStr string
	var createdAtStr string

	err := row.Scan(
		&idStr, &memoryIDStr, &parentIDStr, &lineage.Relation,
		&contextStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateLineage(&lineage, idStr, memoryIDStr, parentIDStr, contextStr, createdAtStr)
}

func (r *MemoryLineageRepo) scanLineageFromRows(rows *sql.Rows) (*model.MemoryLineage, error) {
	var lineage model.MemoryLineage
	var idStr, memoryIDStr string
	var parentIDStr sql.NullString
	var contextStr string
	var createdAtStr string

	err := rows.Scan(
		&idStr, &memoryIDStr, &parentIDStr, &lineage.Relation,
		&contextStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory lineage scan rows: %w", err)
	}

	return r.populateLineage(&lineage, idStr, memoryIDStr, parentIDStr, contextStr, createdAtStr)
}

func (r *MemoryLineageRepo) scanLineages(rows *sql.Rows) ([]model.MemoryLineage, error) {
	result := []model.MemoryLineage{}
	for rows.Next() {
		lineage, err := r.scanLineageFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *lineage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory lineage scan iteration: %w", err)
	}
	return result, nil
}

func (r *MemoryLineageRepo) populateLineage(
	lineage *model.MemoryLineage,
	idStr, memoryIDStr string,
	parentIDStr sql.NullString,
	contextStr, createdAtStr string,
) (*model.MemoryLineage, error) {
	var err error

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("memory lineage parse id: %w", err)
	}
	lineage.ID = id

	memID, err := uuid.Parse(memoryIDStr)
	if err != nil {
		return nil, fmt.Errorf("memory lineage parse memory_id: %w", err)
	}
	lineage.MemoryID = memID

	if parentIDStr.Valid {
		pID, err := uuid.Parse(parentIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory lineage parse parent_id: %w", err)
		}
		lineage.ParentID = &pID
	}

	lineage.Context = json.RawMessage(contextStr)

	lineage.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("memory lineage parse created_at: %w", err)
	}

	return lineage, nil
}
