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

	query := `INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context)
		VALUES (?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context)
			VALUES ($1, $2, $3, $4, $5, $6)`
	}

	_, err := r.db.Exec(ctx, query,
		lineage.ID.String(), lineage.NamespaceID.String(), lineage.MemoryID.String(), parentID,
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
func (r *MemoryLineageRepo) ListByMemory(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error) {
	query := selectLineageColumns + ` FROM memory_lineage
		WHERE namespace_id = ? AND (memory_id = ? OR parent_id = ?)
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectLineageColumns + ` FROM memory_lineage
			WHERE namespace_id = $1 AND (memory_id = $2 OR parent_id = $3)
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), memoryID.String(), memoryID.String())
	if err != nil {
		return nil, fmt.Errorf("memory lineage list by memory: %w", err)
	}
	defer rows.Close()

	return r.scanLineages(rows)
}

// FindConflicts returns lineage records with relation = 'conflicts_with'
// involving the given memory (as either memory_id or parent_id).
func (r *MemoryLineageRepo) FindConflicts(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error) {
	query := selectLineageColumns + ` FROM memory_lineage
		WHERE namespace_id = ? AND relation = ? AND (memory_id = ? OR parent_id = ?)
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectLineageColumns + ` FROM memory_lineage
			WHERE namespace_id = $1 AND relation = $2 AND (memory_id = $3 OR parent_id = $4)
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), model.LineageConflictsWith, memoryID.String(), memoryID.String())
	if err != nil {
		return nil, fmt.Errorf("memory lineage find conflicts: %w", err)
	}
	defer rows.Close()

	return r.scanLineages(rows)
}

// CountConflictsBetween counts conflicts_with edges between the two memories
// in either direction (a→b or b→a). Used by the contradiction phase to
// diminish the haircut applied on reaffirmation: the Nth detection of the
// same pair receives a smaller penalty than the first.
func (r *MemoryLineageRepo) CountConflictsBetween(ctx context.Context, namespaceID, aID, bID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM memory_lineage
		WHERE namespace_id = ? AND relation = ?
		AND ((memory_id = ? AND parent_id = ?) OR (memory_id = ? AND parent_id = ?))`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM memory_lineage
			WHERE namespace_id = $1 AND relation = $2
			AND ((memory_id = $3 AND parent_id = $4) OR (memory_id = $5 AND parent_id = $6))`
	}

	var n int
	err := r.db.QueryRow(ctx, query,
		namespaceID.String(), model.LineageConflictsWith,
		aID.String(), bID.String(),
		bID.String(), aID.String(),
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("memory lineage count conflicts between: %w", err)
	}
	return n, nil
}

// FindChildIDsByRelation returns the memory IDs of direct children of the
// given parent memory, restricted to the supplied relations. An empty
// relations slice returns no rows (avoids generating an invalid IN ()).
func (r *MemoryLineageRepo) FindChildIDsByRelation(ctx context.Context, namespaceID uuid.UUID, parentID uuid.UUID, relations []string) ([]uuid.UUID, error) {
	if len(relations) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(relations))
	args := make([]any, 0, 2+len(relations))
	args = append(args, namespaceID.String(), parentID.String())
	for i, rel := range relations {
		args = append(args, rel)
		if r.db.Backend() == BackendPostgres {
			placeholders[i] = fmt.Sprintf("$%d", i+3)
		} else {
			placeholders[i] = "?"
		}
	}

	nsPH, parentPH := "?", "?"
	if r.db.Backend() == BackendPostgres {
		nsPH, parentPH = "$1", "$2"
	}
	query := fmt.Sprintf(
		`SELECT memory_id FROM memory_lineage WHERE namespace_id = %s AND parent_id = %s AND relation IN (%s)`,
		nsPH, parentPH, strings.Join(placeholders, ","),
	)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("lineage find child ids by relation: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("lineage find child ids by relation scan: %w", err)
		}
		id, perr := uuid.Parse(idStr)
		if perr != nil {
			return nil, fmt.Errorf("lineage find child ids by relation parse: %w", perr)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lineage find child ids by relation iteration: %w", err)
	}
	return ids, nil
}

// FindParentIDs returns a map of memory_id → parent_id for all given memory IDs
// that have a parent in the lineage table. Uses a single batch query.
func (r *MemoryLineageRepo) FindParentIDs(ctx context.Context, namespaceID uuid.UUID, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	if len(memoryIDs) == 0 {
		return nil, nil
	}

	// Build IN clause with appropriate placeholders.
	placeholders := make([]string, len(memoryIDs))
	args := make([]any, 1+len(memoryIDs))
	args[0] = namespaceID.String()
	for i, id := range memoryIDs {
		args[i+1] = id.String()
		if r.db.Backend() == BackendPostgres {
			placeholders[i] = fmt.Sprintf("$%d", i+2)
		} else {
			placeholders[i] = "?"
		}
	}

	var query string
	if r.db.Backend() == BackendPostgres {
		query = fmt.Sprintf(
			`SELECT memory_id, parent_id FROM memory_lineage WHERE namespace_id = $1 AND memory_id IN (%s) AND parent_id IS NOT NULL`,
			strings.Join(placeholders, ", "),
		)
	} else {
		query = fmt.Sprintf(
			`SELECT memory_id, parent_id FROM memory_lineage WHERE namespace_id = ? AND memory_id IN (%s) AND parent_id IS NOT NULL`,
			strings.Join(placeholders, ", "),
		)
	}

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
func (r *MemoryLineageRepo) DeleteByMemoryID(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) error {
	query := `DELETE FROM memory_lineage WHERE namespace_id = ? AND (memory_id = ? OR parent_id = ?)`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memory_lineage WHERE namespace_id = $1 AND (memory_id = $2 OR parent_id = $3)`
	}
	_, err := r.db.Exec(ctx, query, namespaceID.String(), memoryID.String(), memoryID.String())
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

const selectLineageColumns = `SELECT id, namespace_id, memory_id, parent_id, relation, context, created_at`

func (r *MemoryLineageRepo) scanLineage(row *sql.Row) (*model.MemoryLineage, error) {
	var lineage model.MemoryLineage
	var idStr, namespaceIDStr, memoryIDStr string
	var parentIDStr sql.NullString
	var contextStr string
	var createdAtStr string

	err := row.Scan(
		&idStr, &namespaceIDStr, &memoryIDStr, &parentIDStr, &lineage.Relation,
		&contextStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateLineage(&lineage, idStr, namespaceIDStr, memoryIDStr, parentIDStr, contextStr, createdAtStr)
}

func (r *MemoryLineageRepo) scanLineageFromRows(rows *sql.Rows) (*model.MemoryLineage, error) {
	var lineage model.MemoryLineage
	var idStr, namespaceIDStr, memoryIDStr string
	var parentIDStr sql.NullString
	var contextStr string
	var createdAtStr string

	err := rows.Scan(
		&idStr, &namespaceIDStr, &memoryIDStr, &parentIDStr, &lineage.Relation,
		&contextStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory lineage scan rows: %w", err)
	}

	return r.populateLineage(&lineage, idStr, namespaceIDStr, memoryIDStr, parentIDStr, contextStr, createdAtStr)
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
	idStr, namespaceIDStr, memoryIDStr string,
	parentIDStr sql.NullString,
	contextStr, createdAtStr string,
) (*model.MemoryLineage, error) {
	var err error

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("memory lineage parse id: %w", err)
	}
	lineage.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("memory lineage parse namespace_id: %w", err)
	}
	lineage.NamespaceID = nsID

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
