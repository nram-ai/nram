package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// IngestionLogRepo provides CRUD operations for the ingestion_log table.
type IngestionLogRepo struct {
	db DB
}

// NewIngestionLogRepo creates a new IngestionLogRepo backed by the given DB.
func NewIngestionLogRepo(db DB) *IngestionLogRepo {
	return &IngestionLogRepo{db: db}
}

// Create inserts a new ingestion log entry. ID is generated if zero-valued.
// MemoryIDs defaults to `[]` if nil. Error and Metadata default to their
// respective zero JSON values if nil.
func (r *IngestionLogRepo) Create(ctx context.Context, log *model.IngestionLog) error {
	if log.ID == uuid.Nil {
		log.ID = uuid.New()
	}
	if log.MemoryIDs == nil {
		log.MemoryIDs = []uuid.UUID{}
	}
	if log.Error == nil {
		log.Error = json.RawMessage("null")
	}
	if log.Metadata == nil {
		log.Metadata = json.RawMessage("{}")
	}

	memoryIDsJSON, err := json.Marshal(log.MemoryIDs)
	if err != nil {
		return fmt.Errorf("ingestion_log create marshal memory_ids: %w", err)
	}

	query := `INSERT INTO ingestion_log (id, namespace_id, source, content_hash, raw_content, memory_ids, status, error, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO ingestion_log (id, namespace_id, source, content_hash, raw_content, memory_ids, status, error, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err = r.db.Exec(ctx, query,
		log.ID.String(), log.NamespaceID.String(), log.Source, log.ContentHash,
		log.RawContent, string(memoryIDsJSON), log.Status,
		string(log.Error), string(log.Metadata),
	)
	if err != nil {
		return fmt.Errorf("ingestion_log create: %w", err)
	}

	return r.reload(ctx, log)
}

// GetByID returns an ingestion log entry by its UUID.
func (r *IngestionLogRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.IngestionLog, error) {
	query := selectIngestionLogColumns + ` FROM ingestion_log WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectIngestionLogColumns + ` FROM ingestion_log WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanIngestionLog(row)
}

// ListByNamespace returns ingestion log entries for a namespace, ordered by
// created_at DESC with pagination via limit and offset.
func (r *IngestionLogRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.IngestionLog, error) {
	query := selectIngestionLogColumns + ` FROM ingestion_log
		WHERE namespace_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = selectIngestionLogColumns + ` FROM ingestion_log
			WHERE namespace_id = $1
			ORDER BY created_at DESC
			LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ingestion_log list by namespace: %w", err)
	}
	defer rows.Close()

	return r.scanIngestionLogs(rows)
}

// reload fetches the ingestion log by ID and populates the struct in place.
func (r *IngestionLogRepo) reload(ctx context.Context, log *model.IngestionLog) error {
	fetched, err := r.GetByID(ctx, log.ID)
	if err != nil {
		return fmt.Errorf("ingestion_log reload: %w", err)
	}
	*log = *fetched
	return nil
}

const selectIngestionLogColumns = `SELECT id, namespace_id, source, content_hash, raw_content, memory_ids, status, error, metadata, created_at`

func (r *IngestionLogRepo) scanIngestionLog(row *sql.Row) (*model.IngestionLog, error) {
	var log model.IngestionLog
	var idStr, nsIDStr string
	var contentHash sql.NullString
	var memoryIDsStr string
	var errorStr sql.NullString
	var metadataStr string
	var createdAtStr string

	err := row.Scan(
		&idStr, &nsIDStr, &log.Source, &contentHash, &log.RawContent,
		&memoryIDsStr, &log.Status, &errorStr, &metadataStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateIngestionLog(&log, idStr, nsIDStr, contentHash, memoryIDsStr, errorStr, metadataStr, createdAtStr)
}

func (r *IngestionLogRepo) scanIngestionLogFromRows(rows *sql.Rows) (*model.IngestionLog, error) {
	var log model.IngestionLog
	var idStr, nsIDStr string
	var contentHash sql.NullString
	var memoryIDsStr string
	var errorStr sql.NullString
	var metadataStr string
	var createdAtStr string

	err := rows.Scan(
		&idStr, &nsIDStr, &log.Source, &contentHash, &log.RawContent,
		&memoryIDsStr, &log.Status, &errorStr, &metadataStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("ingestion_log scan rows: %w", err)
	}

	return r.populateIngestionLog(&log, idStr, nsIDStr, contentHash, memoryIDsStr, errorStr, metadataStr, createdAtStr)
}

func (r *IngestionLogRepo) scanIngestionLogs(rows *sql.Rows) ([]model.IngestionLog, error) {
	var result []model.IngestionLog
	for rows.Next() {
		log, err := r.scanIngestionLogFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ingestion_log scan iteration: %w", err)
	}
	return result, nil
}

func (r *IngestionLogRepo) populateIngestionLog(
	log *model.IngestionLog,
	idStr, nsIDStr string,
	contentHash sql.NullString,
	memoryIDsStr string,
	errorStr sql.NullString,
	metadataStr, createdAtStr string,
) (*model.IngestionLog, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("ingestion_log parse id: %w", err)
	}
	log.ID = id

	nsID, err := uuid.Parse(nsIDStr)
	if err != nil {
		return nil, fmt.Errorf("ingestion_log parse namespace_id: %w", err)
	}
	log.NamespaceID = nsID

	if contentHash.Valid {
		log.ContentHash = &contentHash.String
	}

	if err := json.Unmarshal([]byte(memoryIDsStr), &log.MemoryIDs); err != nil {
		return nil, fmt.Errorf("ingestion_log parse memory_ids: %w", err)
	}

	if errorStr.Valid {
		log.Error = json.RawMessage(errorStr.String)
	} else {
		log.Error = json.RawMessage("null")
	}

	log.Metadata = json.RawMessage(metadataStr)

	log.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("ingestion_log parse created_at: %w", err)
	}

	return log, nil
}
