package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

var ErrAPIKeyExpired = errors.New("api key expired")
var ErrAPIKeyNotFound = errors.New("api key not found")

type APIKeyRepo struct {
	db DB
}

func NewAPIKeyRepo(db DB) *APIKeyRepo {
	return &APIKeyRepo{db: db}
}

// Create generates a new API key with the nram_k_ prefix, stores the SHA-256 hash,
// and returns the raw key string. The raw key is only available at creation time.
func (r *APIKeyRepo) Create(ctx context.Context, key *model.APIKey) (string, error) {
	if key.ID == uuid.Nil {
		key.ID = uuid.New()
	}

	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return "", fmt.Errorf("api key create generate: %w", err)
	}
	rawKey := "nram_k_" + hex.EncodeToString(rawBytes)

	hash := sha256.Sum256([]byte(rawKey))
	key.KeyHash = hex.EncodeToString(hash[:])
	key.KeyPrefix = rawKey[:15]

	scopesVal := encodeUUIDArray(r.db.Backend(), key.Scopes)

	var expiresAt interface{}
	if key.ExpiresAt != nil {
		expiresAt = key.ExpiresAt.UTC().Format(time.RFC3339)
	}

	query := `INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`
	}

	_, err := r.db.Exec(ctx, query,
		key.ID.String(), key.UserID.String(), key.KeyPrefix, key.KeyHash,
		key.Name, scopesVal, expiresAt,
	)
	if err != nil {
		return "", fmt.Errorf("api key create: %w", err)
	}

	if err := r.reload(ctx, key); err != nil {
		return "", err
	}

	return rawKey, nil
}

// Validate takes a raw API key string, hashes it, and looks up the matching key.
// Returns ErrAPIKeyNotFound if no match, ErrAPIKeyExpired if expired.
func (r *APIKeyRepo) Validate(ctx context.Context, rawKey string) (*model.APIKey, error) {
	hash := sha256.Sum256([]byte(rawKey))
	keyHash := hex.EncodeToString(hash[:])

	query := `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys WHERE key_hash = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
			FROM api_keys WHERE key_hash = $1`
	}

	row := r.db.QueryRow(ctx, query, keyHash)
	key, err := r.scanKey(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("api key validate: %w", err)
	}

	if key.ExpiresAt != nil && key.ExpiresAt.Before(time.Now().UTC()) {
		return nil, ErrAPIKeyExpired
	}

	// Update last_used
	now := time.Now().UTC().Format(time.RFC3339)
	updateQuery := `UPDATE api_keys SET last_used = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		updateQuery = `UPDATE api_keys SET last_used = $1 WHERE id = $2`
	}
	_, err = r.db.Exec(ctx, updateQuery, now, key.ID.String())
	if err != nil {
		return nil, fmt.Errorf("api key validate update last_used: %w", err)
	}

	t, _ := time.Parse(time.RFC3339, now)
	key.LastUsed = &t

	return key, nil
}

// ListByUser returns all API keys belonging to a user, ordered by creation time descending.
func (r *APIKeyRepo) ListByUser(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error) {
	query := `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys WHERE user_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
			FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, userID.String())
	if err != nil {
		return nil, fmt.Errorf("api key list by user: %w", err)
	}
	defer rows.Close()

	result := []model.APIKey{}
	for rows.Next() {
		key, err := r.scanKeyFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("api key list by user iteration: %w", err)
	}
	return result, nil
}

// CountByUser returns the total number of API keys belonging to a user.
func (r *APIKeyRepo) CountByUser(ctx context.Context, userID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM api_keys WHERE user_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM api_keys WHERE user_id = $1`
	}
	row := r.db.QueryRow(ctx, query, userID.String())
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("api key count by user: %w", err)
	}
	return count, nil
}

// ListByUserPaged returns API keys for a user with LIMIT and OFFSET applied.
func (r *APIKeyRepo) ListByUserPaged(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.APIKey, error) {
	query := `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
			FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, userID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("api key list by user paged: %w", err)
	}
	defer rows.Close()

	result := []model.APIKey{}
	for rows.Next() {
		key, err := r.scanKeyFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("api key list by user paged iteration: %w", err)
	}
	return result, nil
}

// Revoke deletes an API key by ID.
func (r *APIKeyRepo) Revoke(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM api_keys WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM api_keys WHERE id = $1`
	}

	result, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("api key revoke: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("api key revoke rows affected: %w", err)
	}
	if rows == 0 {
		return ErrAPIKeyNotFound
	}
	return nil
}

// CheckExpiry returns all expired API keys for a user.
func (r *APIKeyRepo) CheckExpiry(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys WHERE user_id = ? AND expires_at IS NOT NULL AND expires_at < ?
		ORDER BY expires_at`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
			FROM api_keys WHERE user_id = $1 AND expires_at IS NOT NULL AND expires_at < $2
			ORDER BY expires_at`
	}

	rows, err := r.db.Query(ctx, query, userID.String(), now)
	if err != nil {
		return nil, fmt.Errorf("api key check expiry: %w", err)
	}
	defer rows.Close()

	var result []model.APIKey
	for rows.Next() {
		key, err := r.scanKeyFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("api key check expiry iteration: %w", err)
	}
	return result, nil
}

func (r *APIKeyRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.APIKey, error) {
	query := `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
			FROM api_keys WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanKey(row)
}

func (r *APIKeyRepo) reload(ctx context.Context, key *model.APIKey) error {
	fetched, err := r.GetByID(ctx, key.ID)
	if err != nil {
		return fmt.Errorf("api key reload: %w", err)
	}
	*key = *fetched
	return nil
}

func (r *APIKeyRepo) scanKey(row *sql.Row) (*model.APIKey, error) {
	var key model.APIKey
	var idStr, userIDStr string
	var scopesStr string
	var createdAtStr string
	var lastUsedStr, expiresAtStr sql.NullString

	err := row.Scan(
		&idStr, &userIDStr, &key.KeyPrefix, &key.KeyHash,
		&key.Name, &scopesStr, &lastUsedStr, &expiresAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateKey(&key, idStr, userIDStr, scopesStr, createdAtStr, lastUsedStr, expiresAtStr)
}

func (r *APIKeyRepo) scanKeyFromRows(rows *sql.Rows) (*model.APIKey, error) {
	var key model.APIKey
	var idStr, userIDStr string
	var scopesStr string
	var createdAtStr string
	var lastUsedStr, expiresAtStr sql.NullString

	err := rows.Scan(
		&idStr, &userIDStr, &key.KeyPrefix, &key.KeyHash,
		&key.Name, &scopesStr, &lastUsedStr, &expiresAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("api key scan rows: %w", err)
	}

	return r.populateKey(&key, idStr, userIDStr, scopesStr, createdAtStr, lastUsedStr, expiresAtStr)
}

func (r *APIKeyRepo) populateKey(key *model.APIKey, idStr, userIDStr, scopesStr, createdAtStr string, lastUsedStr, expiresAtStr sql.NullString) (*model.APIKey, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("api key parse id: %w", err)
	}
	key.ID = id

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return nil, fmt.Errorf("api key parse user_id: %w", err)
	}
	key.UserID = userID

	scopes, err := decodeUUIDArray(r.db.Backend(), scopesStr)
	if err != nil {
		return nil, fmt.Errorf("api key parse scopes: %w", err)
	}
	key.Scopes = scopes

	key.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("api key parse created_at: %w", err)
	}

	if lastUsedStr.Valid {
		t, err := time.Parse(time.RFC3339, lastUsedStr.String)
		if err != nil {
			return nil, fmt.Errorf("api key parse last_used: %w", err)
		}
		key.LastUsed = &t
	}

	if expiresAtStr.Valid {
		t, err := time.Parse(time.RFC3339, expiresAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("api key parse expires_at: %w", err)
		}
		key.ExpiresAt = &t
	}

	return key, nil
}
