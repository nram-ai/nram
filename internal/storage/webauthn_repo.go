package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

var ErrWebAuthnCredentialNotFound = errors.New("webauthn credential not found")

type WebAuthnRepo struct {
	db DB
}

func NewWebAuthnRepo(db DB) *WebAuthnRepo {
	return &WebAuthnRepo{db: db}
}

// Create stores a new WebAuthn credential.
func (r *WebAuthnRepo) Create(ctx context.Context, cred *model.WebAuthnCredential) error {
	if cred.ID == uuid.Nil {
		cred.ID = uuid.New()
	}

	transportsVal := encodeStringArray(r.db.Backend(), cred.Transports)

	query := `INSERT INTO webauthn_credentials
		(id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
		 user_verified, backup_eligible, backup_state, attestation_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO webauthn_credentials
			(id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
			 user_verified, backup_eligible, backup_state, attestation_type, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	}

	now := time.Now().UTC().Format(time.RFC3339)

	_, err := r.db.Exec(ctx, query,
		cred.ID.String(), cred.UserID.String(), cred.Name, cred.CredentialID,
		cred.PublicKey, cred.AAGUID, cred.SignCount, transportsVal,
		encodeBool(r.db.Backend(), cred.UserVerified),
		encodeBool(r.db.Backend(), cred.BackupEligible),
		encodeBool(r.db.Backend(), cred.BackupState),
		cred.AttestationType, now,
	)
	if err != nil {
		return fmt.Errorf("webauthn credential create: %w", err)
	}

	return r.reload(ctx, cred)
}

// ListByUser returns all credentials for a user, ordered by creation time descending.
func (r *WebAuthnRepo) ListByUser(ctx context.Context, userID uuid.UUID) ([]model.WebAuthnCredential, error) {
	query := `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
		user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
		FROM webauthn_credentials WHERE user_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
			user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
			FROM webauthn_credentials WHERE user_id = $1 ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, userID.String())
	if err != nil {
		return nil, fmt.Errorf("webauthn credential list by user: %w", err)
	}
	defer rows.Close()

	result := []model.WebAuthnCredential{}
	for rows.Next() {
		cred, err := r.scanFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *cred)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webauthn credential list by user iteration: %w", err)
	}
	return result, nil
}

// GetByCredentialID looks up a credential by its WebAuthn credential ID (base64url).
func (r *WebAuthnRepo) GetByCredentialID(ctx context.Context, credentialID string) (*model.WebAuthnCredential, error) {
	query := `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
		user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
		FROM webauthn_credentials WHERE credential_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
			user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
			FROM webauthn_credentials WHERE credential_id = $1`
	}

	row := r.db.QueryRow(ctx, query, credentialID)
	return r.scanFromRow(row)
}

// GetByID looks up a credential by its UUID.
func (r *WebAuthnRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.WebAuthnCredential, error) {
	query := `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
		user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
		FROM webauthn_credentials WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, user_id, name, credential_id, public_key, aaguid, sign_count, transports,
			user_verified, backup_eligible, backup_state, attestation_type, created_at, last_used_at
			FROM webauthn_credentials WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanFromRow(row)
}

// UpdateSignCount updates the sign count after a successful authentication.
func (r *WebAuthnRepo) UpdateSignCount(ctx context.Context, id uuid.UUID, signCount uint32) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE webauthn_credentials SET sign_count = ?, last_used_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE webauthn_credentials SET sign_count = $1, last_used_at = $2 WHERE id = $3`
	}

	result, err := r.db.Exec(ctx, query, signCount, now, id.String())
	if err != nil {
		return fmt.Errorf("webauthn credential update sign count: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("webauthn credential update sign count rows affected: %w", err)
	}
	if rows == 0 {
		return ErrWebAuthnCredentialNotFound
	}
	return nil
}

// Delete removes a credential by ID.
func (r *WebAuthnRepo) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM webauthn_credentials WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM webauthn_credentials WHERE id = $1`
	}

	result, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("webauthn credential delete: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("webauthn credential delete rows affected: %w", err)
	}
	if rows == 0 {
		return ErrWebAuthnCredentialNotFound
	}
	return nil
}

// HasCredentials returns true if the user has at least one WebAuthn credential registered.
func (r *WebAuthnRepo) HasCredentials(ctx context.Context, userID uuid.UUID) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM webauthn_credentials WHERE user_id = ?)`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT EXISTS(SELECT 1 FROM webauthn_credentials WHERE user_id = $1)`
	}

	var raw interface{}
	row := r.db.QueryRow(ctx, query, userID.String())
	if err := row.Scan(&raw); err != nil {
		return false, fmt.Errorf("webauthn credential has credentials: %w", err)
	}
	return decodeBoolVal(raw), nil
}

func (r *WebAuthnRepo) reload(ctx context.Context, cred *model.WebAuthnCredential) error {
	fetched, err := r.GetByID(ctx, cred.ID)
	if err != nil {
		return fmt.Errorf("webauthn credential reload: %w", err)
	}
	*cred = *fetched
	return nil
}

type webauthnScanner interface {
	Scan(dest ...interface{}) error
}

type webauthnScanResult struct {
	idStr, userIDStr, transportsStr, createdAtStr string
	lastUsedAtStr                                  sql.NullString
	userVerified, backupEligible, backupState       interface{}
}

func (r *WebAuthnRepo) scanFromRow(row *sql.Row) (*model.WebAuthnCredential, error) {
	cred, err := r.scanFrom(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrWebAuthnCredentialNotFound
		}
		return nil, fmt.Errorf("webauthn credential scan: %w", err)
	}
	return cred, nil
}

func (r *WebAuthnRepo) scanFromRows(rows *sql.Rows) (*model.WebAuthnCredential, error) {
	cred, err := r.scanFrom(rows)
	if err != nil {
		return nil, fmt.Errorf("webauthn credential scan rows: %w", err)
	}
	return cred, nil
}

func (r *WebAuthnRepo) scanFrom(s webauthnScanner) (*model.WebAuthnCredential, error) {
	var cred model.WebAuthnCredential
	var sr webauthnScanResult

	err := s.Scan(
		&sr.idStr, &sr.userIDStr, &cred.Name, &cred.CredentialID,
		&cred.PublicKey, &cred.AAGUID, &cred.SignCount, &sr.transportsStr,
		&sr.userVerified, &sr.backupEligible, &sr.backupState,
		&cred.AttestationType, &sr.createdAtStr, &sr.lastUsedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populate(&cred, &sr)
}

func (r *WebAuthnRepo) populate(cred *model.WebAuthnCredential, sr *webauthnScanResult) (*model.WebAuthnCredential, error) {
	id, err := uuid.Parse(sr.idStr)
	if err != nil {
		return nil, fmt.Errorf("webauthn credential parse id: %w", err)
	}
	cred.ID = id

	userID, err := uuid.Parse(sr.userIDStr)
	if err != nil {
		return nil, fmt.Errorf("webauthn credential parse user_id: %w", err)
	}
	cred.UserID = userID

	transports, err := decodeStringArray(r.db.Backend(), sr.transportsStr)
	if err != nil {
		return nil, fmt.Errorf("webauthn credential parse transports: %w", err)
	}
	cred.Transports = transports

	cred.CreatedAt, err = time.Parse(time.RFC3339, sr.createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("webauthn credential parse created_at: %w", err)
	}

	if sr.lastUsedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, sr.lastUsedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("webauthn credential parse last_used_at: %w", err)
		}
		cred.LastUsedAt = &t
	}

	cred.UserVerified = decodeBoolVal(sr.userVerified)
	cred.BackupEligible = decodeBoolVal(sr.backupEligible)
	cred.BackupState = decodeBoolVal(sr.backupState)

	return cred, nil
}

