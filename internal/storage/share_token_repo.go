package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ShareTokenWirePrefix is the wire-format prefix for share-bearer credentials.
// The middleware dispatch path uses auth.ShareTokenBearerPrefix; both must
// stay in lockstep. The first 8 hex characters after the prefix are also
// stored separately as token_prefix so the owner can identify a share in the
// admin UI without seeing the secret again.
const ShareTokenWirePrefix = "nram_s_"

var (
	ErrShareTokenNotFound = errors.New("share token not found")
	ErrShareTokenExpired  = errors.New("share token expired")
	ErrShareTokenRevoked  = errors.New("share token revoked")
	ErrShareTokenConsumed = errors.New("one-shot share token already consumed")
)

// ShareTokenRepo provides CRUD operations for share_tokens and
// share_token_grants. The hashed-credential pattern mirrors api_key_repo:
// the raw secret is generated and returned exactly once on Create; only the
// SHA-256 hash and the leading 8-char prefix persist.
type ShareTokenRepo struct {
	db DB
}

// NewShareTokenRepo creates a new ShareTokenRepo backed by the given DB.
func NewShareTokenRepo(db DB) *ShareTokenRepo {
	return &ShareTokenRepo{db: db}
}

// Create generates a new share token with the nram_s_ prefix, stores the
// SHA-256 hash + the supplied grants atomically, and returns the raw secret.
// The raw value is only available at creation time.
//
// Grants must be non-empty and every Permission must be valid; an empty
// grant list would produce a share that grants nothing, and the auto-revoke-
// on-zero-grants invariant would then immediately invalidate it.
func (r *ShareTokenRepo) Create(ctx context.Context, share *model.ShareToken, grants []model.ShareTokenGrant) (string, error) {
	if share.ID == uuid.Nil {
		share.ID = uuid.New()
	}
	if len(grants) == 0 {
		return "", errors.New("share token create: at least one project grant is required")
	}
	for i, g := range grants {
		if !g.Permission.Valid() {
			return "", fmt.Errorf("share token create: grant[%d] has invalid permission %q", i, g.Permission)
		}
	}

	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return "", fmt.Errorf("share token create generate: %w", err)
	}
	rawSecret := ShareTokenWirePrefix + hex.EncodeToString(rawBytes)

	hash := sha256.Sum256([]byte(rawSecret))
	share.TokenHash = hex.EncodeToString(hash[:])
	share.TokenPrefix = rawSecret[len(ShareTokenWirePrefix) : len(ShareTokenWirePrefix)+8]

	expiresAt := share.ExpiresAt.UTC().Format(time.RFC3339)

	var description *string
	if share.Description != "" {
		d := share.Description
		description = &d
	}

	insertShare := `INSERT INTO share_tokens (id, owner_user_id, token_hash, token_prefix, name, description, is_one_shot, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	grantInsert := `INSERT INTO share_token_grants (share_token_id, project_id, permission) VALUES (?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		insertShare = `INSERT INTO share_tokens (id, owner_user_id, token_hash, token_prefix, name, description, is_one_shot, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
		grantInsert = `INSERT INTO share_token_grants (share_token_id, project_id, permission) VALUES ($1, $2, $3)`
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("share token create begin: %w", err)
	}
	defer func() {
		// Rollback is a no-op after a successful Commit; this catches every
		// early-return path below without per-branch _ = tx.Rollback() calls.
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, insertShare,
		share.ID.String(), share.OwnerUserID.String(), share.TokenHash, share.TokenPrefix,
		share.Name, description, EncodeBool(r.db.Backend(), share.IsOneShot), expiresAt,
	); err != nil {
		return "", fmt.Errorf("share token create insert: %w", err)
	}

	for _, g := range grants {
		if _, err := tx.ExecContext(ctx, grantInsert, share.ID.String(), g.ProjectID.String(), string(g.Permission)); err != nil {
			return "", fmt.Errorf("share token grant insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("share token create commit: %w", err)
	}

	if err := r.reload(ctx, share); err != nil {
		return "", err
	}
	return rawSecret, nil
}

// ValidateBySecret hashes the raw secret, looks up the share, checks active
// state, and increments use_count + last_used_at. Returns the share + its
// current grants. Callers needing the one-shot consumption side-effect must
// call MarkConsumed separately.
//
// Rejects consumed one-shots BEFORE touching counters so attacker probes
// against a leaked-but-already-consumed secret cannot inflate use_count.
func (r *ShareTokenRepo) ValidateBySecret(ctx context.Context, rawSecret string) (*model.ShareToken, []model.ShareTokenGrant, error) {
	if !strings.HasPrefix(rawSecret, ShareTokenWirePrefix) {
		return nil, nil, ErrShareTokenNotFound
	}

	hash := sha256.Sum256([]byte(rawSecret))
	hashHex := hex.EncodeToString(hash[:])

	share, err := r.getByHash(ctx, hashHex)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now().UTC()
	if share.RevokedAt != nil {
		return nil, nil, ErrShareTokenRevoked
	}
	if !now.Before(share.ExpiresAt) {
		return nil, nil, ErrShareTokenExpired
	}
	if share.IsOneShot && share.ConsumedAt != nil {
		return nil, nil, ErrShareTokenConsumed
	}

	grants, err := r.ListGrants(ctx, share.ID)
	if err != nil {
		return nil, nil, err
	}

	// A share whose grant set has been cascade-deleted (every project the
	// share covered was deleted) is functionally dead. Reject BEFORE touching
	// use_count/last_used_at so attacker-driven probes of a zero-grant share
	// cannot inflate the owner's "last used" counters indefinitely.
	if len(grants) == 0 {
		return nil, nil, ErrShareTokenNotFound
	}

	if err := r.touch(ctx, share.ID, now); err != nil {
		return nil, nil, err
	}
	t := now
	share.LastUsedAt = &t
	share.UseCount++

	return share, grants, nil
}

func (r *ShareTokenRepo) touch(ctx context.Context, shareID uuid.UUID, now time.Time) error {
	nowStr := now.UTC().Format(time.RFC3339)
	query := `UPDATE share_tokens SET last_used_at = ?, use_count = use_count + 1 WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE share_tokens SET last_used_at = $1, use_count = use_count + 1 WHERE id = $2`
	}
	if _, err := r.db.Exec(ctx, query, nowStr, shareID.String()); err != nil {
		return fmt.Errorf("share token touch: %w", err)
	}
	return nil
}

// GetByID fetches a single share by its primary key. Grants are not loaded.
func (r *ShareTokenRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.ShareToken, error) {
	query := selectShareTokenColumns + ` FROM share_tokens WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectShareTokenColumns + ` FROM share_tokens WHERE id = $1`
	}
	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanShare(row)
}

func (r *ShareTokenRepo) getByHash(ctx context.Context, hashHex string) (*model.ShareToken, error) {
	query := selectShareTokenColumns + ` FROM share_tokens WHERE token_hash = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectShareTokenColumns + ` FROM share_tokens WHERE token_hash = $1`
	}
	row := r.db.QueryRow(ctx, query, hashHex)
	share, err := r.scanShare(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrShareTokenNotFound
		}
		return nil, err
	}
	return share, nil
}

// ListByOwner returns shares owned by a user, newest first.
func (r *ShareTokenRepo) ListByOwner(ctx context.Context, ownerUserID uuid.UUID) ([]model.ShareToken, error) {
	query := selectShareTokenColumns + ` FROM share_tokens WHERE owner_user_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectShareTokenColumns + ` FROM share_tokens WHERE owner_user_id = $1 ORDER BY created_at DESC`
	}
	rows, err := r.db.Query(ctx, query, ownerUserID.String())
	if err != nil {
		return nil, fmt.Errorf("share token list by owner: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := []model.ShareToken{}
	for rows.Next() {
		share, err := r.scanShareFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *share)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("share token list by owner iteration: %w", err)
	}
	return result, nil
}

// ListGrantsByOwner returns all share_token_grants for shares owned by the
// given user, indexed by share id. One query instead of N — handlers
// rendering the owner's share list use this to avoid 1+N round trips.
func (r *ShareTokenRepo) ListGrantsByOwner(ctx context.Context, ownerUserID uuid.UUID) (map[uuid.UUID][]model.ShareTokenGrant, error) {
	query := `SELECT g.share_token_id, g.project_id, g.permission
		FROM share_token_grants g
		JOIN share_tokens s ON s.id = g.share_token_id
		WHERE s.owner_user_id = ?
		ORDER BY g.share_token_id, g.project_id`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT g.share_token_id, g.project_id, g.permission
			FROM share_token_grants g
			JOIN share_tokens s ON s.id = g.share_token_id
			WHERE s.owner_user_id = $1
			ORDER BY g.share_token_id, g.project_id`
	}
	rows, err := r.db.Query(ctx, query, ownerUserID.String())
	if err != nil {
		return nil, fmt.Errorf("share token grants by owner: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[uuid.UUID][]model.ShareTokenGrant)
	for rows.Next() {
		var shareIDStr, projectIDStr, perm string
		if err := rows.Scan(&shareIDStr, &projectIDStr, &perm); err != nil {
			return nil, fmt.Errorf("share token grants by owner scan: %w", err)
		}
		sid, err := uuid.Parse(shareIDStr)
		if err != nil {
			return nil, fmt.Errorf("share token grants by owner parse share_token_id: %w", err)
		}
		pid, err := uuid.Parse(projectIDStr)
		if err != nil {
			return nil, fmt.Errorf("share token grants by owner parse project_id: %w", err)
		}
		out[sid] = append(out[sid], model.ShareTokenGrant{
			ShareTokenID: sid,
			ProjectID:    pid,
			Permission:   model.SharePermission(perm),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("share token grants by owner iterate: %w", err)
	}
	return out, nil
}

// ListGrants returns the project-permission grants attached to a share.
func (r *ShareTokenRepo) ListGrants(ctx context.Context, shareID uuid.UUID) ([]model.ShareTokenGrant, error) {
	query := `SELECT share_token_id, project_id, permission FROM share_token_grants WHERE share_token_id = ? ORDER BY project_id`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT share_token_id, project_id, permission FROM share_token_grants WHERE share_token_id = $1 ORDER BY project_id`
	}
	rows, err := r.db.Query(ctx, query, shareID.String())
	if err != nil {
		return nil, fmt.Errorf("share token grant list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := []model.ShareTokenGrant{}
	for rows.Next() {
		var shareIDStr, projectIDStr, perm string
		if err := rows.Scan(&shareIDStr, &projectIDStr, &perm); err != nil {
			return nil, fmt.Errorf("share token grant scan: %w", err)
		}
		sid, err := uuid.Parse(shareIDStr)
		if err != nil {
			return nil, fmt.Errorf("share token grant parse share_token_id: %w", err)
		}
		pid, err := uuid.Parse(projectIDStr)
		if err != nil {
			return nil, fmt.Errorf("share token grant parse project_id: %w", err)
		}
		result = append(result, model.ShareTokenGrant{
			ShareTokenID: sid,
			ProjectID:    pid,
			Permission:   model.SharePermission(perm),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("share token grant list iteration: %w", err)
	}
	return result, nil
}

// SetGrants replaces the grants for a share atomically: delete-all then
// re-insert, both inside one transaction. Caller must supply at least one
// grant; passing an empty slice is rejected because the auto-revoke-on-
// zero-grants invariant means the share would immediately become useless.
func (r *ShareTokenRepo) SetGrants(ctx context.Context, shareID uuid.UUID, grants []model.ShareTokenGrant) error {
	if len(grants) == 0 {
		return errors.New("share token set grants: at least one project grant is required")
	}
	for i, g := range grants {
		if !g.Permission.Valid() {
			return fmt.Errorf("share token set grants: grant[%d] has invalid permission %q", i, g.Permission)
		}
	}

	del := `DELETE FROM share_token_grants WHERE share_token_id = ?`
	insert := `INSERT INTO share_token_grants (share_token_id, project_id, permission) VALUES (?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		del = `DELETE FROM share_token_grants WHERE share_token_id = $1`
		insert = `INSERT INTO share_token_grants (share_token_id, project_id, permission) VALUES ($1, $2, $3)`
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("share token set grants begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, del, shareID.String()); err != nil {
		return fmt.Errorf("share token set grants delete: %w", err)
	}
	for _, g := range grants {
		if _, err := tx.ExecContext(ctx, insert, shareID.String(), g.ProjectID.String(), string(g.Permission)); err != nil {
			return fmt.Errorf("share token set grants insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("share token set grants commit: %w", err)
	}
	return nil
}

// Revoke sets revoked_at on the share. Idempotent — re-revoking a revoked
// share is a no-op. Returns ErrShareTokenNotFound if the share does not
// exist. Callers must separately RevokeRefreshTokensByShareToken on the
// OAuth repo to kill derived bearer access on the recipient's next refresh.
func (r *ShareTokenRepo) Revoke(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE share_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE share_tokens SET revoked_at = $1 WHERE id = $2 AND revoked_at IS NULL`
	}
	res, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("share token revoke: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		// Either already revoked or not found. Distinguish with a follow-up
		// lookup so the caller can tell.
		if _, err := r.GetByID(ctx, id); err != nil {
			return ErrShareTokenNotFound
		}
	}
	return nil
}

// RevokeZeroGrantSharesForOwner revokes every non-revoked share owned by
// ownerUserID whose grant set is empty. This is the post-commit sweep the
// project-delete cascade calls so that shares left with zero grants after
// the FK cascade dropped their grant rows do not linger as "active" in the
// owner's UI. Returns the IDs of the shares that were freshly revoked so
// the caller can cascade refresh-token revocation against exactly those.
//
// Idempotent: already-revoked shares are skipped; shares with at least one
// grant are skipped.
func (r *ShareTokenRepo) RevokeZeroGrantSharesForOwner(ctx context.Context, ownerUserID uuid.UUID) ([]uuid.UUID, error) {
	selectQ := `SELECT id FROM share_tokens
		WHERE owner_user_id = ?
		  AND revoked_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM share_token_grants g WHERE g.share_token_id = share_tokens.id)`
	if r.db.Backend() == BackendPostgres {
		selectQ = `SELECT id FROM share_tokens
			WHERE owner_user_id = $1
			  AND revoked_at IS NULL
			  AND NOT EXISTS (SELECT 1 FROM share_token_grants g WHERE g.share_token_id = share_tokens.id)`
	}
	rows, err := r.db.Query(ctx, selectQ, ownerUserID.String())
	if err != nil {
		return nil, fmt.Errorf("share token revoke zero-grant select: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if scanErr := rows.Scan(&idStr); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("share token revoke zero-grant scan: %w", scanErr)
		}
		id, parseErr := uuid.Parse(idStr)
		if parseErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("share token revoke zero-grant parse id: %w", parseErr)
		}
		ids = append(ids, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("share token revoke zero-grant iterate: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	placeholders, args := uuidInPlaceholders(r.db, ids, 2)
	updateQ := `UPDATE share_tokens SET revoked_at = ?
		WHERE revoked_at IS NULL AND id IN (` + strings.Join(placeholders, ", ") + `)`
	if r.db.Backend() == BackendPostgres {
		updateQ = `UPDATE share_tokens SET revoked_at = $1
			WHERE revoked_at IS NULL AND id IN (` + strings.Join(placeholders, ", ") + `)`
	}
	execArgs := make([]any, 0, len(args)+1)
	execArgs = append(execArgs, now)
	execArgs = append(execArgs, args...)
	if _, err := r.db.Exec(ctx, updateQ, execArgs...); err != nil {
		return nil, fmt.Errorf("share token revoke zero-grant update: %w", err)
	}
	return ids, nil
}

// MarkConsumed sets consumed_at on a one-shot share. Returns
// ErrShareTokenConsumed if the share is already consumed (zero rows affected)
// so the caller can fail closed instead of silently double-minting an OAuth
// chain when two consent submissions race on the same secret.
func (r *ShareTokenRepo) MarkConsumed(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE share_tokens SET consumed_at = ? WHERE id = ? AND consumed_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE share_tokens SET consumed_at = $1 WHERE id = $2 AND consumed_at IS NULL`
	}
	res, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("share token mark consumed: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return ErrShareTokenConsumed
	}
	return nil
}

// RemoveGrant drops a single project grant from a share. If this leaves zero
// grants, the share is auto-revoked (because a grant-less share would be
// inactive but still appear active in the owner's UI).
func (r *ShareTokenRepo) RemoveGrant(ctx context.Context, shareID, projectID uuid.UUID) error {
	del := `DELETE FROM share_token_grants WHERE share_token_id = ? AND project_id = ?`
	if r.db.Backend() == BackendPostgres {
		del = `DELETE FROM share_token_grants WHERE share_token_id = $1 AND project_id = $2`
	}
	if _, err := r.db.Exec(ctx, del, shareID.String(), projectID.String()); err != nil {
		return fmt.Errorf("share token grant remove: %w", err)
	}

	remaining, err := r.ListGrants(ctx, shareID)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		return r.Revoke(ctx, shareID)
	}
	return nil
}

func (r *ShareTokenRepo) reload(ctx context.Context, share *model.ShareToken) error {
	fetched, err := r.GetByID(ctx, share.ID)
	if err != nil {
		return fmt.Errorf("share token reload: %w", err)
	}
	*share = *fetched
	return nil
}

const selectShareTokenColumns = `SELECT id, owner_user_id, token_hash, token_prefix, name, description, is_one_shot, expires_at, consumed_at, created_at, last_used_at, use_count, revoked_at`

func (r *ShareTokenRepo) scanShare(row *sql.Row) (*model.ShareToken, error) {
	var s model.ShareToken
	var idStr, ownerStr, expiresStr, createdStr string
	var description, consumedStr, lastUsedStr, revokedStr sql.NullString
	var oneShot bool

	err := row.Scan(
		&idStr, &ownerStr, &s.TokenHash, &s.TokenPrefix, &s.Name,
		&description, &oneShot, &expiresStr, &consumedStr, &createdStr,
		&lastUsedStr, &s.UseCount, &revokedStr,
	)
	if err != nil {
		return nil, err
	}
	return r.populateShare(&s, idStr, ownerStr, description, oneShot, expiresStr, consumedStr, createdStr, lastUsedStr, revokedStr)
}

func (r *ShareTokenRepo) scanShareFromRows(rows *sql.Rows) (*model.ShareToken, error) {
	var s model.ShareToken
	var idStr, ownerStr, expiresStr, createdStr string
	var description, consumedStr, lastUsedStr, revokedStr sql.NullString
	var oneShot bool

	err := rows.Scan(
		&idStr, &ownerStr, &s.TokenHash, &s.TokenPrefix, &s.Name,
		&description, &oneShot, &expiresStr, &consumedStr, &createdStr,
		&lastUsedStr, &s.UseCount, &revokedStr,
	)
	if err != nil {
		return nil, fmt.Errorf("share token scan rows: %w", err)
	}
	return r.populateShare(&s, idStr, ownerStr, description, oneShot, expiresStr, consumedStr, createdStr, lastUsedStr, revokedStr)
}

func (r *ShareTokenRepo) populateShare(
	s *model.ShareToken,
	idStr, ownerStr string,
	description sql.NullString,
	oneShot bool,
	expiresStr string,
	consumedStr sql.NullString,
	createdStr string,
	lastUsedStr, revokedStr sql.NullString,
) (*model.ShareToken, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("share token parse id: %w", err)
	}
	s.ID = id

	owner, err := uuid.Parse(ownerStr)
	if err != nil {
		return nil, fmt.Errorf("share token parse owner_user_id: %w", err)
	}
	s.OwnerUserID = owner

	if description.Valid {
		s.Description = description.String
	}
	s.IsOneShot = oneShot

	s.ExpiresAt, err = time.Parse(time.RFC3339, expiresStr)
	if err != nil {
		return nil, fmt.Errorf("share token parse expires_at: %w", err)
	}

	if consumedStr.Valid {
		t, err := time.Parse(time.RFC3339, consumedStr.String)
		if err != nil {
			return nil, fmt.Errorf("share token parse consumed_at: %w", err)
		}
		s.ConsumedAt = &t
	}

	s.CreatedAt, err = time.Parse(time.RFC3339, createdStr)
	if err != nil {
		return nil, fmt.Errorf("share token parse created_at: %w", err)
	}

	if lastUsedStr.Valid {
		t, err := time.Parse(time.RFC3339, lastUsedStr.String)
		if err != nil {
			return nil, fmt.Errorf("share token parse last_used_at: %w", err)
		}
		s.LastUsedAt = &t
	}

	if revokedStr.Valid {
		t, err := time.Parse(time.RFC3339, revokedStr.String)
		if err != nil {
			return nil, fmt.Errorf("share token parse revoked_at: %w", err)
		}
		s.RevokedAt = &t
	}

	return s, nil
}
