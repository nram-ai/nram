package storage

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"golang.org/x/crypto/argon2"
)

var ErrUserDisabled = errors.New("user is disabled")
var ErrNoPassword = errors.New("user has no password set")
var ErrInvalidCredentials = errors.New("invalid credentials")

type UserRepo struct {
	db DB
}

func NewUserRepo(db DB) *UserRepo {
	return &UserRepo{db: db}
}

func (r *UserRepo) Create(ctx context.Context, user *model.User, nsRepo *NamespaceRepo, projectRepo *ProjectRepo, orgNamespacePath string) error {
	if user.ID == uuid.Nil {
		user.ID = uuid.New()
	}
	if user.Settings == nil {
		user.Settings = json.RawMessage(`{}`)
	}

	userNSID := uuid.New()
	userNSSlug := userNSID.String()
	userNSPath := orgNamespacePath + "/" + userNSSlug

	ns := &model.Namespace{
		ID:       userNSID,
		Name:     "User " + user.ID.String()[:8],
		Slug:     userNSSlug,
		Kind:     "user",
		ParentID: &user.OrgID,
		Path:     userNSPath,
		Depth:    2,
	}

	// Look up org namespace to use its ID as parent
	orgNS, err := nsRepo.GetByPath(ctx, orgNamespacePath)
	if err != nil {
		return fmt.Errorf("user create resolve org namespace: %w", err)
	}
	ns.ParentID = &orgNS.ID

	if err := nsRepo.Create(ctx, ns); err != nil {
		return fmt.Errorf("user create namespace: %w", err)
	}

	user.NamespaceID = userNSID

	query := `INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role, settings)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role, settings)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	}

	_, err = r.db.Exec(ctx, query,
		user.ID.String(), user.Email, user.DisplayName, user.PasswordHash,
		user.OrgID.String(), user.NamespaceID.String(), user.Role, string(user.Settings),
	)
	if err != nil {
		return fmt.Errorf("user create: %w", err)
	}

	// Auto-create a "global" project so the user has one ready immediately.
	// This serves as the implicit scope when MCP tools omit the project parameter.
	if projectRepo != nil {
		if _, err := projectRepo.AutoCreateUnderUser(ctx, nsRepo, userNSID, "global"); err != nil {
			return fmt.Errorf("user create global project: %w", err)
		}
	}

	return r.reload(ctx, user)
}

func (r *UserRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanUser(row)
}

// GetByNamespaceID returns the user whose personal namespace is the given ID.
// A user owns exactly one namespace (created at user-Create time), so this is
// the inverse of looking up "which user lives at this namespace." Returns
// sql.ErrNoRows when the namespace is not a user's personal namespace
// (typically because it is a project namespace instead).
func (r *UserRepo) GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users WHERE namespace_id = $1`
	}

	row := r.db.QueryRow(ctx, query, namespaceID.String())
	return r.scanUser(row)
}

func (r *UserRepo) GetByEmail(ctx context.Context, email string) (*model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users WHERE email = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users WHERE email = $1`
	}

	row := r.db.QueryRow(ctx, query, email)
	return r.scanUser(row)
}

func (r *UserRepo) Authenticate(ctx context.Context, email, password string) (*model.User, error) {
	user, err := r.GetByEmail(ctx, email)
	if err != nil {
		return nil, fmt.Errorf("user authenticate: %w", err)
	}

	if user.DisabledAt != nil {
		return nil, ErrUserDisabled
	}

	if user.PasswordHash == nil {
		return nil, ErrNoPassword
	}

	if !verifyArgon2id(*user.PasswordHash, password) {
		return nil, ErrInvalidCredentials
	}

	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE users SET last_login = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET last_login = $1 WHERE id = $2`
	}

	_, err = r.db.Exec(ctx, query, now, user.ID.String())
	if err != nil {
		return nil, fmt.Errorf("user authenticate update last_login: %w", err)
	}

	return r.GetByID(ctx, user.ID)
}

func (r *UserRepo) ListByOrg(ctx context.Context, orgID uuid.UUID) ([]model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users WHERE org_id = ? ORDER BY email`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users WHERE org_id = $1 ORDER BY email`
	}

	rows, err := r.db.Query(ctx, query, orgID.String())
	if err != nil {
		return nil, fmt.Errorf("user list by org: %w", err)
	}
	defer rows.Close()

	result := []model.User{}
	for rows.Next() {
		u, err := r.scanUserFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("user list by org iteration: %w", err)
	}
	return result, nil
}

// CountByOrg returns the number of users in the given organization.
func (r *UserRepo) CountByOrg(ctx context.Context, orgID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM users WHERE org_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM users WHERE org_id = $1`
	}
	row := r.db.QueryRow(ctx, query, orgID.String())
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("user count by org: %w", err)
	}
	return count, nil
}

// ListByOrgPaged returns users in the given organization with LIMIT and OFFSET applied.
func (r *UserRepo) ListByOrgPaged(ctx context.Context, orgID uuid.UUID, limit, offset int) ([]model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users WHERE org_id = ? ORDER BY email LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users WHERE org_id = $1 ORDER BY email LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, orgID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("user list by org paged: %w", err)
	}
	defer rows.Close()

	result := []model.User{}
	for rows.Next() {
		u, err := r.scanUserFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("user list by org paged iteration: %w", err)
	}
	return result, nil
}

// ListAll returns all users ordered by email.
func (r *UserRepo) ListAll(ctx context.Context) ([]model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users ORDER BY email`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("user list all: %w", err)
	}
	defer rows.Close()

	result := []model.User{}
	for rows.Next() {
		u, err := r.scanUserFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("user list all iteration: %w", err)
	}
	return result, nil
}

// CountAll returns the total number of users.
func (r *UserRepo) CountAll(ctx context.Context) (int, error) {
	row := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM users`)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("user count all: %w", err)
	}
	return count, nil
}

// ListAllPaged returns all users ordered by email with LIMIT and OFFSET applied.
func (r *UserRepo) ListAllPaged(ctx context.Context, limit, offset int) ([]model.User, error) {
	query := `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
		FROM users ORDER BY email LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at, last_login, disabled_at
			FROM users ORDER BY email LIMIT $1 OFFSET $2`
	}

	rows, err := r.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("user list all paged: %w", err)
	}
	defer rows.Close()

	result := []model.User{}
	for rows.Next() {
		u, err := r.scanUserFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("user list all paged iteration: %w", err)
	}
	return result, nil
}

// CountAdmins returns the number of active (non-disabled) administrator users.
func (r *UserRepo) CountAdmins(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM users WHERE role = 'administrator' AND disabled_at IS NULL`
	row := r.db.QueryRow(ctx, query)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("user count admins: %w", err)
	}
	return count, nil
}

func (r *UserRepo) Update(ctx context.Context, user *model.User) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE users SET display_name = ?, role = ?, settings = ?, updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET display_name = $1, role = $2, settings = $3, updated_at = $4
			WHERE id = $5`
	}

	if user.Settings == nil {
		user.Settings = json.RawMessage(`{}`)
	}

	_, err := r.db.Exec(ctx, query,
		user.DisplayName, user.Role, string(user.Settings), now, user.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("user update: %w", err)
	}

	return r.reload(ctx, user)
}

func (r *UserRepo) Disable(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE users SET disabled_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET disabled_at = $1 WHERE id = $2`
	}

	_, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("user disable: %w", err)
	}
	return nil
}

func (r *UserRepo) Enable(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE users SET disabled_at = NULL WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET disabled_at = NULL WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("user enable: %w", err)
	}
	return nil
}

func (r *UserRepo) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM users WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM users WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("user delete: %w", err)
	}
	return nil
}

// UpdatePassword sets a new password hash for the given user.
func (r *UserRepo) UpdatePassword(ctx context.Context, id uuid.UUID, newHash string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET password_hash = $1, updated_at = $2 WHERE id = $3`
	}
	_, err := r.db.Exec(ctx, query, newHash, now, id.String())
	if err != nil {
		return fmt.Errorf("user update password: %w", err)
	}
	return nil
}

func (r *UserRepo) reload(ctx context.Context, user *model.User) error {
	fetched, err := r.GetByID(ctx, user.ID)
	if err != nil {
		return fmt.Errorf("user reload: %w", err)
	}
	*user = *fetched
	return nil
}

func (r *UserRepo) scanUser(row *sql.Row) (*model.User, error) {
	var user model.User
	var idStr, orgIDStr, namespaceIDStr string
	var settingsStr string
	var createdAtStr, updatedAtStr string
	var passwordHash sql.NullString
	var lastLoginStr, disabledAtStr sql.NullString

	err := row.Scan(
		&idStr, &user.Email, &user.DisplayName, &passwordHash,
		&orgIDStr, &namespaceIDStr, &user.Role,
		&settingsStr, &createdAtStr, &updatedAtStr,
		&lastLoginStr, &disabledAtStr,
	)
	if err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("user scan parse id: %w", err)
	}
	user.ID = id

	orgID, err := uuid.Parse(orgIDStr)
	if err != nil {
		return nil, fmt.Errorf("user scan parse org_id: %w", err)
	}
	user.OrgID = orgID

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("user scan parse namespace_id: %w", err)
	}
	user.NamespaceID = nsID

	if passwordHash.Valid {
		user.PasswordHash = &passwordHash.String
	}

	user.Settings = json.RawMessage(settingsStr)

	user.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("user scan parse created_at: %w", err)
	}
	user.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("user scan parse updated_at: %w", err)
	}

	if lastLoginStr.Valid {
		t, err := time.Parse(time.RFC3339, lastLoginStr.String)
		if err != nil {
			return nil, fmt.Errorf("user scan parse last_login: %w", err)
		}
		user.LastLogin = &t
	}

	if disabledAtStr.Valid {
		t, err := time.Parse(time.RFC3339, disabledAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("user scan parse disabled_at: %w", err)
		}
		user.DisabledAt = &t
	}

	return &user, nil
}

func (r *UserRepo) scanUserFromRows(rows *sql.Rows) (*model.User, error) {
	var user model.User
	var idStr, orgIDStr, namespaceIDStr string
	var settingsStr string
	var createdAtStr, updatedAtStr string
	var passwordHash sql.NullString
	var lastLoginStr, disabledAtStr sql.NullString

	err := rows.Scan(
		&idStr, &user.Email, &user.DisplayName, &passwordHash,
		&orgIDStr, &namespaceIDStr, &user.Role,
		&settingsStr, &createdAtStr, &updatedAtStr,
		&lastLoginStr, &disabledAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("user scan rows: %w", err)
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("user scan rows parse id: %w", err)
	}
	user.ID = id

	orgID, err := uuid.Parse(orgIDStr)
	if err != nil {
		return nil, fmt.Errorf("user scan rows parse org_id: %w", err)
	}
	user.OrgID = orgID

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("user scan rows parse namespace_id: %w", err)
	}
	user.NamespaceID = nsID

	if passwordHash.Valid {
		user.PasswordHash = &passwordHash.String
	}

	user.Settings = json.RawMessage(settingsStr)

	user.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("user scan rows parse created_at: %w", err)
	}
	user.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("user scan rows parse updated_at: %w", err)
	}

	if lastLoginStr.Valid {
		t, err := time.Parse(time.RFC3339, lastLoginStr.String)
		if err != nil {
			return nil, fmt.Errorf("user scan rows parse last_login: %w", err)
		}
		user.LastLogin = &t
	}

	if disabledAtStr.Valid {
		t, err := time.Parse(time.RFC3339, disabledAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("user scan rows parse disabled_at: %w", err)
		}
		user.DisabledAt = &t
	}

	return &user, nil
}

// VerifyPassword checks whether the given plaintext password matches the
// encoded argon2id hash.
func VerifyPassword(encoded, password string) bool {
	return verifyArgon2id(encoded, password)
}

// GetIdentityByID returns the role and org ID of an active (non-disabled) user by ID.
// Returns an error if the user does not exist or is disabled.
func (r *UserRepo) GetIdentityByID(ctx context.Context, id uuid.UUID) (string, uuid.UUID, error) {
	query := `SELECT role, org_id FROM users WHERE id = ? AND disabled_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT role, org_id FROM users WHERE id = $1 AND disabled_at IS NULL`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	var role string
	var orgIDStr string
	if err := row.Scan(&role, &orgIDStr); err != nil {
		if err == sql.ErrNoRows {
			return "", uuid.Nil, fmt.Errorf("user not found or disabled: %w", err)
		}
		return "", uuid.Nil, fmt.Errorf("user get identity by id: %w", err)
	}
	orgID, err := uuid.Parse(orgIDStr)
	if err != nil {
		return "", uuid.Nil, fmt.Errorf("user get identity parse org_id: %w", err)
	}
	return role, orgID, nil
}

// UpdateLastLogin sets the last_login timestamp for the given user to now.
func (r *UserRepo) UpdateLastLogin(ctx context.Context, userID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE users SET last_login = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE users SET last_login = $1 WHERE id = $2`
	}
	_, err := r.db.Exec(ctx, query, now, userID.String())
	if err != nil {
		return fmt.Errorf("user update last_login: %w", err)
	}
	return nil
}

func HashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("hash password generate salt: %w", err)
	}

	hash := argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32)

	saltB64 := base64.RawStdEncoding.EncodeToString(salt)
	hashB64 := base64.RawStdEncoding.EncodeToString(hash)

	return fmt.Sprintf("$argon2id$v=19$m=65536,t=1,p=4$%s$%s", saltB64, hashB64), nil
}

func verifyArgon2id(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return false
	}

	var mem uint32
	var iters uint32
	var threads uint8
	_, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &mem, &iters, &threads)
	if err != nil {
		return false
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}

	computed := argon2.IDKey([]byte(password), salt, iters, mem, threads, uint32(len(expectedHash)))
	return subtle.ConstantTimeCompare(computed, expectedHash) == 1
}
