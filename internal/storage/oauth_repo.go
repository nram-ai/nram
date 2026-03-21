package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// OAuthRepo provides CRUD operations for OAuth-related tables:
// oauth_clients, oauth_authorization_codes, oauth_refresh_tokens, and oauth_idp_configs.
type OAuthRepo struct {
	db DB
}

// NewOAuthRepo creates a new OAuthRepo backed by the given DB.
func NewOAuthRepo(db DB) *OAuthRepo {
	return &OAuthRepo{db: db}
}

// ---------------------------------------------------------------------------
// OAuth Client methods
// ---------------------------------------------------------------------------

// CreateClient inserts a new OAuth client. ID is generated if zero-valued.
// RedirectURIs and GrantTypes default to empty slices if nil.
func (r *OAuthRepo) CreateClient(ctx context.Context, client *model.OAuthClient) error {
	if client.ID == uuid.Nil {
		client.ID = uuid.New()
	}
	if client.RedirectURIs == nil {
		client.RedirectURIs = []string{}
	}
	if client.GrantTypes == nil {
		client.GrantTypes = []string{}
	}

	redirectVal := encodeStringArray(r.db.Backend(), client.RedirectURIs)
	grantVal := encodeStringArray(r.db.Backend(), client.GrantTypes)
	autoRegVal := encodeBool(r.db.Backend(), client.AutoRegistered)

	var orgID *string
	if client.OrgID != nil {
		s := client.OrgID.String()
		orgID = &s
	}

	var userID *string
	if client.UserID != nil {
		s := client.UserID.String()
		userID = &s
	}

	query := `INSERT INTO oauth_clients (id, client_id, client_secret, name, redirect_uris, grant_types, org_id, user_id, auto_registered)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO oauth_clients (id, client_id, client_secret, name, redirect_uris, grant_types, org_id, user_id, auto_registered)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err := r.db.Exec(ctx, query,
		client.ID.String(), client.ClientID, client.ClientSecret, client.Name,
		redirectVal, grantVal, orgID, userID, autoRegVal,
	)
	if err != nil {
		return fmt.Errorf("oauth client create: %w", err)
	}

	return r.reloadClient(ctx, client)
}

// GetClientByID returns an OAuth client by its client_id string.
func (r *OAuthRepo) GetClientByID(ctx context.Context, clientID string) (*model.OAuthClient, error) {
	query := selectOAuthClientColumns + ` FROM oauth_clients WHERE client_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectOAuthClientColumns + ` FROM oauth_clients WHERE client_id = $1`
	}

	row := r.db.QueryRow(ctx, query, clientID)
	return r.scanClient(row)
}

// getClientByPK returns an OAuth client by its primary key (id UUID).
func (r *OAuthRepo) getClientByPK(ctx context.Context, id uuid.UUID) (*model.OAuthClient, error) {
	query := selectOAuthClientColumns + ` FROM oauth_clients WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectOAuthClientColumns + ` FROM oauth_clients WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanClient(row)
}

// ListClientsByUser returns all OAuth clients belonging to the organization
// that the given user is a member of.
func (r *OAuthRepo) ListClientsByUser(ctx context.Context, userID uuid.UUID) ([]model.OAuthClient, error) {
	query := selectOAuthClientColumns + ` FROM oauth_clients
		WHERE user_id = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectOAuthClientColumns + ` FROM oauth_clients
			WHERE user_id = $1
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, userID.String())
	if err != nil {
		return nil, fmt.Errorf("oauth client list by user: %w", err)
	}
	defer rows.Close()

	return r.scanClients(rows)
}

// ListAllClients returns all OAuth clients, ordered by created_at DESC.
func (r *OAuthRepo) ListAllClients(ctx context.Context) ([]model.OAuthClient, error) {
	query := selectOAuthClientColumns + ` FROM oauth_clients WHERE user_id IS NULL ORDER BY created_at DESC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("oauth client list all: %w", err)
	}
	defer rows.Close()

	return r.scanClients(rows)
}

// DeleteClientByPK removes an OAuth client by its primary key UUID.
func (r *OAuthRepo) DeleteClientByPK(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM oauth_clients WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_clients WHERE id = $1`
	}

	result, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("oauth client delete by pk: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("oauth client delete by pk rows affected: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("oauth client not found")
	}
	return nil
}

// DeleteClient removes an OAuth client by its client_id string.
func (r *OAuthRepo) DeleteClient(ctx context.Context, clientID string) error {
	query := `DELETE FROM oauth_clients WHERE client_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_clients WHERE client_id = $1`
	}

	_, err := r.db.Exec(ctx, query, clientID)
	if err != nil {
		return fmt.Errorf("oauth client delete: %w", err)
	}
	return nil
}

// reloadClient fetches the client by PK and populates the struct in place.
func (r *OAuthRepo) reloadClient(ctx context.Context, client *model.OAuthClient) error {
	fetched, err := r.getClientByPK(ctx, client.ID)
	if err != nil {
		return fmt.Errorf("oauth client reload: %w", err)
	}
	*client = *fetched
	return nil
}

const selectOAuthClientColumns = `SELECT id, client_id, client_secret, name, redirect_uris, grant_types, org_id, user_id, auto_registered, created_at`

func (r *OAuthRepo) scanClient(row *sql.Row) (*model.OAuthClient, error) {
	var client model.OAuthClient
	var idStr string
	var redirectStr, grantStr string
	var orgIDStr sql.NullString
	var userIDStr sql.NullString
	var autoReg bool
	var createdAtStr string

	err := row.Scan(
		&idStr, &client.ClientID, &client.ClientSecret, &client.Name,
		&redirectStr, &grantStr, &orgIDStr, &userIDStr, &autoReg, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateClient(&client, idStr, redirectStr, grantStr, orgIDStr, userIDStr, autoReg, createdAtStr)
}

func (r *OAuthRepo) scanClientFromRows(rows *sql.Rows) (*model.OAuthClient, error) {
	var client model.OAuthClient
	var idStr string
	var redirectStr, grantStr string
	var orgIDStr sql.NullString
	var userIDStr sql.NullString
	var autoReg bool
	var createdAtStr string

	err := rows.Scan(
		&idStr, &client.ClientID, &client.ClientSecret, &client.Name,
		&redirectStr, &grantStr, &orgIDStr, &userIDStr, &autoReg, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("oauth client scan rows: %w", err)
	}

	return r.populateClient(&client, idStr, redirectStr, grantStr, orgIDStr, userIDStr, autoReg, createdAtStr)
}

func (r *OAuthRepo) scanClients(rows *sql.Rows) ([]model.OAuthClient, error) {
	result := []model.OAuthClient{}
	for rows.Next() {
		client, err := r.scanClientFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *client)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("oauth client scan iteration: %w", err)
	}
	return result, nil
}

func (r *OAuthRepo) populateClient(
	client *model.OAuthClient,
	idStr, redirectStr, grantStr string,
	orgIDStr, userIDStr sql.NullString,
	autoReg bool,
	createdAtStr string,
) (*model.OAuthClient, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("oauth client parse id: %w", err)
	}
	client.ID = id

	redirectURIs, err := decodeStringArray(r.db.Backend(), redirectStr)
	if err != nil {
		return nil, fmt.Errorf("oauth client parse redirect_uris: %w", err)
	}
	if redirectURIs == nil {
		redirectURIs = []string{}
	}
	client.RedirectURIs = redirectURIs

	grantTypes, err := decodeStringArray(r.db.Backend(), grantStr)
	if err != nil {
		return nil, fmt.Errorf("oauth client parse grant_types: %w", err)
	}
	if grantTypes == nil {
		grantTypes = []string{}
	}
	client.GrantTypes = grantTypes

	if orgIDStr.Valid {
		oid, err := uuid.Parse(orgIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("oauth client parse org_id: %w", err)
		}
		client.OrgID = &oid
	}

	if userIDStr.Valid {
		uid, err := uuid.Parse(userIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("oauth client parse user_id: %w", err)
		}
		client.UserID = &uid
	}

	client.AutoRegistered = autoReg

	client.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth client parse created_at: %w", err)
	}

	return client, nil
}

// ---------------------------------------------------------------------------
// Authorization Code methods
// ---------------------------------------------------------------------------

// CreateAuthCode inserts a new OAuth authorization code.
func (r *OAuthRepo) CreateAuthCode(ctx context.Context, code *model.OAuthAuthorizationCode) error {
	query := `INSERT INTO oauth_authorization_codes (code, client_id, user_id, redirect_uri, scope, code_challenge, code_challenge_method, resource, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO oauth_authorization_codes (code, client_id, user_id, redirect_uri, scope, code_challenge, code_challenge_method, resource, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	var resource *string
	if code.Resource != "" {
		r := code.Resource
		resource = &r
	}

	_, err := r.db.Exec(ctx, query,
		code.Code, code.ClientID, code.UserID.String(), code.RedirectURI,
		code.Scope, code.CodeChallenge, code.CodeChallengeMethod,
		resource, code.ExpiresAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("oauth auth code create: %w", err)
	}

	return r.reloadAuthCode(ctx, code)
}

// GetAuthCode returns an authorization code by its code value.
func (r *OAuthRepo) GetAuthCode(ctx context.Context, code string) (*model.OAuthAuthorizationCode, error) {
	query := selectAuthCodeColumns + ` FROM oauth_authorization_codes WHERE code = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectAuthCodeColumns + ` FROM oauth_authorization_codes WHERE code = $1`
	}

	row := r.db.QueryRow(ctx, query, code)
	return r.scanAuthCode(row)
}

// ConsumeAuthCode deletes the authorization code, making it single-use.
// Authorization codes are consumed upon exchange for tokens.
func (r *OAuthRepo) ConsumeAuthCode(ctx context.Context, code string) error {
	query := `DELETE FROM oauth_authorization_codes WHERE code = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_authorization_codes WHERE code = $1`
	}

	result, err := r.db.Exec(ctx, query, code)
	if err != nil {
		return fmt.Errorf("oauth auth code consume: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("oauth auth code consume rows affected: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteExpiredCodes removes all authorization codes whose expires_at is in the past.
// Returns the number of deleted rows.
func (r *OAuthRepo) DeleteExpiredCodes(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `DELETE FROM oauth_authorization_codes WHERE expires_at < ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_authorization_codes WHERE expires_at < $1`
	}

	result, err := r.db.Exec(ctx, query, now)
	if err != nil {
		return 0, fmt.Errorf("oauth auth code delete expired: %w", err)
	}

	return result.RowsAffected()
}

// reloadAuthCode fetches the auth code and populates the struct in place.
func (r *OAuthRepo) reloadAuthCode(ctx context.Context, code *model.OAuthAuthorizationCode) error {
	fetched, err := r.GetAuthCode(ctx, code.Code)
	if err != nil {
		return fmt.Errorf("oauth auth code reload: %w", err)
	}
	*code = *fetched
	return nil
}

const selectAuthCodeColumns = `SELECT code, client_id, user_id, redirect_uri, scope, code_challenge, code_challenge_method, resource, expires_at, created_at`

func (r *OAuthRepo) scanAuthCode(row *sql.Row) (*model.OAuthAuthorizationCode, error) {
	var ac model.OAuthAuthorizationCode
	var userIDStr string
	var resourceStr sql.NullString
	var expiresAtStr, createdAtStr string

	err := row.Scan(
		&ac.Code, &ac.ClientID, &userIDStr, &ac.RedirectURI,
		&ac.Scope, &ac.CodeChallenge, &ac.CodeChallengeMethod,
		&resourceStr, &expiresAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateAuthCode(&ac, userIDStr, resourceStr, expiresAtStr, createdAtStr)
}

func (r *OAuthRepo) populateAuthCode(
	ac *model.OAuthAuthorizationCode,
	userIDStr string,
	resourceStr sql.NullString,
	expiresAtStr, createdAtStr string,
) (*model.OAuthAuthorizationCode, error) {
	var err error
	ac.UserID, err = uuid.Parse(userIDStr)
	if err != nil {
		return nil, fmt.Errorf("oauth auth code parse user_id: %w", err)
	}

	if resourceStr.Valid {
		ac.Resource = resourceStr.String
	}

	ac.ExpiresAt, err = time.Parse(time.RFC3339, expiresAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth auth code parse expires_at: %w", err)
	}

	ac.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth auth code parse created_at: %w", err)
	}

	return ac, nil
}

// ---------------------------------------------------------------------------
// Refresh Token methods
// ---------------------------------------------------------------------------

// CreateRefreshToken inserts a new OAuth refresh token.
func (r *OAuthRepo) CreateRefreshToken(ctx context.Context, token *model.OAuthRefreshToken) error {
	var expiresAt *string
	if token.ExpiresAt != nil {
		s := token.ExpiresAt.UTC().Format(time.RFC3339)
		expiresAt = &s
	}

	query := `INSERT INTO oauth_refresh_tokens (token_hash, client_id, user_id, scope, expires_at)
		VALUES (?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO oauth_refresh_tokens (token_hash, client_id, user_id, scope, expires_at)
			VALUES ($1, $2, $3, $4, $5)`
	}

	_, err := r.db.Exec(ctx, query,
		token.TokenHash, token.ClientID, token.UserID.String(), token.Scope, expiresAt,
	)
	if err != nil {
		return fmt.Errorf("oauth refresh token create: %w", err)
	}

	return r.reloadRefreshToken(ctx, token)
}

// GetRefreshToken returns a refresh token by its token_hash value.
func (r *OAuthRepo) GetRefreshToken(ctx context.Context, tokenHash string) (*model.OAuthRefreshToken, error) {
	query := selectRefreshTokenColumns + ` FROM oauth_refresh_tokens WHERE token_hash = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectRefreshTokenColumns + ` FROM oauth_refresh_tokens WHERE token_hash = $1`
	}

	row := r.db.QueryRow(ctx, query, tokenHash)
	return r.scanRefreshToken(row)
}

// RevokeRefreshToken sets revoked_at on a refresh token.
func (r *OAuthRepo) RevokeRefreshToken(ctx context.Context, tokenHash string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE oauth_refresh_tokens SET revoked_at = ? WHERE token_hash = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE oauth_refresh_tokens SET revoked_at = $1 WHERE token_hash = $2`
	}

	result, err := r.db.Exec(ctx, query, now, tokenHash)
	if err != nil {
		return fmt.Errorf("oauth refresh token revoke: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("oauth refresh token revoke rows affected: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// RevokeAllForUser revokes all non-revoked refresh tokens belonging to a user.
// Returns the number of tokens revoked.
func (r *OAuthRepo) RevokeAllForUser(ctx context.Context, userID uuid.UUID) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE oauth_refresh_tokens SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE oauth_refresh_tokens SET revoked_at = $1 WHERE user_id = $2 AND revoked_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, now, userID.String())
	if err != nil {
		return 0, fmt.Errorf("oauth refresh token revoke all for user: %w", err)
	}

	return result.RowsAffected()
}

// reloadRefreshToken fetches the token and populates the struct in place.
func (r *OAuthRepo) reloadRefreshToken(ctx context.Context, token *model.OAuthRefreshToken) error {
	fetched, err := r.GetRefreshToken(ctx, token.TokenHash)
	if err != nil {
		return fmt.Errorf("oauth refresh token reload: %w", err)
	}
	*token = *fetched
	return nil
}

const selectRefreshTokenColumns = `SELECT token_hash, client_id, user_id, scope, expires_at, revoked_at, created_at`

func (r *OAuthRepo) scanRefreshToken(row *sql.Row) (*model.OAuthRefreshToken, error) {
	var token model.OAuthRefreshToken
	var userIDStr string
	var expiresAtStr, revokedAtStr sql.NullString
	var createdAtStr string

	err := row.Scan(
		&token.TokenHash, &token.ClientID, &userIDStr, &token.Scope,
		&expiresAtStr, &revokedAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateRefreshToken(&token, userIDStr, expiresAtStr, revokedAtStr, createdAtStr)
}

func (r *OAuthRepo) populateRefreshToken(
	token *model.OAuthRefreshToken,
	userIDStr string,
	expiresAtStr, revokedAtStr sql.NullString,
	createdAtStr string,
) (*model.OAuthRefreshToken, error) {
	var err error
	token.UserID, err = uuid.Parse(userIDStr)
	if err != nil {
		return nil, fmt.Errorf("oauth refresh token parse user_id: %w", err)
	}

	if expiresAtStr.Valid {
		t, err := time.Parse(time.RFC3339, expiresAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("oauth refresh token parse expires_at: %w", err)
		}
		token.ExpiresAt = &t
	}

	if revokedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, revokedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("oauth refresh token parse revoked_at: %w", err)
		}
		token.RevokedAt = &t
	}

	token.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth refresh token parse created_at: %w", err)
	}

	return token, nil
}

// ---------------------------------------------------------------------------
// Identity Provider methods
// ---------------------------------------------------------------------------

// CreateIdP inserts a new OAuth identity provider configuration.
// ID is generated if zero-valued.
func (r *OAuthRepo) CreateIdP(ctx context.Context, idp *model.OAuthIdPConfig) error {
	if idp.ID == uuid.Nil {
		idp.ID = uuid.New()
	}
	if idp.AllowedDomains == nil {
		idp.AllowedDomains = []string{}
	}

	domainsVal := encodeStringArray(r.db.Backend(), idp.AllowedDomains)
	autoProvVal := encodeBool(r.db.Backend(), idp.AutoProvision)

	var orgID *string
	if idp.OrgID != nil {
		s := idp.OrgID.String()
		orgID = &s
	}

	query := `INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret, issuer_url, allowed_domains, auto_provision, default_role)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret, issuer_url, allowed_domains, auto_provision, default_role)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err := r.db.Exec(ctx, query,
		idp.ID.String(), orgID, idp.ProviderType, idp.ClientID, idp.ClientSecret,
		idp.IssuerURL, domainsVal, autoProvVal, idp.DefaultRole,
	)
	if err != nil {
		return fmt.Errorf("oauth idp create: %w", err)
	}

	return r.reloadIdP(ctx, idp)
}

// GetIdPByID returns an identity provider config by its UUID.
func (r *OAuthRepo) GetIdPByID(ctx context.Context, id uuid.UUID) (*model.OAuthIdPConfig, error) {
	query := selectIdPColumns + ` FROM oauth_idp_configs WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectIdPColumns + ` FROM oauth_idp_configs WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanIdP(row)
}

// ListIdPs returns all configured identity providers, ordered by created_at DESC.
func (r *OAuthRepo) ListIdPs(ctx context.Context) ([]model.OAuthIdPConfig, error) {
	query := selectIdPColumns + ` FROM oauth_idp_configs ORDER BY created_at DESC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("oauth idp list: %w", err)
	}
	defer rows.Close()

	return r.scanIdPs(rows)
}

// ListIdPsByOrg returns identity providers for a specific organization.
func (r *OAuthRepo) ListIdPsByOrg(ctx context.Context, orgID uuid.UUID) ([]model.OAuthIdPConfig, error) {
	query := selectIdPColumns + ` FROM oauth_idp_configs WHERE org_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectIdPColumns + ` FROM oauth_idp_configs WHERE org_id = $1 ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, orgID.String())
	if err != nil {
		return nil, fmt.Errorf("oauth idp list by org: %w", err)
	}
	defer rows.Close()

	return r.scanIdPs(rows)
}

// DeleteIdPByOrg removes an identity provider config only if it belongs to the given org.
func (r *OAuthRepo) DeleteIdPByOrg(ctx context.Context, id, orgID uuid.UUID) error {
	query := `DELETE FROM oauth_idp_configs WHERE id = ? AND org_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_idp_configs WHERE id = $1 AND org_id = $2`
	}

	res, err := r.db.Exec(ctx, query, id.String(), orgID.String())
	if err != nil {
		return fmt.Errorf("oauth idp delete by org: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("oauth idp delete by org rows affected: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("idp config not found")
	}
	return nil
}

// DeleteIdP removes an identity provider config by its UUID.
func (r *OAuthRepo) DeleteIdP(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM oauth_idp_configs WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM oauth_idp_configs WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("oauth idp delete: %w", err)
	}
	return nil
}

// reloadIdP fetches the IdP by ID and populates the struct in place.
func (r *OAuthRepo) reloadIdP(ctx context.Context, idp *model.OAuthIdPConfig) error {
	fetched, err := r.GetIdPByID(ctx, idp.ID)
	if err != nil {
		return fmt.Errorf("oauth idp reload: %w", err)
	}
	*idp = *fetched
	return nil
}

const selectIdPColumns = `SELECT id, org_id, provider_type, client_id, client_secret, issuer_url, allowed_domains, auto_provision, default_role, created_at, updated_at`

func (r *OAuthRepo) scanIdP(row *sql.Row) (*model.OAuthIdPConfig, error) {
	var idp model.OAuthIdPConfig
	var idStr string
	var orgIDStr sql.NullString
	var issuerURL sql.NullString
	var domainsStr string
	var autoProv bool
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &orgIDStr, &idp.ProviderType, &idp.ClientID, &idp.ClientSecret,
		&issuerURL, &domainsStr, &autoProv, &idp.DefaultRole,
		&createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateIdP(&idp, idStr, orgIDStr, issuerURL, domainsStr, autoProv, createdAtStr, updatedAtStr)
}

func (r *OAuthRepo) scanIdPFromRows(rows *sql.Rows) (*model.OAuthIdPConfig, error) {
	var idp model.OAuthIdPConfig
	var idStr string
	var orgIDStr sql.NullString
	var issuerURL sql.NullString
	var domainsStr string
	var autoProv bool
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &orgIDStr, &idp.ProviderType, &idp.ClientID, &idp.ClientSecret,
		&issuerURL, &domainsStr, &autoProv, &idp.DefaultRole,
		&createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("oauth idp scan rows: %w", err)
	}

	return r.populateIdP(&idp, idStr, orgIDStr, issuerURL, domainsStr, autoProv, createdAtStr, updatedAtStr)
}

func (r *OAuthRepo) scanIdPs(rows *sql.Rows) ([]model.OAuthIdPConfig, error) {
	result := []model.OAuthIdPConfig{}
	for rows.Next() {
		idp, err := r.scanIdPFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *idp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("oauth idp scan iteration: %w", err)
	}
	return result, nil
}

func (r *OAuthRepo) populateIdP(
	idp *model.OAuthIdPConfig,
	idStr string,
	orgIDStr, issuerURL sql.NullString,
	domainsStr string,
	autoProv bool,
	createdAtStr, updatedAtStr string,
) (*model.OAuthIdPConfig, error) {
	var err error
	idp.ID, err = uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("oauth idp parse id: %w", err)
	}

	if orgIDStr.Valid {
		oid, err := uuid.Parse(orgIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("oauth idp parse org_id: %w", err)
		}
		idp.OrgID = &oid
	}

	if issuerURL.Valid {
		idp.IssuerURL = &issuerURL.String
	}

	domains, err := decodeStringArray(r.db.Backend(), domainsStr)
	if err != nil {
		return nil, fmt.Errorf("oauth idp parse allowed_domains: %w", err)
	}
	if domains == nil {
		domains = []string{}
	}
	idp.AllowedDomains = domains

	idp.AutoProvision = autoProv

	idp.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth idp parse created_at: %w", err)
	}

	idp.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("oauth idp parse updated_at: %w", err)
	}

	return idp, nil
}
