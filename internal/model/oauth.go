package model

import (
	"time"

	"github.com/google/uuid"
)

type OAuthClient struct {
	ID             uuid.UUID  `json:"id"`
	ClientID       string     `json:"client_id"`
	ClientSecret   *string    `json:"-"`
	Name           string     `json:"name"`
	RedirectURIs   []string   `json:"redirect_uris"`
	GrantTypes     []string   `json:"grant_types"`
	OrgID          *uuid.UUID `json:"org_id"`
	AutoRegistered bool       `json:"auto_registered"`
	CreatedAt      time.Time  `json:"created_at"`
}

type OAuthAuthorizationCode struct {
	Code                string    `json:"code"`
	ClientID            string    `json:"client_id"`
	UserID              uuid.UUID `json:"user_id"`
	RedirectURI         string    `json:"redirect_uri"`
	Scope               string    `json:"scope"`
	CodeChallenge       *string   `json:"code_challenge"`
	CodeChallengeMethod string    `json:"code_challenge_method"`
	Resource            string    `json:"resource,omitempty"`
	ExpiresAt           time.Time `json:"expires_at"`
	CreatedAt           time.Time `json:"created_at"`
}

type OAuthRefreshToken struct {
	TokenHash string     `json:"token_hash"`
	ClientID  string     `json:"client_id"`
	UserID    uuid.UUID  `json:"user_id"`
	Scope     string     `json:"scope"`
	ExpiresAt *time.Time `json:"expires_at"`
	RevokedAt *time.Time `json:"revoked_at"`
	CreatedAt time.Time  `json:"created_at"`
}

type OAuthIdPConfig struct {
	ID             uuid.UUID  `json:"id"`
	OrgID          *uuid.UUID `json:"org_id"`
	ProviderType   string     `json:"provider_type"`
	ClientID       string     `json:"client_id"`
	ClientSecret   string     `json:"-"`
	IssuerURL      *string    `json:"issuer_url"`
	AllowedDomains []string   `json:"allowed_domains"`
	AutoProvision  bool       `json:"auto_provision"`
	DefaultRole    string     `json:"default_role"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}
