package model

import (
	"time"

	"github.com/google/uuid"
)

// SharePermission is the per-project capability tier granted by a share.
// Tiers are inclusive: read_store implies read; read_store_modify implies
// read_store. delete_project and update_project are never granted to a
// share-bearer regardless of tier; the owner retains those exclusively.
type SharePermission string

const (
	SharePermissionRead            SharePermission = "read"
	SharePermissionReadStore       SharePermission = "read_store"
	SharePermissionReadStoreModify SharePermission = "read_store_modify"
)

// Allows reports whether this tier covers the requested tier.
// read_store_modify >= read_store >= read.
func (p SharePermission) Allows(required SharePermission) bool {
	rank := func(t SharePermission) int {
		switch t {
		case SharePermissionRead:
			return 1
		case SharePermissionReadStore:
			return 2
		case SharePermissionReadStoreModify:
			return 3
		}
		return 0
	}
	return rank(p) >= rank(required)
}

// Valid reports whether the value is one of the three accepted tiers.
func (p SharePermission) Valid() bool {
	switch p {
	case SharePermissionRead, SharePermissionReadStore, SharePermissionReadStoreModify:
		return true
	}
	return false
}

// ShareToken is a capability-bearer credential issued by an owner. The raw
// secret (nram_s_<hex>) is shown to the owner exactly once at creation; only
// the SHA-256 hash is persisted. token_prefix is the first 8 chars after the
// prefix and is safe to surface in admin UIs so the owner can identify which
// share a row refers to.
type ShareToken struct {
	ID          uuid.UUID  `json:"id"`
	OwnerUserID uuid.UUID  `json:"owner_user_id"`
	TokenHash   string     `json:"-"`
	TokenPrefix string     `json:"token_prefix"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	IsOneShot   bool       `json:"is_one_shot"`
	ExpiresAt   time.Time  `json:"expires_at"`
	ConsumedAt  *time.Time `json:"consumed_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	UseCount    int        `json:"use_count"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

// ShareTokenGrant pairs a share with a project and the capability tier the
// share-bearer holds on that project. Multiple grants per share are typical;
// a share with zero grants is auto-revoked by the repo when the last project
// is removed.
type ShareTokenGrant struct {
	ShareTokenID uuid.UUID       `json:"share_token_id"`
	ProjectID    uuid.UUID       `json:"project_id"`
	Permission   SharePermission `json:"permission"`
}

// ProjectGrant carries the resolved project-permission pair attached to an
// authenticated share-bearer's AuthContext. The middleware loads these from
// share_token_grants on every request so grant edits take effect immediately.
type ProjectGrant struct {
	ProjectID  uuid.UUID
	Permission SharePermission
}

// Active reports whether the share is currently usable: not revoked, not
// expired, and (for one-shot) not consumed.
//
// One-shot consumption is checked separately from the OAuth-binding path:
// callers that mint a binding via the consent flow set ConsumedAt as part of
// that mint; bearer-direct paths reject one-shot tokens unconditionally after
// consumption.
func (s *ShareToken) Active(now time.Time) bool {
	if s.RevokedAt != nil {
		return false
	}
	if !now.Before(s.ExpiresAt) {
		return false
	}
	return true
}
