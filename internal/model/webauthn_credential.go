package model

import (
	"time"

	"github.com/google/uuid"
)

type WebAuthnCredential struct {
	ID              uuid.UUID  `json:"id"`
	UserID          uuid.UUID  `json:"user_id"`
	Name            string     `json:"name"`
	CredentialID    string     `json:"credential_id"`
	PublicKey       string     `json:"-"`
	AAGUID          string     `json:"aaguid"`
	SignCount       uint32     `json:"sign_count"`
	Transports      []string   `json:"transports"`
	UserVerified    bool       `json:"user_verified"`
	BackupEligible  bool       `json:"backup_eligible"`
	BackupState     bool       `json:"backup_state"`
	AttestationType string     `json:"attestation_type"`
	CreatedAt       time.Time  `json:"created_at"`
	LastUsedAt      *time.Time `json:"last_used_at"`
}
