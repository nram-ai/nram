package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID           uuid.UUID       `json:"id"`
	Email        string          `json:"email"`
	DisplayName  string          `json:"display_name"`
	PasswordHash *string         `json:"-"`
	OrgID        uuid.UUID       `json:"org_id"`
	Organization *ProjectOrg     `json:"organization,omitempty"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	Role         string          `json:"role"`
	Settings     json.RawMessage `json:"settings"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	LastLogin    *time.Time      `json:"last_login"`
	DisabledAt   *time.Time      `json:"disabled_at"`
}

// GetSettingString returns a string value from the Settings JSON blob.
// Returns "" if the key is missing, the value is not a string, or Settings
// is empty/invalid JSON.
func (u *User) GetSettingString(key string) string {
	if len(u.Settings) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(u.Settings, &m); err != nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// SetSettingString sets a string value in the Settings JSON blob, preserving
// other keys. Empty string clears the key. The caller must Update the user
// to persist the change.
func (u *User) SetSettingString(key, value string) error {
	m := map[string]any{}
	if len(u.Settings) > 0 {
		if err := json.Unmarshal(u.Settings, &m); err != nil {
			return err
		}
	}
	if value == "" {
		delete(m, key)
	} else {
		m[key] = value
	}
	out, err := json.Marshal(m)
	if err != nil {
		return err
	}
	u.Settings = out
	return nil
}
