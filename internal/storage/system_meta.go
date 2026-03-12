package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// GetSystemMeta retrieves a value from the system_meta table.
// Returns an empty string if the key does not exist.
func GetSystemMeta(ctx context.Context, db DB, key string) (string, error) {
	var value string
	err := db.QueryRow(ctx, "SELECT value FROM system_meta WHERE key = $1", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get system_meta %q: %w", key, err)
	}
	return value, nil
}

// SetSystemMeta upserts a key/value pair in the system_meta table.
func SetSystemMeta(ctx context.Context, db DB, key, value string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(ctx,
		`INSERT INTO system_meta (key, value, created_at, updated_at)
		 VALUES ($1, $2, $3, $3)
		 ON CONFLICT(key) DO UPDATE SET value = $2, updated_at = $3`,
		key, value, now,
	)
	if err != nil {
		return fmt.Errorf("set system_meta %q: %w", key, err)
	}
	return nil
}

// LoadOrCreateJWTSecret reads or generates a persistent JWT signing secret.
// The secret is stored in system_meta under the key "jwt_signing_secrets" as a
// JSON array of base64-encoded strings. The first element is the active secret.
func LoadOrCreateJWTSecret(ctx context.Context, db DB) ([]byte, error) {
	raw, err := GetSystemMeta(ctx, db, "jwt_signing_secrets")
	if err != nil {
		return nil, err
	}

	if raw != "" {
		var secrets []string
		if err := json.Unmarshal([]byte(raw), &secrets); err != nil {
			return nil, fmt.Errorf("parse jwt_signing_secrets: %w", err)
		}
		if len(secrets) == 0 {
			return nil, fmt.Errorf("jwt_signing_secrets array is empty")
		}
		return base64.StdEncoding.DecodeString(secrets[0])
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate jwt secret: %w", err)
	}

	encoded := base64.StdEncoding.EncodeToString(secret)
	arr, _ := json.Marshal([]string{encoded})
	if err := SetSystemMeta(ctx, db, "jwt_signing_secrets", string(arr)); err != nil {
		return nil, err
	}

	return secret, nil
}
