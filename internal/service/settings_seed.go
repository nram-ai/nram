package service

import (
	"context"
	"fmt"
)

// SettingsSeederRepo is the minimal interface SeedSettingsDefaults needs.
// Implemented by *storage.SettingsRepo via its InsertManyIfMissing method.
type SettingsSeederRepo interface {
	InsertManyIfMissing(ctx context.Context, scope string, kv map[string]string) error
}

// SeedSettingsDefaults inserts a row at scope "global" for every (key, value)
// pair when no row already exists for that composite key. Operator-set values
// are never overwritten. Idempotent — safe to run on every server boot.
//
// The defaults map is built by the caller from the schema registry
// (storage/admin.SettingsSchemas) so the JSON encoding of each value matches
// what storage.SettingsRepo.Set would have written. This keeps the schema
// registry as the single source of truth for both the admin UI's "default"
// hint and the bootstrap row contents.
func SeedSettingsDefaults(ctx context.Context, repo SettingsSeederRepo, defaults map[string]string) error {
	if repo == nil {
		return fmt.Errorf("seed settings defaults: nil repo")
	}
	if err := repo.InsertManyIfMissing(ctx, "global", defaults); err != nil {
		return fmt.Errorf("seed settings defaults: %w", err)
	}
	return nil
}
