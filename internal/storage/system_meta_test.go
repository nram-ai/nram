package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestGetSetSystemMeta(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()

		// Get non-existent key returns empty string.
		val, err := GetSystemMeta(ctx, db, "nonexistent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "" {
			t.Fatalf("expected empty string, got %q", val)
		}

		// Set and get.
		if err := SetSystemMeta(ctx, db, "test_key", "test_value"); err != nil {
			t.Fatalf("set: %v", err)
		}
		val, err = GetSystemMeta(ctx, db, "test_key")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if val != "test_value" {
			t.Fatalf("expected %q, got %q", "test_value", val)
		}

		// Upsert overwrites.
		if err := SetSystemMeta(ctx, db, "test_key", "updated"); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		val, err = GetSystemMeta(ctx, db, "test_key")
		if err != nil {
			t.Fatalf("get after upsert: %v", err)
		}
		if val != "updated" {
			t.Fatalf("expected %q, got %q", "updated", val)
		}
	})
}

func TestLoadOrCreateJWTSecret(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()

		// First call creates the secret.
		secret1, err := LoadOrCreateJWTSecret(ctx, db)
		if err != nil {
			t.Fatalf("first call: %v", err)
		}
		if len(secret1) != 32 {
			t.Fatalf("expected 32 bytes, got %d", len(secret1))
		}

		// Verify it was persisted as a JSON array.
		raw, err := GetSystemMeta(ctx, db, "jwt_signing_secrets")
		if err != nil {
			t.Fatalf("get meta: %v", err)
		}
		var arr []string
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(arr) != 1 {
			t.Fatalf("expected 1 element, got %d", len(arr))
		}
		decoded, err := base64.StdEncoding.DecodeString(arr[0])
		if err != nil {
			t.Fatalf("base64 decode: %v", err)
		}
		if string(decoded) != string(secret1) {
			t.Fatal("stored secret does not match returned secret")
		}

		// Second call returns the same secret.
		secret2, err := LoadOrCreateJWTSecret(ctx, db)
		if err != nil {
			t.Fatalf("second call: %v", err)
		}
		if string(secret1) != string(secret2) {
			t.Fatal("second call returned different secret")
		}
	})
}
