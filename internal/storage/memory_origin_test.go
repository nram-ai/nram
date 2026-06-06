package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestMemoryRepo_Origin_RoundTrips confirms the origin column persists and is
// scanned back, and that a memory created without an explicit origin defaults
// to OriginUser (the column default), never an empty/invalid value.
func TestMemoryRepo_Origin_RoundTrips(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		cases := []struct {
			name string
			set  model.MemoryOrigin
			want model.MemoryOrigin
		}{
			{"dream", model.OriginDream, model.OriginDream},
			{"import", model.OriginImport, model.OriginImport},
			{"explicit user", model.OriginUser, model.OriginUser},
			{"unset defaults to user", "", model.OriginUser},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				mem := &model.Memory{
					NamespaceID: nsID,
					Content:     "origin round-trip " + tc.name,
					Origin:      tc.set,
					Confidence:  0.5,
					Importance:  0.5,
					Metadata:    json.RawMessage(`{}`),
				}
				if err := repo.Create(ctx, mem); err != nil {
					t.Fatalf("create: %v", err)
				}
				fetched, err := repo.GetByID(ctx, mem.ID)
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				if fetched.Origin != tc.want {
					t.Errorf("origin: set %q, want %q, got %q", tc.set, tc.want, fetched.Origin)
				}
				if fetched.IsDream() != (tc.want == model.OriginDream) {
					t.Errorf("IsDream()=%v for origin %q", fetched.IsDream(), fetched.Origin)
				}
			})
		}
	})
}

// TestMemoryRepo_Origin_SurvivesUpdate confirms an in-place Update preserves the
// origin column: a re-worded dream stays a dream and remains subject to the
// dream-recursion guard.
func TestMemoryRepo_Origin_SurvivesUpdate(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := &model.Memory{
			NamespaceID: nsID,
			Content:     "a dream synthesis",
			Origin:      model.OriginDream,
			Confidence:  0.5,
			Importance:  0.5,
			Metadata:    json.RawMessage(`{}`),
		}
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		mem.Content = "a re-worded dream synthesis"
		if err := repo.Update(ctx, mem); err != nil {
			t.Fatalf("update: %v", err)
		}
		// Update reloads in place; assert directly and via a fresh read.
		if mem.Origin != model.OriginDream {
			t.Errorf("origin after update: want %q, got %q", model.OriginDream, mem.Origin)
		}
		fetched, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if fetched.Origin != model.OriginDream {
			t.Errorf("origin after reload: want %q, got %q", model.OriginDream, fetched.Origin)
		}
	})
}
