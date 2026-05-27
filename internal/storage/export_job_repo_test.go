package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func newTestExportJob(userID uuid.UUID) *model.ExportJob {
	return &model.ExportJob{
		UserID: userID,
		Scope:  model.ExportScopeAccount,
		Format: model.ExportFormatZip,
	}
}

func TestExportJobRepo_Enqueue(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		if job.ID == uuid.Nil {
			t.Fatal("expected non-nil ID")
		}
		if job.Status != model.ExportStatusPending {
			t.Fatalf("expected status pending, got %q", job.Status)
		}
		if job.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if job.ProjectID != nil {
			t.Fatal("expected nil ProjectID for account scope")
		}

		got, err := repo.GetByID(ctx, user.ID, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.ID != job.ID || got.UserID != user.ID {
			t.Fatalf("round-trip mismatch: %+v", got)
		}
	})
}

func TestExportJobRepo_GetByID_OtherUserIsNotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		owner := createTestUser(t, ctx, db)
		other := createTestUser(t, ctx, db)

		job := newTestExportJob(owner.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		_, err := repo.GetByID(ctx, other.ID, job.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows for cross-user fetch, got %v", err)
		}
	})
}

func TestExportJobRepo_ClaimNext(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		// Empty queue: ErrNoRows.
		if _, err := repo.ClaimNext(ctx, "worker-1"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows on empty queue, got %v", err)
		}

		// Enqueue and claim.
		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		claimed, err := repo.ClaimNext(ctx, "worker-1")
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed.ID != job.ID {
			t.Fatalf("claimed wrong job: %s vs %s", claimed.ID, job.ID)
		}
		if claimed.Status != model.ExportStatusProcessing {
			t.Fatalf("expected status processing, got %q", claimed.Status)
		}
		if claimed.ClaimedBy == nil || *claimed.ClaimedBy != "worker-1" {
			t.Fatalf("expected claimed_by=worker-1, got %+v", claimed.ClaimedBy)
		}
		if claimed.StartedAt == nil {
			t.Fatal("expected non-nil started_at")
		}

		// Second claim with empty queue: ErrNoRows.
		if _, err := repo.ClaimNext(ctx, "worker-2"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after drain, got %v", err)
		}
	})
}

func TestExportJobRepo_Complete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		exp := time.Now().Add(7 * 24 * time.Hour)
		if err := repo.Complete(ctx, job.ID, "worker-1", "/tmp/x.zip", 1024, "deadbeef", exp); err != nil {
			t.Fatalf("complete: %v", err)
		}

		got, err := repo.GetByID(ctx, user.ID, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status != model.ExportStatusSucceeded {
			t.Fatalf("expected status succeeded, got %q", got.Status)
		}
		if got.ArtifactPath == nil || *got.ArtifactPath != "/tmp/x.zip" {
			t.Fatalf("artifact_path: %+v", got.ArtifactPath)
		}
		if got.ArtifactBytes == nil || *got.ArtifactBytes != 1024 {
			t.Fatalf("artifact_bytes: %+v", got.ArtifactBytes)
		}
		if got.CompletedAt == nil {
			t.Fatal("expected non-nil completed_at")
		}
		if got.ExpiresAt == nil {
			t.Fatal("expected non-nil expires_at")
		}
	})
}

func TestExportJobRepo_Complete_ClaimLost(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		err := repo.Complete(ctx, job.ID, "worker-2", "/tmp/x.zip", 1, "h", time.Now().Add(time.Hour))
		if !errors.Is(err, ErrExportJobClaimLost) {
			t.Fatalf("expected ErrExportJobClaimLost, got %v", err)
		}
	})
}

func TestExportJobRepo_Fail(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		if err := repo.Fail(ctx, job.ID, "worker-1", "out of disk"); err != nil {
			t.Fatalf("fail: %v", err)
		}

		got, err := repo.GetByID(ctx, user.ID, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status != model.ExportStatusFailed {
			t.Fatalf("expected status failed, got %q", got.Status)
		}
		if got.Error == nil || *got.Error != "out of disk" {
			t.Fatalf("error: %+v", got.Error)
		}
	})
}

func TestExportJobRepo_ListByUser_ScopedAndOrdered(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)
		other := createTestUser(t, ctx, db)

		for range 3 {
			if err := repo.Enqueue(ctx, newTestExportJob(user.ID)); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		if err := repo.Enqueue(ctx, newTestExportJob(other.ID)); err != nil {
			t.Fatalf("enqueue other: %v", err)
		}

		got, err := repo.ListByUser(ctx, user.ID, 10, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 jobs for user, got %d", len(got))
		}
		for _, j := range got {
			if j.UserID != user.ID {
				t.Fatalf("cross-user leak in ListByUser: %s", j.UserID)
			}
		}
	})
}

func TestExportJobRepo_DeleteByUserAndID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		owner := createTestUser(t, ctx, db)
		other := createTestUser(t, ctx, db)

		job := newTestExportJob(owner.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		// Cross-user delete is a no-op (ErrNoRows).
		if err := repo.DeleteByUserAndID(ctx, other.ID, job.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows on cross-user delete, got %v", err)
		}
		// Job still exists.
		if _, err := repo.GetByID(ctx, owner.ID, job.ID); err != nil {
			t.Fatalf("expected job to survive cross-user delete: %v", err)
		}

		// Owner delete succeeds.
		if err := repo.DeleteByUserAndID(ctx, owner.ID, job.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := repo.GetByID(ctx, owner.ID, job.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}
	})
}

func TestExportJobRepo_ListExpired(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		job := newTestExportJob(user.ID)
		if err := repo.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}
		past := time.Now().Add(-time.Hour)
		if err := repo.Complete(ctx, job.ID, "worker-1", "/tmp/x.zip", 1, "h", past); err != nil {
			t.Fatalf("complete: %v", err)
		}

		expired, err := repo.ListExpired(ctx, 100)
		if err != nil {
			t.Fatalf("list expired: %v", err)
		}
		if len(expired) != 1 || expired[0].ID != job.ID {
			t.Fatalf("expected one expired job, got %d", len(expired))
		}

		if err := repo.MarkExpired(ctx, job.ID); err != nil {
			t.Fatalf("mark expired: %v", err)
		}
		got, _ := repo.GetByID(ctx, user.ID, job.ID)
		if got.Status != model.ExportStatusExpired {
			t.Fatalf("expected status expired, got %q", got.Status)
		}
		if got.ArtifactPath != nil {
			t.Fatalf("expected nil artifact_path post-MarkExpired, got %+v", got.ArtifactPath)
		}
	})
}

func TestExportJobRepo_CountInFlightByUser(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewExportJobRepo(db)
		user := createTestUser(t, ctx, db)

		n, err := repo.CountInFlightByUser(ctx, user.ID)
		if err != nil || n != 0 {
			t.Fatalf("expected 0 in-flight, got n=%d err=%v", n, err)
		}

		if err := repo.Enqueue(ctx, newTestExportJob(user.ID)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		n, _ = repo.CountInFlightByUser(ctx, user.ID)
		if n != 1 {
			t.Fatalf("expected 1 in-flight after enqueue, got %d", n)
		}
	})
}
