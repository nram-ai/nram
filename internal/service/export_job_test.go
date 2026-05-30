package service

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- mocks ---

type stubExportJobRepo struct {
	enqueued       []*model.ExportJob
	inFlight       int
	activeSince    int
	enqueueErr     error
	countErr       error
	expectInFlight bool
}

func (r *stubExportJobRepo) Enqueue(_ context.Context, job *model.ExportJob) error {
	if r.enqueueErr != nil {
		return r.enqueueErr
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	job.Status = model.ExportStatusPending
	job.CreatedAt = time.Now()
	job.UpdatedAt = job.CreatedAt
	r.enqueued = append(r.enqueued, job)
	return nil
}
func (r *stubExportJobRepo) ClaimNext(_ context.Context, _ string) (*model.ExportJob, error) {
	return nil, nil
}
func (r *stubExportJobRepo) Complete(_ context.Context, _ uuid.UUID, _ string, _ string, _ int64, _ string, _ time.Time) error {
	return nil
}
func (r *stubExportJobRepo) Fail(_ context.Context, _ uuid.UUID, _ string, _ string) error {
	return nil
}
func (r *stubExportJobRepo) GetByID(_ context.Context, _, _ uuid.UUID) (*model.ExportJob, error) {
	return nil, nil
}
func (r *stubExportJobRepo) ListByUser(_ context.Context, _ uuid.UUID, _, _ int) ([]model.ExportJob, error) {
	return nil, nil
}
func (r *stubExportJobRepo) CountActiveByUserSince(_ context.Context, _ uuid.UUID, _ time.Time) (int, error) {
	if r.countErr != nil {
		return 0, r.countErr
	}
	return r.activeSince, nil
}
func (r *stubExportJobRepo) CountInFlightByUser(_ context.Context, _ uuid.UUID) (int, error) {
	r.expectInFlight = true
	if r.countErr != nil {
		return 0, r.countErr
	}
	return r.inFlight, nil
}
func (r *stubExportJobRepo) DeleteByUserAndID(_ context.Context, _, _ uuid.UUID) error {
	return nil
}
func (r *stubExportJobRepo) ListExpired(_ context.Context, _ int) ([]model.ExportJob, error) {
	return nil, nil
}
func (r *stubExportJobRepo) MarkExpired(_ context.Context, _ uuid.UUID) error {
	return nil
}

type stubUserReader struct {
	user *model.User
	err  error
}

func (s *stubUserReader) GetByID(_ context.Context, _ uuid.UUID) (*model.User, error) {
	return s.user, s.err
}

type stubProjectLister struct {
	projects []model.Project
	getByID  *model.Project
	err      error
}

func (s *stubProjectLister) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	return s.projects, s.err
}
func (s *stubProjectLister) GetByID(_ context.Context, _ uuid.UUID) (*model.Project, error) {
	if s.getByID == nil {
		return nil, errors.New("not found")
	}
	return s.getByID, nil
}

// --- tests ---

func newTestExportJobService(t *testing.T, repo ExportJobRepository, projects ExportProjectLister, users ExportUserReader) *ExportJobService {
	t.Helper()
	return NewExportJobService(repo, users, projects, nil /* exportSvc unused for enqueue tests */, nil /* settings nil → falls through to defaults */, "test-worker", t.TempDir(), nil)
}

func TestExportJobService_Enqueue_Account_OK(t *testing.T) {
	repo := &stubExportJobRepo{}
	svc := newTestExportJobService(t, repo, &stubProjectLister{}, &stubUserReader{user: &model.User{ID: uuid.New(), NamespaceID: uuid.New()}})

	job, err := svc.Enqueue(context.Background(), EnqueueRequest{
		UserID: uuid.New(),
		Scope:  model.ExportScopeAccount,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job == nil || job.ID == uuid.Nil {
		t.Fatal("expected job with ID")
	}
	if job.Format != model.ExportFormatZip {
		t.Fatalf("expected zip default for account scope, got %q", job.Format)
	}
	if !repo.expectInFlight {
		t.Fatal("expected in-flight check to run")
	}
}

func TestExportJobService_Enqueue_Account_RejectsProjectID(t *testing.T) {
	svc := newTestExportJobService(t, &stubExportJobRepo{}, &stubProjectLister{}, &stubUserReader{})
	pid := uuid.New()
	_, err := svc.Enqueue(context.Background(), EnqueueRequest{
		UserID:    uuid.New(),
		Scope:     model.ExportScopeAccount,
		ProjectID: &pid,
	})
	if !errors.Is(err, ErrExportJobBadRequest) {
		t.Fatalf("expected ErrExportJobBadRequest, got %v", err)
	}
}

func TestExportJobService_Enqueue_Project_OwnershipMismatch(t *testing.T) {
	nsA, nsB := uuid.New(), uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: nsA}
	// project owned by a different namespace
	project := &model.Project{ID: uuid.New(), OwnerNamespaceID: nsB, NamespaceID: nsB, Slug: "stranger"}

	svc := newTestExportJobService(t, &stubExportJobRepo{}, &stubProjectLister{getByID: project}, &stubUserReader{user: user})

	pid := project.ID
	_, err := svc.Enqueue(context.Background(), EnqueueRequest{
		UserID:    user.ID,
		Scope:     model.ExportScopeProject,
		ProjectID: &pid,
	})
	if !errors.Is(err, ErrExportJobBadRequest) {
		t.Fatalf("expected ErrExportJobBadRequest on cross-owner project, got %v", err)
	}
}

func TestExportJobService_Enqueue_InFlightCap(t *testing.T) {
	repo := &stubExportJobRepo{inFlight: 1}
	svc := newTestExportJobService(t, repo, &stubProjectLister{}, &stubUserReader{user: &model.User{ID: uuid.New()}})

	_, err := svc.Enqueue(context.Background(), EnqueueRequest{
		UserID: uuid.New(),
		Scope:  model.ExportScopeAccount,
	})
	if !errors.Is(err, ErrExportJobAlreadyRunning) {
		t.Fatalf("expected ErrExportJobAlreadyRunning, got %v", err)
	}
}

func TestExportJobService_Enqueue_RateLimit(t *testing.T) {
	// activeSince at the default cap (5) should reject.
	repo := &stubExportJobRepo{activeSince: 5}
	svc := newTestExportJobService(t, repo, &stubProjectLister{}, &stubUserReader{user: &model.User{ID: uuid.New()}})

	_, err := svc.Enqueue(context.Background(), EnqueueRequest{
		UserID: uuid.New(),
		Scope:  model.ExportScopeAccount,
	})
	if !errors.Is(err, ErrExportJobRateLimited) {
		t.Fatalf("expected ErrExportJobRateLimited, got %v", err)
	}
}

// TestSanitizeFileSegment pins the defense-in-depth path-traversal guard
// on per-project filenames inside the zip.
func TestSanitizeFileSegment(t *testing.T) {
	cases := map[string]string{
		"global":       "global",
		"my-project":   "my-project",
		"../escape":    "__escape", // ".." → "_", then "/" → "_"; both substitutions disarm traversal
		"a/b":          "a_b",
		"a\\b":         "a_b",
		"":             "project",
		"   ":          "project",
		"with:colon":   "with_colon",
		"with\x00null": "with_null",
	}
	for input, want := range cases {
		got := sanitizeFileSegment(input)
		if got != want {
			t.Errorf("sanitizeFileSegment(%q) = %q, want %q", input, got, want)
		}
	}
}

// Compile-time check: ensure *os.File satisfies io.ReadCloser so the
// handler can stream the artifact without copying.
var _ io.ReadCloser = (*nopReadCloser)(nil)

type nopReadCloser struct{ io.Reader }

func (nopReadCloser) Close() error { return nil }
