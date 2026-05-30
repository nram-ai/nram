package service

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// Validation errors returned by ExportJobService.Enqueue. The handler maps
// each to a specific HTTP status code so the UI can surface the right
// remediation.
var (
	ErrExportJobRateLimited    = errors.New("export rate limit exceeded")
	ErrExportJobAlreadyRunning = errors.New("an export is already in flight for this user")
	ErrExportJobBadRequest     = errors.New("invalid export job request")
)

// ExportJobRepository is the storage surface required by ExportJobService.
type ExportJobRepository interface {
	Enqueue(ctx context.Context, job *model.ExportJob) error
	ClaimNext(ctx context.Context, workerID string) (*model.ExportJob, error)
	Complete(ctx context.Context, jobID uuid.UUID, workerID string, artifactPath string, artifactBytes int64, artifactSHA256 string, expiresAt time.Time) error
	Fail(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) error
	GetByID(ctx context.Context, userID, jobID uuid.UUID) (*model.ExportJob, error)
	ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.ExportJob, error)
	CountActiveByUserSince(ctx context.Context, userID uuid.UUID, since time.Time) (int, error)
	CountInFlightByUser(ctx context.Context, userID uuid.UUID) (int, error)
	DeleteByUserAndID(ctx context.Context, userID, jobID uuid.UUID) error
	ListExpired(ctx context.Context, limit int) ([]model.ExportJob, error)
	MarkExpired(ctx context.Context, jobID uuid.UUID) error
}

// ExportProjectLister enumerates a user's owned projects (one per
// OwnerNamespaceID). Defined here as a tiny interface so the worker can be
// constructed without pulling the full storage.ProjectRepo type into the
// service layer's public surface.
type ExportProjectLister interface {
	ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error)
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// ExportUserReader fetches the user's NamespaceID so the worker can resolve
// owned-project listings. The handler validates the user before enqueueing;
// the worker re-fetches to keep the claim path self-contained.
type ExportUserReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// ExportJobService runs the asynchronous export pipeline. Enqueue stamps
// a new row; Run polls the queue and produces the artifact on disk;
// SweepExpired deletes stale artifacts on a slower cadence.
type ExportJobService struct {
	repo         ExportJobRepository
	users        ExportUserReader
	projects     ExportProjectLister
	exportSvc    *ExportService
	settings     *SettingsService
	workerID     string
	dataDir      string // resolved at construction so the default works without operator setup
	logger       *slog.Logger
	pollInterval time.Duration
	sweepEvery   time.Duration
}

// NewExportJobService wires the dependencies. workerID is the value
// written into export_jobs.claimed_by; the cmd/server boot uses a per-
// process UUID so claim-loss detection can distinguish workers across
// instances. dataDir is the fallback used when SettingExportArtifactDir
// is empty; pass the same value cmd/server uses for SQLite to keep the
// artifact tree next to the database file in dev.
func NewExportJobService(
	repo ExportJobRepository,
	users ExportUserReader,
	projects ExportProjectLister,
	exportSvc *ExportService,
	settings *SettingsService,
	workerID string,
	dataDir string,
	logger *slog.Logger,
) *ExportJobService {
	if logger == nil {
		logger = slog.Default()
	}
	return &ExportJobService{
		repo:         repo,
		users:        users,
		projects:     projects,
		exportSvc:    exportSvc,
		settings:     settings,
		workerID:     workerID,
		dataDir:      dataDir,
		logger:       logger,
		pollInterval: 2 * time.Second,
		sweepEvery:   1 * time.Hour,
	}
}

// EnqueueRequest is the validated payload for Enqueue. UserID is bound
// from the authenticated context; never from the wire.
type EnqueueRequest struct {
	UserID            uuid.UUID
	Scope             string
	ProjectID         *uuid.UUID
	Format            string
	IncludeSuperseded bool
}

// Enqueue validates the request, enforces rate limits and ownership, then
// writes the pending row. Returns the seeded model.ExportJob.
func (s *ExportJobService) Enqueue(ctx context.Context, req EnqueueRequest) (*model.ExportJob, error) {
	switch req.Scope {
	case model.ExportScopeAccount:
		if req.ProjectID != nil {
			return nil, fmt.Errorf("%w: project_id must be omitted for account scope", ErrExportJobBadRequest)
		}
		if req.Format == "" {
			req.Format = model.ExportFormatZip
		}
		if req.Format != model.ExportFormatZip {
			return nil, fmt.Errorf("%w: account-wide exports only support zip format", ErrExportJobBadRequest)
		}
	case model.ExportScopeProject:
		if req.ProjectID == nil {
			return nil, fmt.Errorf("%w: project_id is required for project scope", ErrExportJobBadRequest)
		}
		// Ownership check: project's owner_namespace_id must match the
		// user's namespace. Without it, a non-owner could enqueue an export
		// of any project ID they happen to know.
		project, err := s.projects.GetByID(ctx, *req.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("%w: project not found", ErrExportJobBadRequest)
		}
		user, err := s.users.GetByID(ctx, req.UserID)
		if err != nil {
			return nil, fmt.Errorf("%w: user lookup failed", ErrExportJobBadRequest)
		}
		if project.OwnerNamespaceID != user.NamespaceID {
			return nil, fmt.Errorf("%w: project not owned by caller", ErrExportJobBadRequest)
		}
		if req.Format == "" {
			req.Format = model.ExportFormatZip
		}
		if req.Format != model.ExportFormatZip && req.Format != model.ExportFormatJSON && req.Format != model.ExportFormatNDJSON {
			return nil, fmt.Errorf("%w: unsupported format %q", ErrExportJobBadRequest, req.Format)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported scope %q", ErrExportJobBadRequest, req.Scope)
	}

	// In-flight cap: 1 concurrent export per user, regardless of scope.
	inFlight, err := s.repo.CountInFlightByUser(ctx, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("count in-flight: %w", err)
	}
	if inFlight > 0 {
		return nil, ErrExportJobAlreadyRunning
	}

	// 24h rolling rate limit.
	maxPerDay := s.settings.ResolveIntWithDefault(ctx, SettingExportMaxPerUserPerDay, "global")
	if maxPerDay < 1 {
		maxPerDay = 5
	}
	since := time.Now().Add(-24 * time.Hour)
	count, err := s.repo.CountActiveByUserSince(ctx, req.UserID, since)
	if err != nil {
		return nil, fmt.Errorf("count active: %w", err)
	}
	if count >= maxPerDay {
		return nil, ErrExportJobRateLimited
	}

	job := &model.ExportJob{
		UserID:            req.UserID,
		Scope:             req.Scope,
		ProjectID:         req.ProjectID,
		Format:            req.Format,
		IncludeSuperseded: req.IncludeSuperseded,
	}
	if err := s.repo.Enqueue(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

// GetForUser returns a job scoped to userID. sql.ErrNoRows when missing
// or not owned by the caller.
func (s *ExportJobService) GetForUser(ctx context.Context, userID, jobID uuid.UUID) (*model.ExportJob, error) {
	return s.repo.GetByID(ctx, userID, jobID)
}

// ListForUser returns the caller's jobs ordered by created_at DESC.
func (s *ExportJobService) ListForUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.ExportJob, error) {
	return s.repo.ListByUser(ctx, userID, limit, offset)
}

// DeleteForUser removes a job and its on-disk artifact (best-effort).
// Returns sql.ErrNoRows when the job is missing or not owned by the caller.
func (s *ExportJobService) DeleteForUser(ctx context.Context, userID, jobID uuid.UUID) error {
	job, err := s.repo.GetByID(ctx, userID, jobID)
	if err != nil {
		return err
	}
	if job.ArtifactPath != nil && *job.ArtifactPath != "" {
		if err := os.Remove(*job.ArtifactPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logger.Warn("export job delete: remove artifact failed", "job_id", jobID, "path", *job.ArtifactPath, "error", err)
		}
	}
	return s.repo.DeleteByUserAndID(ctx, userID, jobID)
}

// Run drives the worker loop until ctx is canceled. One claim attempt per
// pollInterval; on hit, the job is processed before the next sleep. The
// sweep ticker fires on the slower sweepEvery cadence.
func (s *ExportJobService) Run(ctx context.Context) {
	pollTick := time.NewTicker(s.pollInterval)
	sweepTick := time.NewTicker(s.sweepEvery)
	defer pollTick.Stop()
	defer sweepTick.Stop()

	// Eager first sweep on startup so stale artifacts from a prior crash
	// are reclaimed before the user sees the export page.
	s.sweepOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTick.C:
			s.tickClaim(ctx)
		case <-sweepTick.C:
			s.sweepOnce(ctx)
		}
	}
}

// tickClaim attempts one claim. On hit, the job is processed inline so the
// worker can't be tricked into accumulating in-flight claims under heavy
// queue depth — slow but predictable.
func (s *ExportJobService) tickClaim(ctx context.Context) {
	job, err := s.repo.ClaimNext(ctx, s.workerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		s.logger.Warn("export job claim failed", "error", err)
		return
	}
	s.processJob(ctx, job)
}

// processJob produces the artifact and marks the row complete. Any error
// in the produce path is recorded via Fail(); the worker continues.
func (s *ExportJobService) processJob(ctx context.Context, job *model.ExportJob) {
	s.logger.Info("export job processing", "job_id", job.ID, "user_id", job.UserID, "scope", job.Scope)

	path, size, hash, err := s.produceArtifact(ctx, job)
	if err != nil {
		s.logger.Warn("export job produce failed", "job_id", job.ID, "error", err)
		// Best-effort artifact cleanup so a partial file does not linger.
		if path != "" {
			_ = os.Remove(path)
		}
		// Cap the persisted error so the DB write stays small and
		// internal paths don't leak into the user-facing row.
		errMsg := err.Error()
		if len(errMsg) > 500 {
			errMsg = errMsg[:500]
		}
		if ferr := s.repo.Fail(ctx, job.ID, s.workerID, errMsg); ferr != nil && !errors.Is(ferr, storage.ErrExportJobClaimLost) {
			s.logger.Warn("export job fail-write failed", "job_id", job.ID, "error", ferr)
		}
		return
	}

	ttl := time.Duration(s.settings.ResolveIntWithDefault(ctx, SettingExportTTLHours, "global")) * time.Hour
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	exp := time.Now().Add(ttl)
	if err := s.repo.Complete(ctx, job.ID, s.workerID, path, size, hash, exp); err != nil {
		s.logger.Warn("export job complete-write failed", "job_id", job.ID, "error", err)
		if errors.Is(err, storage.ErrExportJobClaimLost) {
			// Another worker rebooted with the same workerID and claimed
			// this job after we did. Drop our artifact so it isn't
			// orphaned on disk.
			_ = os.Remove(path)
		}
		return
	}
	s.logger.Info("export job succeeded", "job_id", job.ID, "bytes", size, "path", path)
}

// produceArtifact writes the zip to disk and returns the path, size, and
// sha256. Caller is responsible for marking the row succeeded/failed.
func (s *ExportJobService) produceArtifact(ctx context.Context, job *model.ExportJob) (path string, size int64, sha256Hex string, err error) {
	rootDir := s.resolveArtifactDir(ctx)
	userDir := filepath.Join(rootDir, job.UserID.String())
	if err := os.MkdirAll(userDir, 0o755); err != nil {
		return "", 0, "", fmt.Errorf("mkdir user dir: %w", err)
	}
	path = filepath.Join(userDir, job.ID.String()+".zip")

	f, err := os.Create(path)
	if err != nil {
		return "", 0, "", fmt.Errorf("create artifact: %w", err)
	}
	hasher := sha256.New()
	mw := io.MultiWriter(f, hasher)
	zw := zip.NewWriter(mw)

	if err := s.writeArtifactBody(ctx, job, zw); err != nil {
		_ = zw.Close()
		_ = f.Close()
		return path, 0, "", err
	}
	if err := zw.Close(); err != nil {
		_ = f.Close()
		return path, 0, "", fmt.Errorf("close zip: %w", err)
	}
	if err := f.Close(); err != nil {
		return path, 0, "", fmt.Errorf("close artifact: %w", err)
	}

	stat, err := os.Stat(path)
	if err != nil {
		return path, 0, "", fmt.Errorf("stat artifact: %w", err)
	}
	return path, stat.Size(), hex.EncodeToString(hasher.Sum(nil)), nil
}

// writeArtifactBody populates the zip with manifest.json + per-project
// payload files. The dispatch is one branch per scope; the per-project
// collection is delegated to ExportService.Export so the existing
// pagination + lineage + dedup logic stays in one place.
func (s *ExportJobService) writeArtifactBody(ctx context.Context, job *model.ExportJob, zw *zip.Writer) error {
	type manifestProject struct {
		ID                uuid.UUID `json:"id"`
		Name              string    `json:"name"`
		Slug              string    `json:"slug"`
		File              string    `json:"file"`
		MemoryCount       int       `json:"memory_count"`
		EntityCount       int       `json:"entity_count"`
		RelationshipCount int       `json:"relationship_count"`
	}
	type manifest struct {
		Version           string            `json:"version"`
		ExportedAt        time.Time         `json:"exported_at"`
		UserID            uuid.UUID         `json:"user_id"`
		Scope             string            `json:"scope"`
		IncludeSuperseded bool              `json:"include_superseded"`
		Projects          []manifestProject `json:"projects"`
	}

	var projects []model.Project
	switch job.Scope {
	case model.ExportScopeAccount:
		user, err := s.users.GetByID(ctx, job.UserID)
		if err != nil {
			return fmt.Errorf("user lookup: %w", err)
		}
		// ListByUser returns projects whose OwnerNamespaceID matches the
		// user's namespace — the user's owned set. Memories in the shared
		// global namespace are NOT included here; the global project is
		// shared across many users and emitting its full namespace would
		// leak others' content. A user-filtered global export is a
		// follow-up enhancement (requires plumbing UserID through
		// MemoryListFilters).
		projects, err = s.projects.ListByUser(ctx, user.NamespaceID)
		if err != nil {
			return fmt.Errorf("list projects: %w", err)
		}
	case model.ExportScopeProject:
		if job.ProjectID == nil {
			return fmt.Errorf("project scope job missing project_id")
		}
		project, err := s.projects.GetByID(ctx, *job.ProjectID)
		if err != nil {
			return fmt.Errorf("project lookup: %w", err)
		}
		projects = []model.Project{*project}
	default:
		return fmt.Errorf("unsupported scope %q", job.Scope)
	}

	m := manifest{
		Version:           "1.0",
		ExportedAt:        time.Now().UTC(),
		UserID:            job.UserID,
		Scope:             job.Scope,
		IncludeSuperseded: job.IncludeSuperseded,
		Projects:          make([]manifestProject, 0, len(projects)),
	}

	for _, p := range projects {
		req := &ExportRequest{
			ProjectID:         p.ID,
			Format:            ExportFormatJSON,
			IncludeSuperseded: job.IncludeSuperseded,
		}
		data, err := s.exportSvc.Export(ctx, req)
		if err != nil {
			return fmt.Errorf("export project %q: %w", p.Slug, err)
		}
		fileName := "projects/" + sanitizeFileSegment(p.Slug) + ".json"
		w, err := zw.Create(fileName)
		if err != nil {
			return fmt.Errorf("zip create %s: %w", fileName, err)
		}
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(data); err != nil {
			return fmt.Errorf("zip encode %s: %w", fileName, err)
		}
		m.Projects = append(m.Projects, manifestProject{
			ID:                p.ID,
			Name:              p.Name,
			Slug:              p.Slug,
			File:              fileName,
			MemoryCount:       data.Stats.MemoryCount,
			EntityCount:       data.Stats.EntityCount,
			RelationshipCount: data.Stats.RelationshipCount,
		})
	}

	// manifest.json last so it carries the final per-project counts.
	mw, err := zw.Create("manifest.json")
	if err != nil {
		return fmt.Errorf("zip create manifest: %w", err)
	}
	enc := json.NewEncoder(mw)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("zip encode manifest: %w", err)
	}
	return nil
}

// sweepOnce expires stale artifacts. Read-list, remove on disk, mark
// expired. The DB transition runs unconditionally — a transient rm
// failure (permissions, disk glitch) is logged but cannot pin the row in
// 'succeeded' forever and trigger re-sweeps. MarkExpired clears the
// artifact_path column, so the next reconciliation cycle is responsible
// for cleaning up any orphaned file on disk (or operator cleanup of the
// configured artifact_dir).
func (s *ExportJobService) sweepOnce(ctx context.Context) {
	expired, err := s.repo.ListExpired(ctx, 100)
	if err != nil {
		s.logger.Warn("export sweep list failed", "error", err)
		return
	}
	for _, job := range expired {
		if job.ArtifactPath != nil && *job.ArtifactPath != "" {
			if err := os.Remove(*job.ArtifactPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.logger.Warn("export sweep remove failed", "job_id", job.ID, "path", *job.ArtifactPath, "error", err)
				// Fall through to MarkExpired anyway — leaving the row
				// in 'succeeded' would cause every subsequent sweep to
				// re-attempt the same rm forever.
			}
		}
		if err := s.repo.MarkExpired(ctx, job.ID); err != nil {
			s.logger.Warn("export sweep mark-expired failed", "job_id", job.ID, "error", err)
		}
	}
}

// OpenArtifact returns the caller's artifact as an io.ReadCloser plus the
// job row. sql.ErrNoRows when the job is missing, not owned, or has no
// artifact (still pending/failed/expired) — the second return value is
// non-nil in the non-terminal cases so the handler can surface "job not
// ready" distinctly from "job not found."
func (s *ExportJobService) OpenArtifact(ctx context.Context, userID, jobID uuid.UUID) (io.ReadCloser, *model.ExportJob, error) {
	job, err := s.repo.GetByID(ctx, userID, jobID)
	if err != nil {
		return nil, nil, err
	}
	if job.Status != model.ExportStatusSucceeded || job.ArtifactPath == nil || *job.ArtifactPath == "" {
		return nil, job, sql.ErrNoRows
	}
	f, err := os.Open(*job.ArtifactPath)
	if err != nil {
		return nil, job, fmt.Errorf("open artifact: %w", err)
	}
	return f, job, nil
}

// resolveArtifactDir resolves the configured artifact root. Empty falls
// through to <dataDir>/exports so the default works without operator setup.
func (s *ExportJobService) resolveArtifactDir(ctx context.Context) string {
	configured := s.settings.ResolveStringWithDefault(ctx, SettingExportArtifactDir, "global")
	if configured != "" {
		return configured
	}
	base := s.dataDir
	if base == "" {
		base = "."
	}
	return filepath.Join(base, "exports")
}

// sanitizeFileSegment converts a project slug into a safe filename
// fragment. The slug regex already forbids the awkward chars but a defense
// in depth here protects against future slug-format changes from leaking
// path traversal into the archive.
func sanitizeFileSegment(s string) string {
	r := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		"..", "_",
		":", "_",
		"\x00", "_",
	)
	clean := strings.TrimSpace(r.Replace(s))
	if clean == "" {
		return "project"
	}
	return clean
}
