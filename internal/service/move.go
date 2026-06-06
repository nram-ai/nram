package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryByIDReader provides the single read needed to relocate a memory: fetch
// the source row so its content/tags/source/metadata/importance can be re-stored
// in the destination project.
type MemoryByIDReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

// StoreServicer is the subset of *StoreService the move path needs. Declared as
// an interface so the move service can be unit-tested without a full store.
type StoreServicer interface {
	Store(ctx context.Context, req *StoreRequest) (*StoreResponse, error)
}

// ForgetServicer is the subset of *ForgetService the move path needs.
type ForgetServicer interface {
	Forget(ctx context.Context, req *ForgetRequest) (*ForgetResponse, error)
}

// MoveRequest contains all parameters needed to move one or more memories from
// a source project to a destination project owned by the same caller.
//
// HardDelete is deliberately not a request field: a move always hard-deletes the
// source after a successful re-store. Leaving a soft-deleted shadow would strand
// the original's enrichment children and graph footprint in the source namespace.
type MoveRequest struct {
	SourceProjectID uuid.UUID   `json:"-"`
	TargetProjectID uuid.UUID   `json:"target_project_id"`
	MemoryIDs       []uuid.UUID `json:"-"`
	// Caller context (set by handler/middleware).
	UserID   *uuid.UUID `json:"-"`
	OrgID    *uuid.UUID `json:"-"`
	APIKeyID *uuid.UUID `json:"-"`
}

// MoveResult records the outcome of relocating a single memory: the original ID
// and the ID of the freshly created memory in the destination project.
type MoveResult struct {
	OldID uuid.UUID `json:"old_id"`
	NewID uuid.UUID `json:"new_id"`
}

// MoveResponse contains the result of a move operation.
type MoveResponse struct {
	Moved     int          `json:"moved"`
	Results   []MoveResult `json:"results"`
	LatencyMs int64        `json:"latency_ms"`
}

// MoveService relocates memories between projects by re-storing each source
// memory into the destination project and then hard-deleting the source. It
// composes the existing store and forget paths so the entity graph is rebuilt
// in the destination namespace and the source footprint is reaped via the
// forget cascade — no schema change and no denormalized-namespace_id bookkeeping.
//
// Ordering matters: each memory is STORED in the destination first and only then
// deleted from the source. A failure between the two steps therefore never loses
// data — at worst it leaves a transient duplicate that dedup or the user can
// reconcile, never an empty hole.
type MoveService struct {
	memories MemoryByIDReader
	projects ProjectRepository
	store    StoreServicer
	forget   ForgetServicer
}

// NewMoveService creates a MoveService from the existing store and forget
// services plus a memory reader and project lookup.
func NewMoveService(memories MemoryByIDReader, projects ProjectRepository, store StoreServicer, forget ForgetServicer) *MoveService {
	return &MoveService{
		memories: memories,
		projects: projects,
		store:    store,
		forget:   forget,
	}
}

// Move relocates every memory in req.MemoryIDs from the source project to the
// destination project. Caller-facing authorization (does the caller own both
// projects?) is enforced by the handler before this is called; Move re-verifies
// that each memory actually belongs to the source project's namespace so a
// forged ID in the body cannot pull a memory out of another project.
func (s *MoveService) Move(ctx context.Context, req *MoveRequest) (*MoveResponse, error) {
	start := time.Now()

	if req.SourceProjectID == uuid.Nil {
		return nil, fmt.Errorf("source project_id is required")
	}
	if req.TargetProjectID == uuid.Nil {
		return nil, fmt.Errorf("target_project_id is required")
	}
	if req.TargetProjectID == req.SourceProjectID {
		return nil, fmt.Errorf("target_project_id must differ from the source project")
	}
	if len(req.MemoryIDs) == 0 {
		return nil, fmt.Errorf("at least one memory id is required")
	}

	// Resolve the source project's namespace once so every memory can be checked
	// against it. A missing source project is a client error, not a per-memory
	// skip.
	srcProject, err := s.projects.GetByID(ctx, req.SourceProjectID)
	if err != nil {
		return nil, fmt.Errorf("source project not found: %w", err)
	}

	resp := &MoveResponse{Results: make([]MoveResult, 0, len(req.MemoryIDs))}

	for _, id := range req.MemoryIDs {
		mem, err := s.memories.GetByID(ctx, id)
		if err != nil {
			// Not found / unreadable — skip this ID rather than abort the batch.
			continue
		}

		// Ownership guard: the memory must live in the source project's
		// namespace. Mirrors the ForgetService guard so a body-supplied ID
		// cannot relocate a memory out of a project the caller named but does
		// not actually contain it.
		if mem.NamespaceID != srcProject.NamespaceID {
			continue
		}

		var source string
		if mem.Source != nil {
			source = *mem.Source
		}
		// "dream" (and any reserved source) is rejected by the store path. The
		// moved copy is a fresh user-origin memory regardless, so drop the
		// reserved label rather than failing the move.
		if isReservedSource(source) {
			source = ""
		}

		importance := mem.Importance
		storeResp, err := s.store.Store(ctx, &StoreRequest{
			ProjectID:  req.TargetProjectID,
			Content:    mem.Content,
			Source:     source,
			Tags:       mem.Tags,
			Importance: &importance,
			Metadata:   mem.Metadata,
			UserID:     req.UserID,
			OrgID:      req.OrgID,
			APIKeyID:   req.APIKeyID,
		})
		if err != nil {
			// Store failed — leave the source intact (no data loss) and skip.
			continue
		}

		// Store succeeded (or hit dedup in the destination). Now hard-delete the
		// source. A failure here leaves a duplicate, never a hole.
		if _, err := s.forget.Forget(ctx, &ForgetRequest{
			ProjectID:  req.SourceProjectID,
			MemoryID:   &id,
			HardDelete: true,
			UserID:     req.UserID,
			OrgID:      req.OrgID,
		}); err != nil {
			// The memory now exists in both projects. Surface nothing fatal: the
			// destination copy is valid; report it as moved so the caller can
			// retry the source cleanup if needed.
			resp.Results = append(resp.Results, MoveResult{OldID: id, NewID: storeResp.ID})
			resp.Moved++
			continue
		}

		resp.Results = append(resp.Results, MoveResult{OldID: id, NewID: storeResp.ID})
		resp.Moved++
	}

	resp.LatencyMs = time.Since(start).Milliseconds()
	return resp, nil
}
