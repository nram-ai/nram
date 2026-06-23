package dreaming

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// Marker the project-description phase stamps onto its backing memories and the
// shield (pruning / contradiction / paraphrase) reads back to leave them alone.
const (
	// descriptionTag tags the single backing memory per project so the phase
	// can find it with a tag-filtered list and so memory-browser users can
	// recognise it. It is the lookup key, kept stable across edits.
	descriptionTag = "project-description"

	// metadataKindKey / metadataKindValue identify a memory as a
	// project-description backing row regardless of its tags, so the dreaming
	// shield can exclude it from prune/supersede/merge by metadata alone.
	metadataKindKey   = "nram_kind"
	metadataKindValue = "project_description"

	// metadataProjectIDKey records the source project; metadataSourceUpdatedKey
	// records project.UpdatedAt at sync time so steady-state cycles cheap-skip.
	metadataProjectIDKey     = "project_id"
	metadataSourceUpdatedKey = "source_updated_at"
)

// projectDescriptionMarker is the cheap negative filter for isProjectDescription:
// a memory whose raw metadata bytes do not even contain the kind value cannot be
// a project description, so the hot-loop shield callers (prune / contradiction /
// paraphrase, one check per memory) skip the JSON unmarshal entirely for the
// overwhelming majority of rows.
var projectDescriptionMarker = []byte(metadataKindValue)

// isProjectDescription reports whether a memory is a system-owned project
// description backing row. The dreaming shield uses it to keep prune,
// contradiction-supersede, and paraphrase-merge phases off these rows: they
// are reconciled from the projects table, not user content, so dreaming must
// never delete or merge them out from under the phase that owns them.
func isProjectDescription(mem *model.Memory) bool {
	if mem == nil || !bytes.Contains(mem.Metadata, projectDescriptionMarker) {
		return false
	}
	kind, _ := decodeMetadata(mem.Metadata)[metadataKindKey].(string)
	return kind == metadataKindValue
}

// ProjectDescriptionPhase reconciles a project's description column into a
// single embedded backing memory in that project's namespace, so the
// authoritative, classification-dense description ("Ranshaw is a C++17
// elliptic-curve library") becomes recallable, citable, and decomposable
// through the ordinary pipeline instead of being invisible metadata.
//
// It runs every cycle but is near-no-op in steady state: a metadata stamp of
// project.UpdatedAt lets an unchanged description short-circuit after a single
// read. The first cycle for a project that has a description creates the
// backing memory (the backfill); a later description edit replaces it
// (soft-delete old + create new, reusing the two existing dream operations
// that already roll back, render, and count); clearing the description removes
// it. Reserved tiers (global, about_me) carry nram-managed boilerplate and are
// excluded.
//
// The backing memory mirrors the consolidation synthesis path: Origin=dream
// (it is authored by dreaming) and Enriched=true, which by the dream-recursion
// guard contract still embeds the row (vector-searchable) while producing no
// facts/entities/relationships. Zero LLM tokens are spent here; embedding
// happens later in the enrichment pipeline.
type ProjectDescriptionPhase struct {
	projects     ProjectDescriptionReader
	memories     DescriptionMemoryLister
	memWriter    DescriptionMemoryWriter
	enrichment   EnrichmentQueueWriter
	settings     SettingsResolver
	vectorPurger VectorPurger
}

// NewProjectDescriptionPhase constructs the phase. enrichment may be nil (the
// backing memory is still created, just not auto-embedded); vectorPurger is
// wired via AttachVectorPurger and nil is safe.
func NewProjectDescriptionPhase(
	projects ProjectDescriptionReader,
	memories DescriptionMemoryLister,
	memWriter DescriptionMemoryWriter,
	enrichment EnrichmentQueueWriter,
	settings SettingsResolver,
) *ProjectDescriptionPhase {
	return &ProjectDescriptionPhase{
		projects:   projects,
		memories:   memories,
		memWriter:  memWriter,
		enrichment: enrichment,
		settings:   settings,
	}
}

// AttachVectorPurger wires a VectorPurger so that removing or replacing a
// backing memory also drops its vector from the active store. Nil is safe and
// leaves the stale vector indexed until the retention sweep reclaims it.
func (p *ProjectDescriptionPhase) AttachVectorPurger(vp VectorPurger) {
	p.vectorPurger = vp
}

func (p *ProjectDescriptionPhase) Name() string { return model.DreamPhaseProjectDescSync }

func (p *ProjectDescriptionPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	var created, deleted, skipped, errs int
	defer func() {
		p.writePhaseSummary(ctx, logger, map[string]any{
			"created": created, "deleted": deleted, "skipped": skipped, "errors": errs,
		})
	}()
	// bump records the outcome of a remove/create call: an error counts toward
	// errs, success toward the given counter.
	bump := func(err error, ok *int) {
		if err != nil {
			errs++
		} else {
			*ok++
		}
	}

	proj, err := p.projects.GetByNamespaceID(ctx, cycle.NamespaceID)
	if err != nil || proj == nil {
		// A namespace with no owning project (or a transient read error) has
		// nothing to reconcile. Non-fatal; report and move on.
		if err != nil {
			slog.Warn("dreaming: project-description phase could not resolve project",
				"namespace", cycle.NamespaceID, "err", err)
		}
		return PhaseResult{}, nil
	}

	// Reserved tiers (global, about_me) carry nram-managed boilerplate that is
	// useless as a classification signal; never back them with a memory.
	if model.IsReservedProjectSlug(proj.Slug) {
		skipped++
		return PhaseResult{}, nil
	}

	desc := strings.TrimSpace(proj.Description)
	sourceStamp := proj.UpdatedAt.UTC().Format(time.RFC3339Nano)

	// At most one live backing memory per project, found by its stable tag.
	existing, lerr := p.memories.ListByNamespaceFiltered(ctx, cycle.NamespaceID,
		storage.MemoryListFilters{Tags: []string{descriptionTag}, HideSuperseded: true}, 1, 0)
	if lerr != nil {
		slog.Warn("dreaming: project-description phase list failed",
			"namespace", cycle.NamespaceID, "err", lerr)
		errs++
		return PhaseResult{}, nil
	}
	var current *model.Memory
	if len(existing) > 0 {
		current = &existing[0]
	}

	switch {
	case desc == "" && current == nil:
		// Nothing to do: blank description, no backing memory. ("skip if empty.")

	case desc == "":
		// Description cleared: remove the backing memory.
		bump(p.removeBacking(ctx, cycle, logger, current), &deleted)

	case current == nil:
		// First-cycle backfill: substantive description, no backing memory yet.
		bump(p.createBacking(ctx, cycle, logger, proj, desc, sourceStamp), &created)

	default:
		// Backing memory exists. Cheap-skip when the source is unchanged; the
		// stamp is the primary signal and the content compare is a safety net
		// for a row that lost its stamp.
		curStamp, _ := decodeMetadata(current.Metadata)[metadataSourceUpdatedKey].(string)
		if curStamp == sourceStamp && strings.TrimSpace(current.Content) == desc {
			skipped++
			break
		}
		// Description edited: replace (soft-delete old + create new).
		bump(p.removeBacking(ctx, cycle, logger, current), &deleted)
		bump(p.createBacking(ctx, cycle, logger, proj, desc, sourceStamp), &created)
	}

	if tracker := CycleTrackerFromContext(ctx); tracker != nil {
		tracker.EmitPhaseProgress(ctx, 1, 1, "projects")
	}
	return PhaseResult{}, nil
}

// createBacking writes the backing memory and enqueues enrichment so it embeds.
func (p *ProjectDescriptionPhase) createBacking(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter, proj *model.Project, desc, sourceStamp string) error {
	meta, err := json.Marshal(map[string]any{
		metadataKindKey:          metadataKindValue,
		metadataProjectIDKey:     proj.ID.String(),
		metadataSourceUpdatedKey: sourceStamp,
	})
	if err != nil {
		return err
	}

	mem := &model.Memory{
		ID:          uuid.New(),
		NamespaceID: cycle.NamespaceID,
		Content:     desc,
		Origin:      model.OriginDream,
		Confidence:  0.8,
		Importance:  0.5,
		Enriched:    true,
		Tags:        []string{descriptionTag},
		Metadata:    json.RawMessage(meta),
	}
	if cerr := p.memWriter.Create(ctx, mem); cerr != nil {
		slog.Warn("dreaming: project-description backing create failed",
			"project", proj.ID, "err", cerr)
		return cerr
	}

	_ = logger.LogOperation(ctx, model.DreamPhaseProjectDescSync, "",
		model.DreamOpMemoryCreated, "memory", mem.ID, nil, mem)

	// Enqueue augmentation + embedding so the row becomes vector-searchable,
	// exactly as consolidation does for its syntheses. Enriched=true keeps the
	// fact/entity-extraction phases off it (dream-recursion guard) while
	// embedding still runs.
	if p.enrichment != nil {
		now := time.Now().UTC()
		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    mem.ID,
			NamespaceID: cycle.NamespaceID,
			Status:      model.EnrichmentStatusPending,
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if _, eerr := p.enrichment.Enqueue(ctx, job); eerr != nil {
			slog.Warn("dreaming: project-description enrichment enqueue failed",
				"memory", mem.ID, "err", eerr)
		}
	}
	return nil
}

// removeBacking soft-deletes a backing memory and purges its vector. Logged as
// DreamOpMemoryDeleted so rollback restores it from the before-state snapshot.
func (p *ProjectDescriptionPhase) removeBacking(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter, mem *model.Memory) error {
	if derr := p.memWriter.SoftDelete(ctx, mem.ID, cycle.NamespaceID); derr != nil {
		slog.Warn("dreaming: project-description backing soft-delete failed",
			"memory", mem.ID, "err", derr)
		return derr
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseProjectDescSync, "",
		model.DreamOpMemoryDeleted, "memory", mem.ID, mem, nil)
	if p.vectorPurger != nil {
		if perr := p.vectorPurger.Delete(ctx, storage.VectorKindMemory, mem.ID); perr != nil {
			slog.Warn("dreaming: project-description vector purge failed",
				"memory", mem.ID, "err", perr)
		}
	}
	return nil
}

// writePhaseSummary records the per-phase stats as a phase_summary log row, the
// same mechanism the pruning phase uses; the entry is not counted in the cycle
// op tally but feeds the cycle-detail UI's per-phase metrics.
func (p *ProjectDescriptionPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any) {
	_ = logger.LogOperation(ctx, model.DreamPhaseProjectDescSync, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
