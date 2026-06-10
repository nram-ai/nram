package service

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// ImportFormat defines the source format for data import.
type ImportFormat string

const (
	ImportFormatNRAM ImportFormat = "nram"
	ImportFormatMem0 ImportFormat = "mem0"
	ImportFormatZep  ImportFormat = "zep"
)

// ImportRequest contains the parameters for a project data import.
type ImportRequest struct {
	ProjectID uuid.UUID
	Format    ImportFormat
	Data      io.Reader
}

// ImportResponse contains the result of a data import operation.
type ImportResponse struct {
	Imported              int           `json:"imported"`
	Skipped               int           `json:"skipped"`
	EntitiesImported      int           `json:"entities_imported"`
	RelationshipsImported int           `json:"relationships_imported"`
	LineageImported       int           `json:"lineage_imported"`
	Errors                []ImportError `json:"errors"`
	LatencyMs             int64         `json:"latency_ms"`
}

// ImportError describes a per-item error during import.
type ImportError struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

// importItem is the internal normalized representation of an imported memory.
type importItem struct {
	// ExportID is the memory's ID in the source export, used to remap
	// relationship provenance to the re-created memory. Zero for formats
	// (mem0/zep) that carry no graph.
	ExportID   uuid.UUID
	Content    string
	Tags       []string
	Source     *string
	Metadata   json.RawMessage
	Confidence float64
	Importance float64
	CreatedAt  *time.Time
}

// ImportService orchestrates importing memories from various formats.
type ImportService struct {
	memories      MemoryRepository
	projects      ProjectRepository
	namespaces    NamespaceRepository
	ingestionLogs IngestionLogRepository
	entities      EntityCreator
	relationships RelationshipCreator
	lineage       LineageCreator
	settings      *SettingsService
}

// NewImportService creates a new ImportService with the given dependencies.
// settings may be nil; the importance/confidence defaults fall back to the
// values registered in service.settingDefaults. entities, relationships, and
// lineage may be nil; when entities or relationships is nil the nram-format
// graph round-trip is skipped, and when lineage is nil memory-lineage reimport
// is skipped. In all cases memories themselves still import.
func NewImportService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
	entities EntityCreator,
	relationships RelationshipCreator,
	lineage LineageCreator,
	settings *SettingsService,
) *ImportService {
	return &ImportService{
		memories:      memories,
		projects:      projects,
		namespaces:    namespaces,
		ingestionLogs: ingestionLogs,
		entities:      entities,
		relationships: relationships,
		lineage:       lineage,
		settings:      settings,
	}
}

// Import parses the incoming data in the specified format and persists memories.
func (s *ImportService) Import(ctx context.Context, req *ImportRequest) (*ImportResponse, error) {
	start := time.Now()

	if req == nil {
		return nil, fmt.Errorf("import request is required")
	}
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if req.Format == "" {
		return nil, fmt.Errorf("format is required")
	}
	if req.Data == nil {
		return nil, fmt.Errorf("data is required")
	}

	// Validate format.
	switch req.Format {
	case ImportFormatNRAM, ImportFormatMem0, ImportFormatZep:
		// valid
	default:
		return nil, fmt.Errorf("unsupported import format %q", req.Format)
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Look up namespace.
	ns, err := s.namespaces.GetByID(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("namespace not found: %w", err)
	}

	// Read all data.
	rawData, err := io.ReadAll(req.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to read import data: %w", err)
	}

	// Parse items based on format. Only the nram format carries graph data
	// (entities, relationships, lineage); the others yield memories alone.
	var items []importItem
	var exportEntities []ExportEntity
	var exportRels []ExportRelationship
	var exportLineage []ExportLineage
	switch req.Format {
	case ImportFormatNRAM:
		items, exportEntities, exportRels, exportLineage, err = parseNRAMImport(rawData)
	case ImportFormatMem0:
		items, err = parseMem0Import(rawData)
	case ImportFormatZep:
		items, err = parseZepImport(rawData)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s data: %w", req.Format, err)
	}

	resp := &ImportResponse{
		Errors: []ImportError{},
	}

	var createdIDs []uuid.UUID
	// memMap maps each export memory ID to the memory ID it resolved to in this
	// namespace (a newly-created row, or the existing row on a dedup hit) so
	// relationship provenance and lineage parents can be remapped to a live row.
	memMap := make(map[uuid.UUID]uuid.UUID)
	// createdExportIDs holds the export IDs of memories newly created this run.
	// Lineage is rebuilt only for these so re-importing the same file does not
	// duplicate lineage rows (that table has no unique constraint).
	createdExportIDs := make(map[uuid.UUID]bool)

	for i, item := range items {
		// Validate content.
		if strings.TrimSpace(item.Content) == "" {
			resp.Skipped++
			resp.Errors = append(resp.Errors, ImportError{
				Index:   i,
				Message: "empty content",
			})
			continue
		}

		hash := storage.HashContent(item.Content)

		// Skip imports whose content already exists in the namespace. The
		// importer is the most likely producer of duplicates because operators
		// re-run imports with overlapping data.
		existing, lookupErr := s.memories.LookupByContentHash(ctx, ns.ID, hash)
		if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
			resp.Skipped++
			resp.Errors = append(resp.Errors, ImportError{
				Index:   i,
				Message: fmt.Sprintf("dedup lookup: %v", lookupErr),
			})
			continue
		}
		if existing != nil {
			slog.Info("import: dedup hit",
				"namespace", ns.ID, "index", i, "memory", existing.ID, "hash", hash)
			if item.ExportID != uuid.Nil {
				memMap[item.ExportID] = existing.ID
			}
			resp.Skipped++
			continue
		}

		memID := uuid.New()
		now := time.Now()

		createdAt := now
		if item.CreatedAt != nil {
			createdAt = *item.CreatedAt
		}

		confidence := item.Confidence
		if confidence <= 0 {
			confidence = resolveDefaultConfidence(ctx, s.settings)
		}

		importance := item.Importance
		if importance <= 0 {
			importance = resolveDefaultImportance(ctx, s.settings)
		}
		// Drop a reserved "dream" source carried by legacy export data; the row
		// is classified by Origin=OriginImport, never the reserved string.
		src := item.Source
		if src != nil && isReservedSource(*src) {
			src = nil
		}
		mem := &model.Memory{
			ID:          memID,
			NamespaceID: ns.ID,
			Content:     item.Content,
			ContentHash: hash,
			Source:      src,
			Origin:      model.OriginImport,
			Tags:        item.Tags,
			Confidence:  confidence,
			Importance:  importance,
			Metadata:    item.Metadata,
			CreatedAt:   createdAt,
			UpdatedAt:   now,
		}

		if err := s.memories.Create(ctx, mem); err != nil {
			resp.Skipped++
			resp.Errors = append(resp.Errors, ImportError{
				Index:   i,
				Message: fmt.Sprintf("failed to create memory: %v", err),
			})
			continue
		}

		createdIDs = append(createdIDs, memID)
		if item.ExportID != uuid.Nil {
			memMap[item.ExportID] = memID
			createdExportIDs[item.ExportID] = true
		}
		resp.Imported++
	}

	// Round-trip the knowledge graph for nram exports: recreate entities, then
	// relationships with their endpoints remapped to the entities created in
	// this namespace. Skipped when the graph repos are not wired (nil) or the
	// export carried no graph data (mem0/zep never do).
	if req.Format == ImportFormatNRAM && s.entities != nil && s.relationships != nil && len(exportEntities) > 0 {
		s.importGraph(ctx, ns.ID, exportEntities, exportRels, memMap, resp)
	}

	// Round-trip memory lineage (parent/child derivation links) for nram exports.
	if req.Format == ImportFormatNRAM && s.lineage != nil && len(exportLineage) > 0 {
		s.importLineage(ctx, ns.ID, exportLineage, memMap, createdExportIDs, resp)
	}

	// Create ingestion log.
	status := "completed"
	if len(resp.Errors) > 0 && resp.Imported == 0 {
		status = "failed"
	} else if len(resp.Errors) > 0 {
		status = "partial"
	}

	logMeta, _ := json.Marshal(map[string]any{
		"format":                 req.Format,
		"imported":               resp.Imported,
		"skipped":                resp.Skipped,
		"entities_imported":      resp.EntitiesImported,
		"relationships_imported": resp.RelationshipsImported,
		"lineage_imported":       resp.LineageImported,
		"errors":                 len(resp.Errors),
	})

	ingLog := &model.IngestionLog{
		ID:          uuid.New(),
		NamespaceID: ns.ID,
		Source:      fmt.Sprintf("%s-import", req.Format),
		MemoryIDs:   createdIDs,
		Status:      status,
		Metadata:    logMeta,
		CreatedAt:   time.Now(),
	}
	_ = s.ingestionLogs.Create(ctx, ingLog)

	resp.LatencyMs = time.Since(start).Milliseconds()

	return resp, nil
}

// importGraph recreates the exported entities and relationships in the target
// namespace. Entities are upserted (deduplicated by canonical + type within the
// namespace) and a map from each export entity ID to its persisted ID is built
// so relationship endpoints can be remapped. Relationships whose endpoints are
// missing from the export's entity set, or whose source memory was not imported,
// are dropped and counted as skipped.
func (s *ImportService) importGraph(
	ctx context.Context,
	namespaceID uuid.UUID,
	entities []ExportEntity,
	rels []ExportRelationship,
	memMap map[uuid.UUID]uuid.UUID,
	resp *ImportResponse,
) {
	idMap := make(map[uuid.UUID]uuid.UUID, len(entities))
	for _, e := range entities {
		canonical := e.Canonical
		if strings.TrimSpace(canonical) == "" {
			canonical = e.Name
		}
		ent := &model.Entity{
			// Seed a fresh ID; Upsert overwrites it with the persisted row's ID
			// on a dedup hit (same namespace + canonical + type), so the map
			// always points at the real stored entity.
			ID:           uuid.New(),
			NamespaceID:  namespaceID,
			Name:         e.Name,
			Canonical:    canonical,
			EntityType:   e.Type,
			Properties:   e.Properties,
			MentionCount: e.MentionCount,
		}
		if err := s.entities.Upsert(ctx, ent); err != nil {
			resp.Errors = append(resp.Errors, ImportError{
				Index:   -1,
				Message: fmt.Sprintf("failed to upsert entity %q: %v", e.Name, err),
			})
			continue
		}
		idMap[e.ID] = ent.ID
		resp.EntitiesImported++
	}

	if len(rels) == 0 {
		return
	}

	skip := func(reason string) {
		resp.Skipped++
		resp.Errors = append(resp.Errors, ImportError{Index: -1, Message: reason})
	}

	candidates := make([]*model.Relationship, 0, len(rels))
	for _, r := range rels {
		srcID, srcOK := idMap[r.SourceID]
		tgtID, tgtOK := idMap[r.TargetID]
		if !srcOK || !tgtOK {
			skip(fmt.Sprintf("relationship %q skipped: endpoint entity not in export", r.Relation))
			continue
		}

		// Provenance must point at a live memory: an edge with NULL source_memory
		// (or one pointing at a memory absent from the export) is treated as lost
		// provenance and reaped, and is hidden by every graph read path. Such an
		// edge was already lost-provenance in the source, so drop it rather than
		// fabricate a link to an unrelated memory.
		if r.SourceMemory == nil {
			skip(fmt.Sprintf("relationship %q skipped: edge has no source memory", r.Relation))
			continue
		}
		sourceMem, ok := memMap[*r.SourceMemory]
		if !ok {
			skip(fmt.Sprintf("relationship %q skipped: source memory not imported", r.Relation))
			continue
		}
		// sourceMem is a fresh per-iteration local, so &sourceMem is a distinct
		// pointer for each relationship.
		candidates = append(candidates, &model.Relationship{
			ID:           uuid.New(),
			NamespaceID:  namespaceID,
			SourceID:     srcID,
			TargetID:     tgtID,
			Relation:     r.Relation,
			Weight:       r.Weight,
			SourceMemory: &sourceMem,
			ValidFrom:    r.ValidFrom,
			ValidUntil:   r.ValidUntil,
		})
	}

	if len(candidates) == 0 {
		return
	}

	result, err := s.relationships.BatchCreate(ctx, candidates)
	if err != nil {
		resp.Errors = append(resp.Errors, ImportError{
			Index:   -1,
			Message: fmt.Sprintf("failed to import relationships: %v", err),
		})
		return
	}
	resp.RelationshipsImported += int(result.Affected)
	resp.Skipped += int(result.Skipped)
}

// importLineage recreates memory-to-memory lineage edges (e.g. synthesized_from,
// extracted_from), remapping both endpoints through memMap. Each export edge is
// an explicit child -> parent link. An edge is recreated only when its child was
// newly created this run (so a re-import does not duplicate rows; the lineage
// table has no unique constraint) and its parent also resolves to an imported
// memory. The export does not carry the lineage Context, so reconstructed rows
// get the default empty context.
func (s *ImportService) importLineage(
	ctx context.Context,
	namespaceID uuid.UUID,
	lineage []ExportLineage,
	memMap map[uuid.UUID]uuid.UUID,
	createdExportIDs map[uuid.UUID]bool,
	resp *ImportResponse,
) {
	for _, e := range lineage {
		if !createdExportIDs[e.MemoryID] || e.ParentID == nil {
			continue
		}
		parentID, ok := memMap[*e.ParentID]
		if !ok {
			continue
		}
		childID := memMap[e.MemoryID]
		if err := s.lineage.Create(ctx, &model.MemoryLineage{
			NamespaceID: namespaceID,
			MemoryID:    childID,
			ParentID:    &parentID,
			Relation:    e.Relation,
		}); err != nil {
			resp.Errors = append(resp.Errors, ImportError{
				Index:   -1,
				Message: fmt.Sprintf("failed to import lineage (%s): %v", e.Relation, err),
			})
			continue
		}
		resp.LineageImported++
	}
}

// exportMemoryToItem maps an exported memory to the importer's normalized item.
func exportMemoryToItem(m ExportMemory) importItem {
	ts := m.CreatedAt
	return importItem{
		ExportID:   m.ID,
		Content:    m.Content,
		Tags:       m.Tags,
		Source:     m.Source,
		Metadata:   m.Metadata,
		Confidence: m.Confidence,
		Importance: m.Importance,
		CreatedAt:  &ts,
	}
}

// parseNRAMImport handles both JSON (ExportData) and NDJSON formats. It returns
// the memories, entities, relationships, and lineage edges carried by the export
// so the caller can round-trip the full knowledge graph.
func parseNRAMImport(data []byte) ([]importItem, []ExportEntity, []ExportRelationship, []ExportLineage, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []importItem{}, nil, nil, nil, nil
	}

	// Try JSON (ExportData) first. A valid ExportData JSON object is a single
	// top-level object. NDJSON has multiple lines each starting with '{'.
	// We distinguish by attempting a strict JSON unmarshal first.
	if strings.HasPrefix(trimmed, "{") {
		var export ExportData
		if err := json.Unmarshal(data, &export); err == nil {
			items := make([]importItem, 0, len(export.Memories))
			for _, m := range export.Memories {
				items = append(items, exportMemoryToItem(m))
			}
			return items, export.Entities, export.Relationships, export.Lineage, nil
		}
		// If JSON parse fails, fall through to NDJSON.
	}

	// NDJSON: read line by line.
	var items []importItem
	var entities []ExportEntity
	var rels []ExportRelationship
	var lineage []ExportLineage
	scanner := bufio.NewScanner(strings.NewReader(trimmed))
	// Export lines (notably properties/metadata payloads) can exceed the
	// default 64KB token cap, so grow the buffer to tolerate large records.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Keep the data field raw so it decodes straight into the typed record
		// for this line's kind, without an intermediate marshal round-trip.
		var rec struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("invalid nram NDJSON line: %w", err)
		}

		switch rec.Type {
		case "memory":
			var m ExportMemory
			if err := json.Unmarshal(rec.Data, &m); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("failed to parse ndjson memory: %w", err)
			}
			items = append(items, exportMemoryToItem(m))
		case "entity":
			var e ExportEntity
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("failed to parse ndjson entity: %w", err)
			}
			entities = append(entities, e)
		case "relationship":
			var r ExportRelationship
			if err := json.Unmarshal(rec.Data, &r); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("failed to parse ndjson relationship: %w", err)
			}
			rels = append(rels, r)
		case "lineage":
			var l ExportLineage
			if err := json.Unmarshal(rec.Data, &l); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("failed to parse ndjson lineage: %w", err)
			}
			lineage = append(lineage, l)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("error reading ndjson: %w", err)
	}

	if items == nil {
		items = []importItem{}
	}

	return items, entities, rels, lineage, nil
}

// mem0Export represents the Mem0 export format.
type mem0Export struct {
	Results []mem0Memory `json:"results"`
}

// mem0Memory is a single memory in Mem0 format.
type mem0Memory struct {
	ID        string          `json:"id"`
	Memory    string          `json:"memory"`
	Hash      string          `json:"hash"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt *time.Time      `json:"created_at"`
	UpdatedAt *time.Time      `json:"updated_at"`
}

func parseMem0Import(data []byte) ([]importItem, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []importItem{}, nil
	}

	var export mem0Export
	if err := json.Unmarshal(data, &export); err != nil {
		return nil, fmt.Errorf("invalid mem0 JSON: %w", err)
	}

	source := "mem0-import"
	items := make([]importItem, 0, len(export.Results))
	for _, m := range export.Results {
		// Confidence/Importance left zero so the main import loop applies
		// the registered defaults via resolveDefault{Confidence,Importance}.
		items = append(items, importItem{
			Content:   m.Memory,
			Tags:      nil,
			Source:    &source,
			Metadata:  m.Metadata,
			CreatedAt: m.CreatedAt,
		})
	}

	return items, nil
}

// zepExport represents the Zep export format.
type zepExport struct {
	Messages []zepMessage `json:"messages"`
}

// zepMessage is a single message in Zep format.
type zepMessage struct {
	UUID      string          `json:"uuid"`
	Role      string          `json:"role"`
	Content   string          `json:"content"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt *time.Time      `json:"created_at"`
}

func parseZepImport(data []byte) ([]importItem, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []importItem{}, nil
	}

	var export zepExport
	if err := json.Unmarshal(data, &export); err != nil {
		return nil, fmt.Errorf("invalid zep JSON: %w", err)
	}

	source := "zep-import"
	items := make([]importItem, 0, len(export.Messages))
	for _, m := range export.Messages {
		var tags []string
		if m.Role != "" {
			tags = []string{m.Role}
		}
		// Confidence/Importance left zero so the main import loop applies
		// the registered defaults via resolveDefault{Confidence,Importance}.
		items = append(items, importItem{
			Content:   m.Content,
			Tags:      tags,
			Source:    &source,
			Metadata:  m.Metadata,
			CreatedAt: m.CreatedAt,
		})
	}

	return items, nil
}
