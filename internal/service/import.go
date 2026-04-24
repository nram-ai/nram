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
	Imported  int           `json:"imported"`
	Skipped   int           `json:"skipped"`
	Errors    []ImportError `json:"errors"`
	LatencyMs int64         `json:"latency_ms"`
}

// ImportError describes a per-item error during import.
type ImportError struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

// importItem is the internal normalized representation of an imported memory.
type importItem struct {
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
}

// NewImportService creates a new ImportService with the given dependencies.
func NewImportService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
) *ImportService {
	return &ImportService{
		memories:      memories,
		projects:      projects,
		namespaces:    namespaces,
		ingestionLogs: ingestionLogs,
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

	// Parse items based on format.
	var items []importItem
	switch req.Format {
	case ImportFormatNRAM:
		items, err = parseNRAMImport(rawData)
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
			confidence = 1.0
		}

		importance := item.Importance
		if importance <= 0 {
			importance = 0.5
		}
		mem := &model.Memory{
			ID:          memID,
			NamespaceID: ns.ID,
			Content:     item.Content,
			ContentHash: hash,
			Source:      item.Source,
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
		resp.Imported++
	}

	// Create ingestion log.
	status := "completed"
	if len(resp.Errors) > 0 && resp.Imported == 0 {
		status = "failed"
	} else if len(resp.Errors) > 0 {
		status = "partial"
	}

	logMeta, _ := json.Marshal(map[string]interface{}{
		"format":   req.Format,
		"imported": resp.Imported,
		"skipped":  resp.Skipped,
		"errors":   len(resp.Errors),
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

// parseNRAMImport handles both JSON (ExportData) and NDJSON formats.
func parseNRAMImport(data []byte) ([]importItem, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []importItem{}, nil
	}

	// Try JSON (ExportData) first. A valid ExportData JSON object is a single
	// top-level object. NDJSON has multiple lines each starting with '{'.
	// We distinguish by attempting a strict JSON unmarshal first.
	if strings.HasPrefix(trimmed, "{") {
		var export ExportData
		if err := json.Unmarshal(data, &export); err == nil {
			items := make([]importItem, 0, len(export.Memories))
			for _, m := range export.Memories {
				ts := m.CreatedAt
				items = append(items, importItem{
					Content:    m.Content,
					Tags:       m.Tags,
					Source:     m.Source,
					Metadata:   m.Metadata,
					Confidence: m.Confidence,
					Importance: m.Importance,
					CreatedAt:  &ts,
				})
			}
			return items, nil
		}
		// If JSON parse fails, fall through to NDJSON.
	}

	// NDJSON: read line by line.
	var items []importItem
	scanner := bufio.NewScanner(strings.NewReader(trimmed))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var rec ndjsonRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("invalid nram NDJSON line: %w", err)
		}

		if rec.Type != "memory" {
			continue
		}

		// Re-marshal and unmarshal the data field to ExportMemory.
		dataBytes, err := json.Marshal(rec.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal ndjson data: %w", err)
		}

		var m ExportMemory
		if err := json.Unmarshal(dataBytes, &m); err != nil {
			return nil, fmt.Errorf("failed to parse ndjson memory: %w", err)
		}

		ts := m.CreatedAt
		items = append(items, importItem{
			Content:    m.Content,
			Tags:       m.Tags,
			Source:     m.Source,
			Metadata:   m.Metadata,
			Confidence: m.Confidence,
			Importance: m.Importance,
			CreatedAt:  &ts,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading ndjson: %w", err)
	}

	if items == nil {
		items = []importItem{}
	}

	return items, nil
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
		items = append(items, importItem{
			Content:    m.Memory,
			Tags:       nil,
			Source:     &source,
			Metadata:   m.Metadata,
			Confidence: 1.0,
			Importance: 0.5,
			CreatedAt:  m.CreatedAt,
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
		items = append(items, importItem{
			Content:    m.Content,
			Tags:       tags,
			Source:     &source,
			Metadata:   m.Metadata,
			Confidence: 1.0,
			Importance: 0.5,
			CreatedAt:  m.CreatedAt,
		})
	}

	return items, nil
}
