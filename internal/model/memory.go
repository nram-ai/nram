package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Memory struct {
	ID           uuid.UUID `json:"id"`
	NamespaceID  uuid.UUID `json:"namespace_id"`
	Content      string    `json:"content"`
	EmbeddingDim *int      `json:"embedding_dim"`
	Source       *string   `json:"source"`
	// Origin is the coarse, server-assigned provenance category. Unlike Source
	// (a free-form label), Origin is the authoritative discriminator internal
	// logic branches on (e.g. the dream-recursion guard). It is never settable
	// from request input. See MemoryOrigin in origin.go.
	Origin       MemoryOrigin    `json:"origin"`
	Tags         []string        `json:"tags"`
	Confidence   float64         `json:"confidence"`
	Importance   float64         `json:"importance"`
	AccessCount  int             `json:"access_count"`
	LastAccessed *time.Time      `json:"last_accessed"`
	ExpiresAt    *time.Time      `json:"expires_at"`
	SupersededBy *uuid.UUID      `json:"superseded_by"`
	SupersededAt *time.Time      `json:"superseded_at"`
	Enriched     bool            `json:"enriched"`
	Metadata     json.RawMessage `json:"metadata"`
	ContentHash  string          `json:"content_hash,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	DeletedAt    *time.Time      `json:"deleted_at"`
	PurgeAfter   *time.Time      `json:"purge_after"`

	// Query-augmentation state. AugmentedQueries is the paraphrase set the
	// query-augmentation enrichment phase generated and fed into the embedder
	// alongside Content. AugmentedEmbeddingAt records when the augmented
	// embedding was last written; both NULL means the row was embedded against
	// raw Content. The backfill endpoint uses AugmentedEmbeddingAt IS NULL to
	// find pre-flag memories that need re-embedding.
	AugmentedQueries     []string   `json:"augmented_queries,omitempty"`
	AugmentedEmbeddingAt *time.Time `json:"augmented_embedding_at,omitempty"`

	// Derived from lineage table at read time; not persisted in memories table.
	ParentID *uuid.UUID `json:"parent_id,omitempty"`

	// Populated by the parent-anchored list endpoint when ?group_by_parent=true.
	// The slice carries enrichment-derived child memories (extracted_fact,
	// synthesized_from, extracted_from). Omitted entirely from JSON when nil
	// so non-grouped responses (recall, detail, default list) stay unchanged.
	Children []Memory `json:"children,omitempty"`
}

type SystemMeta struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
