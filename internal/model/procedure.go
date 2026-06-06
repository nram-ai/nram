package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ProceduralEntry is a verbatim item in the procedural memory tier: a standing
// instruction or operating rule scoped to a user's root namespace.
//
// It is deliberately NOT a Memory and NOT a Project. Unlike Memory it is never
// embedded, enriched, consolidated, or surfaced by recall; it is fetched whole
// and verbatim by procedural_fetch. The tier is implicit (no container row): a
// user "has" the tier the moment the table exists, and it cannot be deleted as
// a whole; only individual entries are CRUD-able.
type ProceduralEntry struct {
	ID          uuid.UUID `json:"id"`
	NamespaceID uuid.UUID `json:"namespace_id"`
	Content     string    `json:"content"`
	Title       string    `json:"title"`
	Category    string    `json:"category"`
	Tags        []string  `json:"tags"`
	// Priority orders entries in the fetch result (DESC), with created_at DESC
	// as the tiebreak. Higher is returned earlier.
	Priority int `json:"priority"`
	// Enabled gates whether procedural_fetch returns the entry. Disabled
	// entries stay stored and manageable through the REST and UI surfaces but
	// are omitted from the fetch payload. Whether a returned entry is acted on
	// is the consuming agent's concern, not nram's.
	Enabled   bool            `json:"enabled"`
	Origin    string          `json:"origin"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	DeletedAt *time.Time      `json:"deleted_at,omitempty"`
}
