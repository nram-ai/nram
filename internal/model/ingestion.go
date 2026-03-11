package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type IngestionLog struct {
	ID          uuid.UUID       `json:"id"`
	NamespaceID uuid.UUID       `json:"namespace_id"`
	Source      string          `json:"source"`
	ContentHash *string         `json:"content_hash"`
	RawContent  string          `json:"raw_content"`
	MemoryIDs   []uuid.UUID     `json:"memory_ids"`
	Status      string          `json:"status"`
	Error       json.RawMessage `json:"error"`
	Metadata    json.RawMessage `json:"metadata"`
	CreatedAt   time.Time       `json:"created_at"`
}
