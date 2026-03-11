package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Setting struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Scope     string          `json:"scope"`
	UpdatedBy *uuid.UUID      `json:"updated_by"`
	UpdatedAt time.Time       `json:"updated_at"`
}
