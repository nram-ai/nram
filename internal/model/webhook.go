package model

import (
	"time"

	"github.com/google/uuid"
)

type Webhook struct {
	ID           uuid.UUID  `json:"id"`
	URL          string     `json:"url"`
	Secret       *string    `json:"-"`
	Events       []string   `json:"events"`
	Scope        string     `json:"scope"`
	Active       bool       `json:"active"`
	LastFired    *time.Time `json:"last_fired"`
	LastStatus   *int       `json:"last_status"`
	FailureCount int        `json:"failure_count"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}
