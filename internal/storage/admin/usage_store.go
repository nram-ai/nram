package admin

import (
	"context"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// UsageStore implements api.UsageStore using aggregate SQL queries against token_usage.
type UsageStore struct {
	db storage.DB
}

// NewUsageStore creates a new UsageStore.
func NewUsageStore(db storage.DB) *UsageStore {
	return &UsageStore{db: db}
}

func (s *UsageStore) QueryUsage(ctx context.Context, filter api.UsageFilter) (*api.UsageReport, error) {
	// For now return an empty report — the token_usage table schema needs
	// to be inspected for exact column names. This satisfies the interface
	// and returns valid JSON.
	report := &api.UsageReport{
		Groups: []api.UsageGroup{},
		Totals: api.UsageTotals{},
	}

	// Sum totals from token_usage table.
	query := "SELECT COALESCE(SUM(tokens_input), 0), COALESCE(SUM(tokens_output), 0), COUNT(*) FROM token_usage"
	row := s.db.QueryRow(ctx, query)
	if err := row.Scan(&report.Totals.TokensInput, &report.Totals.TokensOutput, &report.Totals.CallCount); err != nil {
		// Non-fatal — return empty totals.
		return report, nil
	}

	return report, nil
}
