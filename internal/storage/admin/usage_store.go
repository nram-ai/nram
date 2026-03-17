package admin

import (
	"context"
	"fmt"
	"strings"

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
	report := &api.UsageReport{
		Groups: []api.UsageGroup{},
		Totals: api.UsageTotals{},
	}

	// Build WHERE clauses from filter.
	var conditions []string
	var args []interface{}
	argIdx := 1

	if filter.OrgID != nil {
		conditions = append(conditions, s.placeholder("org_id", argIdx))
		args = append(args, filter.OrgID.String())
		argIdx++
	}
	if filter.UserID != nil {
		conditions = append(conditions, s.placeholder("user_id", argIdx))
		args = append(args, filter.UserID.String())
		argIdx++
	}
	if filter.ProjectID != nil {
		conditions = append(conditions, s.placeholder("project_id", argIdx))
		args = append(args, filter.ProjectID.String())
		argIdx++
	}
	if filter.From != nil {
		conditions = append(conditions, s.placeholder("created_at >=", argIdx))
		args = append(args, filter.From.Format("2006-01-02T15:04:05Z"))
		argIdx++
	}
	if filter.To != nil {
		conditions = append(conditions, s.placeholder("created_at <=", argIdx))
		args = append(args, filter.To.Format("2006-01-02T15:04:05Z"))
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	// Sum totals.
	query := "SELECT COALESCE(SUM(tokens_input), 0), COALESCE(SUM(tokens_output), 0), COUNT(*) FROM token_usage" + whereClause
	row := s.db.QueryRow(ctx, query, args...)
	if err := row.Scan(&report.Totals.TokensInput, &report.Totals.TokensOutput, &report.Totals.CallCount); err != nil {
		return report, nil
	}

	// Grouped aggregation.
	groupCol := s.groupByColumn(filter.GroupBy)
	if groupCol != "" {
		groupQuery := fmt.Sprintf(
			"SELECT COALESCE(%s, ''), COALESCE(SUM(tokens_input), 0), COALESCE(SUM(tokens_output), 0), COUNT(*) FROM token_usage%s GROUP BY %s ORDER BY COUNT(*) DESC LIMIT 100",
			groupCol, whereClause, groupCol,
		)
		rows, err := s.db.Query(ctx, groupQuery, args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var g api.UsageGroup
				if err := rows.Scan(&g.Key, &g.TokensInput, &g.TokensOutput, &g.CallCount); err != nil {
					continue
				}
				report.Groups = append(report.Groups, g)
			}
		}
	}

	return report, nil
}

// placeholder returns a condition string with the correct placeholder for the backend.
func (s *UsageStore) placeholder(column string, idx int) string {
	// Handle operators like "created_at >="
	if strings.Contains(column, " ") {
		if s.db.Backend() == storage.BackendPostgres {
			return fmt.Sprintf("%s $%d", column, idx)
		}
		return column + " ?"
	}
	if s.db.Backend() == storage.BackendPostgres {
		return fmt.Sprintf("%s = $%d", column, idx)
	}
	return column + " = ?"
}

// groupByColumn maps the filter group_by value to a SQL column name.
func (s *UsageStore) groupByColumn(groupBy string) string {
	switch groupBy {
	case "org":
		return "org_id"
	case "user":
		return "user_id"
	case "project":
		return "project_id"
	case "operation":
		return "operation"
	case "model":
		return "model"
	default:
		return "operation"
	}
}
