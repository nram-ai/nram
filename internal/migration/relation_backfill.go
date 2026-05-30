package migration

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nram-ai/nram/internal/model"
)

// CanonicalizeRelations rewrites every relationships.relation to its canonical
// form (model.CanonicalRelation) and merges rows that collide on the unique key
// (namespace_id, source_id, target_id, relation, valid_from) once canonicalized,
// keeping the max weight. It is the one-time existing-data counterpart to the
// repo's write-time canonicalization: new writes are already canonical; this
// fixes rows written before that change so every consumer — including the admin
// graph visualization, which reads stored rows verbatim — sees clean, merged
// edges.
//
// It calls model.CanonicalRelation directly so the stored form is byte-identical
// to what the write path produces (no SQL-dialect reimplementation), exact on
// both SQLite and Postgres. Idempotent: a fully-canonical table produces zero
// changes, so it is safe to re-run. Returns the number of rows mutated
// (updates + deletes).
func CanonicalizeRelations(ctx context.Context, db *sql.DB, backend string) (int, error) {
	// Stream rows ordered by the collision cluster so memory stays bounded by a
	// single (ns, src, tgt, valid_from) group rather than the whole table, and
	// so the first row seen per canonical relation is the min-id survivor. The
	// mutations are collected and applied AFTER the read cursor closes, so we
	// never modify the table while a SELECT is live on it.
	rows, err := db.QueryContext(ctx, `
		SELECT id, namespace_id, source_id, target_id, valid_from, relation, weight
		FROM relationships
		ORDER BY namespace_id, source_id, target_id, valid_from, id`)
	if err != nil {
		return 0, fmt.Errorf("canonicalize relations: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type clusterRow struct {
		id       string
		relation string
		weight   float64
	}
	type pendingUpdate struct {
		id       string
		relation string
		weight   float64
	}
	var updates []pendingUpdate
	var deletes []string

	var clusterKey [4]string
	var cluster []clusterRow
	haveCluster := false

	// flush resolves the current (ns,src,tgt,valid_from) cluster: sub-group by
	// canonical relation, take the min-id row as survivor (rows arrive ordered
	// by id), raise its weight to the sub-group max, and mark the rest for
	// deletion. A survivor that is already canonical with the max weight yields
	// no work (idempotency).
	flush := func() {
		if len(cluster) == 0 {
			return
		}
		type sub struct {
			canon        string
			survivor     string
			maxWeight    float64
			losers       []string
			needsRewrite bool
		}
		order := make([]string, 0, len(cluster))
		subs := make(map[string]*sub, len(cluster))
		for _, r := range cluster {
			canon := model.CanonicalRelation(r.relation)
			s, ok := subs[canon]
			if !ok {
				subs[canon] = &sub{
					canon: canon, survivor: r.id, maxWeight: r.weight,
					// The survivor needs a row rewrite if its stored relation
					// isn't already canonical (a loser bumping maxWeight below
					// sets this too).
					needsRewrite: r.relation != canon,
				}
				order = append(order, canon)
				continue
			}
			s.losers = append(s.losers, r.id)
			if r.weight > s.maxWeight {
				s.maxWeight = r.weight
				s.needsRewrite = true // survivor's weight must rise to the group max
			}
		}
		for _, canon := range order {
			s := subs[canon]
			if s.needsRewrite {
				updates = append(updates, pendingUpdate{id: s.survivor, relation: s.canon, weight: s.maxWeight})
			}
			deletes = append(deletes, s.losers...)
		}
	}

	for rows.Next() {
		var id, ns, src, tgt, vf, rel string
		var w float64
		if err := rows.Scan(&id, &ns, &src, &tgt, &vf, &rel, &w); err != nil {
			return 0, fmt.Errorf("canonicalize relations: scan: %w", err)
		}
		key := [4]string{ns, src, tgt, vf}
		if !haveCluster || key != clusterKey {
			flush()
			cluster = cluster[:0]
			clusterKey = key
			haveCluster = true
		}
		cluster = append(cluster, clusterRow{id: id, relation: rel, weight: w})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("canonicalize relations: iterate: %w", err)
	}
	flush()
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("canonicalize relations: close cursor: %w", err)
	}

	if len(updates) == 0 && len(deletes) == 0 {
		return 0, nil
	}

	updSQL := "UPDATE relationships SET relation = ?, weight = ? WHERE id = ?"
	delSQL := "DELETE FROM relationships WHERE id = ?"
	if backend == "postgres" {
		updSQL = "UPDATE relationships SET relation = $1, weight = $2 WHERE id = $3"
		delSQL = "DELETE FROM relationships WHERE id = $1"
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("canonicalize relations: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Delete losers BEFORE updating survivors: a survivor taking the canonical
	// form could otherwise transiently collide on the unique key with a
	// not-yet-deleted variant in the same cluster.
	for _, id := range deletes {
		if _, err := tx.ExecContext(ctx, delSQL, id); err != nil {
			return 0, fmt.Errorf("canonicalize relations: delete %s: %w", id, err)
		}
	}
	for _, u := range updates {
		if _, err := tx.ExecContext(ctx, updSQL, u.relation, u.weight, u.id); err != nil {
			return 0, fmt.Errorf("canonicalize relations: update %s: %w", u.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("canonicalize relations: commit: %w", err)
	}
	return len(updates) + len(deletes), nil
}
