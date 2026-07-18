package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nram-ai/nram/internal/maintenance"
	"github.com/nram-ai/nram/internal/periodic"
	"github.com/nram-ai/nram/internal/storage"
)

const (
	// metaLastFullVacuum is the system_meta key holding the RFC3339 timestamp
	// of the last completed full VACUUM, so a server restart does not re-run a
	// heavy vacuum before its interval has elapsed.
	metaLastFullVacuum = "sqlite.maintenance.last_full_vacuum_at"

	// sqliteVacuumOpID is the maintenance-registry op id for the full VACUUM.
	sqliteVacuumOpID = "sqlite-vacuum"

	// autoVacuumIncremental is the PRAGMA auto_vacuum value for INCREMENTAL mode.
	autoVacuumIncremental = 2
)

// SQLiteMaintenanceService runs background upkeep on the SQLite database file.
// It is SQLite-only: main constructs it only when the backend is SQLite, since
// Postgres reclaims and re-analyzes via its own autovacuum.
//
// Two independent loops run on the shared periodic primitive:
//   - a frequent, quiet light pass (incremental vacuum, PRAGMA optimize, WAL
//     checkpoint) that reclaims free pages and refreshes planner statistics;
//   - an occasional full VACUUM that compacts the file and returns space to the
//     OS. The full VACUUM takes an exclusive lock, so it raises a prominent
//     console/log banner and the shared maintenance-status flag while it runs,
//     and it is gated on a persisted timestamp so restarts do not re-trigger it.
type SQLiteMaintenanceService struct {
	db       storage.DB
	settings *SettingsService
	status   *maintenance.Registry

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSQLiteMaintenance constructs the service. status carries the EventBus, so
// no bus is passed here. settings may not be nil (intervals and the enable
// toggle are read from it on every tick).
func NewSQLiteMaintenance(db storage.DB, settings *SettingsService, status *maintenance.Registry) *SQLiteMaintenanceService {
	return &SQLiteMaintenanceService{db: db, settings: settings, status: status}
}

// Start launches the two background loops. It returns immediately; call Stop to
// shut them down. Each loop runs its pass once at startup, then re-resolves its
// interval each tick so admin-UI edits hot-reload without a restart.
func (s *SQLiteMaintenanceService) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Go(func() {
		periodic.Run(ctx, s.resolveLightInterval, func(ctx context.Context, _ bool) {
			s.lightPass(ctx)
		})
	})
	s.wg.Go(func() {
		periodic.Run(ctx, s.resolveFullInterval, func(ctx context.Context, _ bool) {
			s.fullPass(ctx)
		})
	})
}

// Stop cancels both loops and waits for them to finish. If a full VACUUM is in
// flight, the cancel interrupts it (modernc honors context cancellation) but
// Stop may still block up to the time SQLite needs to unwind the statement.
func (s *SQLiteMaintenanceService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *SQLiteMaintenanceService) enabled(ctx context.Context) bool {
	return s.settings.ResolveBoolWithDefault(ctx, SettingSqliteMaintEnabled, "global")
}

func (s *SQLiteMaintenanceService) resolveLightInterval(ctx context.Context) time.Duration {
	return max(s.settings.ResolveDurationSecondsWithDefault(ctx,
		SettingSqliteMaintLightIntervalSeconds, "global"), time.Second)
}

func (s *SQLiteMaintenanceService) resolveFullInterval(ctx context.Context) time.Duration {
	return max(s.settings.ResolveDurationSecondsWithDefault(ctx,
		SettingSqliteMaintFullVacuumIntervalSeconds, "global"), time.Second)
}

// lightPass runs the cheap, quiet upkeep on the write connection. Each step is
// best-effort: a failure (including a WAL checkpoint that can't truncate while
// readers are active) is logged and the pass continues.
func (s *SQLiteMaintenanceService) lightPass(ctx context.Context) {
	if !s.enabled(ctx) {
		return
	}

	wdb := s.db.WriteDB()

	// incremental_vacuum reclaims free pages, but only once the file is in
	// incremental auto-vacuum mode; it is a harmless no-op before conversion.
	if _, err := wdb.ExecContext(ctx, "PRAGMA incremental_vacuum"); err != nil {
		slog.Warn("sqlite maintenance: incremental_vacuum failed", "err", err)
	}
	// analysis_limit bounds the ANALYZE that optimize may run so it cannot do
	// an unbounded full scan on a large table (SQLite-documented pattern).
	if _, err := wdb.ExecContext(ctx, "PRAGMA analysis_limit=400"); err != nil {
		slog.Warn("sqlite maintenance: analysis_limit failed", "err", err)
	}
	if _, err := wdb.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		slog.Warn("sqlite maintenance: optimize failed", "err", err)
	}
	// wal_checkpoint(TRUNCATE) caps WAL growth. A busy result (active readers)
	// is reported in the result row, not as an error, so this only errors on a
	// real failure; either way we continue.
	if _, err := wdb.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		slog.Warn("sqlite maintenance: wal_checkpoint failed", "err", err)
	}

	slog.Debug("sqlite maintenance: light pass complete")
}

// fullPass runs a full VACUUM when it is due, or unconditionally when the file
// still needs its one-time conversion to incremental auto-vacuum mode.
func (s *SQLiteMaintenanceService) fullPass(ctx context.Context) {
	if !s.enabled(ctx) {
		return
	}

	needConvert, err := s.needsAutoVacuumConversion(ctx)
	if err != nil {
		slog.Warn("sqlite maintenance: auto_vacuum mode check failed", "err", err)
	}

	if !needConvert && !s.fullVacuumDue(ctx) {
		return
	}

	s.runFullVacuum(ctx, needConvert)
}

// needsAutoVacuumConversion reports whether the file is not yet in incremental
// auto-vacuum mode. A fresh database is born incremental via the DSN pragma; an
// existing database opened before this feature is still in NONE mode until the
// first full VACUUM converts it.
func (s *SQLiteMaintenanceService) needsAutoVacuumConversion(ctx context.Context) (bool, error) {
	var mode int
	if err := s.db.WriteDB().QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		return false, err
	}
	return mode != autoVacuumIncremental, nil
}

// fullVacuumDue reports whether at least the full-vacuum interval has elapsed
// since the last recorded run. A missing, unreadable, or unparseable marker is
// treated as due.
func (s *SQLiteMaintenanceService) fullVacuumDue(ctx context.Context) bool {
	raw, err := storage.GetSystemMeta(ctx, s.db, metaLastFullVacuum)
	if err != nil {
		slog.Warn("sqlite maintenance: read last-vacuum marker failed", "err", err)
		return true
	}
	if raw == "" {
		return true
	}
	last, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return true
	}
	return time.Since(last) >= s.resolveFullInterval(ctx)
}

// runFullVacuum executes VACUUM under the maintenance banner and status flag,
// then records the completion timestamp. A busy/failed VACUUM is logged and left
// for the next cycle; the timestamp is only advanced on success.
func (s *SQLiteMaintenanceService) runFullVacuum(ctx context.Context, converting bool) {
	end := s.status.Begin(sqliteVacuumOpID, "Database maintenance",
		"Database optimization running; performance may be temporarily reduced")
	defer end()

	reason := "scheduled"
	if converting {
		reason = "one-time conversion to incremental auto-vacuum"
	}

	rule := strings.Repeat("=", 72)
	banner := func(body string) { slog.Warn("\n" + rule + body + "\n" + rule) }

	banner("\n  SQLite maintenance: FULL VACUUM starting (" + reason + ")." +
		"\n  The database is briefly locked; write performance may be degraded" +
		"\n  until this completes. Do not interrupt the server.")

	start := time.Now()
	if _, err := s.db.WriteDB().ExecContext(ctx, "VACUUM"); err != nil {
		if ctx.Err() != nil {
			slog.Warn("sqlite maintenance: VACUUM interrupted by shutdown", "err", err)
			return
		}
		// A concurrent reader can hold the exclusive lock off past busy_timeout;
		// skip this cycle and retry on the next tick rather than spinning.
		slog.Warn("sqlite maintenance: VACUUM failed, will retry next cycle", "err", err)
		return
	}
	elapsed := time.Since(start)

	banner("\n  SQLite maintenance: FULL VACUUM complete in " + elapsed.Round(time.Millisecond).String() + ".")

	if err := storage.SetSystemMeta(ctx, s.db, metaLastFullVacuum,
		time.Now().UTC().Format(time.RFC3339)); err != nil {
		slog.Warn("sqlite maintenance: failed to record last-vacuum marker", "err", err)
	}
}
