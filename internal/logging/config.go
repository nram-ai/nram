package logging

import (
	"log/slog"
	"strings"
	"sync/atomic"
)

// DBConfig holds the runtime-tunable state for the database sink: whether
// capture is on and the minimum level that reaches the table. Both are read on
// every Handle call and updated from settings without a restart, so they use
// atomics rather than locks.
type DBConfig struct {
	enabled atomic.Bool
	level   atomic.Int32 // slog.Level
}

// NewDBConfig creates a DBConfig seeded with the given enabled flag and level.
func NewDBConfig(enabled bool, level slog.Level) *DBConfig {
	c := &DBConfig{}
	c.enabled.Store(enabled)
	c.level.Store(int32(level))
	return c
}

// SetEnabled toggles database capture.
func (c *DBConfig) SetEnabled(b bool) { c.enabled.Store(b) }

// SetLevel changes the minimum level written to the database.
func (c *DBConfig) SetLevel(l slog.Level) { c.level.Store(int32(l)) }

// admits reports whether a record at the given level should be written to the
// database given the current configuration.
func (c *DBConfig) admits(level slog.Level) bool {
	return c.enabled.Load() && level >= slog.Level(c.level.Load())
}

// ParseLevel maps a configured level name to a slog.Level, mirroring the
// mapping in cmd/server's configureLogger. Unknown values fall back to info.
func ParseLevel(name string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug", "trace":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// LevelName maps a slog.Level to the lowercase name persisted in
// log_entries.level (one of debug/info/warn/error).
func LevelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "error"
	case l >= slog.LevelWarn:
		return "warn"
	case l >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}
