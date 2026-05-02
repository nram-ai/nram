package service

import (
	"context"
	"time"

	"github.com/nram-ai/nram/internal/auth"
)

// SettingsBackedSessionTimings implements auth.SessionTimings by reading the
// session-JWT TTL and refresh threshold from the settings registry. The
// SettingsService's read-through cache keeps the per-request cost negligible
// while letting operators retune session lifetime via the admin UI without
// a redeploy.
type SettingsBackedSessionTimings struct {
	Settings *SettingsService
}

// NewSettingsBackedSessionTimings is a small constructor for callers that
// prefer wiring through a function.
func NewSettingsBackedSessionTimings(s *SettingsService) *SettingsBackedSessionTimings {
	return &SettingsBackedSessionTimings{Settings: s}
}

// TokenTTL returns the configured lifetime for newly-issued session JWTs.
func (t *SettingsBackedSessionTimings) TokenTTL(ctx context.Context) time.Duration {
	return t.Settings.ResolveDurationSecondsWithDefault(ctx, SettingAuthSessionTokenTTLSeconds, "global")
}

// RefreshThreshold returns the age past which the auth middleware reissues
// an in-flight session JWT.
func (t *SettingsBackedSessionTimings) RefreshThreshold(ctx context.Context) time.Duration {
	return t.Settings.ResolveDurationSecondsWithDefault(ctx, SettingAuthSessionRefreshThresholdSeconds, "global")
}

// Compile-time interface conformance check.
var _ auth.SessionTimings = (*SettingsBackedSessionTimings)(nil)
