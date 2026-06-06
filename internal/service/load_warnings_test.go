package service

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// captureSlog redirects the default slog logger to a buffer for the
// duration of fn, returning the captured text. Used to assert that
// CheckProviderLoadDefaults emits the expected warning content.
func captureSlog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	fn()
	return buf.String()
}

// putInt persists an int-valued setting in the mock repo. settingsService
// resolves these back as strings so Atoi sees the digit form directly:
// matches how settingDefaults stores everything.
func (m *mockSettingsRepo) putInt(key, scope string, v int) {
	if m.settings[key] == nil {
		m.settings[key] = make(map[string]*model.Setting)
	}
	// Store as the bare digit (not JSON-quoted) so ResolveInt's Atoi
	// matches what the production schema-registration path produces.
	m.settings[key][scope] = &model.Setting{
		Key:   key,
		Value: []byte(itoa(v)),
		Scope: scope,
	}
}

// itoa is strconv.Itoa inlined to avoid a fmt-side import in this small file.
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestCheckProviderLoadDefaults_QuietAtDefaults(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	out := captureSlog(t, func() {
		CheckProviderLoadDefaults(context.Background(), svc)
	})

	if strings.Contains(out, "provider-load knobs raised") {
		t.Fatalf("expected no warning when every knob is at the registered default; got:\n%s", out)
	}
}

func TestCheckProviderLoadDefaults_WarnsWhenRaised(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.putInt(SettingEnrichmentWorkerCountPostgres, "global", 4)
	repo.putInt(SettingDreamContradictionNeighbors, "global", 8)
	svc := NewSettingsService(repo)

	out := captureSlog(t, func() {
		CheckProviderLoadDefaults(context.Background(), svc)
	})

	if !strings.Contains(out, "provider-load knobs raised") {
		t.Fatalf("expected warning header; got:\n%s", out)
	}
	if !strings.Contains(out, SettingEnrichmentWorkerCountPostgres) {
		t.Fatalf("expected raised count_postgres knob in warning; got:\n%s", out)
	}
	if !strings.Contains(out, SettingDreamContradictionNeighbors) {
		t.Fatalf("expected raised contradiction.neighbors knob in warning; got:\n%s", out)
	}
	// Knobs at default must not be listed.
	if strings.Contains(out, SettingEnrichmentWorkerPreEmbedConcurrency) {
		t.Fatalf("knob at default should not appear in warning; got:\n%s", out)
	}
}

func TestCheckProviderLoadDefaults_QuietWhenLoweredBelowSafe(t *testing.T) {
	repo := newMockSettingsRepo()
	// Setting below default is fine: operator deliberately chose 0/1 for
	// a constrained environment, no need to nag.
	repo.putInt(SettingEnrichmentWorkerCountPostgres, "global", 0)
	svc := NewSettingsService(repo)

	out := captureSlog(t, func() {
		CheckProviderLoadDefaults(context.Background(), svc)
	})

	if strings.Contains(out, "provider-load knobs raised") {
		t.Fatalf("expected no warning when knob is below default; got:\n%s", out)
	}
}

func TestCheckProviderLoadDefaults_NilSettingsServiceIsSafe(t *testing.T) {
	// Should not panic when the settings service hasn't been wired (test
	// harness or extremely-early startup path).
	out := captureSlog(t, func() {
		CheckProviderLoadDefaults(context.Background(), nil)
	})
	if strings.Contains(out, "provider-load knobs raised") {
		t.Fatalf("expected no warning with nil service; got:\n%s", out)
	}
}
