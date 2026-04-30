package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// clearEnv unsets every environment variable the loader touches — both the
// supported bootstrap variables and the deprecated ones — so each test starts
// from a clean baseline regardless of what the developer's shell exports.
func clearEnv(t *testing.T) {
	t.Helper()
	vars := []string{
		"PORT", "LOG_LEVEL", "DATABASE_URL",
		"NRAM_CONFIG",
		"NRAM_ADMIN_EMAIL", "NRAM_ADMIN_PASS",
	}
	vars = append(vars, deprecatedEnvVars...)
	for _, v := range vars {
		t.Setenv(v, "")
		os.Unsetenv(v)
	}
}

// captureSlog redirects the default slog handler to a buffer for the duration
// of the test and returns the buffer. The original handler is restored on
// cleanup.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	buf := new(bytes.Buffer)
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	return buf
}

func TestDefaultValues(t *testing.T) {
	clearEnv(t)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("host = %q, want %q", cfg.Server.Host, "0.0.0.0")
	}
	if cfg.Server.Port != 8674 {
		t.Errorf("port = %d, want %d", cfg.Server.Port, 8674)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("log_level = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.Database.URL != "" {
		t.Errorf("database.url = %q, want empty", cfg.Database.URL)
	}
	if cfg.Database.MaxConnections != 20 {
		t.Errorf("database.max_connections = %d, want %d", cfg.Database.MaxConnections, 20)
	}
	if !cfg.Database.MigrateOnStart {
		t.Error("database.migrate_on_start = false, want true")
	}
	if cfg.Admin.Email != "" {
		t.Errorf("admin.email = %q, want empty", cfg.Admin.Email)
	}
	if cfg.Admin.Password != "" {
		t.Errorf("admin.password = %q, want empty", cfg.Admin.Password)
	}
}

func TestYAMLFileParsing(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
server:
  host: 127.0.0.1
  port: 9090

database:
  url: postgres://user:pass@localhost:5432/testdb
  max_connections: 50
  migrate_on_start: false

log_level: debug

admin:
  email: admin@example.com
  password: hunter2
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("host = %q, want %q", cfg.Server.Host, "127.0.0.1")
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("port = %d, want %d", cfg.Server.Port, 9090)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("log_level = %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.Database.URL != "postgres://user:pass@localhost:5432/testdb" {
		t.Errorf("database.url = %q, want postgres URL", cfg.Database.URL)
	}
	if cfg.Database.MaxConnections != 50 {
		t.Errorf("database.max_connections = %d, want %d", cfg.Database.MaxConnections, 50)
	}
	if cfg.Database.MigrateOnStart {
		t.Error("database.migrate_on_start = true, want false")
	}
	if cfg.Admin.Email != "admin@example.com" {
		t.Errorf("admin.email = %q, want %q", cfg.Admin.Email, "admin@example.com")
	}
	if cfg.Admin.Password != "hunter2" {
		t.Errorf("admin.password = %q, want %q", cfg.Admin.Password, "hunter2")
	}
}

func TestEnvironmentVariableOverlay(t *testing.T) {
	clearEnv(t)

	t.Setenv("PORT", "3000")
	t.Setenv("LOG_LEVEL", "WARN")
	t.Setenv("DATABASE_URL", "postgres://env@localhost/envdb")
	t.Setenv("NRAM_ADMIN_EMAIL", "admin@test.com")
	t.Setenv("NRAM_ADMIN_PASS", "secret123")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 3000 {
		t.Errorf("port = %d, want %d", cfg.Server.Port, 3000)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("log_level = %q, want %q", cfg.LogLevel, "warn")
	}
	if cfg.Database.URL != "postgres://env@localhost/envdb" {
		t.Errorf("database.url = %q, want env value", cfg.Database.URL)
	}
	if cfg.Admin.Email != "admin@test.com" {
		t.Errorf("admin.email = %q, want %q", cfg.Admin.Email, "admin@test.com")
	}
	if cfg.Admin.Password != "secret123" {
		t.Errorf("admin.password = %q, want %q", cfg.Admin.Password, "secret123")
	}
}

func TestVariableInterpolation(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	t.Setenv("CUSTOM_PORT", "7777")

	yamlContent := `
server:
  host: 0.0.0.0
  port: ${CUSTOM_PORT:-8674}

database:
  url: ${CUSTOM_DB_URL:-}
  max_connections: 20
  migrate_on_start: true

log_level: ${CUSTOM_LOG:-info}
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 7777 {
		t.Errorf("port = %d, want %d (from CUSTOM_PORT env)", cfg.Server.Port, 7777)
	}
	if cfg.Database.URL != "" {
		t.Errorf("database.url = %q, want empty (default fallback)", cfg.Database.URL)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("log_level = %q, want %q (default fallback)", cfg.LogLevel, "info")
	}
}

func TestPrecedenceEnvOverYAML(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
server:
  host: 127.0.0.1
  port: 9090

database:
  url: postgres://yaml@localhost/yamldb

log_level: debug
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	t.Setenv("PORT", "5555")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("DATABASE_URL", "postgres://env@localhost/envdb")

	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 5555 {
		t.Errorf("port = %d, want %d (env should override yaml)", cfg.Server.Port, 5555)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("log_level = %q, want %q (env should override yaml)", cfg.LogLevel, "error")
	}
	if cfg.Database.URL != "postgres://env@localhost/envdb" {
		t.Errorf("database.url = %q, want env value (env should override yaml)", cfg.Database.URL)
	}

	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("host = %q, want %q (yaml value should persist)", cfg.Server.Host, "127.0.0.1")
	}
}

func TestNRAMConfigEnvVar(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "custom-config.yaml")

	yamlContent := `
server:
  port: 4444
log_level: trace
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	t.Setenv("NRAM_CONFIG", yamlPath)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 4444 {
		t.Errorf("port = %d, want %d", cfg.Server.Port, 4444)
	}
	if cfg.LogLevel != "trace" {
		t.Errorf("log_level = %q, want %q", cfg.LogLevel, "trace")
	}
}

func TestMissingConfigFileNotError(t *testing.T) {
	clearEnv(t)

	cfg, err := Load("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("missing config file should not cause error, got: %v", err)
	}

	if cfg.Server.Port != 8674 {
		t.Errorf("port = %d, want default %d", cfg.Server.Port, 8674)
	}
}

func TestConfigFileInWorkingDirectory(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
server:
  port: 6666
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getting working directory: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("changing to temp dir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 6666 {
		t.Errorf("port = %d, want %d", cfg.Server.Port, 6666)
	}
}

func TestInvalidYAMLReturnsError(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "bad.yaml")

	if err := os.WriteFile(yamlPath, []byte("{{{{not yaml"), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	_, err := Load(yamlPath)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestInterpolateEnvVars(t *testing.T) {
	t.Setenv("TEST_VAR", "hello")

	tests := []struct {
		input string
		want  string
	}{
		{"${TEST_VAR:-world}", "hello"},
		{"${UNSET_VAR:-fallback}", "fallback"},
		{"${UNSET_VAR}", ""},
		{"prefix-${TEST_VAR:-x}-suffix", "prefix-hello-suffix"},
		{"no variables here", "no variables here"},
		{"${TEST_VAR:-}", "hello"},
	}

	for _, tt := range tests {
		got := interpolateEnvVars(tt.input)
		if got != tt.want {
			t.Errorf("interpolateEnvVars(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestDeprecatedYAMLKeysWarn verifies that legacy top-level keys (carried
// from the pre-cleanup config surface) produce a WARN log line and are not
// silently applied. The cleanup explicitly excluded these from the bootstrap
// struct; if a future change accidentally adds them back, this test fails.
func TestDeprecatedYAMLKeysWarn(t *testing.T) {
	clearEnv(t)
	logs := captureSlog(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
server:
  port: 8674

embed:
  provider: openai
  url: https://api.openai.com/v1
  key: sk-deprecated
  model: text-embedding-3-small

fact:
  provider: anthropic
  key: sk-ant-deprecated

entity:
  provider: gemini
  key: AIza-deprecated

qdrant:
  addr: localhost:6334
  api_key: qdrant-deprecated

hnsw:
  m: 32
  ef_construction: 400

enrichment_orphan_grace_seconds: 7200
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := logs.String()
	for _, key := range deprecatedYAMLKeys {
		if !strings.Contains(output, "key="+key) {
			t.Errorf("expected deprecation warning mentioning key=%q, got log:\n%s", key, output)
		}
	}

	if cfg.Server.Port != 8674 {
		t.Errorf("supported keys should still apply: port = %d, want 8674", cfg.Server.Port)
	}
}

// TestDeprecatedEnvVarsWarn verifies that legacy environment variables
// produce a WARN log line at load time.
func TestDeprecatedEnvVarsWarn(t *testing.T) {
	clearEnv(t)
	logs := captureSlog(t)

	t.Setenv("NRAM_EMBED_PROVIDER", "openai")
	t.Setenv("NRAM_FACT_KEY", "sk-deprecated")
	t.Setenv("NRAM_ENRICHMENT_ORPHAN_GRACE_SECONDS", "1234")

	if _, err := Load(""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := logs.String()
	for _, name := range []string{"NRAM_EMBED_PROVIDER", "NRAM_FACT_KEY", "NRAM_ENRICHMENT_ORPHAN_GRACE_SECONDS"} {
		if !strings.Contains(output, "env="+name) {
			t.Errorf("expected deprecation warning for env=%q, got log:\n%s", name, output)
		}
	}
}
