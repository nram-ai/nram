package config

import (
	"os"
	"path/filepath"
	"testing"
)

// clearEnv unsets all environment variables that affect config loading.
func clearEnv(t *testing.T) {
	t.Helper()
	vars := []string{
		"PORT", "LOG_LEVEL", "DATABASE_URL",
		"NRAM_CONFIG",
		"NRAM_ADMIN_EMAIL", "NRAM_ADMIN_PASS",
		"NRAM_EMBED_PROVIDER", "NRAM_EMBED_URL", "NRAM_EMBED_MODEL",
		"NRAM_FACT_PROVIDER", "NRAM_FACT_KEY", "NRAM_FACT_MODEL",
		"NRAM_ENTITY_PROVIDER", "NRAM_ENTITY_KEY", "NRAM_ENTITY_MODEL",
	}
	for _, v := range vars {
		t.Setenv(v, "")
		os.Unsetenv(v)
	}
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
}

func TestEnvironmentVariableOverlay(t *testing.T) {
	clearEnv(t)

	t.Setenv("PORT", "3000")
	t.Setenv("LOG_LEVEL", "WARN")
	t.Setenv("DATABASE_URL", "postgres://env@localhost/envdb")
	t.Setenv("NRAM_ADMIN_EMAIL", "admin@test.com")
	t.Setenv("NRAM_ADMIN_PASS", "secret123")
	t.Setenv("NRAM_EMBED_PROVIDER", "ollama")
	t.Setenv("NRAM_EMBED_URL", "http://localhost:11434")
	t.Setenv("NRAM_EMBED_MODEL", "nomic-embed-text")
	t.Setenv("NRAM_FACT_PROVIDER", "openai")
	t.Setenv("NRAM_FACT_KEY", "sk-test")
	t.Setenv("NRAM_FACT_MODEL", "gpt-4.1-nano")
	t.Setenv("NRAM_ENTITY_PROVIDER", "gemini")
	t.Setenv("NRAM_ENTITY_KEY", "AIza-test")
	t.Setenv("NRAM_ENTITY_MODEL", "gemini-2.5-flash")

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
	if cfg.Embed.Provider != "ollama" {
		t.Errorf("embed.provider = %q, want %q", cfg.Embed.Provider, "ollama")
	}
	if cfg.Embed.URL != "http://localhost:11434" {
		t.Errorf("embed.url = %q, want %q", cfg.Embed.URL, "http://localhost:11434")
	}
	if cfg.Embed.Model != "nomic-embed-text" {
		t.Errorf("embed.model = %q, want %q", cfg.Embed.Model, "nomic-embed-text")
	}
	if cfg.Fact.Provider != "openai" {
		t.Errorf("fact.provider = %q, want %q", cfg.Fact.Provider, "openai")
	}
	if cfg.Fact.Key != "sk-test" {
		t.Errorf("fact.key = %q, want %q", cfg.Fact.Key, "sk-test")
	}
	if cfg.Fact.Model != "gpt-4.1-nano" {
		t.Errorf("fact.model = %q, want %q", cfg.Fact.Model, "gpt-4.1-nano")
	}
	if cfg.Entity.Provider != "gemini" {
		t.Errorf("entity.provider = %q, want %q", cfg.Entity.Provider, "gemini")
	}
	if cfg.Entity.Key != "AIza-test" {
		t.Errorf("entity.key = %q, want %q", cfg.Entity.Key, "AIza-test")
	}
	if cfg.Entity.Model != "gemini-2.5-flash" {
		t.Errorf("entity.model = %q, want %q", cfg.Entity.Model, "gemini-2.5-flash")
	}
}

func TestVariableInterpolation(t *testing.T) {
	clearEnv(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")

	// Set one env var, leave the other unset to test default fallback.
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

	// Set env vars that should override YAML values.
	t.Setenv("PORT", "5555")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("DATABASE_URL", "postgres://env@localhost/envdb")

	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Env should win over YAML.
	if cfg.Server.Port != 5555 {
		t.Errorf("port = %d, want %d (env should override yaml)", cfg.Server.Port, 5555)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("log_level = %q, want %q (env should override yaml)", cfg.LogLevel, "error")
	}
	if cfg.Database.URL != "postgres://env@localhost/envdb" {
		t.Errorf("database.url = %q, want env value (env should override yaml)", cfg.Database.URL)
	}

	// YAML value that has no env override should remain.
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

	// Load with a path that does not exist — should not error, just use defaults.
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

	// Create config.yaml in a temp dir and chdir to it.
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
