package config

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// envVarPattern matches ${VAR_NAME} and ${VAR_NAME:-default} syntax.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// deprecatedYAMLKeys are top-level YAML keys that used to seed runtime
// settings but now live exclusively in the DB-backed settings registry. The
// loader emits a slog warning for each one it sees in a config file so
// operators are nudged to clean up; the values are silently dropped.
var deprecatedYAMLKeys = []string{
	"embed",
	"fact",
	"entity",
	"qdrant",
	"hnsw",
	"enrichment_orphan_grace_seconds",
}

// deprecatedEnvVars are environment variables that used to seed runtime
// settings. Same treatment as deprecatedYAMLKeys: warn and ignore.
var deprecatedEnvVars = []string{
	"NRAM_EMBED_PROVIDER", "NRAM_EMBED_URL", "NRAM_EMBED_KEY", "NRAM_EMBED_MODEL",
	"NRAM_FACT_PROVIDER", "NRAM_FACT_URL", "NRAM_FACT_KEY", "NRAM_FACT_MODEL",
	"NRAM_ENTITY_PROVIDER", "NRAM_ENTITY_URL", "NRAM_ENTITY_KEY", "NRAM_ENTITY_MODEL",
	"NRAM_ENRICHMENT_ORPHAN_GRACE_SECONDS",
	"NRAM_MCP_MAX_RESULT_TOKENS",
}

// Load reads configuration from a YAML file (optional), overlays environment
// variables, and returns a fully resolved Config. The lookup order for the
// config file path is:
//  1. The explicit path argument (non-empty string, e.g. from --config flag)
//  2. NRAM_CONFIG environment variable
//  3. config.yaml in the working directory
//
// If no config file is found, only defaults and environment variables are used.
//
// Deprecated YAML keys and environment variables are detected during load and
// reported via slog at WARN level; their values are not applied. The
// authoritative source for runtime settings is /v1/admin/settings (DB-backed).
func Load(configPath string) (Config, error) {
	cfg := DefaultConfig()

	path := resolveConfigPath(configPath)
	if path != "" {
		if err := loadYAML(path, &cfg); err != nil {
			return Config{}, fmt.Errorf("loading config file %s: %w", path, err)
		}
	}

	applyEnv(&cfg)
	warnDeprecatedEnv()

	return cfg, nil
}

// resolveConfigPath determines which config file to use. Returns empty string
// if no file is found.
func resolveConfigPath(explicit string) string {
	if explicit != "" {
		if fileExists(explicit) {
			return explicit
		}
		return ""
	}

	if envPath := os.Getenv("NRAM_CONFIG"); envPath != "" {
		if fileExists(envPath) {
			return envPath
		}
		return ""
	}

	if fileExists("config.yaml") {
		return "config.yaml"
	}

	return ""
}

// fileExists reports whether the named file exists and is a regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// loadYAML reads and parses a YAML config file into cfg, performing
// environment variable interpolation on the raw YAML before unmarshalling.
// Top-level keys that are no longer part of the bootstrap surface produce a
// WARN log line and are otherwise ignored.
func loadYAML(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	expanded := interpolateEnvVars(string(data))

	// Pass 1: detect deprecated top-level keys for the warning log. We tolerate
	// a non-mapping document here (someone with `~` or `[]` at the top level)
	// because the second pass will surface the real parse error against the
	// strict Config struct.
	var raw map[string]any
	if err := yaml.Unmarshal([]byte(expanded), &raw); err == nil {
		warnDeprecatedYAML(path, raw)
	}

	// Pass 2: decode into the bootstrap struct. yaml.v3 silently drops keys
	// that don't map to struct fields, so deprecated values do not reach cfg.
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	return nil
}

// warnDeprecatedYAML logs one WARN line per deprecated top-level key found
// in the parsed YAML map. Walks only the top level; nested deprecated
// fields (e.g. `embed.url`) are reported via their containing block.
func warnDeprecatedYAML(path string, raw map[string]any) {
	for _, key := range deprecatedYAMLKeys {
		if _, ok := raw[key]; ok {
			slog.Warn("config: ignoring deprecated key; manage at runtime via the admin UI",
				"file", path,
				"key", key,
				"see", "/admin/settings")
		}
	}
}

// warnDeprecatedEnv logs one WARN line per deprecated environment variable
// that is set (any non-empty value). Operators get a single, specific nudge
// per offending variable per startup.
func warnDeprecatedEnv() {
	for _, name := range deprecatedEnvVars {
		if os.Getenv(name) != "" {
			slog.Warn("config: ignoring deprecated environment variable; manage at runtime via the admin UI",
				"env", name,
				"see", "/admin/settings")
		}
	}
}

// interpolateEnvVars replaces all ${VAR:-default} patterns in s with the
// corresponding environment variable value, falling back to the default if
// the variable is unset or empty.
func interpolateEnvVars(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		parts := envVarPattern.FindStringSubmatch(match)
		if parts == nil {
			return match
		}
		varName := parts[1]
		defaultVal := parts[2]

		if val := os.Getenv(varName); val != "" {
			return val
		}
		return defaultVal
	})
}

// applyEnv overlays environment variables on top of the current config.
// Environment variables always take precedence over YAML values. The set is
// limited to the bootstrap surface: provider, vector backend, and tuning
// envs were removed; warnDeprecatedEnv reports any that remain set.
func applyEnv(cfg *Config) {
	if v := os.Getenv("PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		}
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = strings.ToLower(v)
	}

	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}

	if v := os.Getenv("NRAM_ADMIN_EMAIL"); v != "" {
		cfg.Admin.Email = v
	}
	if v := os.Getenv("NRAM_ADMIN_PASS"); v != "" {
		cfg.Admin.Password = v
	}
}
