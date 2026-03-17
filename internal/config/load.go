package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// envVarPattern matches ${VAR_NAME} and ${VAR_NAME:-default} syntax.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// Load reads configuration from a YAML file (optional), overlays environment
// variables, and returns a fully resolved Config. The lookup order for the
// config file path is:
//  1. The explicit path argument (non-empty string, e.g. from --config flag)
//  2. NRAM_CONFIG environment variable
//  3. config.yaml in the working directory
//
// If no config file is found, only defaults and environment variables are used.
func Load(configPath string) (Config, error) {
	cfg := DefaultConfig()

	path := resolveConfigPath(configPath)
	if path != "" {
		if err := loadYAML(path, &cfg); err != nil {
			return Config{}, fmt.Errorf("loading config file %s: %w", path, err)
		}
	}

	applyEnv(&cfg)

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
func loadYAML(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	expanded := interpolateEnvVars(string(data))

	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	return nil
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
// Environment variables always take precedence over YAML values.
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

	if v := os.Getenv("NRAM_EXTERNAL_URL"); v != "" {
		cfg.Server.ExternalURL = strings.TrimRight(v, "/")
	}

	if v := os.Getenv("NRAM_ADMIN_EMAIL"); v != "" {
		cfg.Admin.Email = v
	}
	if v := os.Getenv("NRAM_ADMIN_PASS"); v != "" {
		cfg.Admin.Password = v
	}

	if v := os.Getenv("NRAM_EMBED_PROVIDER"); v != "" {
		cfg.Embed.Provider = v
	}
	if v := os.Getenv("NRAM_EMBED_URL"); v != "" {
		cfg.Embed.URL = v
	}
	if v := os.Getenv("NRAM_EMBED_MODEL"); v != "" {
		cfg.Embed.Model = v
	}

	if v := os.Getenv("NRAM_FACT_PROVIDER"); v != "" {
		cfg.Fact.Provider = v
	}
	if v := os.Getenv("NRAM_FACT_KEY"); v != "" {
		cfg.Fact.Key = v
	}
	if v := os.Getenv("NRAM_FACT_MODEL"); v != "" {
		cfg.Fact.Model = v
	}

	if v := os.Getenv("NRAM_ENTITY_PROVIDER"); v != "" {
		cfg.Entity.Provider = v
	}
	if v := os.Getenv("NRAM_ENTITY_KEY"); v != "" {
		cfg.Entity.Key = v
	}
	if v := os.Getenv("NRAM_ENTITY_MODEL"); v != "" {
		cfg.Entity.Model = v
	}
}
