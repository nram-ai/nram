// Package config holds the bootstrap configuration for the nram server.
//
// This package is deliberately minimal: it carries only settings that must be
// available before the database is open and the runtime settings registry
// (internal/service.SettingsService) is reachable. Everything else —
// providers, vector backends, dreaming, ranking, retention, etc. — is managed
// at runtime through the admin UI / DB-backed settings table.
//
// Bootstrap surface:
//   - Server.Host / Server.Port      bind address
//   - Database.URL                   DB DSN
//   - Database.MaxConnections        pool size
//   - Database.MigrateOnStart        auto-apply migrations on first boot
//   - LogLevel                       slog handler level
//   - Admin.Email / Admin.Password   first-boot administrator (headless)
package config

// Config holds bootstrap-only configuration for the nram server.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	LogLevel string         `yaml:"log_level"`
	Admin    AdminConfig    `yaml:"admin"`
}

// ServerConfig holds HTTP listener settings.
type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	URL            string `yaml:"url"`
	MaxConnections int    `yaml:"max_connections"`
	MigrateOnStart bool   `yaml:"migrate_on_start"`
}

// AdminConfig holds the headless admin bootstrap credentials. When both
// fields are non-empty AND no users exist in the database, the server
// creates the first administrator on startup, bypassing the setup wizard.
// On any boot where setup is already complete, these values are ignored.
type AdminConfig struct {
	Email    string `yaml:"email"`
	Password string `yaml:"password"`
}

// DefaultConfig returns the default bootstrap configuration values.
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Host: "0.0.0.0",
			Port: 8674,
		},
		Database: DatabaseConfig{
			MaxConnections: 20,
			MigrateOnStart: true,
		},
		LogLevel: "info",
	}
}
