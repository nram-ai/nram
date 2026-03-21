package config

// Config holds all bootstrap-level configuration for the nram server.
// Runtime configuration (providers, projects, etc.) lives in the database
// settings table and is managed through the admin UI.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	LogLevel string         `yaml:"log_level"`
	Admin    AdminConfig    `yaml:"admin"`
	Embed    ProviderConfig `yaml:"embed"`
	Fact     ProviderConfig `yaml:"fact"`
	Entity   ProviderConfig `yaml:"entity"`
	Qdrant   QdrantConfig   `yaml:"qdrant"`
}

// ServerConfig holds HTTP server settings.
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

// AdminConfig holds headless admin bootstrap credentials.
type AdminConfig struct {
	Email    string `yaml:"email"`
	Password string `yaml:"password"`
}

// QdrantConfig holds Qdrant vector database connection settings.
type QdrantConfig struct {
	Addr             string `yaml:"addr"`              // gRPC address, e.g. "localhost:6334"
	APIKey           string `yaml:"api_key"`            // API key for authentication
	UseTLS           bool   `yaml:"use_tls"`            // Enable TLS for the gRPC connection
	PoolSize         uint   `yaml:"pool_size"`          // Number of gRPC connections (0 = default of 3)
	KeepAliveTime    int    `yaml:"keepalive_time"`     // Seconds between keepalive pings (0=10s, -1=disabled)
	KeepAliveTimeout uint   `yaml:"keepalive_timeout"`  // Seconds to wait for keepalive ack (0=2s)
}

// ProviderConfig holds LLM/embedding provider settings.
type ProviderConfig struct {
	Provider string `yaml:"provider"`
	URL      string `yaml:"url"`
	Key      string `yaml:"key"`
	Model    string `yaml:"model"`
}

// DefaultConfig returns the default configuration values.
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
