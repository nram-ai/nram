package migrations

import "embed"

//go:embed all:sqlite
var SQLiteFS embed.FS

//go:embed all:postgres
var PostgresFS embed.FS
