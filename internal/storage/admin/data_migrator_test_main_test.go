package admin

import (
	"fmt"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// resolvedPostgresURL is set by TestMain to the embedded Postgres URL.
var resolvedPostgresURL string

// embeddedDB holds the embedded postgres instance for cleanup.
var embeddedDB *embeddedpostgres.EmbeddedPostgres

func TestMain(m *testing.M) {
	db := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(15432).
			Database("nram_test").
			Username("nram_test").
			Password("nram_test"),
	)
	if err := db.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin tests: embedded postgres failed to start: %v\n", err)
		os.Exit(1)
	}
	embeddedDB = db
	resolvedPostgresURL = "postgres://nram_test:nram_test@localhost:15432/nram_test?sslmode=disable"

	code := m.Run()

	embeddedDB.Stop()
	os.Exit(code)
}
