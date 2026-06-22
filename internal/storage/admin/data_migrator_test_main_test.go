package admin

import (
	"fmt"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// resolvedPostgresURL is set by TestMain to the embedded Postgres URL.
var resolvedPostgresURL string

// embeddedDB holds the embedded postgres instance for cleanup.
var embeddedDB *embeddedpostgres.EmbeddedPostgres

// startEmbeddedWithRetry starts embedded Postgres, retrying on any start error.
// The dominant failure is a transient non-200 from the binary-download mirror
// (Maven Central): embedded-postgres reports it as "no version found matching"
// even though the version exists, so a single blip would otherwise fail the
// whole package on a cache-cold runner.
func startEmbeddedWithRetry() (*embeddedpostgres.EmbeddedPostgres, error) {
	const attempts = 3
	var lastErr error
	for i := range attempts {
		db := embeddedpostgres.NewDatabase(
			embeddedpostgres.DefaultConfig().
				Port(15432).
				Database("nram_test").
				Username("nram_test").
				Password("nram_test"),
		)
		if err := db.Start(); err != nil {
			lastErr = err
			if i < attempts-1 {
				time.Sleep(time.Duration(i+1) * 3 * time.Second)
			}
			continue
		}
		return db, nil
	}
	return nil, lastErr
}

func TestMain(m *testing.M) {
	db, err := startEmbeddedWithRetry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin tests: embedded postgres failed to start: %v\n", err)
		os.Exit(1)
	}
	embeddedDB = db
	resolvedPostgresURL = "postgres://nram_test:nram_test@localhost:15432/nram_test?sslmode=disable"

	code := m.Run()

	_ = embeddedDB.Stop()
	os.Exit(code)
}
