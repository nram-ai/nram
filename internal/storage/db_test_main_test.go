package storage

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setupSharedPostgres()
	code := m.Run()
	teardownSharedPostgres()
	os.Exit(code)
}
