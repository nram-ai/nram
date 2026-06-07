package version

import (
	"runtime"
	"testing"
)

func TestGetReportsVersionAndGo(t *testing.T) {
	info := Get()

	if info.Version != Version {
		t.Errorf("Version = %q, want %q", info.Version, Version)
	}
	if info.Version == "" {
		t.Error("Version is empty")
	}
	if info.Go != runtime.Version() {
		t.Errorf("Go = %q, want %q", info.Go, runtime.Version())
	}
	// Commit is environment-dependent (a VCS-stamped build vs. -buildvcs=false),
	// so only assert it is populated rather than asserting a specific value.
	if info.Commit == "" {
		t.Error("Commit is empty; expected a short hash or \"unknown\"")
	}
}
