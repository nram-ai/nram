// Package version is the single source of truth for the nram build identity.
//
// The semantic version is a hand-maintained constant, bumped on release. The
// commit hash, dirty flag, and build time are read from the build's embedded
// VCS stamps, which the Go toolchain records automatically on any build, run,
// or test from inside the repository (Go 1.18+). No ldflags or Makefile wiring
// is required; building with -buildvcs=false simply leaves the commit unknown.
package version

import (
	"runtime"
	"runtime/debug"
)

// Version is the semantic version of nram. Bump it by hand on release.
const Version = "0.8.0"

// BuildInfo describes the identity of a running binary.
type BuildInfo struct {
	// Version is the semantic version (see the Version constant).
	Version string `json:"version"`
	// Commit is the short (7-char) VCS revision, or "unknown" when the build
	// carries no VCS stamp.
	Commit string `json:"commit"`
	// Dirty reports whether the working tree had uncommitted changes at build
	// time.
	Dirty bool `json:"dirty"`
	// Time is the RFC 3339 commit timestamp of the build's revision, empty when
	// unstamped.
	Time string `json:"time"`
	// Go is the Go runtime version the binary was built with.
	Go string `json:"go"`
}

// Get returns the build identity, combining the compiled-in Version constant
// with the VCS stamps embedded by the Go toolchain.
func Get() BuildInfo {
	info := BuildInfo{
		Version: Version,
		Commit:  "unknown",
		Go:      runtime.Version(),
	}

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}

	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if s.Value != "" {
				info.Commit = s.Value[:min(len(s.Value), 7)]
			}
		case "vcs.modified":
			info.Dirty = s.Value == "true"
		case "vcs.time":
			info.Time = s.Value
		}
	}

	return info
}
