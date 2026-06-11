// Package repopath locates the repository root at runtime so build tooling and
// tests can resolve module-relative files (for example docs/openapi.yaml)
// regardless of the working directory they run in.
package repopath

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Root walks up from this source file's location until it finds the directory
// containing go.mod, and returns that directory. Because the walk starts from a
// fixed in-module file, it resolves the same module root for every caller,
// independent of the process working directory.
func Root() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("repopath: cannot resolve caller path")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repopath: go.mod not found above %s", filepath.Dir(file))
		}
		dir = parent
	}
}
