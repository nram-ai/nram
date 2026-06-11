// Command gen_openapi_setting_keys regenerates the SettingKey enum in
// docs/openapi.yaml from the canonical Go settings registry
// (admin.SettingsSchemas) so the OpenAPI spec always lists every known setting
// key. Run it after adding or removing a setting; the sync test
// (internal/storage/admin/openapi_settings_sync_test.go) fails until it is run.
//
//go:generate go run .
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nram-ai/nram/internal/repopath"
	"github.com/nram-ai/nram/internal/storage/admin"
)

const (
	beginMarker = "# BEGIN setting keys"
	endMarker   = "# END setting keys"
)

func main() {
	root, err := repopath.Root()
	if err != nil {
		fatal(err)
	}
	path := filepath.Join(root, "docs", "openapi.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		fatal(err)
	}

	keys := settingKeys()
	out, err := replaceBetweenMarkers(data, renderEnum(keys))
	if err != nil {
		fatal(err)
	}
	if bytes.Equal(out, data) {
		fmt.Println("gen_openapi_setting_keys: docs/openapi.yaml already up to date")
		return
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("gen_openapi_setting_keys: wrote %d setting keys to docs/openapi.yaml\n", len(keys))
}

// settingKeys returns the sorted set of canonical setting keys from the Go
// registry that the admin settings endpoints actually serve.
func settingKeys() []string {
	schemas := admin.SettingsSchemas()
	keys := make([]string, 0, len(schemas))
	for _, s := range schemas {
		keys = append(keys, s.Key)
	}
	sort.Strings(keys)
	return keys
}

func renderEnum(keys []string) string {
	var b strings.Builder
	b.WriteString("      enum:\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "        - %q\n", k)
	}
	return b.String()
}

// replaceBetweenMarkers swaps the lines strictly between the BEGIN and END
// marker lines for block, leaving the marker lines themselves intact.
func replaceBetweenMarkers(data []byte, block string) ([]byte, error) {
	lines := strings.Split(string(data), "\n")
	begin, end := -1, -1
	for i, ln := range lines {
		if strings.Contains(ln, beginMarker) {
			begin = i
		}
		if strings.Contains(ln, endMarker) {
			end = i
			break
		}
	}
	if begin < 0 || end < 0 || end < begin {
		return nil, fmt.Errorf("markers not found in docs/openapi.yaml (begin=%d end=%d)", begin, end)
	}
	out := make([]string, 0, len(lines))
	out = append(out, lines[:begin+1]...)
	out = append(out, strings.Split(strings.TrimRight(block, "\n"), "\n")...)
	out = append(out, lines[end:]...)
	return []byte(strings.Join(out, "\n")), nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gen_openapi_setting_keys:", err)
	os.Exit(1)
}
