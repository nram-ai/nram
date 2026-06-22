package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/version"
)

// TestPrintVersionRendersProductIdentity asserts the --version banner leads with
// the full product identity (name, copyright, license, homepage) and still
// carries the build block. The fields are checked in order so a regression that
// reshuffles or drops a line is caught, not just a missing substring.
func TestPrintVersionRendersProductIdentity(t *testing.T) {
	var buf bytes.Buffer
	printVersion(&buf)
	out := buf.String()

	want := []string{
		version.Name + " (" + version.Short + ") " + version.Version,
		version.Copyright,
		version.License,
		version.Homepage,
		"commit:",
		"go:",
	}

	prev := 0
	for _, w := range want {
		idx := strings.Index(out[prev:], w)
		if idx < 0 {
			t.Fatalf("printVersion output missing %q (or out of order); got:\n%s", w, out)
		}
		prev += idx + len(w)
	}

	// The header must read "Neural Ram (nram) <version>", not the bare "nram".
	if !strings.HasPrefix(out, "Neural Ram (nram) ") {
		firstLine, _, _ := strings.Cut(out, "\n")
		t.Errorf("version banner should start with the full product name; got first line %q",
			firstLine)
	}
}

// TestPrintUsageHeaderUsesProductName asserts the top-level --help screen opens
// with the full product name and carries the semantic version.
func TestPrintUsageHeaderUsesProductName(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)
	first, _, _ := strings.Cut(buf.String(), "\n")

	if !strings.HasPrefix(first, "Neural Ram (nram) "+version.Version) {
		t.Errorf("usage header = %q, want it to start with %q",
			first, "Neural Ram (nram) "+version.Version)
	}
}
