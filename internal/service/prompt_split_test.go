package service

import (
	"testing"

	"github.com/nram-ai/nram/internal/provider"
)

// TestPromptSplitReconstructsCombined verifies that for every phase the
// system-instruction half joined with the dynamic half (via the canonical
// separator) reproduces the original combined default byte for byte, and that
// the registered defaults are the split halves. This guards the authored split:
// the system and dynamic halves must remain a clean, reversible partition of
// the pre-split single-message form.
func TestPromptSplitReconstructsCombined(t *testing.T) {
	for _, p := range promptSplitDefaults {
		if p.combined == "" {
			t.Fatalf("%s: combined default not captured at init", p.combinedKey)
		}
		gotSystem := settingDefaults[p.systemKey]
		gotDynamic := settingDefaults[p.combinedKey]
		if gotSystem != p.systemText {
			t.Errorf("%s: system default does not match authored systemText", p.systemKey)
		}
		rejoined := gotSystem + provider.PromptSplitSeparator + gotDynamic
		if rejoined != p.combined {
			t.Errorf("%s: split halves do not rejoin to the original combined default", p.combinedKey)
		}
		// The dynamic half must be a strict, non-empty suffix of the combined
		// default (the system half plus a blank line was trimmed off the front).
		if gotDynamic == "" || gotDynamic == p.combined {
			t.Errorf("%s: dynamic half was not trimmed from the combined default", p.combinedKey)
		}
	}
}
