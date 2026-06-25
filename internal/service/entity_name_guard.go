package service

import (
	"slices"
	"strings"
	"unicode/utf8"
)

// IsDegenerateEntityName reports whether an LLM-emitted entity name should be
// rejected before it is embedded or persisted. It guards the one extraction
// field that has no closed vocabulary: the entity name is taken verbatim from
// the model, and an 8B model in a degenerate state returns either a whole
// sentence where a noun-phrase was expected or a repetition loop. Three
// independent checks run, each disabled by a zero threshold:
//
//   - length:     more than maxChars runes (a multi-clause wall of text)
//   - word count: more than maxWords whitespace-separated words (a sentence
//     returned as a name that still fits under the char cap)
//   - repetition: a low ratio of distinct words for space-separated loops
//     ("undercutting undercutting ..."), or a short unit repeated consecutively
//     for no-space loops (".svg.svg.svg ..."); both gated by minDistinctRatio.
//
// An empty or whitespace-only name is always degenerate.
func IsDegenerateEntityName(name string, maxChars, maxWords int, minDistinctRatio float64) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return true
	}
	if maxChars > 0 && utf8.RuneCountInString(trimmed) > maxChars {
		return true
	}
	fields := strings.Fields(trimmed)
	if maxWords > 0 && len(fields) > maxWords {
		return true
	}
	if minDistinctRatio > 0 {
		if isWordRepetitionLoop(fields, minDistinctRatio) {
			return true
		}
		if isCharRepetitionLoop(trimmed) {
			return true
		}
	}
	return false
}

// isWordRepetitionLoop reports whether the space-separated words are dominated by
// a handful of repeated tokens: distinct(words)/len(words) < minDistinctRatio.
// It requires at least four words so short legitimate names ("New York City")
// are never flagged on a coincidental repeat.
func isWordRepetitionLoop(fields []string, minDistinctRatio float64) bool {
	if len(fields) < 4 {
		return false
	}
	distinct := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		distinct[strings.ToLower(f)] = struct{}{}
	}
	return float64(len(distinct))/float64(len(fields)) < minDistinctRatio
}

// maxRepetitionScanRunes bounds how much of a name isCharRepetitionLoop scans.
// The length check normally rejects names far shorter than this before the
// repetition check runs; the bound only matters when an operator disables that
// check (maxChars=0), keeping this O(n^2) scan from running on a pathologically
// long name. A real repetition loop is visible in any short prefix, so capping
// the window does not miss one.
const maxRepetitionScanRunes = 512

// isCharRepetitionLoop reports whether the name is mostly a short unit repeated
// consecutively, the no-whitespace failure mode ("...svg.svg.svg...") that
// strings.Fields sees as a single word. It flags only when a unit of up to eight
// runes repeats at least four times in a row and that run covers at least 60% of
// the name, so a legitimate name that merely contains a small repeat is kept.
func isCharRepetitionLoop(s string) bool {
	rs := []rune(s)
	if len(rs) > maxRepetitionScanRunes {
		rs = rs[:maxRepetitionScanRunes]
	}
	n := len(rs)
	if n < 4 {
		return false
	}
	for unit := 1; unit <= 8 && unit*4 <= n; unit++ {
		for start := 0; start+unit*2 <= n; start++ {
			reps := 1
			for pos := start + unit; pos+unit <= n && slices.Equal(rs[start:start+unit], rs[pos:pos+unit]); pos += unit {
				reps++
			}
			if reps >= 4 && reps*unit*100 >= 60*n {
				return true
			}
		}
	}
	return false
}
