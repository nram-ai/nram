// Package tags provides tag-string normalization shared across service
// write paths and one-shot storage backfills. Kept dependency-free so
// both internal/service and internal/storage can import it without
// creating a cycle.
package tags

import "strings"

// Normalize cleans a tag slice in place: every entry has any wrapping
// double or single quote characters stripped, then is whitespace-trimmed,
// then empty results are dropped, then the slice is deduplicated while
// preserving first-seen order.
//
// Idempotent: Normalize(Normalize(x)) is equal to Normalize(x).
//
// Inner quote characters are preserved (a tag whose intended literal text
// contains a quote should keep the inner quote). Only quotes that wrap
// the entire trimmed string are stripped, and the strip repeats until no
// further wrapping pair is removed (handles the doubly-escaped case
// produced by some LLM outputs).
func Normalize(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		t := normalizeOne(raw)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func normalizeOne(s string) string {
	s = strings.TrimSpace(s)
	for len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			s = strings.TrimSpace(s[1 : len(s)-1])
			continue
		}
		break
	}
	return s
}
