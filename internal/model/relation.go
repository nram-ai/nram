package model

import (
	"strings"
	"unicode"
)

// CanonicalRelation normalizes a relationship label to a single canonical
// formatting variant so edges that differ ONLY in punctuation, casing, or
// spacing collapse onto one row and one graph-slice edge. The transform:
//
//  1. trim surrounding whitespace
//  2. lowercase (Unicode-aware)
//  3. collapse every run of underscore, hyphen, and Unicode whitespace into a
//     single ASCII space; no leading or trailing separator survives
//
// Examples that MERGE: "related_to" / "related to" / "Related  To" /
// "related-to" -> "related to".
//
// Semantic equivalence is deliberately OUT OF SCOPE: "maps_to_architecture"
// (-> "maps to architecture") and "maps to architecture of" differ by a
// trailing token, not by separators, so they stay distinct. The function only
// merges pure formatting variants; it never stems, drops words, or reorders.
//
// CanonicalRelation is the single source of truth for relation normalization
// across the write path (RelationshipRepo), the read path (graph slice dedup),
// and the one-time backfill, so the canonical form never diverges between them.
// It is idempotent: CanonicalRelation(CanonicalRelation(x)) == CanonicalRelation(x).
func CanonicalRelation(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	prevSep := false
	for _, r := range s {
		if r == '_' || r == '-' || unicode.IsSpace(r) {
			// Emit at most one space per separator run, and never a leading one
			// (b.Len() == 0 means nothing has been written yet).
			if !prevSep && b.Len() > 0 {
				b.WriteByte(' ')
				prevSep = true
			}
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		prevSep = false
	}
	// A separator run at the very end leaves a trailing space; strip it.
	return strings.TrimRight(b.String(), " ")
}
