package model

import (
	"encoding/json"
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

// RelationRelatedTo is the escape-hatch relation for a label that does not map
// to the closed vocabulary.
const RelationRelatedTo = "related to"

// CanonicalRelations is the closed relation vocabulary. CanonicalRelation does
// formatting-only normalization (and stays the read-path / graph-slice dedup
// key); CanonicalRelationVocab additionally coerces a label into this set,
// dropping unmapped labels to "related_to". The write path uses the vocab form;
// the formatting-only form is kept for backward-compatible read dedup.
var CanonicalRelations = []string{
	"is a", "part of", "has part", "located in", "uses", "depends on",
	"produces", "affects", "interacts with", "references", "implements",
	"supports", "compares to", "member of", "family of", "has property",
	RelationRelatedTo,
}

// relationAliases maps each canonical relation to the natural-language verbs the
// extraction model actually emits. Kept deliberately broad: an 8b model uses a
// wide vocabulary of verbs (author, written_in, served_by, calls, runs, lives
// near, ...), and an unmapped verb collapses to "related_to", which erases the
// signal. Keys are matched after normalization (underscores/hyphens -> space),
// so "written_in" and "written in" both resolve via the key "written in".
var relationAliases = map[string][]string{
	"is a":           {"is a", "instance of", "type of", "kind of", "is", "is an", "is type", "subclass of", "categorized as", "classified as", "a type of", "an example of", "example of", "is example of", "represents"},
	"part of":        {"part of", "is part of", "belongs to", "part of operating model", "subset of", "within", "component of", "contained in", "included in", "falls under", "is component of", "is a part of"},
	"has part":       {"contains", "includes", "comprises", "consists of", "has part", "made up of", "has component", "encompasses", "has member", "is composed of", "incorporates", "groups"},
	"located in":     {"located in", "based in", "in", "situated in", "resides in", "lives in", "lives near", "located near", "based near", "resides near", "near", "found in", "located at", "lives at", "resides at", "from"},
	"uses":           {"used in", "used by", "uses", "used", "use", "leverages", "leverages experience from", "utilizes", "consumes", "selects", "calls", "invokes", "queries", "requests", "written in", "implemented in", "built with", "coded in", "reads", "accesses", "employs", "applies", "powered by", "relies on tool", "operates on", "processes", "deployed", "installed", "set up", "configured", "works on", "working on", "operated", "ran", "adopted", "migrated to", "switched to", "paid for", "ran sendmail server"},
	"depends on":     {"depends on", "requires", "needs", "relies on", "blocked by", "served by", "hosted by", "provided by", "deployed on", "runs on", "hosted on", "depends upon", "needs to", "subscribes to", "constrained by", "limited by", "bound by"},
	"produces":       {"produces", "generates", "creates", "emits", "outputs", "yields", "returns", "created", "created by", "authored", "author", "authored by", "wrote", "written by", "developed", "developed by", "developer", "developer of", "built", "built by", "made", "made by", "designed", "designed by", "founded", "founded by", "established", "established by", "constructs", "builds", "maintains", "owns"},
	"affects":        {"affects", "modifies", "influences", "impacts", "changes", "might downregulate production of", "downregulates", "upregulates", "regulates", "alters", "updates", "controls", "drives", "improves", "reduces", "increases", "decreases", "causes", "triggers", "managed", "manages", "manage", "oversaw", "oversees", "administered", "led", "leads", "directed", "headed", "spearheaded", "managed migration to", "managed closure of", "drained by", "improves on"},
	"interacts with": {"interacts with", "converses with", "works with", "communicates with", "integration", "connects to", "can connect to", "talks to", "converses", "collaborates with", "interfaces with", "integrates with", "pairs with", "coordinates with", "syncs with", "closer to", "close to", "maintains connection with", "has relationship with", "relationship with", "connected to", "spends time with"},
	"references":     {"references", "referenced by", "mentions", "mentioned in", "maps to", "maps to architecture of", "maps to architecture", "cites", "cited in", "points to", "links to", "refers to", "describes", "documents", "notes", "discusses", "covers", "addresses", "relates to topic", "appears in", "featured in", "taught", "teaches", "instructs", "taught subject"},
	"implements":     {"implements", "defines", "realizes", "satisfies", "conforms to", "adheres to", "fulfills", "complies with", "follows"},
	"supports":       {"supports", "provides", "enables", "is platform for", "is system for", "is system of record for", "is export format for", "can query", "serves", "offers", "exposes", "runs", "executes", "hosts", "powers", "facilitates", "allows", "delivers", "served"},
	"compares to":    {"competitor", "is analogous to", "compares against", "compared to", "compares with", "differs from", "overlaps with", "compatible with", "similar to", "versus", "analogous to", "contrasts with", "like", "unlike", "rivals", "alternative to", "competes with", "equivalent to"},
	"member of":      {"member of", "core responsibilities at", "works at", "worked at", "work at", "employed at", "employed", "worked for", "employed by", "affiliated with", "contributor to", "works for", "belongs to org", "part of team", "employee of", "reports to", "joined", "enrolled in", "participates in", "taught at", "studied at", "graduated from", "attended", "hired", "hired as", "hired in", "promoted to", "founded company"},
	"family of":      {"married to", "mother of", "father of", "parent of", "parent", "child of", "child", "son of", "daughter of", "sibling of", "brother of", "sister of", "spouse of", "spouse", "wife of", "husband of", "related to family", "family of", "grandparent of", "grandchild of", "grandmother of", "grandfather of", "cousin of", "relative of", "aunt of", "uncle of", "niece of", "nephew of"},
	"has property":   {"has", "has trait", "has subtrait", "has property", "characterized by", "exhibits", "features", "possesses", "has attribute", "has value", "has status", "has a", "held title", "held position", "has title", "has role", "title", "role at", "earned", "holds", "obtained", "achieved", "received", "occupation of", "is occupation of", "has occupation"},
}

// relationKinds maps a normalized kinship label to the kinship subtype stamped
// into Relationship.Properties["kind"] when the label collapses to family_of,
// so the persona-tier graph keeps the spouse/parent/child/sibling distinction
// that the "family_of" label drops. Edge direction (source->target) carries the
// rest (e.g. "mother of": source is the parent).
var relationKinds = map[string]string{
	"married to": "spouse", "spouse of": "spouse", "spouse": "spouse", "wife of": "spouse", "husband of": "spouse",
	"mother of": "parent", "father of": "parent", "parent of": "parent", "parent": "parent",
	"child of": "child", "son of": "child", "daughter of": "child", "child": "child",
	"sibling of": "sibling", "brother of": "sibling", "sister of": "sibling",
}

var relationLookup = buildRelationLookup()

func buildRelationLookup() map[string]string {
	m := make(map[string]string, 256)
	for _, c := range CanonicalRelations {
		m[normalizeLabel(c)] = c
	}
	for canonical, aliases := range relationAliases {
		for _, a := range aliases {
			m[normalizeLabel(a)] = canonical
		}
	}
	return m
}

// relationCopulas are leading auxiliary/copula words stripped before a second
// vocabulary lookup, so the model's "is mother of" / "was developed by" resolve
// the same as "mother of" / "developed by". "has"/"have" are deliberately NOT
// stripped because they are themselves meaningful ("has trait" -> has_property).
var relationCopulas = map[string]struct{}{
	"is": {}, "was": {}, "are": {}, "were": {}, "be": {}, "been": {}, "being": {}, "am": {},
}

// CanonicalRelationVocab coerces a raw relation label into the closed
// vocabulary. It tries the direct lookup, then retries after stripping a leading
// copula. Unmapped labels collapse to "related_to". Idempotent.
func CanonicalRelationVocab(raw string) string {
	n := normalizeLabel(raw)
	if n == "" {
		return RelationRelatedTo
	}
	if c, ok := relationLookup[n]; ok {
		return c
	}
	// Retry after dropping a leading copula ("is mother of" -> "mother of").
	if i := strings.IndexByte(n, ' '); i > 0 {
		if _, isCopula := relationCopulas[n[:i]]; isCopula {
			if c, ok := relationLookup[n[i+1:]]; ok {
				return c
			}
		}
	}
	return RelationRelatedTo
}

// RelationKind returns the kinship subtype (spouse/parent/child/sibling) for a
// raw relation label that collapses to family_of, or "" when the label is not a
// recognized kinship relation. Callers stamp the result into
// Relationship.Properties["kind"] before the family_of collapse erases it.
func RelationKind(raw string) string {
	n := normalizeLabel(raw)
	if k, ok := relationKinds[n]; ok {
		return k
	}
	if i := strings.IndexByte(n, ' '); i > 0 {
		if _, isCopula := relationCopulas[n[:i]]; isCopula {
			return relationKinds[n[i+1:]]
		}
	}
	return ""
}

// ApplyRelationVocab coerces a relationship's label into the closed vocabulary
// and, when the original label was a kinship relation, stamps the kinship
// subtype into properties.kind before the family_of collapse erases it. The
// kind is computed from the ORIGINAL label and only written when "kind" is not
// already present. Applied at the extraction write path (not the low-level repo)
// so imports and programmatic edges keep their original labels. Mutates rel in
// place; idempotent.
func ApplyRelationVocab(rel *Relationship) {
	kind := RelationKind(rel.Relation)
	rel.Relation = CanonicalRelationVocab(rel.Relation)
	if kind != "" {
		rel.Properties = stampJSONField(rel.Properties, "kind", kind)
	}
}

// StampRelationKindProperty writes the kinship subtype into properties.kind
// (only when absent), for callers that resolve the relation themselves (e.g. the
// semantic classifier) but still want the kinship subtype preserved.
func StampRelationKindProperty(props json.RawMessage, kind string) json.RawMessage {
	return stampJSONField(props, "kind", kind)
}

// stampJSONField sets key=value in a JSON object only when the key is absent,
// returning the re-marshaled object. A nil/empty/"null"/unparseable input
// starts from an empty object. On a marshal error the original is returned.
func stampJSONField(raw json.RawMessage, key, value string) json.RawMessage {
	m := map[string]any{}
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &m); err != nil || m == nil {
			m = map[string]any{}
		}
	}
	if _, exists := m[key]; exists {
		return raw
	}
	m[key] = value
	out, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return out
}
