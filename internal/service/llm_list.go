package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrEmptyLLMStringList is returned by ParseLLMStringList when parsing
// succeeded structurally but no non-empty element survived cleaning, so callers
// can distinguish "nothing usable" from a hard parse error.
var ErrEmptyLLMStringList = errors.New("empty array")

// ParseLLMStringList extracts a JSON array of strings from a small-model LLM
// reply. Tolerates five documented small-model failure modes before declaring
// parse failure, in order of preference:
//
//  1. Bare JSON array of strings (the contract). Strips markdown fences and
//     leading/trailing prose by clipping to the first '[' and last ']'.
//  2. JSON object wrapping the array under any single key
//     ({"queries": [...]}, {"subqueries": [...]}, etc). Used when the model
//     ignores "no envelope" prompt language. No key name is hardcoded.
//  3. Bare JSON array with mixed element types ([123, "x", true]); each
//     element is stringified and treated as a candidate. Catches the case
//     where the model interpolates a number or boolean into one slot.
//  4. Bracketed list with missing or mixed quoting ([foo, bar, baz] or
//     ["foo", bar, 'baz']). Observed on qwen3:8b at higher temperatures: the
//     model emits the brackets but drops the per-element double quotes.
//     Split on commas (preferred) or newlines (fallback), strip stray quote
//     chars per token, drop empties.
//  5. Truncation-prefix recovery via json.Decoder.Token streaming. Catches
//     the case where the model emitted a well-formed prefix of a JSON array
//     of strings but ran out of tokens (or otherwise stopped) before the
//     closing ']'. Recovers every cleanly-decoded string element up to the
//     first decode error. Mirrors the longest-valid-prefix recovery in
//     extraction_llm.go used by fact extraction.
//
// Empties and whitespace-only entries are dropped at the end regardless of
// which path succeeded. Returns ErrEmptyLLMStringList when nothing usable
// survives, or the native decode error when no pass could parse the body.
func ParseLLMStringList(raw string) ([]string, error) {
	body := strings.TrimSpace(raw)
	if start, end := strings.Index(body, "["), strings.LastIndex(body, "]"); start >= 0 && end > start {
		body = body[start : end+1]
	} else if start, end := strings.Index(body, "{"), strings.LastIndex(body, "}"); start >= 0 && end > start {
		body = body[start : end+1]
	}

	candidates, err := decodeStringListCandidates([]byte(body))
	if err != nil {
		return nil, err
	}

	cleaned := make([]string, 0, len(candidates))
	for _, s := range candidates {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		cleaned = append(cleaned, s)
	}
	if len(cleaned) == 0 {
		return nil, ErrEmptyLLMStringList
	}
	return cleaned, nil
}

// decodeStringListCandidates runs the five tolerant decode passes described on
// ParseLLMStringList and returns the raw candidate slice with no post-cleaning.
// Split out so the cleaning loop stays a single site.
func decodeStringListCandidates(body []byte) ([]string, error) {
	// Pass 1: bare []string. The contract path.
	var arr []string
	pass1Err := json.Unmarshal(body, &arr)
	if pass1Err == nil {
		return arr, nil
	}

	// Pass 2: object envelope. Pick the first value (any key) that decodes
	// as either []string or []any with all-string-coercible elements.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err == nil {
		// Map iteration is non-deterministic; in the common case the object
		// has exactly one key so this is fine. If it has multiple keys, the
		// first one that matches wins, which is acceptable for fail-soft.
		for _, v := range obj {
			var inner []string
			if err := json.Unmarshal(v, &inner); err == nil {
				return inner, nil
			}
			var innerAny []any
			if err := json.Unmarshal(v, &innerAny); err == nil {
				if out, ok := stringifyAnySlice(innerAny); ok {
					return out, nil
				}
			}
		}
	}

	// Pass 3: bare []any with mixed element types. Stringify each.
	var mixed []any
	if err := json.Unmarshal(body, &mixed); err == nil {
		if out, ok := stringifyAnySlice(mixed); ok {
			return out, nil
		}
	}

	// Pass 4: lenient, bracketed list with missing or mixed quoting. See
	// ParseLLMStringList docstring for the failure mode this rescues.
	if out, ok := lenientSplitArray(body); ok {
		return out, nil
	}

	// Pass 5: truncation-prefix recovery. The strict passes fail with
	// "unexpected end of JSON input" when the model emits the opening '['
	// plus some valid string elements but never writes the closing ']'
	// (token-budget exhaustion is the dominant cause; qwen3:8b in reasoning
	// mode is the canonical offender). Stream string tokens with json.Decoder
	// until the first decode error and return whatever cleanly decoded.
	if out, ok := recoverStringArrayPrefix(body); ok {
		return out, nil
	}

	// All passes failed; return pass 1's native error for logging fidelity.
	return nil, pass1Err
}

// recoverStringArrayPrefix walks the body with a json.Decoder seeded at the
// first '[', returning every successfully-decoded string element before the
// first error. The decoder is lenient about trailing content past the last
// decoded token, so a truncated array (no closing ']') still yields its
// well-formed prefix. Returns (nil, false) when no '[' is present or zero
// string elements survive so the caller can preserve the strict-pass error
// for logs.
func recoverStringArrayPrefix(body []byte) ([]string, bool) {
	lb := bytes.IndexByte(body, '[')
	if lb < 0 {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(body[lb:]))

	tok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '[' {
		return nil, false
	}

	var out []string
	for dec.More() {
		var s string
		if err := dec.Decode(&s); err != nil {
			// Non-string element or truncation. Stop on first error and
			// return whatever survived.
			break
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// lenientSplitArray rescues a bracketed list whose elements are not
// (consistently) double-quoted JSON strings. Requires brackets; without them
// there is no list structure to extract. Splits the interior on commas
// (preferred) or newlines (fallback when no comma is present), trims
// whitespace, and strips a single layer of wrapping ASCII single, double, or
// backtick quote characters per token. Returns ok=false when no non-empty
// token survives so the caller can preserve the strict-pass error for logs.
func lenientSplitArray(body []byte) ([]string, bool) {
	s := strings.TrimSpace(string(body))
	l, r := strings.Index(s, "["), strings.LastIndex(s, "]")
	if l < 0 || r <= l {
		return nil, false
	}
	interior := s[l+1 : r]
	var raw []string
	if strings.Contains(interior, ",") {
		raw = strings.Split(interior, ",")
	} else {
		raw = strings.Split(interior, "\n")
	}
	out := make([]string, 0, len(raw))
	for _, tok := range raw {
		tok = strings.TrimSpace(tok)
		tok = strings.Trim(tok, "\"'`")
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		out = append(out, tok)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// stringifyAnySlice coerces each element of a []any to its string form,
// rejecting nested objects/arrays (which would round-trip to "map[...]" or
// "[...]" noise and are not useful list entries). Returns (slice, true) when
// every element coerced; (nil, false) otherwise so the caller can fall
// through to the next pass.
func stringifyAnySlice(in []any) ([]string, bool) {
	out := make([]string, 0, len(in))
	for _, v := range in {
		switch t := v.(type) {
		case string:
			out = append(out, t)
		case float64, bool:
			out = append(out, fmt.Sprintf("%v", t))
		case json.Number:
			out = append(out, t.String())
		case nil:
			// Skip; the cleaning pass would drop an empty string anyway.
			continue
		default:
			return nil, false
		}
	}
	return out, true
}
