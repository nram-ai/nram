package service

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseLLMStringList_PlainArray(t *testing.T) {
	queries, err := ParseLLMStringList(`["q one","q two","q three"]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 3 || queries[0] != "q one" {
		t.Fatalf("queries = %v", queries)
	}
}

func TestParseLLMStringList_FencedJSON(t *testing.T) {
	body := "Sure, here you go:\n```json\n[\"alpha\", \"beta\"]\n```\nDone."
	queries, err := ParseLLMStringList(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 2 || queries[1] != "beta" {
		t.Fatalf("queries = %v", queries)
	}
}

func TestParseLLMStringList_DropsEmpties(t *testing.T) {
	queries, err := ParseLLMStringList(`["one","","   ","two"]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 2 || queries[0] != "one" || queries[1] != "two" {
		t.Fatalf("expected only non-empty queries; got %v", queries)
	}
}

func TestParseLLMStringList_MalformedReturnsError(t *testing.T) {
	if _, err := ParseLLMStringList(`{"not": "an array"}`); err == nil {
		t.Fatalf("expected error on object payload")
	}
	if _, err := ParseLLMStringList(`not json at all`); err == nil {
		t.Fatalf("expected error on garbage")
	}
	if _, err := ParseLLMStringList(`[]`); !errors.Is(err, ErrEmptyLLMStringList) {
		t.Fatalf("expected ErrEmptyLLMStringList on empty array; got %v", err)
	}
}

func TestParseLLMStringList_ObjectEnvelope(t *testing.T) {
	cases := map[string]string{
		"queries key":    `{"queries": ["one", "two", "three"]}`,
		"questions key":  `{"questions": ["alpha", "beta"]}`,
		"subqueries key": `{"subqueries": ["a", "b"]}`,
		"arbitrary key":  `{"output": ["only", "this"]}`,
		"with prose":     "Here you go:\n```json\n{\"queries\": [\"x\", \"y\"]}\n```",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			queries, err := ParseLLMStringList(body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(queries) == 0 {
				t.Fatalf("expected queries; got empty")
			}
		})
	}
}

func TestParseLLMStringList_MixedElementTypes(t *testing.T) {
	queries, err := ParseLLMStringList(`["how many", 42, "or about so", true]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 4 {
		t.Fatalf("expected 4 stringified queries; got %v", queries)
	}
	if queries[1] != "42" {
		t.Fatalf("expected numeric element coerced to \"42\"; got %q", queries[1])
	}
	if queries[3] != "true" {
		t.Fatalf("expected bool element coerced to \"true\"; got %q", queries[3])
	}
}

func TestParseLLMStringList_NestedObjectStillFails(t *testing.T) {
	if _, err := ParseLLMStringList(`{"queries": {"a": "b"}}`); err == nil {
		t.Fatalf("expected error on object-of-object payload")
	}
}

func TestParseLLMStringList_LenientUnquotedElements(t *testing.T) {
	cases := map[string]struct {
		body string
		want []string
	}{
		"bare unquoted comma list": {
			body: `[who is Emma's husband, Brandon's spouse name, Emma's marital status, wife of Brandon]`,
			want: []string{"who is Emma's husband", "Brandon's spouse name", "Emma's marital status", "wife of Brandon"},
		},
		"single-quoted elements": {
			body: `['who is Emma married to', 'Brandon spouse', 'Emma husband']`,
			want: []string{"who is Emma married to", "Brandon spouse", "Emma husband"},
		},
		"mixed quoting": {
			body: `["who is Emma married to", Brandon spouse, 'Emma husband']`,
			want: []string{"who is Emma married to", "Brandon spouse", "Emma husband"},
		},
		"backtick wrap": {
			body: "[`alpha`, `beta`, `gamma`]",
			want: []string{"alpha", "beta", "gamma"},
		},
		"newline delimited inside brackets": {
			body: "[\n  alpha\n  beta\n  gamma\n]",
			want: []string{"alpha", "beta", "gamma"},
		},
		"unquoted with prose envelope": {
			body: "Here you go: [alpha query, beta query, gamma query]. Done.",
			want: []string{"alpha query", "beta query", "gamma query"},
		},
		"unquoted inside object envelope": {
			body: `{"queries": [alpha, beta, gamma]}`,
			want: []string{"alpha", "beta", "gamma"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseLLMStringList(tc.body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseLLMStringList_LenientStillRejectsTrueGarbage(t *testing.T) {
	for _, body := range []string{
		`not json at all`,
		`{"not": "an array"}`,
		`[]`,
		`[ , , ]`,
	} {
		if _, err := ParseLLMStringList(body); err == nil {
			t.Fatalf("expected error on %q; lenient pass overshot", body)
		}
	}
}

func TestParseLLMStringList_TruncatedArrayRecovery(t *testing.T) {
	cases := map[string]struct {
		body string
		want []string
	}{
		"trailing comma, no closing bracket": {
			body: `["who is Brandon", "Brandon's company", "Brandon role at Velocity",`,
			want: []string{"who is Brandon", "Brandon's company", "Brandon role at Velocity"},
		},
		"three full elements then truncated mid-string": {
			body: `["alpha", "beta", "gamma", "del`,
			want: []string{"alpha", "beta", "gamma"},
		},
		"two elements then truncated immediately after comma": {
			body: `["one", "two",`,
			want: []string{"one", "two"},
		},
		"object envelope truncated mid-array": {
			body: `{"queries": ["first", "second", "third",`,
			want: []string{"first", "second", "third"},
		},
		"object envelope truncated mid-string": {
			body: `{"queries": ["aaa", "bbb", "cc`,
			want: []string{"aaa", "bbb"},
		},
		"prose preamble plus truncated array": {
			body: "Here are the queries:\n[\"q1\", \"q2\", \"q3\",",
			want: []string{"q1", "q2", "q3"},
		},
		"fenced markdown with truncated array body": {
			body: "```json\n[\"alpha query\", \"beta query\", \"gam",
			want: []string{"alpha query", "beta query"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseLLMStringList(tc.body)
			if err != nil {
				t.Fatalf("unexpected error on truncated payload: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseLLMStringList_TruncatedArrayRecoveryStillFailsOnZeroElements(t *testing.T) {
	for _, body := range []string{
		`[`,
		`[ ,`,
		`[123, 456,`,            // non-string elements only; pass 3 also fails because of truncation
		`{"queries": [1, 2, 3,`, // truncated object envelope, non-string elements
	} {
		if _, err := ParseLLMStringList(body); err == nil {
			t.Fatalf("expected error on %q; recovery overshot to fabricate queries", body)
		}
	}
}

// TestParseLLMStringList_DecompositionCases pins the cases inherited from the
// ask-decomposition parser so the consolidation preserves that contract too.
func TestParseLLMStringList_DecompositionCases(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{"subqueries envelope", `{"subqueries":["a","b"]}`, []string{"a", "b"}, false},
		{"bare array", `["a","b"]`, []string{"a", "b"}, false},
		{"fenced envelope", "```json\n{\"subqueries\":[\"a\"]}\n```", []string{"a"}, false},
		{"prose around envelope", `Sure: {"subqueries":["x"]} done`, []string{"x"}, false},
		{"empty list", `{"subqueries":[]}`, nil, true},
		{"whitespace dropped", `{"subqueries":["a","  "]}`, []string{"a"}, false},
		{"prose only", `no decomposition needed`, nil, true},
		{"garbage", `}{[`, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLLMStringList(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error; got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
