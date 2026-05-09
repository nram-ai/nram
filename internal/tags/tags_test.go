package tags

import (
	"reflect"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "nil in nil out",
			in:   nil,
			want: nil,
		},
		{
			name: "empty in empty out",
			in:   []string{},
			want: []string{},
		},
		{
			name: "wrapping double quotes stripped",
			in:   []string{`"behavioral contract"`},
			want: []string{"behavioral contract"},
		},
		{
			name: "wrapping single quotes stripped",
			in:   []string{`'failure modes'`},
			want: []string{"failure modes"},
		},
		{
			name: "doubly wrapped quotes stripped both layers",
			in:   []string{`""behavioral contract""`},
			want: []string{"behavioral contract"},
		},
		{
			name: "leading and trailing whitespace trimmed",
			in:   []string{"  foo  "},
			want: []string{"foo"},
		},
		{
			name: "quotes with whitespace stripped",
			in:   []string{`  "foo"  `},
			want: []string{"foo"},
		},
		{
			name: "inner quotes preserved",
			in:   []string{`a "b" c`},
			want: []string{`a "b" c`},
		},
		{
			name: "wrapping outer with preserved inner quotes",
			in:   []string{`"a "b" c"`},
			want: []string{`a "b" c`},
		},
		{
			name: "empty entries dropped",
			in:   []string{"", " ", `""`, `''`, "ok"},
			want: []string{"ok"},
		},
		{
			name: "duplicates collapsed first-seen order",
			in:   []string{"foo", `"foo"`, "bar", "foo"},
			want: []string{"foo", "bar"},
		},
		{
			name: "real-world mixed clean and quoted",
			in: []string{
				"behavioral-contract",
				"core-index",
				`"behavioral contract"`,
				"Claude",
				"Brandon",
				`"operating modes"`,
				`"failure modes"`,
				"protocols",
			},
			want: []string{
				"behavioral-contract",
				"core-index",
				"behavioral contract",
				"Claude",
				"Brandon",
				"operating modes",
				"failure modes",
				"protocols",
			},
		},
		{
			name: "case-sensitive distinct kept separate",
			in:   []string{"Foo", "foo"},
			want: []string{"Foo", "foo"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Normalize(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Normalize(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeIdempotent(t *testing.T) {
	in := []string{
		`"behavioral contract"`,
		"foo",
		`""bar""`,
		"  baz  ",
		"foo",
		"",
	}
	once := Normalize(in)
	twice := Normalize(once)
	if !reflect.DeepEqual(once, twice) {
		t.Fatalf("not idempotent:\nonce=%v\ntwice=%v", once, twice)
	}
}
