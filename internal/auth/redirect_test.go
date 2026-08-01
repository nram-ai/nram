package auth

import "testing"

func TestSafeInternalRedirect(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		// Same-origin root-relative paths are preserved verbatim.
		{"simple path", "/dashboard", "/dashboard"},
		{"path with query and fragment", "/dashboard?tab=1#frag", "/dashboard?tab=1#frag"},
		{"bare root", "/", "/"},

		// Off-origin or malformed values collapse to the safe default.
		{"empty", "", "/"},
		{"relative no leading slash", "dashboard", "/"},
		{"absolute https", "https://evil.example", "/"},
		{"protocol-relative", "//evil.example", "/"},
		{"backslash variant", "/\\evil.example", "/"},
		{"leading backslash slash", "\\/evil.example", "/"},
		{"triple slash", "///evil.example", "/"},
		{"scheme single slash", "http:/evil.example", "/"},
		{"javascript scheme", "javascript:alert(1)", "/"},

		// Control-char smuggling: browsers strip tab/LF/CR from the Location
		// before navigating, so these must be stripped and re-checked, not
		// passed through. "/\t/evil" would otherwise become "//evil".
		{"tab smuggled protocol-relative", "/\t/evil.example", "/"},
		{"newline smuggled protocol-relative", "/\n/evil.example", "/"},
		{"cr smuggled protocol-relative", "/\r/evil.example", "/"},
		{"tab then backslash", "/\t\\evil.example", "/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := safeInternalRedirect(tc.raw); got != tc.want {
				t.Fatalf("safeInternalRedirect(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
