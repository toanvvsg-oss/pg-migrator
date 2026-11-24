package migrator

import (
	"strings"
	"testing"
)

func TestValidateIdentifier(t *testing.T) {
	cases := []struct {
		name    string
		ident   string
		wantErr bool
	}{
		{"simple", "pub_1", false},
		{"leading_underscore", "_sub", false},
		{"single_char", "a", false},
		{"max_63_chars", "a" + strings.Repeat("b", 62), false},
		{"empty", "", true},
		{"overflow_64_chars", "a" + strings.Repeat("b", 63), true},
		{"leading_digit", "9pub", true},
		{"hyphen", "pub-1", true},
		{"schema_dot", "public.pub", true},
		{"space", "pub name", true},
		{"quotes", `"pub"`, true},
		{"sqli_semicolon", "pub; DROP TABLE x;--", true},
		{"null_byte", "pub\x00name", true},
		{"non_ascii", "püb", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateIdentifier(tc.ident)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateIdentifier(%q) err=%v, wantErr=%v", tc.ident, err, tc.wantErr)
			}
		})
	}
}

func TestEscapeConnVal(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"simple", "simple"},
		{"host with space", `'host with space'`},
		{"pa'ss", `'pa\'ss'`},
		{`back\slash`, `back\\slash`},
		{"tab\tval", "'tab\tval'"},
		{"line\nval", "'line\nval'"},
	}

	for _, tc := range cases {
		if got := escapeConnVal(tc.in); got != tc.want {
			t.Errorf("escapeConnVal(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
