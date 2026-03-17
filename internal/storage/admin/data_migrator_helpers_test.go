package admin

import (
	"database/sql"
	"testing"
)

func TestJsonArrayToPostgresTextArray(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"empty array", `[]`, `{}`, false},
		{"single element", `["a"]`, `{"a"}`, false},
		{"multiple elements", `["a","b","c"]`, `{"a","b","c"}`, false},
		{"tags example", `["tag1","tag2"]`, `{"tag1","tag2"}`, false},
		{"embedded quote", `["has \"quote\""]`, `{"has \"quote\""}`, false},
		{"empty string input", ``, `{}`, false},
		{"null string", `null`, `{}`, false},
		{"whitespace only", `   `, `{}`, false},
		{"whitespace around null", `  null  `, `{}`, false},
		{"postgres passthrough", `{already,postgres}`, `{already,postgres}`, false},
		{"invalid json", `not json`, `{}`, true},
		{"urls", `["https://example.com","https://foo.bar"]`, `{"https://example.com","https://foo.bar"}`, false},
		{"empty strings in array", `["",""]`, `{"",""}`, false},
		{"unicode", `["café","naïve"]`, `{"café","naïve"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := jsonArrayToPostgresTextArray(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestJsonArrayToPostgresUUIDArray(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", `[]`, `{}`},
		{"single uuid", `["550e8400-e29b-41d4-a716-446655440000"]`, `{"550e8400-e29b-41d4-a716-446655440000"}`},
		{"multiple uuids", `["550e8400-e29b-41d4-a716-446655440000","6ba7b810-9dad-11d1-80b4-00c04fd430c8"]`,
			`{"550e8400-e29b-41d4-a716-446655440000","6ba7b810-9dad-11d1-80b4-00c04fd430c8"}`},
		{"null", `null`, `{}`},
		{"empty string", ``, `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := jsonArrayToPostgresUUIDArray(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNullStringToInterface(t *testing.T) {
	tests := []struct {
		name string
		ns   sql.NullString
		want interface{}
	}{
		{"invalid returns nil", sql.NullString{Valid: false}, nil},
		{"valid returns string", sql.NullString{String: "hello", Valid: true}, "hello"},
		{"valid empty string", sql.NullString{String: "", Valid: true}, ""},
		{"invalid with value still nil", sql.NullString{String: "ghost", Valid: false}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nullStringToInterface(tt.ns)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestNullInt64ToInterface(t *testing.T) {
	tests := []struct {
		name string
		ni   sql.NullInt64
		want interface{}
	}{
		{"invalid returns nil", sql.NullInt64{Valid: false}, nil},
		{"valid returns int64", sql.NullInt64{Int64: 42, Valid: true}, int64(42)},
		{"valid zero", sql.NullInt64{Int64: 0, Valid: true}, int64(0)},
		{"valid negative", sql.NullInt64{Int64: -1, Valid: true}, int64(-1)},
		{"invalid with value still nil", sql.NullInt64{Int64: 99, Valid: false}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nullInt64ToInterface(tt.ni)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestBooleanConversionPattern(t *testing.T) {
	// Tests the inline `enriched != 0`, `active != 0`, `autoRegistered != 0` pattern
	// used throughout the migrator to convert SQLite INTEGER booleans to Go bool.
	tests := []struct {
		name  string
		input int
		want  bool
	}{
		{"zero is false", 0, false},
		{"one is true", 1, true},
		{"negative one is true", -1, true},
		{"large positive is true", 999, true},
		{"large negative is true", -999, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input != 0
			if got != tt.want {
				t.Errorf("(%d != 0) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
