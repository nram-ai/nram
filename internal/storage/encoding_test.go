package storage

import "testing"

func TestDecodeStringArray_NullJSON(t *testing.T) {
	result, err := decodeStringArray("sqlite", "null")
	if err != nil {
		t.Fatalf("decodeStringArray(sqlite, \"null\") returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil slice from decodeStringArray(sqlite, \"null\"), got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 elements, got %d", len(result))
	}
}

func TestDecodeStringArray_EmptyArray(t *testing.T) {
	result, err := decodeStringArray("sqlite", "[]")
	if err != nil {
		t.Fatalf("decodeStringArray(sqlite, \"[]\") returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil slice from decodeStringArray(sqlite, \"[]\"), got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 elements, got %d", len(result))
	}
}

func TestDecodeStringArray_Postgres_Empty(t *testing.T) {
	result, err := decodeStringArray("postgres", "{}")
	if err != nil {
		t.Fatalf("decodeStringArray(postgres, \"{}\") returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil slice from decodeStringArray(postgres, \"{}\"), got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 elements, got %d", len(result))
	}
}
