package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// encodeStringArray converts a string slice for storage.
// SQLite: JSON string. Postgres: TEXT[] literal {a,b,c} with elements
// quoted and escaped per the Postgres array input grammar so values
// containing whitespace, commas, quotes, backslashes, or braces — or the
// literal text "NULL" — round-trip cleanly through the SQL TEXT[] type.
func encodeStringArray(backend string, arr []string) string {
	if backend == BackendPostgres {
		if len(arr) == 0 {
			return "{}"
		}
		parts := make([]string, len(arr))
		for i, s := range arr {
			parts[i] = encodePostgresArrayElement(s)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	b, _ := json.Marshal(arr)
	return string(b)
}

// encodePostgresArrayElement returns the wire form of a single TEXT[]
// element. Elements that contain no special characters and are not the
// literal "NULL" stay unquoted (matching what pgx returns for those
// elements on read, which keeps the encode/decode round trip stable).
// Everything else is quoted with backslash escapes.
func encodePostgresArrayElement(s string) string {
	if s != "" && !strings.EqualFold(s, "NULL") && !strings.ContainsAny(s, " ,\"\\{}\t\n\r") {
		return s
	}
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

// decodeStringArray parses a stored string array.
func decodeStringArray(backend string, s string) ([]string, error) {
	if backend == BackendPostgres {
		return parsePostgresArray(s)
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil, err
	}
	if arr == nil {
		arr = []string{}
	}
	return arr, nil
}

// parsePostgresArray decodes a Postgres TEXT[] literal of the form
// {a,"quoted, with comma","with \"escaped\" quotes",NULL}. Returns the
// elements as Go strings; the unquoted token NULL becomes the empty
// string (this codebase does not store NULLs in TEXT[] columns, so the
// distinction is not load-bearing).
func parsePostgresArray(s string) ([]string, error) {
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("invalid postgres array literal: %q", s)
	}
	body := s[1 : len(s)-1]
	if body == "" {
		return []string{}, nil
	}

	out := make([]string, 0)
	var cur strings.Builder
	inQuotes := false
	quoted := false
	escaped := false
	flush := func() {
		raw := cur.String()
		if !quoted && strings.EqualFold(raw, "NULL") {
			out = append(out, "")
		} else {
			out = append(out, raw)
		}
		cur.Reset()
		quoted = false
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if escaped {
			cur.WriteByte(c)
			escaped = false
			continue
		}
		if inQuotes {
			switch c {
			case '\\':
				escaped = true
			case '"':
				inQuotes = false
			default:
				cur.WriteByte(c)
			}
			continue
		}
		switch c {
		case '"':
			inQuotes = true
			quoted = true
		case ',':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return out, nil
}

// EncodeBool returns the appropriate value for a BOOLEAN column.
// Postgres: native bool. SQLite: INTEGER 0/1.
func EncodeBool(backend string, val bool) any {
	if backend == BackendPostgres {
		return val
	}
	if val {
		return 1
	}
	return 0
}

// escapeLike escapes the SQL LIKE wildcards %, _ and the escape backslash so
// that user-supplied substrings match literally. Pair with `ESCAPE '\'` in
// the LIKE clause for both SQLite and Postgres.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	s = strings.ReplaceAll(s, "_", `\_`)
	return s
}

// uuidsToStrings converts a UUID slice to a string slice.
func uuidsToStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

// stringsToUUIDs converts a string slice to a UUID slice.
func stringsToUUIDs(ss []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(ss))
	for i, s := range ss {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("parse uuid %q: %w", s, err)
		}
		out[i] = id
	}
	return out, nil
}

// decodeBoolVal converts a scanned interface{} (bool or int64) to a Go bool.
// Postgres returns native bool; SQLite returns int64 (0/1).
func decodeBoolVal(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	default:
		return false
	}
}

// encodeUUIDArray converts a UUID slice for storage.
// SQLite: JSON string. Postgres: UUID[] literal {uuid1,uuid2}.
func encodeUUIDArray(backend string, ids []uuid.UUID) string {
	return encodeStringArray(backend, uuidsToStrings(ids))
}

// decodeUUIDArray parses a stored UUID array.
func decodeUUIDArray(backend string, s string) ([]uuid.UUID, error) {
	strs, err := decodeStringArray(backend, s)
	if err != nil {
		return nil, err
	}
	if len(strs) == 0 {
		return []uuid.UUID{}, nil
	}
	return stringsToUUIDs(strs)
}
