package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// encodeStringArray converts a string slice for storage.
// SQLite: JSON string. Postgres: TEXT[] literal {a,b,c}.
func encodeStringArray(backend string, arr []string) string {
	if backend == BackendPostgres {
		return "{" + strings.Join(arr, ",") + "}"
	}
	b, _ := json.Marshal(arr)
	return string(b)
}

// decodeStringArray parses a stored string array.
func decodeStringArray(backend string, s string) ([]string, error) {
	if backend == BackendPostgres {
		s = strings.TrimPrefix(s, "{")
		s = strings.TrimSuffix(s, "}")
		if s == "" {
			return []string{}, nil
		}
		return strings.Split(s, ","), nil
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

// encodeBool returns the appropriate value for a BOOLEAN column.
// Postgres: native bool. SQLite: INTEGER 0/1.
func encodeBool(backend string, val bool) interface{} {
	if backend == BackendPostgres {
		return val
	}
	if val {
		return 1
	}
	return 0
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
