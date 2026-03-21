package api

import (
	"encoding/json"
	"log"
	"net/http"
	"net/mail"
	"reflect"
)

// isValidEmail checks whether the given string is a valid email address.
func isValidEmail(email string) bool {
	addr, err := mail.ParseAddress(email)
	if err != nil {
		return false
	}
	return addr.Address == email
}

// writeJSON encodes v as JSON and writes it to w with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	sanitized := sanitizeNils(v)
	if err := json.NewEncoder(w).Encode(sanitized); err != nil {
		log.Printf("api: failed to encode JSON response: %v", err)
	}
}

// jsonMarshalerType is the reflect.Type for json.Marshaler interface.
var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

// sanitizeNils recursively walks a value and replaces nil slices with empty
// slices and nil maps with empty maps so that JSON encoding produces [] and {}
// instead of null.
func sanitizeNils(v interface{}) interface{} {
	if v == nil {
		return v
	}
	return sanitizeValue(reflect.ValueOf(v)).Interface()
}

func sanitizeValue(v reflect.Value) reflect.Value {
	// Special-case json.RawMessage: if nil, default to "{}".
	if v.Type() == reflect.TypeOf(json.RawMessage{}) {
		if v.IsNil() || v.Len() == 0 {
			return reflect.ValueOf(json.RawMessage("{}"))
		}
		return v
	}

	// Skip types that implement json.Marshaler — they control their own encoding.
	if v.Type().Implements(jsonMarshalerType) || reflect.PointerTo(v.Type()).Implements(jsonMarshalerType) {
		return v
	}

	switch v.Kind() {
	case reflect.Slice:
		// Skip []byte (raw bytes, not a JSON array).
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}
		if v.IsNil() {
			return reflect.MakeSlice(v.Type(), 0, 0)
		}
		n := v.Len()
		out := reflect.MakeSlice(v.Type(), n, n)
		for i := 0; i < n; i++ {
			out.Index(i).Set(sanitizeValue(v.Index(i)))
		}
		return out

	case reflect.Map:
		if v.IsNil() {
			return reflect.MakeMap(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), sanitizeValue(iter.Value()))
		}
		return out

	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			out.Field(i).Set(sanitizeValue(v.Field(i)))
		}
		return out

	case reflect.Ptr:
		if v.IsNil() {
			return v
		}
		elem := sanitizeValue(v.Elem())
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(elem)
		return out

	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		elem := sanitizeValue(v.Elem())
		// Wrap the sanitized value back into an interface-typed Value.
		out := reflect.New(v.Type()).Elem()
		out.Set(elem)
		return out

	default:
		return v
	}
}
