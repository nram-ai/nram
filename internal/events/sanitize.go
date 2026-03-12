package events

import (
	"encoding/json"
	"reflect"
)

var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

// sanitizeForJSON recursively replaces nil slices with empty slices and nil
// maps with empty maps so that json.Marshal produces [] and {} instead of null.
func sanitizeForJSON(v interface{}) interface{} {
	if v == nil {
		return v
	}
	return sanitizeVal(reflect.ValueOf(v)).Interface()
}

func sanitizeVal(v reflect.Value) reflect.Value {
	// Special-case json.RawMessage: if nil, default to "{}".
	if v.Type() == reflect.TypeOf(json.RawMessage{}) {
		if v.IsNil() || v.Len() == 0 {
			return reflect.ValueOf(json.RawMessage("{}"))
		}
		return v
	}

	if v.Type().Implements(jsonMarshalerType) || reflect.PointerTo(v.Type()).Implements(jsonMarshalerType) {
		return v
	}

	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}
		if v.IsNil() {
			return reflect.MakeSlice(v.Type(), 0, 0)
		}
		n := v.Len()
		out := reflect.MakeSlice(v.Type(), n, n)
		for i := 0; i < n; i++ {
			out.Index(i).Set(sanitizeVal(v.Index(i)))
		}
		return out

	case reflect.Map:
		if v.IsNil() {
			return reflect.MakeMap(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), sanitizeVal(iter.Value()))
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
			out.Field(i).Set(sanitizeVal(v.Field(i)))
		}
		return out

	case reflect.Ptr:
		if v.IsNil() {
			return v
		}
		elem := sanitizeVal(v.Elem())
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(elem)
		return out

	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		elem := sanitizeVal(v.Elem())
		out := reflect.New(v.Type()).Elem()
		out.Set(elem)
		return out

	default:
		return v
	}
}
