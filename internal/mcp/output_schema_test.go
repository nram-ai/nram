package mcp

import "testing"

// TestSchemaForRejectsNonStruct pins the warn-and-degrade contract on
// schemaFor[T any]. The function force-sets the schema's root Type to
// "object", which is only consistent when the reflected schema is already
// object-shaped (i.e. T is a struct). A slice, scalar, or map T would
// otherwise yield a malformed schema (type:object with array/scalar fields);
// degrading to nil keeps the tool registered without an outputSchema rather
// than misadvertising one to strict clients.
func TestSchemaForRejectsNonStruct(t *testing.T) {
	type okShape struct {
		Name string `json:"name"`
	}
	if out := schemaFor[okShape](); out == nil {
		t.Error("schemaFor[struct] returned nil; expected a schema")
	}
	if out := schemaFor[[]int](); out != nil {
		t.Errorf("schemaFor[[]int] returned non-nil schema %s; want nil", string(out))
	}
	if out := schemaFor[string](); out != nil {
		t.Errorf("schemaFor[string] returned non-nil schema %s; want nil", string(out))
	}
	if out := schemaFor[map[string]int](); out != nil {
		t.Errorf("schemaFor[map] returned non-nil schema %s; want nil", string(out))
	}
}
