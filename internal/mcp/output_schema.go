package mcp

import (
	"encoding/json"
	"log/slog"
	"reflect"

	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
)

// schemaFor generates a JSON Schema for T using the same reflector flags
// mark3labs/mcp-go's WithOutputSchema applies, plus a Mapper that fixes the
// two reflection mismatches that would otherwise misadvertise the wire:
//
//  1. uuid.UUID: invopop/jsonschema reflects [16]byte arrays as
//     {type:"array", items:{type:"integer"}, minItems:16, maxItems:16}, but
//     uuid.UUID.MarshalJSON emits a quoted UUID string. Mapper rewrites it to
//     {type:"string", format:"uuid"} so strict clients can validate the wire.
//
//  2. json.RawMessage: invopop emits an unconstrained schema by default; nram
//     RawMessage fields (memory.metadata, entity.properties) carry arbitrary
//     JSON values, which by spec may be object/array/string/number/bool/null.
//     Mapper rewrites to the empty schema `{}` (match-anything) so strict
//     validators accept whatever JSON shape the field actually holds. Using
//     {type:"object"} would falsely reject legitimate non-object payloads.
//
// Marshal failures degrade to nil; mcp.WithRawOutputSchema(nil) is a no-op
// in mcp-go v0.45.0, so a tool with reflection trouble registers without an
// outputSchema rather than crashing the entire server at NewServer time. The
// failure is logged so operators can fix the offending T; other tools keep
// working.
//
// Use with mcp.WithRawOutputSchema(schemaFor[T]()) instead of
// mcp.WithOutputSchema[T]() so the Mapper takes effect.
func schemaFor[T any]() json.RawMessage {
	var zero T
	// MCP spec mandates the root be an object; the force-set below assumes
	// T's reflected schema is already object-shaped. A non-struct T (slice,
	// map at the top level, scalar) would produce an inconsistent schema
	// (e.g. type:object with items:{}, which is invalid). Degrade rather
	// than misadvertise: log and return nil so mcp.WithRawOutputSchema(nil)
	// registers the tool without an outputSchema. Sibling warn-and-degrade
	// pattern with the json.Marshal failure branch below.
	if k := reflect.TypeOf(zero).Kind(); k != reflect.Struct {
		slog.Warn("mcp: schemaFor expects a struct type; tool will register without outputSchema",
			"type", reflect.TypeOf(zero).String(), "kind", k.String())
		return nil
	}
	r := &jsonschema.Reflector{
		DoNotReference:            true,
		Anonymous:                 true,
		AllowAdditionalProperties: true,
		Mapper:                    outputSchemaMapper,
	}
	s := r.Reflect(zero)
	s.Version = ""
	// Reflecting a struct already yields type=object; force it explicitly
	// to match mcp-go's WithOutputSchema post-processing behavior.
	s.Type = "object"

	out, err := json.Marshal(s)
	if err != nil {
		// Degrade rather than crash the server. Log so operators see it.
		slog.Warn("mcp: schemaFor marshal failed; tool will register without outputSchema",
			"type", reflect.TypeOf(zero).String(), "err", err)
		return nil
	}
	return out
}

var (
	uuidType       = reflect.TypeFor[uuid.UUID]()
	rawMessageType = reflect.TypeFor[json.RawMessage]()
)

func outputSchemaMapper(t reflect.Type) *jsonschema.Schema {
	switch t {
	case uuidType:
		return &jsonschema.Schema{Type: "string", Format: "uuid"}
	case rawMessageType:
		// Empty schema = match any JSON value. RawMessage can hold object,
		// array, string, number, bool, or null; do not narrow to one shape.
		return &jsonschema.Schema{}
	}
	return nil
}
