package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSanitizeNils_NilSlice(t *testing.T) {
	type resp struct {
		Items []string `json:"items"`
	}
	v := resp{Items: nil}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	if string(b) != `{"items":[]}` {
		t.Fatalf("expected empty array, got %s", b)
	}
}

func TestSanitizeNils_NilMap(t *testing.T) {
	type resp struct {
		Meta map[string]string `json:"meta"`
	}
	v := resp{Meta: nil}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	if string(b) != `{"meta":{}}` {
		t.Fatalf("expected empty object, got %s", b)
	}
}

func TestSanitizeNils_NestedNilSlice(t *testing.T) {
	type inner struct {
		Tags []string `json:"tags"`
	}
	type outer struct {
		Items []inner `json:"items"`
	}
	v := outer{Items: []inner{{Tags: nil}}}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"items":[{"tags":[]}]}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_MapWrapper(t *testing.T) {
	// Simulates handler pattern: map[string]interface{}{"data": nilSlice}
	var items []string
	v := map[string]interface{}{"data": items}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"data":[]}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_NonNilPreserved(t *testing.T) {
	type resp struct {
		Items []string `json:"items"`
	}
	v := resp{Items: []string{"a", "b"}}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"items":["a","b"]}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestWriteJSON_SanitizesNilSlice(t *testing.T) {
	type resp struct {
		Items []string `json:"items"`
	}
	w := httptest.NewRecorder()
	writeJSON(w, 200, resp{Items: nil})
	body := w.Body.String()
	// json.Encoder adds trailing newline
	expected := "{\"items\":[]}\n"
	if body != expected {
		t.Fatalf("expected %q, got %q", expected, body)
	}
}

func TestSanitizeNils_NilRawMessage(t *testing.T) {
	type resp struct {
		Data json.RawMessage `json:"data"`
	}
	v := resp{Data: nil}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"data":{}}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_EmptyRawMessage(t *testing.T) {
	type resp struct {
		Data json.RawMessage `json:"data"`
	}
	v := resp{Data: json.RawMessage{}}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"data":{}}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_ValidRawMessage(t *testing.T) {
	type resp struct {
		Data json.RawMessage `json:"data"`
	}
	v := resp{Data: json.RawMessage(`{"key":"val"}`)}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"data":{"key":"val"}}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_PointerToStructWithNilSlice(t *testing.T) {
	type inner struct {
		Tags []string `json:"tags"`
	}
	v := &inner{Tags: nil}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"tags":[]}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_SliceOfStructsWithNilFields(t *testing.T) {
	type item struct {
		Tags []string `json:"tags"`
	}
	v := []item{{Tags: nil}}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `[{"tags":[]}]`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestSanitizeNils_NilSliceAndNilMap(t *testing.T) {
	type resp struct {
		Items []string          `json:"items"`
		Meta  map[string]string `json:"meta"`
	}
	v := resp{Items: nil, Meta: nil}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	expected := `{"items":[],"meta":{}}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestWriteJSON_ErrorResponse_OmitsNullDetails(t *testing.T) {
	w := httptest.NewRecorder()
	WriteError(w, ErrBadRequest("test"))
	body := w.Body.String()
	if strings.Contains(body, `"details"`) {
		t.Fatalf("expected no details key in response, got %s", body)
	}
	if w.Code != 400 {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}

func TestWriteJSON_ErrorResponse_IncludesDetails(t *testing.T) {
	w := httptest.NewRecorder()
	apiErr := ErrBadRequest("validation failed")
	apiErr.Details = map[string]string{"field": "required"}
	WriteError(w, apiErr)
	body := w.Body.String()
	if !strings.Contains(body, `"details"`) {
		t.Fatalf("expected details key in response, got %s", body)
	}
	if !strings.Contains(body, `"field"`) || !strings.Contains(body, `"required"`) {
		t.Fatalf("expected details content in response, got %s", body)
	}
}

func TestSanitizeNils_ComplexNestedResponse(t *testing.T) {
	type memory struct {
		Tags     []string        `json:"tags"`
		Metadata json.RawMessage `json:"metadata"`
	}
	type entity struct {
		Name string `json:"name"`
	}
	type recallResp struct {
		Memories []memory `json:"memories"`
		Entities []entity `json:"entities"`
	}
	v := recallResp{
		Memories: []memory{{Tags: nil, Metadata: nil}},
		Entities: nil,
	}
	sanitized := sanitizeNils(v)
	b, _ := json.Marshal(sanitized)
	// Verify Memories[0].Tags is [], Memories[0].Metadata is {}, Entities is []
	expected := `{"memories":[{"tags":[],"metadata":{}}],"entities":[]}`
	if string(b) != expected {
		t.Fatalf("expected %s, got %s", expected, b)
	}
}

func TestWriteJSON_NilTopLevelSlice(t *testing.T) {
	var items []string
	w := httptest.NewRecorder()
	writeJSON(w, 200, items)
	body := strings.TrimSpace(w.Body.String())
	if body != `[]` {
		t.Fatalf("expected [], got %s", body)
	}
}
