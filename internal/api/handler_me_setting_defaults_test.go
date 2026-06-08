package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// fullSettingDefaultSchemas returns a schema slice covering every key in
// settingDefaultKeys plus one unrelated setting, so tests can assert the
// handler returns exactly the allow-listed keys and leaks nothing else.
func fullSettingDefaultSchemas() []SettingSchema {
	gMin, gMax, gStep := 0.0, 3.0, 0.05
	cMin, cMax, cStep := -100.0, 0.0, 1.0
	lMin, lMax, lStep := 5.0, 100.0, 1.0
	dMin, dMax, dStep := 0.0, 1.0, 0.01
	return []SettingSchema{
		{Key: service.SettingGraphCenterGravity, Type: "number", DefaultValue: json.RawMessage(`0.75`), Min: &gMin, Max: &gMax, Step: &gStep},
		{Key: service.SettingGraphChargeStrength, Type: "number", DefaultValue: json.RawMessage(`-100`), Min: &cMin, Max: &cMax, Step: &cStep},
		{Key: service.SettingGraphLinkDistance, Type: "number", DefaultValue: json.RawMessage(`100`), Min: &lMin, Max: &lMax, Step: &lStep},
		{Key: service.SettingDedupThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Min: &dMin, Max: &dMax, Step: &dStep},
		{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`)},
	}
}

func TestMeSettingDefaults_NonAdminReturnsAllowlistedKeys(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: fullSettingDefaultSchemas()}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []MeSettingDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != len(settingDefaultKeys) {
		t.Fatalf("expected %d entries, got %d", len(settingDefaultKeys), len(resp.Data))
	}

	got := make(map[string]MeSettingDefault, len(resp.Data))
	for _, e := range resp.Data {
		got[e.Key] = e
	}
	for _, key := range settingDefaultKeys {
		entry, ok := got[key]
		if !ok {
			t.Errorf("missing key %q in response", key)
			continue
		}
		// Default and effective values must match when no override is set.
		if entry.Value != entry.DefaultValue {
			t.Errorf("key %q: value %v != default_value %v with no override stored", key, entry.Value, entry.DefaultValue)
		}
		// Min, Max, Step propagate through from the schema entry.
		if entry.Min == nil || entry.Max == nil || entry.Step == nil {
			t.Errorf("key %q: expected min/max/step to be set", key)
		}
	}
	if _, leaked := got["enrichment.batch_size"]; leaked {
		t.Error("response leaked an unrelated schema key (enrichment.batch_size); allow-list should filter it out")
	}
}

func TestMeSettingDefaults_GraphChargeNegativeDefaultPreserved(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: fullSettingDefaultSchemas()}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Data []MeSettingDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := map[string]float64{
		service.SettingGraphChargeStrength: -100,
		service.SettingGraphLinkDistance:   100,
		service.SettingGraphCenterGravity:  0.75,
	}
	for _, e := range resp.Data {
		if exp, ok := want[e.Key]; ok && e.Value != exp {
			t.Errorf("key %q: expected default %v, got %v", e.Key, exp, e.Value)
		}
	}
}

func TestMeSettingDefaults_GlobalOverrideWinsOverDefault(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullSettingDefaultSchemas(),
		// Operator narrowed the link distance to 40 at scope=global.
		settings: []model.Setting{
			{Key: service.SettingGraphLinkDistance, Value: json.RawMessage(`40`), Scope: "global"},
		},
	}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Data []MeSettingDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var link *MeSettingDefault
	for i := range resp.Data {
		if resp.Data[i].Key == service.SettingGraphLinkDistance {
			link = &resp.Data[i]
			break
		}
	}
	if link == nil {
		t.Fatalf("link distance entry missing from response")
	}
	if link.Value != 40 {
		t.Errorf("expected effective value 40 from operator override, got %v", link.Value)
	}
	if link.DefaultValue != 100 {
		t.Errorf("expected schema default 100 unchanged, got %v", link.DefaultValue)
	}
}

func TestMeSettingDefaults_StringEncodedOverrideAccepted(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullSettingDefaultSchemas(),
		settings: []model.Setting{
			{Key: service.SettingDedupThreshold, Value: json.RawMessage(`"0.80"`), Scope: "global"},
		},
	}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Data []MeSettingDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var dedup *MeSettingDefault
	for i := range resp.Data {
		if resp.Data[i].Key == service.SettingDedupThreshold {
			dedup = &resp.Data[i]
			break
		}
	}
	if dedup == nil {
		t.Fatalf("dedup threshold entry missing")
	}
	if dedup.Value != 0.80 {
		t.Errorf("expected effective value 0.80 from string-encoded override, got %v", dedup.Value)
	}
}

func TestMeSettingDefaults_MissingSchemaEntryFailsClosed(t *testing.T) {
	full := fullSettingDefaultSchemas()
	partial := make([]SettingSchema, 0, len(full)-1)
	for _, s := range full {
		if s.Key == service.SettingGraphLinkDistance {
			continue
		}
		partial = append(partial, s)
	}
	store := &mockSettingsAdminStore{schemas: partial}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on missing schema entry, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeSettingDefaults_NoAuthContextRejected(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: fullSettingDefaultSchemas()}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/setting-defaults", nil, nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeSettingDefaults_PostRejected(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: fullSettingDefaultSchemas()}
	h := NewMeSettingDefaultsHandler(MeSettingDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodPost, "/v1/me/setting-defaults", nil, ac)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}
