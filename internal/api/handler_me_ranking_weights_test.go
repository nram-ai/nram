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

// fullRankingWeightSchemas returns a schema slice that includes one entry
// for every key in rankingWeightKeys plus one unrelated setting. Tests use
// it to verify the handler returns exactly the eight ranking.weight.* keys
// and does not leak the unrelated entry.
func fullRankingWeightSchemas() []SettingSchema {
	minV := 0.0
	maxV := 1.0
	stepV := 0.05
	return []SettingSchema{
		{Key: service.SettingRankWeightSim, Type: "number", DefaultValue: json.RawMessage(`0.50`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightRec, Type: "number", DefaultValue: json.RawMessage(`0.15`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightImp, Type: "number", DefaultValue: json.RawMessage(`0.10`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightFreq, Type: "number", DefaultValue: json.RawMessage(`0.00`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightGraph, Type: "number", DefaultValue: json.RawMessage(`0.20`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightConf, Type: "number", DefaultValue: json.RawMessage(`0.05`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightOrigin, Type: "number", DefaultValue: json.RawMessage(`0.00`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: service.SettingRankWeightMmr, Type: "number", DefaultValue: json.RawMessage(`0.75`), Min: &minV, Max: &maxV, Step: &stepV},
		{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`)},
	}
}

func TestMeRankingWeightsDefaults_NonAdminReturnsAllEightKeys(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullRankingWeightSchemas(),
	}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []MeRankingWeightDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != len(rankingWeightKeys) {
		t.Fatalf("expected %d entries, got %d", len(rankingWeightKeys), len(resp.Data))
	}

	got := make(map[string]MeRankingWeightDefault, len(resp.Data))
	for _, e := range resp.Data {
		got[e.Key] = e
	}
	for _, key := range rankingWeightKeys {
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

func TestMeRankingWeightsDefaults_AdminGetsSameShape(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullRankingWeightSchemas(),
	}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "administrator"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []MeRankingWeightDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != len(rankingWeightKeys) {
		t.Fatalf("expected %d entries for admin, got %d", len(rankingWeightKeys), len(resp.Data))
	}
}

func TestMeRankingWeightsDefaults_NoAuthContextRejected(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullRankingWeightSchemas(),
	}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	// Pass nil auth context — the AuthMiddleware would normally reject the
	// request before it reaches the handler, but the handler defends in depth.
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeRankingWeightsDefaults_GlobalOverrideWinsOverDefault(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullRankingWeightSchemas(),
		// Operator set ranking.weight.similarity = 0.65 at scope=global.
		// The handler's GetSetting lookup must surface this as Value while
		// DefaultValue stays at the schema default (0.50).
		settings: []model.Setting{
			{
				Key:   service.SettingRankWeightSim,
				Value: json.RawMessage(`0.65`),
				Scope: "global",
			},
		},
	}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []MeRankingWeightDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var sim *MeRankingWeightDefault
	for i := range resp.Data {
		if resp.Data[i].Key == service.SettingRankWeightSim {
			sim = &resp.Data[i]
			break
		}
	}
	if sim == nil {
		t.Fatalf("similarity entry missing from response")
	}
	if sim.Value != 0.65 {
		t.Errorf("expected effective value 0.65 from operator override, got %v", sim.Value)
	}
	if sim.DefaultValue != 0.50 {
		t.Errorf("expected schema default 0.50 unchanged, got %v", sim.DefaultValue)
	}
}

func TestMeRankingWeightsDefaults_StringEncodedOverrideAccepted(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: fullRankingWeightSchemas(),
		// Some clients round-trip JSON numbers as quoted strings; the runtime
		// resolver already tolerates this via decodeNumeric. Confirm the
		// handler does too.
		settings: []model.Setting{
			{
				Key:   service.SettingRankWeightMmr,
				Value: json.RawMessage(`"0.40"`),
				Scope: "global",
			},
		},
	}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []MeRankingWeightDefault `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var mmr *MeRankingWeightDefault
	for i := range resp.Data {
		if resp.Data[i].Key == service.SettingRankWeightMmr {
			mmr = &resp.Data[i]
			break
		}
	}
	if mmr == nil {
		t.Fatalf("mmr_lambda entry missing")
	}
	if mmr.Value != 0.40 {
		t.Errorf("expected effective value 0.40 from string-encoded override, got %v", mmr.Value)
	}
}

func TestMeRankingWeightsDefaults_MissingSchemaEntryFailsClosed(t *testing.T) {
	// Drop one ranking-weight key from the schema slice. The handler must
	// 500 rather than silently returning a partial set; a partial set would
	// silently corrupt the placeholders on the editor and is the exact
	// failure mode the SPA's missingKeys banner was added to surface.
	full := fullRankingWeightSchemas()
	partial := make([]SettingSchema, 0, len(full)-1)
	for _, s := range full {
		if s.Key == service.SettingRankWeightMmr {
			continue
		}
		partial = append(partial, s)
	}
	store := &mockSettingsAdminStore{schemas: partial}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodGet, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on missing schema entry, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeRankingWeightsDefaults_PostRejected(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: fullRankingWeightSchemas()}
	h := NewMeRankingWeightsDefaultsHandler(MeRankingWeightsDefaultsConfig{Store: store})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	w := doSelfServiceRequest(h, http.MethodPost, "/v1/me/ranking-weights/defaults", nil, ac)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}
