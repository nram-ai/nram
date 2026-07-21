package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/service"
)

// TestBuildRouterConfigWiresEverything is the only thing standing between a
// deleted wiring line and a silent production no-op.
//
// server.NewRouter treats a nil middleware field as "feature absent" so tests
// can build minimal routers. That makes nil ambiguous: it means both "this test
// does not exercise that middleware" and "someone dropped the line". Every
// other test in the repo builds its own RouterConfig, so the suite checks what
// NewRouter does GIVEN a config and never checks the config main() builds.
// Verified by deleting each field in turn: removing HostGuard, AskGate,
// SetupGuard, or ProjectAccess from the production wiring left the entire suite
// green. ProjectAccess is the tenant-isolation boundary, so that is not a
// theoretical concern.
//
// This walks the struct by reflection rather than naming fields, so a field
// added to RouterConfig later is covered without anyone remembering to update
// this test. That automatic coverage is the whole point: forgetting is the
// failure mode being defended against.
func TestBuildRouterConfigWiresEverything(t *testing.T) {
	// NewRateLimiter unconditionally starts a cleanup goroutine (a zero
	// interval substitutes a default rather than disabling it), so it has to be
	// stopped or this test leaks one per run. Matches internal/server/router_test.go.
	rl := auth.NewRateLimiter(1, 1, 0, 0)
	t.Cleanup(rl.Stop)

	// Dependencies are otherwise deliberately inert. buildRouterConfig only
	// closes over them, so nothing is dereferenced here, and the test stays
	// honest about the one thing it checks: that every field gets populated.
	cfg := buildRouterConfig(routerDeps{
		Metrics:             metrics.New(),
		AuthMiddleware:      auth.NewAuthMiddleware(nil, nil, []byte("test"), nil),
		RateLimiter:         rl,
		SetupComplete:       func(context.Context) bool { return true },
		ProjectAccess:       api.ProjectAccessConfig{},
		EnrichmentAvailable: func() bool { return true },
		Settings:            service.NewSettingsService(nil),
	})

	v := reflect.ValueOf(cfg)
	typ := v.Type()
	if typ.NumField() == 0 {
		t.Fatal("RouterConfig has no fields; this test is checking nothing")
	}

	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		switch kind := v.Field(i).Kind(); kind {
		case reflect.Func, reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.UnsafePointer:
			if v.Field(i).IsNil() {
				t.Errorf("RouterConfig.%s is nil: production wiring is missing it, "+
					"so the feature it gates is silently inactive", field.Name)
			}
		default:
			// Fail closed. A value-typed field cannot be checked for "unwired"
			// by a nil test, so rather than skip it silently, stop and make
			// someone decide: either give it a nillable type so this test
			// covers it, or assert it here explicitly once you have confirmed
			// its zero value is safe in production.
			t.Errorf("RouterConfig.%s has kind %s, which this test cannot check for being unwired. "+
				"Assert it explicitly, or confirm its zero value is safe.", field.Name, kind)
		}
	}
}
