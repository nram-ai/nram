package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
)

// NewEventsHandler returns an HTTP handler that streams SSE events from the bus.
// Supports optional "scope" query param for prefix filtering and the
// "Last-Event-ID" header for replay on reconnect. keepalive controls the
// interval between SSE keepalive pings; cmd/server/main.go resolves it from
// SettingEventsSSEKeepaliveSeconds at startup. Zero or negative falls back
// to 30s.
//
// The client-supplied "scope" is only a client-side narrowing filter; it is NOT
// a security boundary. Every event is independently authorized against the
// caller's identity (accessCfg) before delivery so a caller cannot receive
// another tenant's events by spoofing or omitting the scope. See
// authorizeEventScope for the tier rules.
func NewEventsHandler(bus events.EventBus, keepalive time.Duration, accessCfg ProjectAccessConfig) http.HandlerFunc {
	if keepalive <= 0 {
		keepalive = 30 * time.Second
	}
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ac := auth.FromContext(r.Context())
		isAdmin := ac != nil && ac.Role == auth.RoleAdministrator

		// authorized reports whether the caller may receive an event carrying
		// the given scope. Decisions are memoized for the life of the connection
		// (the handler runs in a single goroutine: the replay loop and the live
		// select loop below are sequential, so the map needs no lock). An
		// administrator receives every scope (authorizeEventScope short-circuits
		// on the role), so admins bypass both the lookup and the cache write —
		// otherwise a long-lived admin firehose would accumulate one map entry
		// per distinct scope for no benefit. Other callers pay at most one
		// resolution per distinct scope string.
		authzCache := make(map[string]bool)
		authorized := func(evtScope string) bool {
			if isAdmin {
				return true
			}
			if allowed, ok := authzCache[evtScope]; ok {
				return allowed
			}
			allowed := authorizeEventScope(r.Context(), accessCfg, ac, evtScope)
			authzCache[evtScope] = allowed
			return allowed
		}

		scope := r.URL.Query().Get("scope")

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		// Disable buffering on nginx, Cloudflare, and similar proxies; without
		// this they hold SSE frames until the response closes.
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// Replay buffered events if Last-Event-ID is provided. EventSource in
		// the browser cannot set arbitrary headers, so we also accept
		// ?last_event_id= as a query-param fallback used by the React
		// useEventStream hook on reconnect.
		lastEventID := r.Header.Get("Last-Event-ID")
		if lastEventID == "" {
			lastEventID = r.URL.Query().Get("last_event_id")
		}
		if lastEventID != "" {
			replayed := bus.Replay(lastEventID)
			for _, evt := range replayed {
				if scope != "" && !strings.HasPrefix(evt.Scope, scope) {
					continue
				}
				if !authorized(evt.Scope) {
					continue
				}
				writeSSE(w, evt)
			}
			flusher.Flush()
		}

		ch, cancel, err := bus.Subscribe(r.Context(), scope)
		if err != nil {
			return
		}
		defer cancel()

		keepaliveTicker := time.NewTicker(keepalive)
		defer keepaliveTicker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				if !authorized(evt.Scope) {
					continue
				}
				writeSSE(w, evt)
				flusher.Flush()
			case <-keepaliveTicker.C:
				_, _ = fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	}
}

// authorizeEventScope reports whether the caller identified by ac may receive an
// event carrying evtScope. It is the security boundary for the SSE stream and is
// applied to every event before delivery, using the event's own fully-formed
// scope rather than the client-supplied filter.
//
// Rules:
//   - administrators receive everything (the full cross-tenant firehose and all
//     system scopes);
//   - "maintenance" is a non-tenant global banner and reaches any authenticated
//     caller;
//   - the system/aggregate scopes ("db-migration", "vector-migration",
//     "global", and the empty scope used by the enrichment pool tick) carry no
//     tenant owner and are admin-only;
//   - "project:<uuid>" and "namespace:<uuid>" are authorized against the
//     caller's org via CheckProjectOrgAccess / CheckNamespaceOrgAccess (a
//     malformed UUID, including a bare "project:"/"namespace:" prefix, is
//     denied);
//   - any other scope is denied.
func authorizeEventScope(ctx context.Context, cfg ProjectAccessConfig, ac *auth.AuthContext, evtScope string) bool {
	if ac == nil {
		return false
	}
	if ac.Role == auth.RoleAdministrator {
		return true
	}

	switch evtScope {
	case events.EventScopeMaintenance:
		return true
	case events.EventScopeDBMigration, events.EventScopeVectorMigration, "global", "":
		// System/aggregate scopes with no tenant owner: admin-only (admins are
		// handled above).
		return false
	}

	if rest, ok := strings.CutPrefix(evtScope, "project:"); ok {
		id, err := uuid.Parse(rest)
		if err != nil {
			return false
		}
		return CheckProjectOrgAccess(ctx, cfg, ac, id) == nil
	}

	if rest, ok := strings.CutPrefix(evtScope, "namespace:"); ok {
		id, err := uuid.Parse(rest)
		if err != nil {
			return false
		}
		return CheckNamespaceOrgAccess(ctx, cfg, ac, id) == nil
	}

	return false
}

// writeSSE writes a single event in SSE wire format.
func writeSSE(w http.ResponseWriter, evt events.Event) {
	if evt.Data == nil {
		evt.Data = json.RawMessage("{}")
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", evt.ID, evt.Type, data)
}
