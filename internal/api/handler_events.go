package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nram-ai/nram/internal/events"
)

const sseKeepaliveInterval = 30 * time.Second

// NewEventsHandler returns an HTTP handler that streams SSE events from the bus.
// Supports optional "scope" query param for prefix filtering and the
// "Last-Event-ID" header for replay on reconnect.
func NewEventsHandler(bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		scope := r.URL.Query().Get("scope")

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// Replay buffered events if Last-Event-ID is provided.
		lastEventID := r.Header.Get("Last-Event-ID")
		if lastEventID != "" {
			replayed := bus.Replay(lastEventID)
			for _, evt := range replayed {
				if scope != "" && !strings.HasPrefix(evt.Scope, scope) {
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

		keepalive := time.NewTicker(sseKeepaliveInterval)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				writeSSE(w, evt)
				flusher.Flush()
			case <-keepalive.C:
				fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	}
}

// writeSSE writes a single event in SSE wire format.
func writeSSE(w http.ResponseWriter, evt events.Event) {
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", evt.ID, evt.Type, data)
}
