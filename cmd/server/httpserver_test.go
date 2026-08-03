package main

import (
	"testing"
	"time"
)

func TestNewHTTPServer_Timeouts(t *testing.T) {
	srv := newHTTPServer(":0", nil)

	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", srv.ReadHeaderTimeout)
	}
	if srv.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120s", srv.IdleTimeout)
	}
	// WriteTimeout must stay 0 so long-lived SSE streams are not severed.
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 (SSE-safe)", srv.WriteTimeout)
	}
}
