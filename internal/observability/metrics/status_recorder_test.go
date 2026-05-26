package metrics

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests pin the optional-interface delegation on statusRecorder.
// A future simplification of the wrapper that drops Flush, Hijack, or
// Unwrap would break SSE (Flush), MCP-style hijack transports (Hijack),
// or any code reaching the underlying writer via http.ResponseController
// (Unwrap). Without explicit tests, that loss would be silent.

// hijackableRecorder is an http.ResponseWriter that records whether
// Hijack() was called, so a test can prove the wrapper actually
// delegated rather than swallowing the call.
type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, errors.New("test stub: no real connection")
}

// flushableRecorder records whether Flush() was called.
type flushableRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushableRecorder) Flush() { f.flushed = true }

func TestStatusRecorder_FlushDelegates(t *testing.T) {
	inner := &flushableRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: inner, statusCode: http.StatusOK}

	// The wrapper must satisfy http.Flusher.
	f, ok := any(sr).(http.Flusher)
	if !ok {
		t.Fatal("statusRecorder does not implement http.Flusher — SSE handlers will 500")
	}
	f.Flush()
	if !inner.flushed {
		t.Error("statusRecorder.Flush did not delegate to inner writer")
	}
}

func TestStatusRecorder_HijackDelegates(t *testing.T) {
	inner := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: inner, statusCode: http.StatusOK}

	// The wrapper must satisfy http.Hijacker.
	h, ok := any(sr).(http.Hijacker)
	if !ok {
		t.Fatal("statusRecorder does not implement http.Hijacker — hijack-based transports will fail")
	}
	_, _, err := h.Hijack()
	if !inner.hijacked {
		t.Error("statusRecorder.Hijack did not delegate to inner writer")
	}
	if err == nil {
		t.Error("expected stub error from inner Hijack to propagate, got nil")
	}
}

func TestStatusRecorder_HijackReturnsErrNotSupportedWhenInnerCannot(t *testing.T) {
	// When the inner writer does not implement http.Hijacker, the
	// wrapper must return http.ErrNotSupported rather than panic.
	sr := &statusRecorder{ResponseWriter: httptest.NewRecorder(), statusCode: http.StatusOK}
	conn, rw, err := sr.Hijack()
	if conn != nil || rw != nil {
		t.Error("expected nil conn and rw when inner does not support Hijack")
	}
	if !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("expected http.ErrNotSupported, got %v", err)
	}
}

func TestStatusRecorder_UnwrapExposesInnerWriter(t *testing.T) {
	// http.ResponseController and other framework code reaches the
	// underlying writer via Unwrap(). Pin the contract so a refactor
	// that removes Unwrap is caught.
	inner := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: inner, statusCode: http.StatusOK}

	type unwrapper interface {
		Unwrap() http.ResponseWriter
	}
	u, ok := any(sr).(unwrapper)
	if !ok {
		t.Fatal("statusRecorder does not expose Unwrap() — http.ResponseController callers will not reach the inner writer")
	}
	if u.Unwrap() != inner {
		t.Error("statusRecorder.Unwrap did not return the inner writer")
	}
}
