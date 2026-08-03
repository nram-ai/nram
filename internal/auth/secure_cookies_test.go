package auth

import (
	"net/http"
	"testing"
)

func TestRequestIsSecure_ForceFlag(t *testing.T) {
	t.Cleanup(func() { SetForceSecureCookies(false) })

	plain := &http.Request{Header: http.Header{}}

	SetForceSecureCookies(false)
	if RequestIsSecure(plain) {
		t.Fatal("a plain http request must not be reported secure by default")
	}

	SetForceSecureCookies(true)
	if !RequestIsSecure(plain) {
		t.Fatal("server.secure_cookies=on must make every request report secure")
	}

	// The X-Forwarded-Proto path stays independent of the flag.
	SetForceSecureCookies(false)
	fwd := &http.Request{Header: http.Header{}}
	fwd.Header.Set("X-Forwarded-Proto", "https")
	if !RequestIsSecure(fwd) {
		t.Fatal("X-Forwarded-Proto=https must report secure")
	}
}
