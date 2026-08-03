package auth

import (
	"testing"

	"github.com/go-webauthn/webauthn/webauthn"
)

// TestAuthenticatorCloned pins SEC-15: the passkey login path rejects a login
// when go-webauthn raises CloneWarning (a signature-counter regression, the
// clone signal). Driving a real counter regression needs a full assertion
// ceremony with a virtual authenticator, so this unit-tests the decision the
// handler gates on before it updates the stored counter.
func TestAuthenticatorCloned(t *testing.T) {
	warned := &webauthn.Credential{}
	warned.Authenticator.CloneWarning = true
	if !authenticatorCloned(warned) {
		t.Error("CloneWarning=true must be detected as a clone")
	}

	ok := &webauthn.Credential{}
	ok.Authenticator.CloneWarning = false
	if authenticatorCloned(ok) {
		t.Error("CloneWarning=false must not be flagged")
	}

	if authenticatorCloned(nil) {
		t.Error("nil credential must not be flagged")
	}
}
