package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// resetInstanceIdentity clears the two system_meta rows so the next
// LoadOrCreateInstanceIdentity exercises first-boot generation. The shared
// Postgres test DB persists rows across subtests, so this is required there.
func resetInstanceIdentity(t *testing.T, db DB) {
	t.Helper()
	if _, err := db.Exec(context.Background(),
		"DELETE FROM system_meta WHERE key IN ($1, $2)",
		systemMetaInstanceID, systemMetaInstanceKey); err != nil {
		t.Fatalf("reset instance identity: %v", err)
	}
}

func TestLoadOrCreateInstanceIdentity(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		resetInstanceIdentity(t, db)

		first, err := LoadOrCreateInstanceIdentity(ctx, db)
		if err != nil {
			t.Fatalf("first load: %v", err)
		}
		if first.ID.Version() != 4 {
			t.Fatalf("want a v4 UUID, got v%d (%s)", first.ID.Version(), first.ID)
		}
		if first.PrivateKey == nil {
			t.Fatal("nil private key")
		}

		// Idempotent reload: same UUID and same key material across boots.
		second, err := LoadOrCreateInstanceIdentity(ctx, db)
		if err != nil {
			t.Fatalf("second load: %v", err)
		}
		if second.ID != first.ID {
			t.Fatalf("UUID changed across reload: %s != %s", first.ID, second.ID)
		}
		if !first.PrivateKey.Equal(second.PrivateKey) {
			t.Fatal("signing key changed across reload")
		}

		// Independence: regenerating from a blank slate yields both a different
		// UUID and a different key, confirming the two draw from independent
		// entropy (the key is not derived from the UUID, nor vice versa).
		resetInstanceIdentity(t, db)
		fresh, err := LoadOrCreateInstanceIdentity(ctx, db)
		if err != nil {
			t.Fatalf("fresh load: %v", err)
		}
		if fresh.ID == first.ID {
			t.Fatal("regenerated UUID collided with the prior one")
		}
		if fresh.PrivateKey.Equal(first.PrivateKey) {
			t.Fatal("regenerated key collided with the prior one")
		}

		// Sign / verify round trip on the raw r||s JOSE form.
		msg := []byte("anonymized-telemetry-batch-001")
		sig, err := fresh.Sign(msg)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		if len(sig) != 64 {
			t.Fatalf("want 64-byte P-256 signature, got %d", len(sig))
		}
		digest := sha256.Sum256(msg)
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		if !ecdsa.Verify(fresh.PublicKey(), digest[:], r, s) {
			t.Fatal("signature failed to verify against the public key")
		}

		// PEM is a well-formed SPKI public-key block.
		pemStr := fresh.PublicKeyPEM()
		if !strings.Contains(pemStr, "BEGIN PUBLIC KEY") {
			t.Fatalf("unexpected PEM block:\n%s", pemStr)
		}

		// JWK is a complete EC P-256 key with a stable thumbprint kid.
		jwk := fresh.PublicKeyJWK()
		if jwk.Kty != "EC" || jwk.Crv != "P-256" || jwk.X == "" || jwk.Y == "" || jwk.Kid == "" {
			t.Fatalf("incomplete JWK: %+v", jwk)
		}
		// The cached JWK is stable across repeated reads.
		if reread := fresh.PublicKeyJWK(); reread.Kid != jwk.Kid {
			t.Fatalf("JWK kid not stable across reads: %q vs %q", reread.Kid, jwk.Kid)
		}

		// SignJWT issues an ES256 token carrying the kid header that verifies
		// against the public key.
		tokenStr, err := fresh.SignJWT(jwt.RegisteredClaims{Subject: fresh.ID.String()})
		if err != nil {
			t.Fatalf("sign jwt: %v", err)
		}
		parsed, err := jwt.Parse(tokenStr, func(tok *jwt.Token) (any, error) {
			if _, ok := tok.Method.(*jwt.SigningMethodECDSA); !ok {
				return nil, fmt.Errorf("unexpected signing method %v", tok.Header["alg"])
			}
			return fresh.PublicKey(), nil
		})
		if err != nil {
			t.Fatalf("parse jwt: %v", err)
		}
		if !parsed.Valid {
			t.Fatal("signed JWT did not validate")
		}
		if parsed.Header["kid"] != jwk.Kid {
			t.Fatalf("JWT kid header %v != %q", parsed.Header["kid"], jwk.Kid)
		}
		if parsed.Header["alg"] != "ES256" {
			t.Fatalf("JWT alg header %v != ES256", parsed.Header["alg"])
		}
	})
}
