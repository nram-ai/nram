package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Instance identity system_meta keys. Both are generated once on first boot and
// loaded verbatim thereafter; neither is settable through the UI. The two values
// come from independent entropy, so the UUID is not derived from the key and the
// key is not derived from the UUID.
const (
	systemMetaInstanceID  = "instance_id"
	systemMetaInstanceKey = "instance_signing_key"
)

// InstanceSigningAlgorithm is the JOSE algorithm name for the instance keypair.
// The key is an ECDSA P-256 keypair, signed/verified as ES256.
const InstanceSigningAlgorithm = "ES256"

// InstanceIdentity is this deployment's persistent, server-wide identity: a v4
// UUID plus an ECDSA P-256 signing keypair. It is generated on first boot,
// persisted in system_meta, and loaded on every subsequent boot. The private key
// signs on the instance's behalf (telemetry attribution, future app
// registration); the public half is exposed read-only via PEM/JWK.
//
// The public representations (PEM, JWK, and the JWK thumbprint kid) are immutable
// for the process lifetime, so they are computed once at load and served from
// the cached fields rather than re-derived per request.
type InstanceIdentity struct {
	ID         uuid.UUID
	PrivateKey *ecdsa.PrivateKey
	CreatedAt  time.Time

	pem string // cached SPKI PEM of the public key
	jwk JWK    // cached public JWK (carries the kid)
}

// JWK is a JSON Web Key (RFC 7517) for an EC public key (RFC 7518 section 6.2).
// Only the public coordinates are ever populated; the private scalar is omitted.
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg,omitempty"`
	Kid string `json:"kid,omitempty"`
}

// JWKS is a JSON Web Key Set (RFC 7517 section 5).
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// LoadOrCreateInstanceIdentity reads the persistent instance identity from
// system_meta, generating any missing component on first boot. It mirrors
// LoadOrCreateJWTSecret: the UUID is stored as a plaintext string and the private
// key as a JSON array of base64-encoded PKCS#8 DER blobs (element 0 is active;
// the array shape leaves room for future rotation). The two values are generated
// independently, so a half-initialized state (one present, one missing) heals by
// generating only the missing component. The call is idempotent across boots.
func LoadOrCreateInstanceIdentity(ctx context.Context, db DB) (*InstanceIdentity, error) {
	id, createdAt, err := loadOrCreateInstanceID(ctx, db)
	if err != nil {
		return nil, err
	}

	priv, err := loadOrCreateInstanceKey(ctx, db)
	if err != nil {
		return nil, err
	}

	ident := &InstanceIdentity{ID: id, PrivateKey: priv, CreatedAt: createdAt}
	if err := ident.computePublic(); err != nil {
		return nil, err
	}
	return ident, nil
}

// loadOrCreateInstanceID returns the persistent instance UUID and the timestamp
// it was first written, generating a v4 UUID on first boot. The value and
// created_at are read in a single round trip.
func loadOrCreateInstanceID(ctx context.Context, db DB) (uuid.UUID, time.Time, error) {
	var value, createdAt string
	err := db.QueryRow(ctx, "SELECT value, created_at FROM system_meta WHERE key = $1", systemMetaInstanceID).
		Scan(&value, &createdAt)
	if err != nil && err != sql.ErrNoRows {
		return uuid.Nil, time.Time{}, fmt.Errorf("read %s: %w", systemMetaInstanceID, err)
	}
	if err == nil && value != "" {
		id, perr := uuid.Parse(value)
		if perr != nil {
			return uuid.Nil, time.Time{}, fmt.Errorf("parse %s: %w", systemMetaInstanceID, perr)
		}
		return id, parseInstanceTime(createdAt), nil
	}

	id := uuid.New() // v4, random
	if err := SetSystemMeta(ctx, db, systemMetaInstanceID, id.String()); err != nil {
		return uuid.Nil, time.Time{}, err
	}
	return id, time.Now().UTC(), nil
}

// loadOrCreateInstanceKey returns the persistent ECDSA P-256 private key,
// generating one on first boot.
func loadOrCreateInstanceKey(ctx context.Context, db DB) (*ecdsa.PrivateKey, error) {
	raw, err := GetSystemMeta(ctx, db, systemMetaInstanceKey)
	if err != nil {
		return nil, err
	}

	if raw != "" {
		var encoded []string
		if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
			return nil, fmt.Errorf("parse %s: %w", systemMetaInstanceKey, err)
		}
		if len(encoded) == 0 {
			return nil, fmt.Errorf("%s array is empty", systemMetaInstanceKey)
		}
		der, derr := base64.StdEncoding.DecodeString(encoded[0])
		if derr != nil {
			return nil, fmt.Errorf("decode %s: %w", systemMetaInstanceKey, derr)
		}
		key, perr := x509.ParsePKCS8PrivateKey(der)
		if perr != nil {
			return nil, fmt.Errorf("parse %s pkcs8: %w", systemMetaInstanceKey, perr)
		}
		priv, ok := key.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("%s is not an ECDSA key (%T)", systemMetaInstanceKey, key)
		}
		if priv.Curve != elliptic.P256() {
			return nil, fmt.Errorf("%s is not a P-256 key", systemMetaInstanceKey)
		}
		return priv, nil
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate instance signing key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal instance signing key: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(der)
	arr, _ := json.Marshal([]string{encoded})
	if err := SetSystemMeta(ctx, db, systemMetaInstanceKey, string(arr)); err != nil {
		return nil, err
	}
	return priv, nil
}

// parseInstanceTime parses an RFC 3339 system_meta timestamp, returning the zero
// time on any error. The instance CreatedAt is best-effort display metadata and
// never load-bearing.
func parseInstanceTime(raw string) time.Time {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t
	}
	return time.Time{}
}

// ReadInstanceID returns the persistent instance UUID if it has already been
// generated, without creating or modifying anything. ok is false when the
// identity has not been initialized yet or cannot be read. Used by the
// --version banner to surface the instance id best-effort.
func ReadInstanceID(ctx context.Context, db DB) (id string, ok bool) {
	raw, err := GetSystemMeta(ctx, db, systemMetaInstanceID)
	if err != nil || raw == "" {
		return "", false
	}
	return raw, true
}

// computePublic derives and caches the immutable public representations (PEM and
// JWK) from the private key. Called once at load so the read-only surfaces serve
// precomputed values instead of re-running the ECDH conversion and thumbprint
// hash on every request.
func (i *InstanceIdentity) computePublic() error {
	der, err := x509.MarshalPKIXPublicKey(i.PublicKey())
	if err != nil {
		return fmt.Errorf("marshal public key: %w", err)
	}
	i.pem = string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	x, y, err := i.publicKeyXY()
	if err != nil {
		return err
	}
	i.jwk = JWK{
		Kty: "EC",
		Crv: "P-256",
		X:   x,
		Y:   y,
		Use: "sig",
		Alg: InstanceSigningAlgorithm,
		Kid: jwkThumbprint(x, y),
	}
	return nil
}

// PublicKey returns the public half of the instance signing key.
func (i *InstanceIdentity) PublicKey() *ecdsa.PublicKey {
	return &i.PrivateKey.PublicKey
}

// PublicKeyPEM returns the instance public key as a PKIX/SPKI PEM block.
func (i *InstanceIdentity) PublicKeyPEM() string {
	return i.pem
}

// PublicKeyJWK returns the instance public key as a JWK (RFC 7518 section 6.2)
// with a stable RFC 7638 thumbprint as the key id.
func (i *InstanceIdentity) PublicKeyJWK() JWK {
	return i.jwk
}

// publicKeyXY returns the base64url-encoded affine coordinates of the instance
// public key. It goes through crypto/ecdh rather than the deprecated
// ecdsa.PublicKey.X/Y fields: ecdh.PublicKey.Bytes() yields the uncompressed
// SEC1 point 0x04 || X || Y, which is sliced into its 32-byte halves.
func (i *InstanceIdentity) publicKeyXY() (x, y string, err error) {
	ecdhPub, err := i.PublicKey().ECDH()
	if err != nil {
		return "", "", fmt.Errorf("convert public key to ecdh: %w", err)
	}
	raw := ecdhPub.Bytes()
	size := (i.PrivateKey.Curve.Params().BitSize + 7) / 8 // 32 for P-256
	if len(raw) != 1+2*size {
		return "", "", fmt.Errorf("unexpected uncompressed point length %d", len(raw))
	}
	x = base64.RawURLEncoding.EncodeToString(raw[1 : 1+size])
	y = base64.RawURLEncoding.EncodeToString(raw[1+size:])
	return x, y, nil
}

// Sign hashes data with SHA-256 and signs the digest with the instance key,
// returning the JWS ES256 signature form: the raw concatenation r||s, each
// big-endian and left-padded to the curve size (64 bytes total for P-256). A
// verifier can split it and check it with ecdsa.Verify.
func (i *InstanceIdentity) Sign(data []byte) ([]byte, error) {
	digest := sha256.Sum256(data)
	r, s, err := ecdsa.Sign(rand.Reader, i.PrivateKey, digest[:])
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	size := (i.PrivateKey.Curve.Params().BitSize + 7) / 8
	sig := make([]byte, 2*size)
	r.FillBytes(sig[:size])
	s.FillBytes(sig[size:])
	return sig, nil
}

// SignJWT signs the given claims as an ES256 JWS with the instance key id in the
// header. This is the convenience path for future instance-signed tokens
// (telemetry submissions, app-registration assertions).
func (i *InstanceIdentity) SignJWT(claims jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = i.jwk.Kid
	signed, err := token.SignedString(i.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

// jwkThumbprint computes the RFC 7638 thumbprint for an EC P-256 public key from
// its base64url x and y coordinates. The hash input is the JWK's required
// members in lexicographic order with no whitespace: crv, kty, x, y.
func jwkThumbprint(x, y string) string {
	canonical := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":%q,"y":%q}`, x, y)
	sum := sha256.Sum256([]byte(canonical))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
