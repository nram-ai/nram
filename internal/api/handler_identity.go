package api

import (
	"net/http"
	"time"

	"github.com/nram-ai/nram/internal/storage"
)

// instanceIdentityResponse is the admin-only view of this deployment's
// persistent instance identity. The private key is never included.
type instanceIdentityResponse struct {
	InstanceID   string      `json:"instance_id"`
	Algorithm    string      `json:"algorithm"`
	PublicKeyPEM string      `json:"public_key_pem"`
	PublicKeyJWK storage.JWK `json:"public_key_jwk"`
	CreatedAt    string      `json:"created_at,omitempty"`
}

// NewInstanceIdentityHandler serves the admin-only instance identity view at
// GET /v1/admin/system/identity: the instance UUID, signing algorithm, and the
// public key in PEM and JWK form. The private key is never exposed.
func NewInstanceIdentityHandler(identity *storage.InstanceIdentity) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if identity == nil {
			WriteError(w, ErrNotFound("instance identity is not configured"))
			return
		}
		resp := instanceIdentityResponse{
			InstanceID:   identity.ID.String(),
			Algorithm:    storage.InstanceSigningAlgorithm,
			PublicKeyPEM: identity.PublicKeyPEM(),
			PublicKeyJWK: identity.PublicKeyJWK(),
		}
		if !identity.CreatedAt.IsZero() {
			resp.CreatedAt = identity.CreatedAt.UTC().Format(time.RFC3339)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// NewJWKSHandler serves the instance public key as a JWK Set (RFC 7517) at the
// public GET /.well-known/jwks.json. This is the standard discovery surface a
// future central router or external app uses to verify instance-signed JWTs.
// Only the public key is ever served.
func NewJWKSHandler(identity *storage.InstanceIdentity) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if identity == nil {
			WriteError(w, ErrNotFound("instance identity is not configured"))
			return
		}
		writeJSON(w, http.StatusOK, storage.JWKS{Keys: []storage.JWK{identity.PublicKeyJWK()}})
	}
}
