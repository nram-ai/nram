package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

const (
	webauthnChallengeExpiry = 5 * time.Minute
	webauthnMaxChallenges   = 10000
	webauthnSessionExpiry   = 1 * time.Hour

	challengePrefixReg    = "reg:"
	challengePrefixLogin  = "login:"
	defaultPasskeyName    = "Passkey"
	headerPasskeyName     = "X-Passkey-Name"
	headerWebAuthnSession = "X-Webauthn-Session"
)

// WebAuthnCredRepo defines storage methods needed by the WebAuthn handler.
type WebAuthnCredRepo interface {
	Create(ctx context.Context, cred *model.WebAuthnCredential) error
	ListByUser(ctx context.Context, userID uuid.UUID) ([]model.WebAuthnCredential, error)
	GetByCredentialID(ctx context.Context, credentialID string) (*model.WebAuthnCredential, error)
	UpdateSignCount(ctx context.Context, id uuid.UUID, signCount uint32) error
	HasCredentials(ctx context.Context, userID uuid.UUID) (bool, error)
}

// WebAuthnUserRepo defines user repository methods needed by the WebAuthn handler.
type WebAuthnUserRepo interface {
	GetByEmail(ctx context.Context, email string) (*model.User, error)
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
	UpdateLastLogin(ctx context.Context, userID uuid.UUID) error
}

// webauthnUser adapts model.User + credentials to the webauthn.User interface.
type webauthnUser struct {
	user  *model.User
	creds []webauthn.Credential
}

func (u *webauthnUser) WebAuthnID() []byte {
	return []byte(u.user.ID.String())
}

func (u *webauthnUser) WebAuthnName() string {
	return u.user.Email
}

func (u *webauthnUser) WebAuthnDisplayName() string {
	if u.user.DisplayName != "" {
		return u.user.DisplayName
	}
	return u.user.Email
}

func (u *webauthnUser) WebAuthnCredentials() []webauthn.Credential {
	return u.creds
}

// challengeEntry stores a WebAuthn session between begin/finish.
type challengeEntry struct {
	Session   *webauthn.SessionData
	ExpiresAt time.Time
}

// challengeStore is an in-memory store for WebAuthn challenges with TTL.
type challengeStore struct {
	mu         sync.Mutex
	challenges map[string]*challengeEntry
	stop       chan struct{}
}

func newChallengeStore() *challengeStore {
	s := &challengeStore{
		challenges: make(map[string]*challengeEntry),
		stop:       make(chan struct{}),
	}
	go s.cleanup()
	return s
}

func (s *challengeStore) Close() {
	close(s.stop)
}

func (s *challengeStore) Set(key string, session *webauthn.SessionData) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.challenges) >= webauthnMaxChallenges {
		return false
	}
	s.challenges[key] = &challengeEntry{
		Session:   session,
		ExpiresAt: time.Now().Add(webauthnChallengeExpiry),
	}
	return true
}

func (s *challengeStore) Get(key string) (*webauthn.SessionData, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.challenges[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.ExpiresAt) {
		delete(s.challenges, key)
		return nil, false
	}
	delete(s.challenges, key) // one-time use
	return entry.Session, true
}

func (s *challengeStore) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for k, v := range s.challenges {
				if now.After(v.ExpiresAt) {
					delete(s.challenges, k)
				}
			}
			s.mu.Unlock()
		}
	}
}

// WebAuthnHandler manages WebAuthn registration and authentication.
type WebAuthnHandler struct {
	credRepo       WebAuthnCredRepo
	userRepo       WebAuthnUserRepo
	jwtSecret      []byte
	challengeStore *challengeStore
}

type WebAuthnHandlerConfig struct {
	CredRepo  WebAuthnCredRepo
	UserRepo  WebAuthnUserRepo
	JWTSecret []byte
}

func NewWebAuthnHandler(cfg WebAuthnHandlerConfig) *WebAuthnHandler {
	return &WebAuthnHandler{
		credRepo:       cfg.CredRepo,
		userRepo:       cfg.UserRepo,
		jwtSecret:      cfg.JWTSecret,
		challengeStore: newChallengeStore(),
	}
}

func (h *WebAuthnHandler) Close() {
	h.challengeStore.Close()
}

// newWebAuthn creates a WebAuthn instance using the request's host for RP ID and origin.
func newWebAuthn(r *http.Request) (*webauthn.WebAuthn, error) {
	base := baseURLFromRequest(r)
	if base == "" {
		return nil, fmt.Errorf("cannot determine base URL from request")
	}

	// Extract host without port for RP ID.
	host := r.Host
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		// Check if it's not an IPv6 address.
		if !strings.Contains(host[idx:], "]") {
			host = host[:idx]
		}
	}

	return webauthn.New(&webauthn.Config{
		RPID:          host,
		RPDisplayName: "nram",
		RPOrigins:     []string{base},
		AuthenticatorSelection: protocol.AuthenticatorSelection{
			AuthenticatorAttachment: protocol.Platform,
			ResidentKey:             protocol.ResidentKeyRequirementPreferred,
			UserVerification:        protocol.VerificationPreferred,
		},
		AttestationPreference: protocol.PreferNoAttestation,
	})
}

// modelCredsToWebAuthn converts stored credentials to the webauthn library type.
func modelCredsToWebAuthn(creds []model.WebAuthnCredential) []webauthn.Credential {
	result := make([]webauthn.Credential, 0, len(creds))
	for _, c := range creds {
		credID, err := base64.RawURLEncoding.DecodeString(c.CredentialID)
		if err != nil {
			continue
		}
		pubKey, err := base64.RawURLEncoding.DecodeString(c.PublicKey)
		if err != nil {
			continue
		}
		aaguid, _ := base64.RawURLEncoding.DecodeString(c.AAGUID)

		transports := make([]protocol.AuthenticatorTransport, len(c.Transports))
		for i, t := range c.Transports {
			transports[i] = protocol.AuthenticatorTransport(t)
		}

		result = append(result, webauthn.Credential{
			ID:              credID,
			PublicKey:       pubKey,
			AttestationType: c.AttestationType,
			Transport:       transports,
			Flags: webauthn.CredentialFlags{
				UserPresent:    true,
				UserVerified:   c.UserVerified,
				BackupEligible: c.BackupEligible,
				BackupState:    c.BackupState,
			},
			Authenticator: webauthn.Authenticator{
				AAGUID:    aaguid,
				SignCount: c.SignCount,
			},
		})
	}
	return result
}

// RegisterBeginHandler starts a WebAuthn registration ceremony.
// POST, authenticated, body: {"name": "my passkey"}
func (h *WebAuthnHandler) RegisterBeginHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ac := FromContext(r.Context())
		if ac == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		if req.Name == "" {
			http.Error(w, "name is required", http.StatusBadRequest)
			return
		}

		userID := ac.UserID

		user, err := h.userRepo.GetByID(r.Context(), userID)
		if err != nil {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}

		existingCreds, err := h.credRepo.ListByUser(r.Context(), userID)
		if err != nil {
			http.Error(w, "failed to list credentials", http.StatusInternalServerError)
			return
		}

		wa, err := newWebAuthn(r)
		if err != nil {
			http.Error(w, "webauthn configuration error", http.StatusInternalServerError)
			return
		}

		waUser := &webauthnUser{
			user:  user,
			creds: modelCredsToWebAuthn(existingCreds),
		}

		creation, session, err := wa.BeginRegistration(waUser)
		if err != nil {
			http.Error(w, "failed to begin registration", http.StatusInternalServerError)
			return
		}

		sessionKey := challengePrefixReg + userID.String()
		if !h.challengeStore.Set(sessionKey, session) {
			http.Error(w, "too many pending registrations", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(creation)
	}
}

// RegisterFinishHandler completes a WebAuthn registration ceremony.
// POST, authenticated.
func (h *WebAuthnHandler) RegisterFinishHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ac := FromContext(r.Context())
		if ac == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		userID := ac.UserID

		// The registration name is passed via X-Passkey-Name header since the
		// body is the raw WebAuthn attestation response.
		name := strings.TrimSpace(r.Header.Get(headerPasskeyName))
		if name == "" {
			name = defaultPasskeyName
		}

		user, err := h.userRepo.GetByID(r.Context(), userID)
		if err != nil {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}

		existingCreds, err := h.credRepo.ListByUser(r.Context(), userID)
		if err != nil {
			http.Error(w, "failed to list credentials", http.StatusInternalServerError)
			return
		}

		sessionKey := challengePrefixReg + userID.String()
		session, ok := h.challengeStore.Get(sessionKey)
		if !ok {
			http.Error(w, "registration session expired or not found", http.StatusBadRequest)
			return
		}

		wa, err := newWebAuthn(r)
		if err != nil {
			http.Error(w, "webauthn configuration error", http.StatusInternalServerError)
			return
		}

		waUser := &webauthnUser{
			user:  user,
			creds: modelCredsToWebAuthn(existingCreds),
		}

		credential, err := wa.FinishRegistration(waUser, *session, r)
		if err != nil {
			http.Error(w, "registration verification failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		transports := make([]string, len(credential.Transport))
		for i, t := range credential.Transport {
			transports[i] = string(t)
		}

		aaguidStr := base64.RawURLEncoding.EncodeToString(credential.Authenticator.AAGUID)

		cred := &model.WebAuthnCredential{
			UserID:          userID,
			Name:            name,
			CredentialID:    base64.RawURLEncoding.EncodeToString(credential.ID),
			PublicKey:       base64.RawURLEncoding.EncodeToString(credential.PublicKey),
			AAGUID:          aaguidStr,
			SignCount:       credential.Authenticator.SignCount,
			Transports:      transports,
			UserVerified:    credential.Flags.UserVerified,
			BackupEligible:  credential.Flags.BackupEligible,
			BackupState:     credential.Flags.BackupState,
			AttestationType: credential.AttestationType,
		}

		if err := h.credRepo.Create(r.Context(), cred); err != nil {
			http.Error(w, "failed to store credential", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cred)
	}
}

// LoginBeginHandler starts a WebAuthn login ceremony.
// POST, unauthenticated, body: {"email": "..."}
func (h *WebAuthnHandler) LoginBeginHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		req.Email = strings.TrimSpace(req.Email)
		if req.Email == "" {
			http.Error(w, "email is required", http.StatusBadRequest)
			return
		}

		user, err := h.userRepo.GetByEmail(r.Context(), req.Email)
		if err != nil {
			// Don't reveal whether user exists.
			http.Error(w, "authentication failed", http.StatusUnauthorized)
			return
		}

		if user.DisabledAt != nil {
			http.Error(w, "account disabled", http.StatusForbidden)
			return
		}

		creds, err := h.credRepo.ListByUser(r.Context(), user.ID)
		if err != nil || len(creds) == 0 {
			http.Error(w, "authentication failed", http.StatusUnauthorized)
			return
		}

		wa, err := newWebAuthn(r)
		if err != nil {
			http.Error(w, "webauthn configuration error", http.StatusInternalServerError)
			return
		}

		waUser := &webauthnUser{
			user:  user,
			creds: modelCredsToWebAuthn(creds),
		}

		assertion, session, err := wa.BeginLogin(waUser)
		if err != nil {
			http.Error(w, "failed to begin login", http.StatusInternalServerError)
			return
		}

		sessionKey := challengePrefixLogin + generateRandomString(16)
		if !h.challengeStore.Set(sessionKey, session) {
			http.Error(w, "too many pending logins", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"publicKey":   assertion.Response,
			"session_key": sessionKey,
		})
	}
}

// LoginFinishHandler completes a WebAuthn login ceremony and returns a JWT.
// POST, unauthenticated.
func (h *WebAuthnHandler) LoginFinishHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		sessionKey := r.Header.Get(headerWebAuthnSession)
		if sessionKey == "" {
			http.Error(w, "missing session key", http.StatusBadRequest)
			return
		}

		session, ok := h.challengeStore.Get(sessionKey)
		if !ok {
			http.Error(w, "login session expired or not found", http.StatusBadRequest)
			return
		}

		// Recover user from session data.
		userIDStr := string(session.UserID)
		userID, err := uuid.Parse(userIDStr)
		if err != nil {
			http.Error(w, "invalid session", http.StatusBadRequest)
			return
		}

		user, err := h.userRepo.GetByID(r.Context(), userID)
		if err != nil {
			http.Error(w, "user not found", http.StatusUnauthorized)
			return
		}

		if user.DisabledAt != nil {
			http.Error(w, "account disabled", http.StatusForbidden)
			return
		}

		creds, err := h.credRepo.ListByUser(r.Context(), user.ID)
		if err != nil {
			http.Error(w, "failed to list credentials", http.StatusInternalServerError)
			return
		}

		wa, err := newWebAuthn(r)
		if err != nil {
			http.Error(w, "webauthn configuration error", http.StatusInternalServerError)
			return
		}

		waUser := &webauthnUser{
			user:  user,
			creds: modelCredsToWebAuthn(creds),
		}

		credential, err := wa.FinishLogin(waUser, *session, r)
		if err != nil {
			http.Error(w, "authentication failed: "+err.Error(), http.StatusUnauthorized)
			return
		}

		credIDStr := base64.RawURLEncoding.EncodeToString(credential.ID)
		storedCred, err := h.credRepo.GetByCredentialID(r.Context(), credIDStr)
		if err == nil {
			_ = h.credRepo.UpdateSignCount(r.Context(), storedCred.ID, credential.Authenticator.SignCount)
		}

		token, err := GenerateJWT(user.ID, user.OrgID, user.Role, h.jwtSecret, webauthnSessionExpiry)
		if err != nil {
			http.Error(w, "failed to create session", http.StatusInternalServerError)
			return
		}

		_ = h.userRepo.UpdateLastLogin(r.Context(), user.ID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"token": token,
			"user": map[string]interface{}{
				"id":           user.ID.String(),
				"email":        user.Email,
				"display_name": user.DisplayName,
				"role":         user.Role,
				"org_id":       user.OrgID.String(),
			},
		})
	}
}
