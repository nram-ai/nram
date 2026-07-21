package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/netutil"
)

// authorizeRequestParams captures the OAuth authorize request after each
// field has been validated. The React consent page reads these from query
// params on GET and round-trips them through hidden inputs on the
// approve/deny form POST so the POST handler can recover them without
// re-parsing the URL.
type authorizeRequestParams struct {
	ClientID            string
	RedirectURI         string
	ResponseType        string
	CodeChallenge       string
	CodeChallengeMethod string
	Scope               string
	Resource            string
	State               string
}

// authorizeOutcome is the result of a successful approve decision: the
// minted authorization code and the assembled callback URL (redirect_uri
// with code + state). Both the form flow (302 to callbackURL) and the
// loopback JSON flow (returns these fields to the SPA) consume it.
type authorizeOutcome struct {
	code        string
	callbackURL string
}

// errAuthorizeNeedsLogin signals that the account-holder approve path found
// no active session. The form flow routes the user to /login; the loopback
// JSON flow returns a login_required error.
var errAuthorizeNeedsLogin = errors.New("login required")

// redirectError is an approve failure that occurs after redirect_uri has
// been trusted. In the form flow it becomes a redirect-with-error to the
// client (RFC 6749 §4.1.2.1); in the loopback JSON flow it becomes a JSON
// error body carrying the same OAuth error code.
type redirectError struct {
	Code        string
	Description string
}

func (e *redirectError) Error() string { return e.Description }

// isLoopbackRedirectURI reports whether redirectURI targets the loopback
// interface (RFC 8252 §7.3): host "localhost", the IPv6 loopback ::1, or any
// IPv4 in 127.0.0.0/8. Native / CLI OAuth clients use a loopback redirect
// because they cannot register a public callback URL; those are the only
// clients the JSON completion endpoint serves, so the authorization code is
// never returned in a readable body to a public web client.
func isLoopbackRedirectURI(redirectURI string) bool {
	u, err := url.Parse(redirectURI)
	if err != nil {
		return false
	}
	// u.Host, not u.Hostname(): IsLoopbackHost strips the port and IPv6
	// brackets itself, and is shared with the HTTP Host-rebinding guard.
	return netutil.IsLoopbackHost(u.Host)
}

// authorizeContextResponse is the JSON payload served at
// GET /v1/oauth/authorize/context. The React consent page calls this on
// mount to validate the OAuth request, learn whether a session is present
// (account-holder path), and discover whether share-paste is configured.
//
// When validation produces an OAuth-spec error redirect (RFC 6749
// §4.1.2.1, error_response sent to redirect_uri), the response is 200
// with {redirect_to: "<callback URL with error params>"} and the SPA
// does a window.location.replace. When validation produces an
// unredirectable error (no trusted redirect_uri yet), the response is
// 400 with {error, error_description}.
type authorizeContextResponse struct {
	ClientID            string               `json:"client_id"`
	ClientName          string               `json:"client_name,omitempty"`
	RedirectURI         string               `json:"redirect_uri"`
	ResponseType        string               `json:"response_type"`
	CodeChallenge       string               `json:"code_challenge"`
	CodeChallengeMethod string               `json:"code_challenge_method"`
	Scope               string               `json:"scope,omitempty"`
	Resource            string               `json:"resource,omitempty"`
	State               string               `json:"state,omitempty"`
	AccountUser         *accountUserResponse `json:"account_user"`
	ShareTokenSupported bool                 `json:"share_token_supported"`
}

// accountUserResponse is the minimal account snapshot surfaced to the
// React consent page so it can render "Continue as <DisplayName>".
type accountUserResponse struct {
	DisplayName string `json:"display_name"`
	Email       string `json:"email"`
}

// sharePreviewRequest is the JSON body for POST /v1/oauth/share/preview.
// All OAuth params travel with the secret so the server re-validates the
// authorization request alongside the share, even though preview itself
// does not consume the share.
// oauthAuthorizeFields holds the standard OAuth authorize parameters carried by
// the JSON request bodies (share preview and loopback complete). Its get method
// exposes them through the func(string) string accessor that
// parseAuthorizeClientAndRedirect / parseAuthorizeRest expect, so both handlers
// validate through the same pipeline without re-declaring the fields or the
// key switch.
type oauthAuthorizeFields struct {
	ClientID            string `json:"client_id"`
	RedirectURI         string `json:"redirect_uri"`
	ResponseType        string `json:"response_type"`
	CodeChallenge       string `json:"code_challenge"`
	CodeChallengeMethod string `json:"code_challenge_method"`
	Scope               string `json:"scope"`
	Resource            string `json:"resource"`
	State               string `json:"state"`
}

func (f oauthAuthorizeFields) get(key string) string {
	switch key {
	case "client_id":
		return f.ClientID
	case "redirect_uri":
		return f.RedirectURI
	case "response_type":
		return f.ResponseType
	case "code_challenge":
		return f.CodeChallenge
	case "code_challenge_method":
		return f.CodeChallengeMethod
	case "scope":
		return f.Scope
	case "resource":
		return f.Resource
	case "state":
		return f.State
	}
	return ""
}

type sharePreviewRequest struct {
	oauthAuthorizeFields
	ShareToken string `json:"share_token"`
}

// sharePreviewResponse is the JSON payload returned to the React consent
// page after a successful preview. It describes what the recipient is
// about to authorize so they can review before the final approve POST.
type sharePreviewResponse struct {
	OwnerName   string           `json:"owner_name"`
	ShareName   string           `json:"share_name"`
	Description string           `json:"description"`
	ExpiresAt   string           `json:"expires_at"`
	IsOneShot   bool             `json:"is_one_shot"`
	Grants      []shareGrantJSON `json:"grants"`
}

// shareGrantJSON is one row in the grants table on the consent / share
// accept React pages.
type shareGrantJSON struct {
	ProjectName string `json:"project_name"`
	ProjectSlug string `json:"project_slug"`
	Permission  string `json:"permission"`
}

// shareAcceptResponse is the JSON payload for GET /v1/share/accept. The
// React landing page renders this for recipients of a share link before
// they configure their MCP client. The page is intentionally tolerant of
// resolution failures: revoked / expired / consumed shares return 200
// with {error} so the React page can show a friendly message instead of
// a stack trace.
type shareAcceptResponse struct {
	OwnerName    string           `json:"owner_name,omitempty"`
	ShareName    string           `json:"share_name,omitempty"`
	Description  string           `json:"description,omitempty"`
	ExpiresAt    string           `json:"expires_at,omitempty"`
	Grants       []shareGrantJSON `json:"grants,omitempty"`
	MCPServerURL string           `json:"mcp_server_url,omitempty"`
	ShareToken   string           `json:"share_token,omitempty"`
	Error        string           `json:"error,omitempty"`
}

// parseAuthorizeClientAndRedirect extracts the client_id and redirect_uri
// from a query/form. These two MUST validate before any other check so that
// later errors can be surfaced via the OAuth redirect-with-error pattern
// (RFC 6749 §4.1.2.1). Returns a generic error_description on failure.
func parseAuthorizeClientAndRedirect(get func(string) string) (authorizeRequestParams, string) {
	p := authorizeRequestParams{
		ClientID:    get("client_id"),
		RedirectURI: get("redirect_uri"),
	}
	if p.ClientID == "" {
		return p, "client_id is required"
	}
	if p.RedirectURI == "" {
		return p, "redirect_uri is required"
	}
	return p, ""
}

// parseAuthorizeRest populates the remaining params (response_type,
// code_challenge, scope, resource, state) and validates them. The caller
// passes in the already-validated client+redirect params. Errors here are
// redirect-with-error candidates because redirect_uri is already trusted.
func parseAuthorizeRest(get func(string) string, p *authorizeRequestParams) string {
	p.ResponseType = get("response_type")
	p.CodeChallenge = get("code_challenge")
	p.CodeChallengeMethod = get("code_challenge_method")
	p.Scope = get("scope")
	p.Resource = get("resource")
	p.State = get("state")

	if p.ResponseType != "code" {
		return "only response_type=code is supported"
	}
	if p.CodeChallenge == "" {
		return "code_challenge is required (PKCE)"
	}
	if p.CodeChallengeMethod == "" {
		p.CodeChallengeMethod = codeChallengeMethodS256
	}
	if p.CodeChallengeMethod != codeChallengeMethodS256 {
		return "only S256 code_challenge_method is supported"
	}
	return ""
}

// resolveAccountUserForConsent returns a non-nil accountUserResponse when
// the caller is already authenticated via the AuthMiddleware (Bearer
// token) or the short-lived nram_session cookie set by the login flow.
func (s *OAuthServer) resolveAccountUserForConsent(r *http.Request) *accountUserResponse {
	uid := s.resolveUserIDFromRequest(r)
	if uid == uuid.Nil {
		return nil
	}
	user, err := s.userRepo.GetByID(r.Context(), uid)
	if err != nil {
		return nil
	}
	return &accountUserResponse{
		DisplayName: user.DisplayName,
		Email:       user.Email,
	}
}

// resolveUserIDFromRequest returns the authenticated user id from the
// first identity source it finds: the AuthMiddleware context, an
// Authorization: Bearer header (the SPA sends this with every fetch from
// the localStorage session JWT), or the short-lived nram_session cookie
// (the IdP-callback bridge). Returns uuid.Nil if no identity is present.
//
// The public OAuth route group does not mount AuthMiddleware, so the
// Bearer-header branch is what makes an admin-UI session visible on the
// consent screen after the 5-minute cookie has expired but while the
// long-lived localStorage JWT is still valid.
func (s *OAuthServer) resolveUserIDFromRequest(r *http.Request) uuid.UUID {
	if ac := FromContext(r.Context()); ac != nil && ac.UserID != uuid.Nil {
		return ac.UserID
	}
	if token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		if uid, err := s.parseSessionCookie(token); err == nil {
			return uid
		}
	}
	cookie, err := r.Cookie("nram_session")
	if err != nil || cookie.Value == "" {
		return uuid.Nil
	}
	uid, _ := s.parseSessionCookie(cookie.Value)
	return uid
}

func (s *OAuthServer) parseSessionCookie(value string) (uuid.UUID, error) {
	claims := &Claims{}
	tok, parseErr := jwt.ParseWithClaims(value, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return s.jwtSecret, nil
	})
	if parseErr != nil || !tok.Valid {
		return uuid.Nil, errors.New("invalid session cookie")
	}
	sub, _ := claims.GetSubject()
	if sub == "" {
		return uuid.Nil, errors.New("session cookie missing subject")
	}
	uid, err := uuid.Parse(sub)
	if err != nil {
		return uuid.Nil, err
	}
	return uid, nil
}

// AuthorizeContextHandler serves GET /v1/oauth/authorize/context. The
// React consent page calls this on mount to validate the OAuth request
// and learn its rendering context.
//
// Validation order matters: client_id + redirect_uri must validate FIRST
// so later parameter errors can be surfaced via redirect-with-error (RFC
// 6749 §4.1.2.1). PKCE failures, unsupported response_type, or wrong
// code_challenge_method are surfaced as {redirect_to: "..."} so the React
// page does window.location.replace; unredirectable failures are 400.
func (s *OAuthServer) AuthorizeContextHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		params, paramErr := parseAuthorizeClientAndRedirect(r.URL.Query().Get)
		if paramErr != "" {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", paramErr)
			return
		}
		client, err := s.validateClientAndRedirect(r.Context(), params)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if restErr := parseAuthorizeRest(r.URL.Query().Get, &params); restErr != "" {
			writeJSONRedirect(w, redirectErrorURL(params.RedirectURI, "invalid_request", restErr, r.URL.Query().Get("state")))
			return
		}
		if msg, ok := s.validateResource(r, params.Resource); !ok {
			writeJSONRedirect(w, redirectErrorURL(params.RedirectURI, "invalid_target", msg, params.State))
			return
		}

		accountUser := s.resolveAccountUserForConsent(r)
		// Roll the nram_session cookie forward whenever the React
		// consent page loads against a recognized session. The
		// approve POST is a native HTML form submit, which carries
		// cookies only (no Authorization header), so without this
		// refresh the form POST silently loses identity 5 minutes
		// after login and bounces the user to /login despite the
		// page already saying "Continue as <user>".
		if accountUser != nil {
			s.refreshSessionCookie(w, r)
		}
		resp := authorizeContextResponse{
			ClientID:            params.ClientID,
			ClientName:          client.Name,
			RedirectURI:         params.RedirectURI,
			ResponseType:        params.ResponseType,
			CodeChallenge:       params.CodeChallenge,
			CodeChallengeMethod: params.CodeChallengeMethod,
			Scope:               params.Scope,
			Resource:            params.Resource,
			State:               params.State,
			AccountUser:         accountUser,
			ShareTokenSupported: s.shareTokens != nil,
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// refreshSessionCookie re-mints the nram_session cookie for the
// currently-recognized user. Called when the consent context endpoint
// has identified the user (via Bearer or an unexpired cookie) so the
// subsequent approve form POST has a fresh cookie. The form POST cannot
// carry an Authorization header, only cookies.
func (s *OAuthServer) refreshSessionCookie(w http.ResponseWriter, r *http.Request) {
	uid := s.resolveUserIDFromRequest(r)
	if uid == uuid.Nil {
		return
	}
	user, err := s.userRepo.GetByID(r.Context(), uid)
	if err != nil {
		return
	}
	token, err := GenerateSessionJWT(user.ID, user.OrgID, user.Role, user.Email, user.DisplayName, s.jwtSecret, 5*time.Minute)
	if err != nil {
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "nram_session",
		Value:    token,
		Path:     "/",
		MaxAge:   300,
		SameSite: http.SameSiteLaxMode,
		Secure:   RequestIsSecure(r),
	})
}

// SharePreviewHandler serves POST /v1/oauth/share/preview. Validates the
// pasted secret and returns the share owner, name, description, expiry,
// one-shot flag, and grants. Does NOT consume the share; the subsequent
// approve POST to /authorize does that via completeShareAuthorize.
func (s *OAuthServer) SharePreviewHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		if s.shareTokens == nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "share-token authorization is not configured on this server")
			return
		}

		var req sharePreviewRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
			return
		}
		get := req.get

		params, paramErr := parseAuthorizeClientAndRedirect(get)
		if paramErr != "" {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", paramErr)
			return
		}
		if _, err := s.validateClientAndRedirect(r.Context(), params); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if restErr := parseAuthorizeRest(get, &params); restErr != "" {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", restErr)
			return
		}
		if msg, ok := s.validateResource(r, params.Resource); !ok {
			writeJSONError(w, http.StatusBadRequest, "invalid_target", msg)
			return
		}

		secret := strings.TrimSpace(req.ShareToken)
		if secret == "" {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "share token is required")
			return
		}

		share, grants, err := s.shareTokens.Resolve(r.Context(), secret)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_grant", fmt.Sprintf("share token rejected: %v", err))
			return
		}

		preview := sharePreviewResponse{
			ShareName:   share.Name,
			Description: share.Description,
			ExpiresAt:   share.ExpiresAt.Format("January 2, 2006 at 15:04 MST"),
			IsOneShot:   share.IsOneShot,
		}
		if owner, oerr := s.userRepo.GetByID(r.Context(), share.OwnerUserID); oerr == nil {
			preview.OwnerName = owner.DisplayName
		}
		preview.Grants = make([]shareGrantJSON, 0, len(grants))
		for _, g := range grants {
			row := shareGrantJSON{
				ProjectSlug: g.ProjectID.String(),
				Permission:  permissionLabel(g.Permission),
			}
			if s.projectLookup != nil {
				if project, perr := s.projectLookup.GetByID(r.Context(), g.ProjectID); perr == nil {
					row.ProjectName = project.Name
					row.ProjectSlug = project.Slug
				}
			}
			if row.ProjectName == "" {
				row.ProjectName = "(project unavailable)"
			}
			preview.Grants = append(preview.Grants, row)
		}
		writeJSON(w, http.StatusOK, preview)
	}
}

// handleAuthorizePOST processes a consent submission. Two paths are
// supported via the auth_mode form field:
//   - "account": account-holder flow, mints a code bound to the caller's
//     user id with no share_token_id.
//   - "share":   share-paste flow, validates the secret, binds the OAuth
//     client to the share, records share_token_id on the code, and (when
//     the share is one-shot) marks it consumed.
//
// A "deny" decision redirects with access_denied per RFC 6749 §4.1.2.1.
//
// Errors after redirect_uri has been validated redirect to the OAuth
// client with access_denied so the client sees a consistent failure
// surface. Errors before redirect_uri validation return 400 JSON for the
// browser to display (the React consent page does not retry these).
func (s *OAuthServer) handleAuthorizePOST(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form body")
		return
	}

	params, paramErr := parseAuthorizeClientAndRedirect(r.PostFormValue)
	if paramErr != "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", paramErr)
		return
	}
	if _, err := s.validateClientAndRedirect(r.Context(), params); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if restErr := parseAuthorizeRest(r.PostFormValue, &params); restErr != "" {
		redirectWithError(w, r, params.RedirectURI, "invalid_request", restErr, r.PostFormValue("state"))
		return
	}
	if msg, ok := s.validateResource(r, params.Resource); !ok {
		redirectWithError(w, r, params.RedirectURI, "invalid_target", msg, params.State)
		return
	}

	decision := r.PostFormValue("decision")
	if decision == "deny" {
		redirectWithError(w, r, params.RedirectURI, "access_denied", "user denied authorization", params.State)
		return
	}
	if decision != "approve" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "missing decision")
		return
	}

	var outcome authorizeOutcome
	var cerr error
	switch r.PostFormValue("auth_mode") {
	case "account":
		outcome, cerr = s.completeAccountAuthorize(r, params)
	case "share":
		outcome, cerr = s.completeShareAuthorize(r, params, r.PostFormValue("share_token"))
	default:
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "unknown auth_mode")
		return
	}
	s.emitFormResult(w, r, params, outcome, cerr)
}

// emitFormResult writes the HTTP response for the native-form approve flow:
// a 302 to the callback on success, a redirect to /login when no session is
// present, or a redirect-with-error to the client on a post-trust failure.
func (s *OAuthServer) emitFormResult(w http.ResponseWriter, r *http.Request, params authorizeRequestParams, outcome authorizeOutcome, err error) {
	switch {
	case errors.Is(err, errAuthorizeNeedsLogin):
		loginURL := "/login?redirect=" + url.QueryEscape("/authorize?"+preservedQuery(params))
		http.Redirect(w, r, loginURL, http.StatusFound)
	case err != nil:
		var re *redirectError
		if errors.As(err, &re) {
			redirectWithError(w, r, params.RedirectURI, re.Code, re.Description, params.State)
			return
		}
		redirectWithError(w, r, params.RedirectURI, "server_error", "failed to create authorization code", params.State)
	default:
		clearSessionCookie(w)
		http.Redirect(w, r, outcome.callbackURL, http.StatusFound)
	}
}

// authorizeCompleteRequest is the JSON body for
// POST /v1/oauth/authorize/complete. It mirrors the fields the native
// approve form posts to /authorize, plus the decision and (for share mode)
// the pasted secret. Used only by the React consent page when the
// redirect_uri is a loopback address.
type authorizeCompleteRequest struct {
	oauthAuthorizeFields
	AuthMode   string `json:"auth_mode"`
	Decision   string `json:"decision"`
	ShareToken string `json:"share_token"`
}

// authorizeCompleteResponse is the JSON payload returned to the React consent
// page after a successful loopback approve. The SPA preflights the loopback
// origin and, if reachable, navigates to callback_url; otherwise it renders
// the manual-completion screen from code + state.
type authorizeCompleteResponse struct {
	Code        string `json:"code"`
	State       string `json:"state,omitempty"`
	CallbackURL string `json:"callback_url"`
	RedirectURI string `json:"redirect_uri"`
}

// AuthorizeCompleteHandler serves POST /v1/oauth/authorize/complete. It is
// the loopback-only counterpart to the native-form POST /authorize: instead
// of a 302 it returns the minted code, state, and callback URL as JSON so
// the React consent page can probe the loopback callback server and either
// forward to it or present a manual paste-back screen.
//
// Security: this handler returns the authorization code in a readable body,
// so it refuses any non-loopback redirect_uri (the check runs after the
// standard validation pipeline). Public web clients always use the 302 form
// flow, where the code is only ever exposed via the redirect Location.
func (s *OAuthServer) AuthorizeCompleteHandler() http.HandlerFunc {
	return s.handleAuthorizeComplete
}

func (s *OAuthServer) handleAuthorizeComplete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	var req authorizeCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	get := req.get

	params, paramErr := parseAuthorizeClientAndRedirect(get)
	if paramErr != "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", paramErr)
		return
	}
	if _, err := s.validateClientAndRedirect(r.Context(), params); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if restErr := parseAuthorizeRest(get, &params); restErr != "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", restErr)
		return
	}
	if msg, ok := s.validateResource(r, params.Resource); !ok {
		writeJSONError(w, http.StatusBadRequest, "invalid_target", msg)
		return
	}

	// This endpoint exists only for loopback redirect URIs; refuse anything
	// else so an authorization code is never returned in a readable body to
	// a public web client.
	if !isLoopbackRedirectURI(params.RedirectURI) {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "redirect_uri is not a loopback address")
		return
	}

	switch req.Decision {
	case "deny":
		writeJSON(w, http.StatusOK, map[string]string{"error": "access_denied"})
		return
	case "approve":
		// fall through to the approve handling below
	default:
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "missing decision")
		return
	}

	var outcome authorizeOutcome
	var err error
	switch req.AuthMode {
	case "account":
		outcome, err = s.completeAccountAuthorize(r, params)
	case "share":
		outcome, err = s.completeShareAuthorize(r, params, req.ShareToken)
	default:
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "unknown auth_mode")
		return
	}
	if err != nil {
		if errors.Is(err, errAuthorizeNeedsLogin) {
			writeJSONError(w, http.StatusUnauthorized, "login_required", "no active session")
			return
		}
		var re *redirectError
		if errors.As(err, &re) {
			writeJSONError(w, http.StatusBadRequest, re.Code, re.Description)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to create authorization code")
		return
	}

	clearSessionCookie(w)
	writeJSON(w, http.StatusOK, authorizeCompleteResponse{
		Code:        outcome.code,
		State:       params.State,
		CallbackURL: outcome.callbackURL,
		RedirectURI: params.RedirectURI,
	})
}

// completeAccountAuthorize mints a code for the account-holder path. It
// writes no HTTP response; on a missing session it returns
// errAuthorizeNeedsLogin so the caller can route to login (form) or return a
// login_required error (loopback JSON).
func (s *OAuthServer) completeAccountAuthorize(r *http.Request, params authorizeRequestParams) (authorizeOutcome, error) {
	// Per-share connector URLs (/mcp/{share_id}) belong to the share-paste flow.
	// An account-holder token must target the bare /mcp endpoint, so reject a
	// per-share resource here even though validateResource accepts the syntax.
	if params.Resource != "" && params.Resource != baseURLFromRequest(r)+"/mcp" {
		return authorizeOutcome{}, &redirectError{Code: "invalid_target", Description: "account authorization must target the base /mcp endpoint"}
	}
	uid := s.resolveUserIDFromRequest(r)
	if uid == uuid.Nil {
		return authorizeOutcome{}, errAuthorizeNeedsLogin
	}
	return s.mintCode(r.Context(), params, uid, nil)
}

// completeShareAuthorize validates the pasted share secret, binds the client
// to the share, consumes one-shot shares, and mints a code bound to the
// share owner. It writes no HTTP response; post-trust failures are returned
// as *redirectError.
func (s *OAuthServer) completeShareAuthorize(r *http.Request, params authorizeRequestParams, shareToken string) (authorizeOutcome, error) {
	if s.shareTokens == nil {
		return authorizeOutcome{}, &redirectError{Code: "access_denied", Description: "share-token authorization is not configured on this server"}
	}
	secret := strings.TrimSpace(shareToken)
	if secret == "" {
		return authorizeOutcome{}, &redirectError{Code: "invalid_request", Description: "share token is required"}
	}

	// The consent flow consumes one-shot shares as part of the mint
	// (MarkConsumed below); Resolve rejects any already-consumed one-shot.
	share, _, err := s.shareTokens.Resolve(r.Context(), secret)
	if err != nil {
		return authorizeOutcome{}, &redirectError{Code: "access_denied", Description: fmt.Sprintf("share token rejected: %v", err)}
	}

	// When a resource indicator is supplied it must address either the bare
	// /mcp endpoint or this share's own per-share URL. Reject a resource that
	// names a different share (e.g. the recipient pasted share A's secret while
	// connecting through share B's URL).
	if params.Resource != "" {
		base := baseURLFromRequest(r)
		if params.Resource != base+"/mcp" && params.Resource != base+"/mcp/"+share.ID.String() {
			return authorizeOutcome{}, &redirectError{Code: "invalid_target", Description: "resource does not match this share's MCP URL"}
		}
	}

	if err := s.oauthRepo.BindClientToShare(r.Context(), params.ClientID, share.ID); err != nil {
		return authorizeOutcome{}, &redirectError{Code: "server_error", Description: fmt.Sprintf("share binding failed: %v", err)}
	}

	if share.IsOneShot {
		if err := s.shareTokens.MarkConsumed(r.Context(), share.ID); err != nil {
			return authorizeOutcome{}, &redirectError{Code: "server_error", Description: fmt.Sprintf("one-shot consume failed: %v", err)}
		}
	}

	shareID := share.ID
	return s.mintCode(r.Context(), params, share.OwnerUserID, &shareID)
}

// mintCode creates and persists the OAuth authorization code, optionally
// recording share_token_id on it, and returns the code with the assembled
// callback URL (redirect_uri carrying code + state). It writes no HTTP
// response so both the 302 form flow and the loopback JSON flow can reuse it.
func (s *OAuthServer) mintCode(ctx context.Context, params authorizeRequestParams, userID uuid.UUID, shareTokenID *uuid.UUID) (authorizeOutcome, error) {
	code := generateAuthCode()
	codeChallenge := params.CodeChallenge
	authCode := &model.OAuthAuthorizationCode{
		Code:                code,
		ClientID:            params.ClientID,
		UserID:              userID,
		RedirectURI:         params.RedirectURI,
		Scope:               params.Scope,
		CodeChallenge:       &codeChallenge,
		CodeChallengeMethod: codeChallengeMethodS256,
		Resource:            params.Resource,
		ShareTokenID:        shareTokenID,
		ExpiresAt:           time.Now().UTC().Add(authCodeExpiry),
	}
	if err := s.oauthRepo.CreateAuthCode(ctx, authCode); err != nil {
		return authorizeOutcome{}, err
	}

	redirectURL, err := url.Parse(params.RedirectURI)
	if err != nil {
		return authorizeOutcome{}, err
	}
	q := redirectURL.Query()
	q.Set("code", code)
	if params.State != "" {
		q.Set("state", params.State)
	}
	redirectURL.RawQuery = q.Encode()
	return authorizeOutcome{code: code, callbackURL: redirectURL.String()}, nil
}

// clearSessionCookie expires the short-lived nram_session consent cookie.
// Called once a code is minted on either completion path.
func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     "nram_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

// validateClientAndRedirect verifies the client exists and the redirect_uri
// matches one of the client's registered URIs. Returns the loaded client
// on success so callers can use fields like Name without re-fetching, or
// an error suitable for surfacing as an OAuth error_description.
func (s *OAuthServer) validateClientAndRedirect(ctx context.Context, params authorizeRequestParams) (*model.OAuthClient, error) {
	client, err := s.oauthRepo.GetClientByID(ctx, params.ClientID)
	if err != nil {
		return nil, errors.New("unknown client_id")
	}
	if !containsString(client.RedirectURIs, params.RedirectURI) {
		return nil, errors.New("redirect_uri not registered")
	}
	return client, nil
}

// validateResource enforces RFC 8707 §2 for the consent flow: when a
// resource parameter is supplied it must identify this server's MCP endpoint,
// either the bare {base}/mcp (own-account or unscoped) or a per-share connector
// URL {base}/mcp/{share_id}. The per-share segment must parse as a UUID for an
// active share; the share-consent path additionally asserts the id matches the
// share actually being bound (see completeShareAuthorize).
func (s *OAuthServer) validateResource(r *http.Request, resource string) (string, bool) {
	if resource == "" {
		return "", true
	}
	base := baseURLFromRequest(r)
	if resource == base+"/mcp" {
		return "", true
	}
	suffix, ok := strings.CutPrefix(resource, base)
	if shareID, isShare := shareIDFromMCPPath(suffix); ok && isShare {
		if _, active := s.activeShareByID(r.Context(), shareID); !active {
			return "resource parameter references an unknown or inactive share", false
		}
		return "", true
	}
	return fmt.Sprintf("resource parameter must be %[1]s/mcp or %[1]s/mcp/{share_id}", base), false
}

// activeShareByID resolves a share by id and reports whether it exists and is
// currently active. Centralizes the shareTokens-nil / GetByID / Active(now)
// sequence used by resource validation and protected-resource discovery.
func (s *OAuthServer) activeShareByID(ctx context.Context, id uuid.UUID) (*model.ShareToken, bool) {
	if s.shareTokens == nil {
		return nil, false
	}
	share, err := s.shareTokens.GetByID(ctx, id)
	if err != nil || share == nil || !share.Active(time.Now().UTC()) {
		return nil, false
	}
	return share, true
}

// ShareAcceptHandler serves GET /v1/share/accept?token=<raw secret>. The
// React landing page calls this when a recipient opens a magic link and
// renders the grants table + MCP server URL + token. Resolution failures
// (revoked, expired, consumed) return 200 with {error} so the React page
// can render the friendly "share unavailable" card instead of an HTTP
// error code (the link is shared via human channels and reuse should not
// look like a stack trace).
func (s *OAuthServer) ShareAcceptHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		resp := shareAcceptResponse{
			MCPServerURL: baseURLFromRequest(r) + "/mcp",
		}

		if s.shareTokens == nil {
			resp.Error = "share-token acceptance is not configured on this server."
			writeJSON(w, http.StatusOK, resp)
			return
		}

		secret := strings.TrimSpace(r.URL.Query().Get("token"))
		if secret == "" {
			resp.Error = "share token is required."
			writeJSON(w, http.StatusOK, resp)
			return
		}

		share, grants, err := s.shareTokens.Resolve(r.Context(), secret)
		if err != nil {
			resp.Error = "this share link is no longer valid (revoked, expired, or already consumed)."
			writeJSON(w, http.StatusOK, resp)
			return
		}

		// Hand the recipient this share's own per-share connector URL so it
		// registers as a distinct connector in their MCP client, separate from
		// any other share or their own-account connection at the bare /mcp.
		resp.MCPServerURL = baseURLFromRequest(r) + "/mcp/" + share.ID.String()

		owner, err := s.userRepo.GetByID(r.Context(), share.OwnerUserID)
		if err == nil {
			resp.OwnerName = owner.DisplayName
		}
		resp.ShareName = share.Name
		resp.Description = share.Description
		resp.ExpiresAt = share.ExpiresAt.Format("January 2, 2006 at 15:04 MST")
		resp.ShareToken = secret

		resp.Grants = make([]shareGrantJSON, 0, len(grants))
		for _, g := range grants {
			row := shareGrantJSON{
				ProjectSlug: g.ProjectID.String(),
				Permission:  permissionLabel(g.Permission),
			}
			if s.projectLookup != nil {
				if project, perr := s.projectLookup.GetByID(r.Context(), g.ProjectID); perr == nil {
					row.ProjectName = project.Name
					row.ProjectSlug = project.Slug
				}
			}
			if row.ProjectName == "" {
				row.ProjectName = "(project unavailable)"
			}
			resp.Grants = append(resp.Grants, row)
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// permissionLabel converts the storage-format permission string to a
// human-readable label for the consent / share-accept screens.
func permissionLabel(p model.SharePermission) string {
	switch p {
	case model.SharePermissionRead:
		return "Read only"
	case model.SharePermissionReadStore:
		return "Read + Store"
	case model.SharePermissionReadStoreModify:
		return "Read + Store + Modify"
	}
	return string(p)
}

// preservedQuery URL-encodes the OAuth params so the login redirect can
// round-trip them back to /authorize after the user signs in.
func preservedQuery(p authorizeRequestParams) string {
	q := url.Values{}
	if p.ClientID != "" {
		q.Set("client_id", p.ClientID)
	}
	if p.RedirectURI != "" {
		q.Set("redirect_uri", p.RedirectURI)
	}
	if p.ResponseType != "" {
		q.Set("response_type", p.ResponseType)
	}
	if p.CodeChallenge != "" {
		q.Set("code_challenge", p.CodeChallenge)
	}
	if p.CodeChallengeMethod != "" {
		q.Set("code_challenge_method", p.CodeChallengeMethod)
	}
	if p.Scope != "" {
		q.Set("scope", p.Scope)
	}
	if p.Resource != "" {
		q.Set("resource", p.Resource)
	}
	if p.State != "" {
		q.Set("state", p.State)
	}
	return q.Encode()
}

// redirectErrorURL builds the redirect_uri callback URL with the OAuth
// error parameters. The React consent context endpoint returns this URL
// in {redirect_to} when validation after redirect_uri-trust fails.
func redirectErrorURL(redirectURI, errCode, description, state string) string {
	u, err := url.Parse(redirectURI)
	if err != nil {
		return redirectURI
	}
	q := u.Query()
	q.Set("error", errCode)
	if description != "" {
		q.Set("error_description", description)
	}
	if state != "" {
		q.Set("state", state)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// writeJSON serializes v to the response writer with status and JSON
// content type. Encoding errors fall through silently; by that point the
// headers are already on the wire.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeJSONError emits an OAuth-style error JSON body with the given
// status. Used by the new context and share-preview endpoints when
// validation fails before redirect_uri can be trusted.
func writeJSONError(w http.ResponseWriter, status int, errCode, description string) {
	writeJSON(w, status, map[string]string{
		"error":             errCode,
		"error_description": description,
	})
}

// writeJSONRedirect tells the React page to do a top-level browser
// navigation to redirectTo. Used when the consent context endpoint
// catches a redirect-with-error case (post redirect_uri trust).
func writeJSONRedirect(w http.ResponseWriter, redirectTo string) {
	writeJSON(w, http.StatusOK, map[string]string{
		"redirect_to": redirectTo,
	})
}
