package auth

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// authorizeRequestParams captures the OAuth authorize request after each
// field has been validated. The consent screen renders the params back as
// hidden inputs so the POST handler can recover them without re-parsing the
// URL (and without trusting client-side preservation across the flow).
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

// consentViewData is the template input for the consent screen.
type consentViewData struct {
	BaseURL           string
	Params            authorizeRequestParams
	AccountUser       *consentAccount // nil → not logged in; offer login link
	Error             string          // populated on POST validation failure
	ShareInputName    string          // matches form field name on POST
	AuthModeFieldName string
	// SharePreview is set on the share-paste preview step: the recipient
	// pasted a valid secret, the server resolved it, and the consent screen
	// now displays the grants/owner/expiry for explicit approval. The full
	// approve form is gated behind this preview to avoid mints without a
	// "you're about to authorize X" review step.
	SharePreview *sharePreviewData
}

// sharePreviewData is the structured "you're about to approve" payload
// rendered between the paste and the final approve in the share-paste
// consent flow.
type sharePreviewData struct {
	OwnerName   string
	ShareName   string
	Description string
	ExpiresAt   string
	IsOneShot   bool
	Grants      []shareGrantView
	// SecretEcho carries the raw secret back into the approve form so the
	// final POST has everything it needs without the recipient re-pasting.
	// The HTML template escapes this value; it sits in a hidden password
	// field that is form-submitted, not displayed.
	SecretEcho string
}

// consentAccount is the minimal user representation surfaced on the consent
// screen for the account-holder path.
type consentAccount struct {
	DisplayName string
	Email       string
}

// shareAcceptViewData is the template input for /share/accept.
type shareAcceptViewData struct {
	BaseURL      string
	OwnerName    string
	ShareName    string
	Description  string
	ExpiresAt    string
	Grants       []shareGrantView
	MCPServerURL string
	Error        string
}

// shareGrantView is one row in the grants table on the consent / share-accept
// screens. Permission is the human-readable tier label.
type shareGrantView struct {
	ProjectName string
	ProjectSlug string
	Permission  string
}

var (
	consentTemplate     = template.Must(template.New("consent").Parse(consentHTML))
	shareAcceptTemplate = template.Must(template.New("share-accept").Parse(shareAcceptHTML))
)

const consentHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Authorize access · nram</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; background: #0b0d10; color: #eaeef2; margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  .card { background: #14181d; border: 1px solid #2b313a; border-radius: 12px; padding: 32px; max-width: 520px; width: calc(100% - 32px); box-shadow: 0 24px 60px rgba(0,0,0,0.4); }
  h1 { font-size: 20px; margin: 0 0 6px; font-weight: 600; }
  .sub { color: #8a93a0; margin-bottom: 28px; font-size: 14px; }
  .option { border: 1px solid #2b313a; border-radius: 8px; padding: 18px; margin-bottom: 14px; }
  .option h2 { font-size: 15px; margin: 0 0 8px; font-weight: 600; }
  .option p { margin: 0 0 12px; font-size: 13px; color: #b9c1cc; }
  button, .btn { background: #5b8dff; color: #fff; border: 0; border-radius: 6px; padding: 9px 16px; font-size: 14px; font-weight: 500; cursor: pointer; }
  button.secondary { background: #2b313a; color: #eaeef2; }
  button:hover { filter: brightness(1.1); }
  input[type=text], input[type=password] { width: 100%; box-sizing: border-box; background: #0b0d10; border: 1px solid #2b313a; color: #eaeef2; border-radius: 6px; padding: 9px 12px; font-size: 14px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .row { display: flex; gap: 10px; align-items: center; }
  .row > * { flex: 1; }
  .row > button { flex: 0 0 auto; }
  .err { background: #2c1316; border: 1px solid #5a2731; color: #ffb1ba; padding: 10px 12px; border-radius: 6px; margin-bottom: 18px; font-size: 13px; }
  .muted { color: #8a93a0; font-size: 12px; }
</style>
</head>
<body>
<div class="card">
  <h1>Authorize {{ if .Params.ClientID }}{{ .Params.ClientID }}{{ else }}MCP client{{ end }}</h1>
  <p class="sub">This application is requesting access to your nram memory. Choose how to authorize.</p>
  {{ if .Error }}<div class="err">{{ .Error }}</div>{{ end }}

  {{ if .AccountUser }}
  <form class="option" method="POST" action="/authorize">
    <h2>Continue as {{ .AccountUser.DisplayName }}</h2>
    <p>Authorize this client to access your full account ({{ .AccountUser.Email }}).</p>
    <input type="hidden" name="{{ .AuthModeFieldName }}" value="account">
    <input type="hidden" name="decision" value="approve">
    {{ template "params" . }}
    <button type="submit">Approve</button>
  </form>
  {{ else }}
  <div class="option">
    <h2>Log in to your nram account</h2>
    <p>Authorize this client with your own account credentials.</p>
    <a class="btn" href="/login?redirect={{ .BaseURL }}/authorize?{{ .Params.PreservedQuery }}">Sign in</a>
  </div>
  {{ end }}

  {{ if .SharePreview }}
  <form class="option" method="POST" action="/authorize">
    <h2>You're about to authorize</h2>
    <p>
      {{ if .SharePreview.OwnerName }}<strong>{{ .SharePreview.OwnerName }}</strong> shared{{ else }}You've been granted{{ end }}
      access to <strong>{{ .SharePreview.ShareName }}</strong>.
    </p>
    {{ if .SharePreview.Description }}<p class="muted">{{ .SharePreview.Description }}</p>{{ end }}
    <p class="muted">Access expires {{ .SharePreview.ExpiresAt }}.{{ if .SharePreview.IsOneShot }} <strong>One-shot:</strong> once approved, this share cannot be redeemed again.{{ end }}</p>
    <table style="width:100%;border-collapse:collapse;margin:14px 0;font-size:13px">
      <thead><tr><th style="text-align:left;padding:6px 4px;color:#8a93a0;font-weight:500;border-bottom:1px solid #20262e">Project</th><th style="text-align:left;padding:6px 4px;color:#8a93a0;font-weight:500;border-bottom:1px solid #20262e">Access</th></tr></thead>
      <tbody>
        {{ range .SharePreview.Grants }}<tr><td style="padding:6px 4px;border-bottom:1px solid #20262e">{{ .ProjectName }} <code>{{ .ProjectSlug }}</code></td><td style="padding:6px 4px;border-bottom:1px solid #20262e">{{ .Permission }}</td></tr>{{ end }}
      </tbody>
    </table>
    <input type="hidden" name="{{ .AuthModeFieldName }}" value="share">
    <input type="hidden" name="decision" value="approve">
    <input type="hidden" name="{{ .ShareInputName }}" value="{{ .SharePreview.SecretEcho }}">
    {{ template "params" . }}
    <button type="submit">Approve</button>
  </form>
  {{ else }}
  <form class="option" method="POST" action="/authorize">
    <h2>I have a share link</h2>
    <p>Paste a share token (starts with <code>nram_s_</code>) you received from another nram user.</p>
    <input type="hidden" name="{{ .AuthModeFieldName }}" value="share">
    <input type="hidden" name="decision" value="preview">
    {{ template "params" . }}
    <div class="row">
      <input type="password" name="{{ .ShareInputName }}" placeholder="nram_s_…" autocomplete="off" required>
      <button type="submit">Continue</button>
    </div>
    <p class="muted" style="margin-top:10px">You'll see what projects this share covers before approving.</p>
  </form>
  {{ end }}

  <form method="POST" action="/authorize" style="margin-top: 12px">
    <input type="hidden" name="decision" value="deny">
    {{ template "params" . }}
    <button type="submit" class="secondary">Deny</button>
  </form>
</div>

{{ define "params" }}
<input type="hidden" name="client_id" value="{{ .Params.ClientID }}">
<input type="hidden" name="redirect_uri" value="{{ .Params.RedirectURI }}">
<input type="hidden" name="response_type" value="{{ .Params.ResponseType }}">
<input type="hidden" name="code_challenge" value="{{ .Params.CodeChallenge }}">
<input type="hidden" name="code_challenge_method" value="{{ .Params.CodeChallengeMethod }}">
<input type="hidden" name="scope" value="{{ .Params.Scope }}">
<input type="hidden" name="resource" value="{{ .Params.Resource }}">
<input type="hidden" name="state" value="{{ .Params.State }}">
{{ end }}
</body>
</html>
`

const shareAcceptHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{ if .OwnerName }}{{ .OwnerName }}{{ else }}Someone{{ end }} shared a project with you · nram</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; background: #0b0d10; color: #eaeef2; margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }
  .card { background: #14181d; border: 1px solid #2b313a; border-radius: 12px; padding: 32px; max-width: 560px; width: 100%; box-shadow: 0 24px 60px rgba(0,0,0,0.4); }
  h1 { font-size: 22px; margin: 0 0 10px; font-weight: 600; }
  .sub { color: #b9c1cc; margin-bottom: 20px; font-size: 14px; }
  .meta { color: #8a93a0; font-size: 13px; margin-bottom: 24px; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 24px; font-size: 14px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #20262e; }
  th { color: #8a93a0; font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
  code { background: #0b0d10; padding: 2px 6px; border-radius: 4px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .url-row { display: flex; gap: 8px; align-items: stretch; }
  .url { flex: 1; background: #0b0d10; border: 1px solid #2b313a; border-radius: 6px; padding: 10px 12px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; word-break: break-all; }
  button.copy { background: #5b8dff; color: #fff; border: 0; border-radius: 6px; padding: 0 16px; font-size: 13px; font-weight: 500; cursor: pointer; }
  button.copy:hover { filter: brightness(1.1); }
  .err { background: #2c1316; border: 1px solid #5a2731; color: #ffb1ba; padding: 10px 12px; border-radius: 6px; margin-bottom: 18px; font-size: 13px; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; background: #1e3a5a; color: #b9d4ff; }
</style>
</head>
<body>
<div class="card">
  {{ if .Error }}
    <h1>Share unavailable</h1>
    <div class="err">{{ .Error }}</div>
  {{ else }}
    <h1>{{ if .OwnerName }}{{ .OwnerName }} shared "{{ .ShareName }}" with you{{ else }}You've been given access to "{{ .ShareName }}"{{ end }}</h1>
    {{ if .Description }}<p class="sub">{{ .Description }}</p>{{ end }}
    <p class="meta">Access expires {{ .ExpiresAt }}.</p>
    <h2 style="font-size:14px;text-transform:uppercase;letter-spacing:0.05em;color:#8a93a0;margin-bottom:8px">Projects in this share</h2>
    <table>
      <thead><tr><th>Project</th><th>Access</th></tr></thead>
      <tbody>
        {{ range .Grants }}<tr><td>{{ .ProjectName }} <code>{{ .ProjectSlug }}</code></td><td><span class="tag">{{ .Permission }}</span></td></tr>{{ end }}
      </tbody>
    </table>
    <h2 style="font-size:14px;text-transform:uppercase;letter-spacing:0.05em;color:#8a93a0;margin-bottom:8px">Add to your MCP client</h2>
    <p class="sub">Paste this URL into Claude.ai's custom connector, ChatGPT's MCP server settings, or any MCP-capable tool. When prompted to authorize, paste the share token you received.</p>
    <div class="url-row">
      <div class="url" id="mcp-url">{{ .MCPServerURL }}</div>
      <button type="button" class="copy" onclick="(function(b){navigator.clipboard.writeText(document.getElementById('mcp-url').textContent);b.textContent='Copied';setTimeout(function(){b.textContent='Copy URL'},2000);})(this)">Copy URL</button>
    </div>
  {{ end }}
</div>
</body>
</html>
`

// PreservedQuery is a helper used by the consent template to round-trip
// OAuth params through the login redirect. Returns the URL-encoded query
// fragment without a leading "?".
func (p authorizeRequestParams) PreservedQuery() string {
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

// renderConsentScreen writes the consent HTML. Caller is responsible for
// setting status code; this function only writes headers and body.
func (s *OAuthServer) renderConsentScreen(w http.ResponseWriter, r *http.Request, view consentViewData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := consentTemplate.Execute(w, view); err != nil {
		http.Error(w, "consent screen render failed", http.StatusInternalServerError)
	}
}

// resolveAccountUserForConsent returns a non-nil consentAccount when the
// caller is already authenticated via the AuthMiddleware (Bearer token) or
// the short-lived nram_session cookie set by the login flow.
func (s *OAuthServer) resolveAccountUserForConsent(r *http.Request) *consentAccount {
	uid := s.resolveUserIDFromRequest(r)
	if uid == uuid.Nil {
		return nil
	}
	user, err := s.userRepo.GetByID(r.Context(), uid)
	if err != nil {
		return nil
	}
	return &consentAccount{
		DisplayName: user.DisplayName,
		Email:       user.Email,
	}
}

// resolveUserIDFromRequest returns the authenticated user id from either the
// AuthMiddleware context or the short-lived nram_session cookie. Returns
// uuid.Nil if no identity is present.
func (s *OAuthServer) resolveUserIDFromRequest(r *http.Request) uuid.UUID {
	if ac := FromContext(r.Context()); ac != nil && ac.UserID != uuid.Nil {
		return ac.UserID
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

// handleAuthorizeGET renders the consent screen for a valid /authorize GET.
// Behavior matches the pre-consent AuthorizeHandler's validation up to the
// point where the auto-approve would mint a code; instead of minting, it
// renders the consent screen. POSTed approvals re-validate every field.
//
// Validation order matters: client_id + redirect_uri must validate FIRST so
// later parameter errors can be surfaced via redirect-with-error (RFC 6749
// §4.1.2.1). PKCE failures, unsupported response_type, or wrong
// code_challenge_method all redirect with error rather than returning 400.
func (s *OAuthServer) handleAuthorizeGET(w http.ResponseWriter, r *http.Request) {
	params, paramErr := parseAuthorizeClientAndRedirect(r.URL.Query().Get)
	if paramErr != "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", paramErr)
		return
	}
	if err := s.validateClientAndRedirect(r.Context(), params); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if restErr := parseAuthorizeRest(r.URL.Query().Get, &params); restErr != "" {
		redirectWithError(w, r, params.RedirectURI, "invalid_request", restErr, r.URL.Query().Get("state"))
		return
	}
	if msg, ok := s.validateResource(r, params.Resource); !ok {
		redirectWithError(w, r, params.RedirectURI, "invalid_target", msg, params.State)
		return
	}

	// The share secret is NEVER read from a URL query param: putting it
	// there would leak it via browser history, server access logs, and any
	// Referer header. The recipient pastes the secret into the form by hand.
	view := consentViewData{
		BaseURL:           baseURLFromRequest(r),
		Params:            params,
		AccountUser:       s.resolveAccountUserForConsent(r),
		AuthModeFieldName: "auth_mode",
		ShareInputName:    "share_token",
	}

	w.WriteHeader(http.StatusOK)
	s.renderConsentScreen(w, r, view)
}

// handleAuthorizePOST processes a consent submission. Two paths are
// supported via the auth_mode form field:
//   - "account": account-holder flow, mints a code bound to the caller's
//     user id with no share_token_id (today's behavior pre-consent).
//   - "share":   share-paste flow, validates the secret, binds the OAuth
//     client to the share, records share_token_id on the code, and (when
//     the share is one-shot) marks it consumed.
//
// A "deny" decision redirects with access_denied per RFC 6749 §4.1.2.1.
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
	if err := s.validateClientAndRedirect(r.Context(), params); err != nil {
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

	mode := r.PostFormValue("auth_mode")

	// The share-paste flow is two-step: first POST decides "preview" (paste
	// → grants/owner/expiry shown for explicit approval), second POST is
	// "approve" (mint). Account-holder is single-step.
	if mode == "share" && decision == "preview" {
		s.renderSharePreview(w, r, params)
		return
	}

	if decision != "approve" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "missing decision")
		return
	}

	switch mode {
	case "account":
		s.completeAccountAuthorize(w, r, params)
	case "share":
		s.completeShareAuthorize(w, r, params)
	default:
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "unknown auth_mode")
	}
}

// renderSharePreview validates the pasted secret and re-renders the consent
// screen with the share's grants, owner display name, expiry, and one-shot
// flag. The recipient sees what they're about to authorize before the final
// approve POST mints the OAuth code.
func (s *OAuthServer) renderSharePreview(w http.ResponseWriter, r *http.Request, params authorizeRequestParams) {
	if s.shareTokens == nil {
		s.renderConsentError(w, r, params, "share-token authorization is not configured on this server")
		return
	}
	secret := strings.TrimSpace(r.PostFormValue("share_token"))
	if secret == "" {
		s.renderConsentError(w, r, params, "share token is required")
		return
	}
	// Preview itself does not consume the share; the subsequent approve POST
	// does that via completeShareAuthorize → MarkConsumed.
	share, grants, err := s.shareTokens.Resolve(r.Context(), secret)
	if err != nil {
		s.renderConsentError(w, r, params, fmt.Sprintf("share token rejected: %v", err))
		return
	}

	preview := &sharePreviewData{
		ShareName:   share.Name,
		Description: share.Description,
		ExpiresAt:   share.ExpiresAt.Format("January 2, 2006 at 15:04 MST"),
		IsOneShot:   share.IsOneShot,
		SecretEcho:  secret,
	}
	if owner, oerr := s.userRepo.GetByID(r.Context(), share.OwnerUserID); oerr == nil {
		preview.OwnerName = owner.DisplayName
	}
	preview.Grants = make([]shareGrantView, 0, len(grants))
	for _, g := range grants {
		row := shareGrantView{
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

	view := consentViewData{
		BaseURL:           baseURLFromRequest(r),
		Params:            params,
		AccountUser:       s.resolveAccountUserForConsent(r),
		AuthModeFieldName: "auth_mode",
		ShareInputName:    "share_token",
		SharePreview:      preview,
	}
	w.WriteHeader(http.StatusOK)
	s.renderConsentScreen(w, r, view)
}

func (s *OAuthServer) completeAccountAuthorize(w http.ResponseWriter, r *http.Request, params authorizeRequestParams) {
	uid := s.resolveUserIDFromRequest(r)
	if uid == uuid.Nil {
		loginURL := "/login?redirect=" + url.QueryEscape("/authorize?"+params.PreservedQuery())
		http.Redirect(w, r, loginURL, http.StatusFound)
		return
	}
	s.mintCodeAndRedirect(w, r, params, uid, nil)
}

func (s *OAuthServer) completeShareAuthorize(w http.ResponseWriter, r *http.Request, params authorizeRequestParams) {
	if s.shareTokens == nil {
		s.renderConsentError(w, r, params, "share-token authorization is not configured on this server")
		return
	}
	secret := strings.TrimSpace(r.PostFormValue("share_token"))
	if secret == "" {
		s.renderConsentError(w, r, params, "share token is required")
		return
	}

	// The consent flow consumes one-shot shares as part of the mint
	// (MarkConsumed below); Resolve rejects any already-consumed one-shot.
	share, _, err := s.shareTokens.Resolve(r.Context(), secret)
	if err != nil {
		s.renderConsentError(w, r, params, fmt.Sprintf("share token rejected: %v", err))
		return
	}

	if err := s.oauthRepo.BindClientToShare(r.Context(), params.ClientID, share.ID); err != nil {
		s.renderConsentError(w, r, params, fmt.Sprintf("share binding failed: %v", err))
		return
	}

	if share.IsOneShot {
		if err := s.shareTokens.MarkConsumed(r.Context(), share.ID); err != nil {
			s.renderConsentError(w, r, params, fmt.Sprintf("one-shot consume failed: %v", err))
			return
		}
	}

	shareID := share.ID
	s.mintCodeAndRedirect(w, r, params, share.OwnerUserID, &shareID)
}

// renderConsentError re-renders the consent screen with an error banner.
// Status is 400; the OAuth params are preserved so the user can retry.
func (s *OAuthServer) renderConsentError(w http.ResponseWriter, r *http.Request, params authorizeRequestParams, msg string) {
	view := consentViewData{
		BaseURL:           baseURLFromRequest(r),
		Params:            params,
		AccountUser:       s.resolveAccountUserForConsent(r),
		AuthModeFieldName: "auth_mode",
		ShareInputName:    "share_token",
		Error:             msg,
	}
	w.WriteHeader(http.StatusBadRequest)
	s.renderConsentScreen(w, r, view)
}

// mintCodeAndRedirect creates the OAuth authorization code, optionally
// records share_token_id on it, and redirects to the recipient's MCP client
// with code + state. Mirrors the pre-consent AuthorizeHandler mint path.
func (s *OAuthServer) mintCodeAndRedirect(w http.ResponseWriter, r *http.Request, params authorizeRequestParams, userID uuid.UUID, shareTokenID *uuid.UUID) {
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
	if err := s.oauthRepo.CreateAuthCode(r.Context(), authCode); err != nil {
		redirectWithError(w, r, params.RedirectURI, "server_error", "failed to create authorization code", params.State)
		return
	}

	redirectURL, err := url.Parse(params.RedirectURI)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid redirect_uri")
		return
	}
	q := redirectURL.Query()
	q.Set("code", code)
	if params.State != "" {
		q.Set("state", params.State)
	}
	redirectURL.RawQuery = q.Encode()

	http.SetCookie(w, &http.Cookie{
		Name:     "nram_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, redirectURL.String(), http.StatusFound)
}

// validateClientAndRedirect verifies the client exists and the redirect_uri
// matches one of the client's registered URIs. Returns nil on success or an
// error suitable for surfacing as an OAuth error_description.
func (s *OAuthServer) validateClientAndRedirect(ctx context.Context, params authorizeRequestParams) error {
	client, err := s.oauthRepo.GetClientByID(ctx, params.ClientID)
	if err != nil {
		return errors.New("unknown client_id")
	}
	if !containsString(client.RedirectURIs, params.RedirectURI) {
		return errors.New("redirect_uri not registered")
	}
	return nil
}

// validateResource enforces RFC 8707 §2 for the consent flow: when a
// resource parameter is supplied it must identify this server's MCP
// endpoint.
func (s *OAuthServer) validateResource(r *http.Request, resource string) (string, bool) {
	if resource == "" {
		return "", true
	}
	base := baseURLFromRequest(r)
	if resource != base+"/mcp" {
		return fmt.Sprintf("resource parameter must be %s/mcp", base), false
	}
	return "", true
}

// ShareAcceptHandler serves /share/accept?token=<raw secret> as the
// friendly magic-link entry. Resolves the share (read-only — does not mark
// consumed) and renders the grants table + MCP server URL. The recipient
// configures their MCP client with the URL and the consent flow at
// /authorize handles the actual code mint.
//
// Bearer-direct-style errors (consumed one-shot, expired, revoked) are
// surfaced as a friendly "share unavailable" message rather than a 4xx
// because the link is shared via human channels and the recipient should
// see something other than a stack trace when reused.
func (s *OAuthServer) ShareAcceptHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")

		view := shareAcceptViewData{
			BaseURL:      baseURLFromRequest(r),
			MCPServerURL: baseURLFromRequest(r) + "/mcp",
		}

		if s.shareTokens == nil {
			view.Error = "share-token acceptance is not configured on this server."
			_ = shareAcceptTemplate.Execute(w, view)
			return
		}

		secret := strings.TrimSpace(r.URL.Query().Get("token"))
		if secret == "" {
			view.Error = "share token is required."
			_ = shareAcceptTemplate.Execute(w, view)
			return
		}

		// This page does not consume the share, it just describes it. The
		// consent flow at /authorize is where redemption actually happens.
		share, grants, err := s.shareTokens.Resolve(r.Context(), secret)
		if err != nil {
			view.Error = "this share link is no longer valid (revoked, expired, or already consumed)."
			_ = shareAcceptTemplate.Execute(w, view)
			return
		}

		owner, err := s.userRepo.GetByID(r.Context(), share.OwnerUserID)
		if err == nil {
			view.OwnerName = owner.DisplayName
		}
		view.ShareName = share.Name
		view.Description = share.Description
		view.ExpiresAt = share.ExpiresAt.Format("January 2, 2006 at 15:04 MST")

		view.Grants = make([]shareGrantView, 0, len(grants))
		for _, g := range grants {
			row := shareGrantView{
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
			view.Grants = append(view.Grants, row)
		}

		_ = shareAcceptTemplate.Execute(w, view)
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
