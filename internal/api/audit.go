package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/netutil"
)

// authContextFromRequest is split out so handler files can call tryAudit
// without importing auth themselves. Returns nil if no auth context.
func authContextFromRequest(r *http.Request) *auth.AuthContext {
	return auth.FromContext(r.Context())
}

// clientIPFromRequest returns the best-effort client IP for the audit row. It
// delegates to netutil.ClientIP so audit logging and the pre-auth throttle key
// on one shared implementation (X-Forwarded-For first hop, RemoteAddr host
// fallback).
func clientIPFromRequest(r *http.Request) string {
	return netutil.ClientIP(r)
}

// Audit action constants. Each privileged operation that mutates tenancy,
// credentials, or system state appends a row with one of these action names.
// Adding a new privileged action means adding a constant here AND wiring an
// AuditStore.Append call at the action's handler.
const (
	AuditActionUserProvision      = "user.provision"
	AuditActionUserDelete         = "user.delete"
	AuditActionUserRoleChange     = "user.role_change"
	AuditActionUserPasswordChange = "user.password_change"

	AuditActionOrgCreate = "org.create"
	AuditActionOrgDelete = "org.delete"
	AuditActionOrgUpdate = "org.update"

	AuditActionAPIKeyIssue  = "api_key.issue"
	AuditActionAPIKeyRevoke = "api_key.revoke"

	AuditActionOAuthClientRegister = "oauth_client.register"
	AuditActionOAuthClientRevoke   = "oauth_client.revoke"

	AuditActionWebhookCreate = "webhook.create"
	AuditActionWebhookDelete = "webhook.delete"

	AuditActionPasskeyRegister = "passkey.register"
	AuditActionPasskeyDelete   = "passkey.delete"

	AuditActionIdPLogin      = "idp.login"
	AuditActionLogin         = "auth.login"
	AuditActionSetupComplete = "system.setup_complete"

	AuditActionProviderConfigure = "provider.configure"
	AuditActionSettingsUpdate    = "settings.update"
)

// AuditEvent represents a single audit log row.
type AuditEvent struct {
	ID          uuid.UUID       `json:"id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	ActorUserID *uuid.UUID      `json:"actor_user_id,omitempty"`
	ActorRole   string          `json:"actor_role,omitempty"`
	Action      string          `json:"action"`
	TargetType  string          `json:"target_type,omitempty"`
	TargetID    *uuid.UUID      `json:"target_id,omitempty"`
	TargetOrgID *uuid.UUID      `json:"target_org_id,omitempty"`
	SourceIP    string          `json:"source_ip,omitempty"`
	UserAgent   string          `json:"user_agent,omitempty"`
	Details     json.RawMessage `json:"details,omitempty"`
}

// AuditScope filters AuditStore.Query results. Each field is an independent
// AND-clause on the underlying SQL.
//
//   - ActorUserID set → events authored by this user (self-tier feed).
//   - TargetOrgID set → events whose target is in this org (org-tier feed).
//   - All nil       → unfiltered (system-tier feed).
type AuditScope struct {
	ActorUserID *uuid.UUID
	TargetOrgID *uuid.UUID
}

// AuditStore is the persistence interface for audit events.
type AuditStore interface {
	// Append writes an audit row. The event's ID and OccurredAt are populated
	// by the store if zero-valued.
	Append(ctx context.Context, event AuditEvent) error

	// Query returns events matching scope, ordered by occurred_at DESC,
	// limited to `limit` rows. Pass since=zero-time to disable the time
	// filter. Pass limit<=0 to use a default of 100. Limits exceeding the
	// store-defined maximum are silently clamped.
	Query(ctx context.Context, scope AuditScope, since time.Time, limit int) ([]AuditEvent, error)
}

// auditFromRequest pulls actor identity off the request's auth context and
// fills the standard fields on a partially-populated AuditEvent. Returns
// the event with actor + source IP + user agent set.
func auditFromRequest(r *http.Request, ev AuditEvent) AuditEvent {
	if ac := authContextFromRequest(r); ac != nil {
		uid := ac.UserID
		if uid != uuid.Nil {
			ev.ActorUserID = &uid
		}
		ev.ActorRole = string(ac.Role)
		if ev.TargetOrgID == nil && ac.OrgID != uuid.Nil {
			oid := ac.OrgID
			ev.TargetOrgID = &oid
		}
	}
	ev.SourceIP = clientIPFromRequest(r)
	ev.UserAgent = r.UserAgent()
	return ev
}

// tryAudit appends an audit event if the AuditStore is set. Errors are
// swallowed (logged via stdlib log) so audit-write failures do not break
// the privileged action that just succeeded.
func tryAudit(store AuditStore, r *http.Request, action string, target *AuditEvent) {
	if store == nil {
		return
	}
	ev := AuditEvent{Action: action}
	if target != nil {
		ev = *target
		ev.Action = action
	}
	ev = auditFromRequest(r, ev)
	if err := store.Append(r.Context(), ev); err != nil {
		// Best-effort; do not break the request on audit failure. The
		// caller already committed the privileged action.
		_ = err
	}
}

// AuditOnSuccess wraps a handler so that it appends an audit event after
// the wrapped handler responds with 2xx. Used to add audit hooks to
// handlers we don't own (e.g. webauthn RegisterFinish, IdP callback)
// without modifying the upstream package.
//
// targetType is the AuditEvent.TargetType label (e.g. "passkey", "user").
// The TargetID is unset because this generic wrapper has no access to
// the resource id; downstream consumers can correlate by actor + action.
func AuditOnSuccess(store AuditStore, action, targetType string, next http.Handler) http.Handler {
	if store == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &auditStatusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if rec.status >= 200 && rec.status < 300 {
			tryAudit(store, r, action, &AuditEvent{TargetType: targetType})
		}
	})
}

// auditStatusRecorder captures the response status so AuditOnSuccess
// can decide whether to fire the audit append. WriteHeader is the only
// way to observe status without buffering the body.
type auditStatusRecorder struct {
	http.ResponseWriter
	status      int
	wroteStatus bool
}

func (r *auditStatusRecorder) WriteHeader(code int) {
	if !r.wroteStatus {
		r.status = code
		r.wroteStatus = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *auditStatusRecorder) Write(b []byte) (int, error) {
	if !r.wroteStatus {
		r.status = http.StatusOK
		r.wroteStatus = true
	}
	return r.ResponseWriter.Write(b)
}
