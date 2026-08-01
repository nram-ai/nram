package provider

import "regexp"

// secretQueryParam matches a sensitive query-string parameter and its value
// inside any URL that appears in a free-form string (typically the message of a
// *url.Error produced by net/http on a transport failure). net/http redacts only
// userinfo passwords, never query parameters, so a URL that carries a credential
// in the query string leaks it verbatim through err.Error(). The value run stops
// at the next '&', whitespace, or a closing quote so only the secret is masked.
//
// The parameter set covers the shapes a provider base URL or SDK might use:
// Gemini's legacy ?key=, plus the common api_key / access_token / token / secret
// / password / signature spellings.
var secretQueryParam = regexp.MustCompile(
	`(?i)([?&](?:key|api_key|apikey|access_token|token|secret|password|sig|signature)=)[^&\s"']+`,
)

// redactURLSecrets replaces the value of any sensitive query parameter embedded
// in s with REDACTED, leaving the rest of the string (host, path, error text)
// intact. It is a pure string transform; a string with no such parameter is
// returned unchanged.
func redactURLSecrets(s string) string {
	return secretQueryParam.ReplaceAllString(s, "${1}REDACTED")
}

// redactedError wraps an error so its rendered message has URL-embedded secrets
// masked, while preserving the original error's identity for errors.Is /
// errors.As (Unwrap returns the original). Callers that log or persist
// err.Error() see the redacted text; callers that type-assert the cause (for
// timeout/temporary classification) still reach the underlying error.
type redactedError struct {
	orig error
	msg  string
}

func (e *redactedError) Error() string { return e.msg }

func (e *redactedError) Unwrap() error { return e.orig }

// redactError returns err with any URL-embedded secret in its message masked. It
// returns nil for nil, and returns the original error unchanged when the message
// carries no secret-bearing query parameter. The no-match check scans without
// allocating, so the common success and clean-error paths are untouched.
func redactError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	// FindStringIndex scans without allocating; only a genuine match pays for the
	// ReplaceAllString in redactURLSecrets.
	if secretQueryParam.FindStringIndex(msg) == nil {
		return err
	}
	return &redactedError{orig: err, msg: redactURLSecrets(msg)}
}
