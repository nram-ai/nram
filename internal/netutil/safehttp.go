package netutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"time"
)

// errBlockedEgressIP is returned by the safe dialer when a resolved destination
// IP is rejected by the caller's deny predicate, before any packet is sent.
var errBlockedEgressIP = errors.New("netutil: destination IP is blocked")

// awsIMDSv6 is the AWS IPv6 instance metadata service address. It sits in the
// IPv6 unique-local range (fc00::/7), which providers may legitimately use for
// self-hosted models, so it is matched exactly rather than by range.
var awsIMDSv6 = netip.MustParseAddr("fd00:ec2::254")

// IsPrivateOrReserved reports whether ip is a loopback, private (RFC 1918 /
// IPv6 ULA fc00::/7), link-local (169.254.0.0/16 including the cloud metadata
// IP, and fe80::/10), unspecified, or multicast address. It is the strict
// egress predicate for destinations that are third parties by nature (webhook
// receivers, external OIDC identity providers): none of those ranges is a
// legitimate target, and reaching one is the SSRF pivot.
func IsPrivateOrReserved(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

// IsCloudMetadata reports whether ip is a cloud instance-metadata endpoint: any
// link-local unicast address (which covers 169.254.169.254 and the fe80::/10
// range) or the AWS IPv6 IMDS address fd00:ec2::254. It is the narrow egress
// predicate for provider destinations, which legitimately point at loopback and
// RFC 1918 hosts (self-hosted models) and so must not be denied those ranges,
// but should never reach an IMDS: reading an instance credential exceeds the
// authority of the admin who configured the provider.
func IsCloudMetadata(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLinkLocalUnicast() || ip == awsIMDSv6
}

// SafeDialContext returns a DialContext function whose net.Dialer.Control hook
// inspects the concrete, already-resolved destination IP for every connection
// attempt and rejects it with errBlockedEgressIP when deny reports true. Because
// Control runs after DNS resolution on the actual address that will be dialed,
// it defeats DNS rebinding and time-of-check/time-of-use races that a
// pre-resolution URL-host check cannot.
func SafeDialContext(deny func(netip.Addr) bool) func(context.Context, string, string) (net.Conn, error) {
	d := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			// address is always a resolved numeric host:port (bracketed for IPv6)
			// by the time Control runs, so ParseAddrPort covers it in one step.
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("%w: unparseable dial address %q", errBlockedEgressIP, address)
			}
			if ip := ap.Addr(); deny(ip) {
				return fmt.Errorf("%w: %s", errBlockedEgressIP, ip)
			}
			return nil
		},
	}
	return d.DialContext
}

// SafeHTTPClient builds an *http.Client whose transport dials only destinations
// that deny does not reject (enforced per-hop at dial time via SafeDialContext,
// so a redirect to a blocked IP is refused just like a direct request), and
// whose CheckRedirect stops following redirects entirely: a followed redirect to
// a new host is exactly the SSRF pivot, and none of these egress paths
// legitimately redirect. The caller receives the 3xx response as the final
// response and treats a non-2xx status as a failure.
//
// A timeout of 0 leaves the client timeout unset for callers that bound each
// request via a context deadline instead (e.g. the webhook deliverer).
func SafeHTTPClient(timeout time.Duration, deny func(netip.Addr) bool) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = SafeDialContext(deny)
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// IsHTTPURL reports whether raw parses as an absolute URL with an http or https
// scheme and a non-empty host. It is the write-time first gate for webhook and
// IdP configuration; the authoritative SSRF check happens at dial time.
func IsHTTPURL(raw string) bool {
	return urlHasScheme(raw, "http", "https")
}

// IsPostgresURL reports whether raw parses as an absolute URL with a postgres or
// postgresql scheme and a non-empty host. It gates the admin database endpoints
// so an operator-supplied connection string cannot be coerced into a non-Postgres
// URL form (e.g. an http:// target or a bare libpq keyword/value DSN).
func IsPostgresURL(raw string) bool {
	return urlHasScheme(raw, "postgres", "postgresql")
}

// urlHasScheme parses raw and reports whether its (case-insensitive) scheme is
// one of schemes and its host is non-empty.
func urlHasScheme(raw string, schemes ...string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return false
	}
	return slices.Contains(schemes, strings.ToLower(u.Scheme))
}
