package netutil

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

// TestIsPrivateOrReserved pins the strict egress predicate used by the webhook
// and IdP clients. The true cases are the SSRF targets that must be refused; the
// public false cases are the legitimate third-party destinations.
func TestIsPrivateOrReserved(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},             // loopback
		{"::1", true},                   // loopback v6
		{"10.0.0.5", true},              // RFC 1918
		{"172.16.9.9", true},            // RFC 1918
		{"192.168.2.43", true},          // RFC 1918
		{"169.254.169.254", true},       // link-local / cloud IMDS
		{"169.254.1.1", true},           // link-local
		{"fe80::1", true},               // link-local v6
		{"fc00::1", true},               // unique-local (v6 private)
		{"fd00:ec2::254", true},         // AWS IPv6 IMDS (in ULA)
		{"0.0.0.0", true},               // unspecified
		{"224.0.0.1", true},             // multicast
		{"8.8.8.8", false},              // public
		{"1.1.1.1", false},              // public
		{"93.184.216.34", false},        // public (example.com)
		{"2001:4860:4860::8888", false}, // public v6
	}
	for _, tc := range cases {
		ip := netip.MustParseAddr(tc.ip)
		if got := IsPrivateOrReserved(ip); got != tc.want {
			t.Errorf("IsPrivateOrReserved(%q) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

// TestIsCloudMetadata pins the narrow provider predicate: it blocks link-local
// and the cloud IMDS addresses while leaving loopback and RFC 1918 reachable so
// self-hosted models (Ollama on 127.0.0.1, SGLang on 192.168.x) keep working.
func TestIsCloudMetadata(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"169.254.169.254", true}, // AWS/GCP/Azure IMDS
		{"169.254.1.1", true},     // link-local
		{"fe80::1", true},         // link-local v6
		{"fd00:ec2::254", true},   // AWS IPv6 IMDS
		{"127.0.0.1", false},      // loopback: allowed for providers
		{"::1", false},            // loopback v6: allowed
		{"192.168.2.43", false},   // RFC 1918: allowed (self-hosted models)
		{"10.0.0.5", false},       // RFC 1918: allowed
		{"fc00::1", false},        // other ULA: allowed
		{"8.8.8.8", false},        // public
	}
	for _, tc := range cases {
		ip := netip.MustParseAddr(tc.ip)
		if got := IsCloudMetadata(ip); got != tc.want {
			t.Errorf("IsCloudMetadata(%q) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

func TestIsHTTPURL(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"http://example.com", true},
		{"https://example.com/path", true},
		{"HTTPS://Example.com", true}, // scheme is case-insensitive
		{"  https://example.com  ", true},
		{"ftp://example.com", false},
		{"javascript:alert(1)", false},
		{"file:///etc/passwd", false},
		{"https://", false}, // no host
		{"example.com", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := IsHTTPURL(tc.raw); got != tc.want {
			t.Errorf("IsHTTPURL(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestIsPostgresURL(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"postgres://user:pass@192.168.2.43/nram", true},
		{"postgresql://host:5432/db", true},
		{"POSTGRES://host/db", true}, // case-insensitive
		{"http://host/db", false},
		{"host=x port=5432 dbname=nram", false}, // libpq keyword DSN
		{"postgres://", false},                  // no host
		{"", false},
	}
	for _, tc := range cases {
		if got := IsPostgresURL(tc.raw); got != tc.want {
			t.Errorf("IsPostgresURL(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

// TestSafeHTTPClientBlocksLoopback proves the strict client refuses to connect
// to a loopback destination at dial time, while a client with a permissive
// predicate reaches the same server. The refusal wraps errBlockedEgressIP.
func TestSafeHTTPClientBlocksLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Strict predicate: the server listens on loopback, so the dial is refused.
	blocked := SafeHTTPClient(5*time.Second, IsPrivateOrReserved)
	if _, err := blocked.Get(srv.URL); err == nil {
		t.Fatalf("strict client reached loopback %s, want blocked", srv.URL)
	} else if !errors.Is(err, errBlockedEgressIP) {
		t.Fatalf("strict client error = %v, want errBlockedEgressIP", err)
	}

	// Permissive predicate: same server is reachable.
	allowed := SafeHTTPClient(5*time.Second, func(netip.Addr) bool { return false })
	resp, err := allowed.Get(srv.URL)
	if err != nil {
		t.Fatalf("permissive client failed to reach loopback: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("permissive client status = %d, want 200", resp.StatusCode)
	}
}

// TestSafeHTTPClientBlocksMetadata proves the metadata-guard client refuses the
// cloud IMDS address without needing a server: Control rejects the resolved IP
// pre-connect, so the result is deterministic and offline.
func TestSafeHTTPClientBlocksMetadata(t *testing.T) {
	client := SafeHTTPClient(2*time.Second, IsCloudMetadata)
	if _, err := client.Get("http://169.254.169.254/latest/meta-data/"); err == nil {
		t.Fatal("metadata-guard client reached 169.254.169.254, want blocked")
	} else if !errors.Is(err, errBlockedEgressIP) {
		t.Fatalf("metadata-guard client error = %v, want errBlockedEgressIP", err)
	}
}

// TestSafeHTTPClientDoesNotFollowRedirect proves a redirect is not followed: the
// client returns the 3xx response itself rather than chasing the Location, which
// is what closes the "benign external URL 302s onto an internal target" vector.
func TestSafeHTTPClientDoesNotFollowRedirect(t *testing.T) {
	var hops int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops++
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/internal", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Permissive predicate so the first (loopback) hop is allowed; the point is
	// that the redirect is not chased regardless of predicate.
	client := SafeHTTPClient(5*time.Second, func(netip.Addr) bool { return false })
	resp, err := client.Get(srv.URL + "/start")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302 (redirect returned, not followed)", resp.StatusCode)
	}
	if hops != 1 {
		t.Fatalf("server saw %d requests, want 1 (redirect must not be followed)", hops)
	}
}
