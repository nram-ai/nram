package auth

import "testing"

func TestIsLoopbackRedirectURI(t *testing.T) {
	cases := []struct {
		name        string
		redirectURI string
		want        bool
	}{
		{"localhost with port", "http://localhost:8765/callback", true},
		{"localhost no port", "http://localhost/cb", true},
		{"ipv4 loopback", "http://127.0.0.1:9999/callback", true},
		{"ipv4 loopback other octet", "http://127.0.0.5:40342/cb", true},
		{"ipv6 loopback", "http://[::1]:1234/callback", true},
		{"public https", "https://example.com/cb", false},
		{"wildcard bind address", "http://0.0.0.0:8080/cb", false},
		{"localhost suffix is not loopback", "https://localhost.evil.com/cb", false},
		{"localhost substring is not loopback", "https://my-localhost.example.com/cb", false},
		{"private LAN is not loopback", "http://192.168.1.10:3000/cb", false},
		{"malformed uri", "://nonsense", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLoopbackRedirectURI(tc.redirectURI); got != tc.want {
				t.Fatalf("isLoopbackRedirectURI(%q) = %v, want %v", tc.redirectURI, got, tc.want)
			}
		})
	}
}
