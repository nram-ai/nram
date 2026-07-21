package netutil

import "testing"

// TestIsLoopbackHost pins the classification the Host-rebinding guard rests on.
// The false cases matter most: they are what keeps a LAN or container
// deployment working when an operator turns protection on, and a listener test
// cannot reach them portably (it would have to bind a routable address that
// varies by machine), so they are covered here instead.
func TestIsLoopbackHost(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1", true},
		{"127.0.0.1:8674", true},
		{"127.0.0.53", true}, // all of 127/8 is loopback, not just .0.1
		{"localhost", true},
		{"localhost:8674", true},
		{"LocalHost", true}, // host comparison is case-insensitive
		{"::1", true},
		{"[::1]", true},
		{"[::1]:8674", true},
		{"192.168.2.43", false}, // the LAN case: must never be rejected
		{"192.168.2.43:8674", false},
		{"10.0.0.5:8674", false}, // container/bridge networks
		{"nram.ai", false},
		{"nram.ai:443", false},
		{"", false},
		{"not an address", false},
	}
	for _, tc := range cases {
		if got := IsLoopbackHost(tc.addr); got != tc.want {
			t.Errorf("IsLoopbackHost(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}
