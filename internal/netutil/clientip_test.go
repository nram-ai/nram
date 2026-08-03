package netutil

import (
	"net/http"
	"testing"
)

func newReq(remoteAddr string, headers map[string]string) *http.Request {
	r := &http.Request{Header: http.Header{}, RemoteAddr: remoteAddr}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestClientIP_HeaderPriority(t *testing.T) {
	cases := []struct {
		name    string
		remote  string
		headers map[string]string
		want    string
	}{
		{
			name:   "CF-Connecting-IP wins over all",
			remote: "10.0.0.1:5000",
			headers: map[string]string{
				"CF-Connecting-IP": "203.0.113.7",
				"X-Real-IP":        "198.51.100.9",
				"X-Forwarded-For":  "192.0.2.1, 70.0.0.1",
			},
			want: "203.0.113.7",
		},
		{
			name:    "X-Real-IP wins over XFF and RemoteAddr",
			remote:  "10.0.0.1:5000",
			headers: map[string]string{"X-Real-IP": "198.51.100.9", "X-Forwarded-For": "192.0.2.1"},
			want:    "198.51.100.9",
		},
		{
			name:    "XFF first hop wins over RemoteAddr",
			remote:  "10.0.0.1:5000",
			headers: map[string]string{"X-Forwarded-For": "192.0.2.1, 70.0.0.1"},
			want:    "192.0.2.1",
		},
		{
			name:    "single XFF value trimmed",
			remote:  "10.0.0.1:5000",
			headers: map[string]string{"X-Forwarded-For": "  192.0.2.1  "},
			want:    "192.0.2.1",
		},
		{
			name:   "RemoteAddr host fallback",
			remote: "10.0.0.1:5000",
			want:   "10.0.0.1",
		},
		{
			name: "empty when nothing available",
			want: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClientIP(newReq(c.remote, c.headers)); got != c.want {
				t.Errorf("ClientIP() = %q, want %q", got, c.want)
			}
		})
	}
}
