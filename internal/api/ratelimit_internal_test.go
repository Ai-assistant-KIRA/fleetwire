package api

import (
	"net/http"
	"testing"
)

func TestClientNetGroupsIPv6By64(t *testing.T) {
	for addr, want := range map[string]string{
		"192.0.2.7:443":                   "192.0.2.7",
		"[::ffff:192.0.2.7]:443":          "192.0.2.7",
		"[2001:db8:1:2:3:4:5:6]:443":      "2001:db8:1:2::/64",
		"[2001:db8:1:2:ffff::1]:8444":     "2001:db8:1:2::/64",
		"[fe80::1%eth0]:443":              "fe80::/64",
		"not an address":                  "not an address",
		"[2001:db8:1:3:3:4:5:6]:443":      "2001:db8:1:3::/64",
		"[2001:db8:1:2:3:4:5:6%zone]:443": "2001:db8:1:2::/64",
	} {
		if got := clientNet(&http.Request{RemoteAddr: addr}); got != want {
			t.Errorf("clientNet(%q) = %q, want %q", addr, got, want)
		}
	}
}
