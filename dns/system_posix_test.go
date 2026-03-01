//go:build !windows

package dns

import (
	"strings"
	"testing"
)

func TestParseResolvNameservers_FilterLoopbackAndFallback(t *testing.T) {
	servers, err := parseResolvNameservers(strings.NewReader("nameserver 127.0.0.1\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 2 || servers[0] != "223.5.5.5" || servers[1] != "223.6.6.6" {
		t.Fatalf("unexpected fallback nameservers: %v", servers)
	}
}

func TestParseResolvNameservers_KeepNonLoopback(t *testing.T) {
	servers, err := parseResolvNameservers(strings.NewReader("nameserver 127.0.0.1\nnameserver 8.8.8.8\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 1 || servers[0] != "8.8.8.8" {
		t.Fatalf("unexpected nameservers: %v", servers)
	}
}
