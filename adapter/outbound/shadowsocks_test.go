package outbound

import (
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestShadowSocksDestinationPrefersResolvedIP(t *testing.T) {
	ip := netip.MustParseAddr("22.22.22.16")
	metadata := &C.Metadata{
		Host:    "chatgpt.com",
		DstIP:   ip,
		DstPort: 443,
	}

	destination := shadowSocksDestination(metadata)
	if !destination.IsIP() {
		t.Fatalf("destination = %v, want IP", destination)
	}
	if destination.Addr != ip || destination.Port != 443 {
		t.Fatalf("destination = %v, want %s:443", destination, ip)
	}
}

func TestShadowSocksDestinationKeepsUnresolvedDomain(t *testing.T) {
	metadata := &C.Metadata{Host: "chatgpt.com", DstPort: 443}

	destination := shadowSocksDestination(metadata)
	if destination.IsIP() || destination.Fqdn != "chatgpt.com" || destination.Port != 443 {
		t.Fatalf("destination = %v, want chatgpt.com:443", destination)
	}
}
