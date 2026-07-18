package tunnel_test

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/stretchr/testify/require"
)

type captureProxy struct {
	*outbound.Base
	dialMetadata *C.Metadata
}

var _ outbound.ProxyAdapter = (*captureProxy)(nil)

func newCaptureProxy() *captureProxy {
	return &captureProxy{
		Base: outbound.NewBase(outbound.BaseOption{
			Name: "DNS-DIALER-TEST",
			Addr: "127.0.0.1:0",
			Type: C.Direct,
			UDP:  true,
		}),
	}
}

func (p *captureProxy) ListenPacketContext(_ context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	p.dialMetadata = metadata.Clone()
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return outbound.NewPacketConn(packetConn, p), nil
}

func TestDNSDialerListenPacketWithResolvedAddress(t *testing.T) {
	tests := []struct {
		name         string
		logicalAddr  string
		resolvedAddr netip.AddrPort
		wantHost     string
	}{
		{
			name:         "IPv4 DoH",
			logicalAddr:  "dns.google:443",
			resolvedAddr: netip.MustParseAddrPort("8.8.8.8:443"),
			wantHost:     "dns.google",
		},
		{
			name:         "IPv6 custom port",
			logicalAddr:  "dns.example:8443",
			resolvedAddr: netip.MustParseAddrPort("[2001:db8::53]:8443"),
			wantHost:     "dns.example",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proxy := newCaptureProxy()
			dnsDialer := tunnel.NewDNSDialer(nil, proxy, "")

			packetConn, err := dnsDialer.ListenPacketWithResolvedAddress(
				context.Background(),
				"udp",
				test.logicalAddr,
				test.resolvedAddr,
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = packetConn.Close() })

			// Dialing stays pinned to the bootstrap endpoint.
			require.Empty(t, proxy.dialMetadata.Host)
			require.Equal(t, test.resolvedAddr.Addr(), proxy.dialMetadata.DstIP)
			require.Equal(t, test.resolvedAddr.Port(), proxy.dialMetadata.DstPort)

			// Rules and connection tracking retain the logical host.
			tracker, ok := packetConn.(statistic.Tracker)
			require.True(t, ok)
			metadata := tracker.Info().Metadata
			require.Equal(t, test.wantHost, metadata.Host)
			require.Equal(t, test.wantHost, metadata.RuleHost())
			require.Equal(t, test.resolvedAddr.Addr(), metadata.DstIP)
			require.Equal(t, test.resolvedAddr.Port(), metadata.DstPort)
		})
	}
}

func TestDNSDialerListenPacketWithInvalidResolvedAddress(t *testing.T) {
	dnsDialer := tunnel.NewDNSDialer(nil, newCaptureProxy(), "")

	_, err := dnsDialer.ListenPacketWithResolvedAddress(
		context.Background(),
		"udp",
		"dns.google:443",
		netip.AddrPort{},
	)
	require.ErrorContains(t, err, "invalid resolved address")
}
