package proxydialer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"testing"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/resolver"
)

type proxyServerTestResolver struct {
	resolver.Resolver
	lookup string
}

func (r *proxyServerTestResolver) Invalid() bool {
	return true
}

func (r *proxyServerTestResolver) LookupIPv4(context.Context, string) ([]netip.Addr, error) {
	r.lookup = "ipv4"
	return []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
	}, nil
}

func (r *proxyServerTestResolver) LookupIPv6(context.Context, string) ([]netip.Addr, error) {
	r.lookup = "ipv6"
	return []netip.Addr{
		netip.MustParseAddr("2001:db8::10"),
		netip.MustParseAddr("2001:db8::11"),
	}, nil
}

type proxyServerTestDialer struct {
	addresses []string
	networks  []string
}

func (d *proxyServerTestDialer) DialContext(_ context.Context, network, address string) (net.Conn, error) {
	d.networks = append(d.networks, network)
	d.addresses = append(d.addresses, address)
	if len(d.addresses) == 1 {
		return nil, errors.New("first address failed")
	}
	conn, peer := net.Pipe()
	_ = peer.Close()
	return conn, nil
}

func TestDialProxyServerContextResolvesBeforeProxy(t *testing.T) {
	proxyServerHostResolver := resolver.ProxyServerHostResolver
	disableIPv6 := resolver.DisableIPv6
	tcpConcurrent := dialer.GetTcpConcurrent()
	t.Cleanup(func() {
		resolver.ProxyServerHostResolver = proxyServerHostResolver
		resolver.DisableIPv6 = disableIPv6
		dialer.SetTcpConcurrent(tcpConcurrent)
	})
	dialer.SetTcpConcurrent(false)

	tests := []struct {
		network     string
		disableIPv6 bool
		wantLookup  string
		want        []string
	}{
		{network: "tcp", disableIPv6: true, wantLookup: "ipv4", want: []string{"192.0.2.10:443", "192.0.2.11:443"}},
		{network: "udp", disableIPv6: true, wantLookup: "ipv4", want: []string{"192.0.2.10:443", "192.0.2.11:443"}},
		{network: "tcp4", wantLookup: "ipv4", want: []string{"192.0.2.10:443", "192.0.2.11:443"}},
		{network: "udp4", wantLookup: "ipv4", want: []string{"192.0.2.10:443", "192.0.2.11:443"}},
		{network: "tcp6", wantLookup: "ipv6", want: []string{"[2001:db8::10]:443", "[2001:db8::11]:443"}},
		{network: "udp6", wantLookup: "ipv6", want: []string{"[2001:db8::10]:443", "[2001:db8::11]:443"}},
	}
	for _, test := range tests {
		t.Run(test.network, func(t *testing.T) {
			testResolver := &proxyServerTestResolver{}
			resolver.ProxyServerHostResolver = testResolver
			resolver.DisableIPv6 = test.disableIPv6
			testDialer := &proxyServerTestDialer{}

			conn, err := dialProxyServerContext(
				context.Background(),
				testDialer,
				test.network,
				"proxy-server.test:443",
			)
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()

			if testResolver.lookup != test.wantLookup {
				t.Fatalf("lookup = %q, want %q", testResolver.lookup, test.wantLookup)
			}
			if !slices.Equal(testDialer.addresses, test.want) {
				t.Fatalf("addresses = %v, want %v", testDialer.addresses, test.want)
			}
			if !slices.Equal(testDialer.networks, []string{test.network, test.network}) {
				t.Fatalf("networks = %v, want two %q attempts", testDialer.networks, test.network)
			}
		})
	}
}

func TestNewByNameProxyServerResolutionScope(t *testing.T) {
	regular := NewByName("proxy", nil).(byNameProxyDialer)
	if regular.resolveProxyServer {
		t.Fatal("regular named dialer unexpectedly resolves its destination locally")
	}
	proxyServer := NewByNameForProxyServer("proxy", nil).(byNameProxyDialer)
	if !proxyServer.resolveProxyServer {
		t.Fatal("proxy server dialer does not resolve its destination locally")
	}
}
