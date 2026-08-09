package tunnel

// WARNING: all function in this file should only be using in dns module

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const DnsRespectRules = "RULES"

type DNSDialer struct {
	r            resolver.Resolver
	proxyAdapter C.ProxyAdapter
	proxyName    string
}

func NewDNSDialer(r resolver.Resolver, proxyAdapter C.ProxyAdapter, proxyName string) *DNSDialer {
	return &DNSDialer{r: r, proxyAdapter: proxyAdapter, proxyName: proxyName}
}

func (d *DNSDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	r := d.r
	proxyName := d.proxyName
	proxyAdapter := d.proxyAdapter
	var opts []dialer.Option
	var rule C.Rule
	metadata := &C.Metadata{
		NetWork: C.TCP,
		Type:    C.INNER,
	}
	routeState := captureMetadataRouteState(metadata)
	err := metadata.SetRemoteAddress(addr) // tcp can resolve host by remote
	if err != nil {
		return nil, err
	}
	if !strings.Contains(network, "tcp") {
		metadata.NetWork = C.UDP
		if !metadata.Resolved() {
			// udp must resolve host first
			dstIP, err := resolver.ResolveIPWithResolver(ctx, metadata.Host, r)
			if err != nil {
				return nil, err
			}
			metadata.DstIP = dstIP
		}
	}

	if proxyAdapter == nil && len(proxyName) != 0 {
		if proxyName == DnsRespectRules {
			if !metadata.Resolved() {
				// resolve here before resolveMetadata to avoid its inner resolver.ResolveIP
				dstIP, err := resolver.ResolveIPWithResolver(ctx, metadata.Host, r)
				if err != nil {
					return nil, err
				}
				metadata.DstIP = dstIP
			}
			proxyAdapter, rule, err = resolveMetadataWithMode(metadata, routeState.mode)
			if err != nil {
				return nil, err
			}
		} else {
			var ok bool
			proxyAdapter, ok = Proxies()[proxyName]
			if ok {
				metadata.SpecialProxy = proxyName // just for log
			} else {
				opts = append(opts, dialer.WithInterface(proxyName))
			}
		}
	}

	if metadata.NetWork == C.TCP {
		if proxyAdapter == nil {
			opts = append(opts, dialer.WithResolver(r))
			return dialer.DialContext(ctx, network, addr, opts...)
		}

		if proxyAdapter.IsL3Protocol(metadata) { // L3 proxy should resolve domain before to avoid loopback
			if !metadata.Resolved() {
				dstIP, err := resolver.ResolveIPWithResolver(ctx, metadata.Host, r)
				if err != nil {
					return nil, err
				}
				metadata.DstIP = dstIP
			}
			metadata.Host = "" // clear host to avoid double resolve in proxy
		}

		conn, err := proxyAdapter.DialContext(ctx, metadata)
		if err != nil {
			logMetadataErr(metadata, rule, proxyAdapter, err)
			return nil, err
		}

		conn, err = joinModeTracker(routeState.revision, metadata, conn, func(conn C.Conn) C.Conn {
			return statistic.NewTCPTracker(conn, statistic.DefaultManager, metadata, rule, 0, 0, false)
		})
		if err != nil {
			return nil, err
		}
		logMetadataWithMode(metadata, rule, conn, routeState.mode)

		return conn, nil
	} else {
		if proxyAdapter == nil {
			return dialer.DialContext(ctx, network, metadata.AddrPort().String(), opts...)
		}

		if !proxyAdapter.SupportUDP() {
			return nil, fmt.Errorf("proxy adapter [%s] UDP is not supported", proxyAdapter)
		}

		packetConn, err := proxyAdapter.ListenPacketContext(ctx, metadata)
		if err != nil {
			logMetadataErr(metadata, rule, proxyAdapter, err)
			return nil, err
		}

		packetConn, err = joinModeTracker(routeState.revision, metadata, packetConn, func(conn C.PacketConn) C.PacketConn {
			return statistic.NewUDPTracker(conn, statistic.DefaultManager, metadata, rule, 0, 0, false)
		})
		if err != nil {
			return nil, err
		}
		logMetadataWithMode(metadata, rule, packetConn, routeState.mode)

		return N.NewBindPacketConn(packetConn, metadata.UDPAddr()), nil
	}
}

func (d *DNSDialer) ListenPacket(ctx context.Context, network, addr string) (net.PacketConn, error) {
	return d.listenPacket(ctx, network, addr, netip.AddrPort{})
}

// ListenPacketWithResolvedAddress opens a packet connection for addr while
// pinning the actual destination to resolvedAddr. The logical host in addr is
// retained for rule matching, logging, and connection tracking.
func (d *DNSDialer) ListenPacketWithResolvedAddress(ctx context.Context, network, addr string, resolvedAddr netip.AddrPort) (net.PacketConn, error) {
	if !resolvedAddr.IsValid() || resolvedAddr.Port() == 0 {
		return nil, fmt.Errorf("invalid resolved address: %s", resolvedAddr)
	}

	return d.listenPacket(ctx, network, addr, resolvedAddr)
}

func (d *DNSDialer) listenPacket(ctx context.Context, network, addr string, resolvedAddr netip.AddrPort) (net.PacketConn, error) {
	r := d.r
	proxyAdapter := d.proxyAdapter
	proxyName := d.proxyName
	var opts []dialer.Option
	metadata := &C.Metadata{
		NetWork: C.UDP,
		Type:    C.INNER,
	}
	routeState := captureMetadataRouteState(metadata)
	err := metadata.SetRemoteAddress(addr)
	if err != nil {
		return nil, err
	}
	if resolvedAddr.IsValid() {
		metadata.DstIP = resolvedAddr.Addr().Unmap()
		metadata.DstPort = resolvedAddr.Port()
	} else if !metadata.Resolved() {
		// udp must resolve host first
		dstIP, err := resolver.ResolveIPWithResolver(ctx, metadata.Host, r)
		if err != nil {
			return nil, err
		}
		metadata.DstIP = dstIP
	}

	var rule C.Rule
	if proxyAdapter == nil {
		if proxyName == DnsRespectRules {
			proxyAdapter, rule, err = resolveMetadataWithMode(metadata, routeState.mode)
			if err != nil {
				return nil, err
			}
		} else {
			var ok bool
			proxyAdapter, ok = Proxies()[proxyName]
			if ok {
				metadata.SpecialProxy = proxyName // just for log
			} else {
				opts = append(opts, dialer.WithInterface(proxyName))
			}
		}
	}

	if proxyAdapter == nil {
		return dialer.NewDialer(opts...).ListenPacket(ctx, network, "", metadata.AddrPort())
	}

	if !proxyAdapter.SupportUDP() {
		return nil, fmt.Errorf("proxy adapter [%s] UDP is not supported", proxyAdapter)
	}

	// Keep the logical host on metadata for rules and tracking, but pass the
	// pinned IP to the proxy so it cannot trigger another DNS resolution.
	dialMetadata := metadata
	if resolvedAddr.IsValid() && metadata.Host != "" {
		dialMetadata = metadata.Clone()
		dialMetadata.Host = ""
	}
	packetConn, err := proxyAdapter.ListenPacketContext(ctx, dialMetadata)
	if err != nil {
		logMetadataErr(metadata, rule, proxyAdapter, err)
		return nil, err
	}

	packetConn, err = joinModeTracker(routeState.revision, metadata, packetConn, func(conn C.PacketConn) C.PacketConn {
		return statistic.NewUDPTracker(conn, statistic.DefaultManager, metadata, rule, 0, 0, false)
	})
	if err != nil {
		return nil, err
	}
	logMetadataWithMode(metadata, rule, packetConn, routeState.mode)

	return packetConn, nil
}
