package listener

import (
	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

func StopListener() {

	if socksListener != nil {
		_ = socksListener.Close()
		socksListener = nil
	}

	if socksUDPListener != nil {
		_ = socksUDPListener.Close()
		socksUDPListener = nil
	}

	if httpListener != nil {
		_ = httpListener.Close()
		httpListener = nil
	}

	if redirListener != nil {
		_ = redirListener.Close()
		redirListener = nil
	}

	if redirUDPListener != nil {
		_ = redirUDPListener.Close()
		redirUDPListener = nil
	}

	if tproxyListener != nil {
		_ = tproxyListener.Close()
		tproxyListener = nil
	}

	if tproxyUDPListener != nil {
		_ = tproxyUDPListener.Close()
		tproxyUDPListener = nil
	}

	var mixedConnections []statistic.Tracker
	mixedMux.Lock()
	if mixedListener != nil || mixedUDPLister != nil {
		_, mixedConnections = advanceDefaultListenerGeneration(inbound.DefaultMixedName)
	}
	if mixedListener != nil {
		_ = mixedListener.Close()
		mixedListener = nil
	}
	if mixedUDPLister != nil {
		_ = mixedUDPLister.Close()
		mixedUDPLister = nil
	}
	mixedMux.Unlock()
	closeDefaultListenerConnections(mixedConnections)

	var tunConnections []statistic.Tracker
	tunMux.Lock()
	if tunLister != nil {
		_, tunConnections = advanceDefaultListenerGeneration(inbound.DefaultTunName)
		_ = tunLister.Close()
		tunLister = nil
	}
	tunMux.Unlock()
	closeDefaultListenerConnections(tunConnections)

	if shadowSocksListener != nil {
		_ = shadowSocksListener.Close()
		shadowSocksListener = nil
	}

	if shadowSocksListener != nil {
		_ = shadowSocksListener.Close()
		shadowSocksListener = nil
	}

	if vmessListener != nil {
		_ = vmessListener.Close()
		vmessListener = nil
	}

	if tuicListener != nil {
		_ = tuicListener.Close()
		tuicListener = nil
	}
}
