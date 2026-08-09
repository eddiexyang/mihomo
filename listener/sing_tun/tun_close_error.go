package sing_tun

import (
	"os"
	"sync/atomic"

	tun "github.com/metacubex/sing-tun"
	"github.com/metacubex/sing/common/buf"
)

type closeAwareDarwinTun struct {
	tun.DarwinTUN
	closed atomic.Bool
}

func (t *closeAwareDarwinTun) BatchRead() ([]*buf.Buffer, error) {
	buffers, err := t.DarwinTUN.BatchRead()
	if err != nil && t.closed.Load() {
		return nil, os.ErrClosed
	}
	return buffers, err
}

func (t *closeAwareDarwinTun) Close() error {
	t.closed.Store(true)
	return t.DarwinTUN.Close()
}

func wrapTunCloseErrors(tunIf tun.Tun) tun.Tun {
	darwinTun, ok := tunIf.(tun.DarwinTUN)
	if !ok {
		return tunIf
	}
	return &closeAwareDarwinTun{DarwinTUN: darwinTun}
}
