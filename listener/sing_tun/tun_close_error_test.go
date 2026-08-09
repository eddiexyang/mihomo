package sing_tun

import (
	"errors"
	"os"
	"syscall"
	"testing"

	tun "github.com/metacubex/sing-tun"
	"github.com/metacubex/sing/common/buf"
)

type failingDarwinTun struct{}

func (f *failingDarwinTun) Read([]byte) (int, error) {
	return 0, syscall.EBADF
}

func (f *failingDarwinTun) Write([]byte) (int, error) {
	return 0, syscall.EBADF
}

func (f *failingDarwinTun) Close() error {
	return nil
}

func (f *failingDarwinTun) BatchRead() ([]*buf.Buffer, error) {
	return nil, syscall.EBADF
}

func (f *failingDarwinTun) BatchWrite([]*buf.Buffer) error {
	return syscall.EBADF
}

func TestCloseAwareDarwinTunStopsBatchReadAfterClose(t *testing.T) {
	wrapped := wrapTunCloseErrors(&failingDarwinTun{})
	darwinTun, ok := wrapped.(tun.DarwinTUN)
	if !ok {
		t.Fatal("wrapped TUN does not implement DarwinTUN")
	}

	_, err := darwinTun.BatchRead()
	if !errors.Is(err, syscall.EBADF) {
		t.Fatalf("BatchRead before Close error = %v, want EBADF", err)
	}

	if err = wrapped.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	_, err = darwinTun.BatchRead()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("BatchRead after Close error = %v, want os.ErrClosed", err)
	}
}
