package executor

import (
	"testing"

	"github.com/metacubex/mihomo/tunnel"
)

func TestTemporaryUpdateGeneralDoesNotPublishMode(t *testing.T) {
	originalMode := tunnel.Mode()
	general := GetGeneral()
	if originalMode == tunnel.Direct {
		general.Mode = tunnel.Rule
	} else {
		general.Mode = tunnel.Direct
	}

	rollback := temporaryUpdateGeneral(general)
	if current := tunnel.Mode(); current != originalMode {
		t.Fatalf("mode during temporary update = %s, want %s", current, originalMode)
	}
	rollback()
	if current := tunnel.Mode(); current != originalMode {
		t.Fatalf("mode after temporary rollback = %s, want %s", current, originalMode)
	}
}
