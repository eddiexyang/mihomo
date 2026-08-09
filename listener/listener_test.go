package listener

import (
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter/inbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type defaultListenerTestTracker struct {
	statistic.Tracker
	id      string
	info    *statistic.TrackerInfo
	manager *statistic.Manager
	closed  atomic.Bool
}

func (t *defaultListenerTestTracker) ID() string {
	return t.id
}

func (t *defaultListenerTestTracker) Info() *statistic.TrackerInfo {
	return t.info
}

func (t *defaultListenerTestTracker) Close() error {
	t.closed.Store(true)
	t.manager.Leave(t)
	return nil
}

func TestDefaultMixedUDPAdditions(t *testing.T) {
	metadata := &C.Metadata{SpecialRules: "stale"}
	generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultMixedName)
	inbound.ApplyAdditions(metadata, defaultMixedAdditions(generation)...)

	if metadata.InName != inbound.DefaultMixedName {
		t.Fatalf("inbound name = %q, want DEFAULT-MIXED", metadata.InName)
	}
	if metadata.InboundGeneration == nil ||
		metadata.InboundGeneration.Name != inbound.DefaultMixedName ||
		metadata.InboundGeneration.Revision != generation {
		t.Fatalf("mixed generation = %#v, want revision %d", metadata.InboundGeneration, generation)
	}
	if metadata.SpecialRules != "" {
		t.Fatalf("special rules = %q, want empty", metadata.SpecialRules)
	}
}

func TestDefaultTunAdditions(t *testing.T) {
	metadata := &C.Metadata{SpecialRules: "stale"}
	generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultTunName)
	inbound.ApplyAdditions(metadata, defaultTunAdditions(generation)...)

	if metadata.InName != inbound.DefaultTunName {
		t.Fatalf("inbound name = %q, want DEFAULT-TUN", metadata.InName)
	}
	if metadata.InboundGeneration == nil ||
		metadata.InboundGeneration.Name != inbound.DefaultTunName ||
		metadata.InboundGeneration.Revision != generation {
		t.Fatalf("TUN generation = %#v, want revision %d", metadata.InboundGeneration, generation)
	}
	if metadata.SpecialRules != "" {
		t.Fatalf("special rules = %q, want empty", metadata.SpecialRules)
	}
}

func TestInvalidateDefaultMixedGenerationClosesTransientConnections(t *testing.T) {
	previousManager := statistic.DefaultManager
	manager := &statistic.Manager{}
	statistic.DefaultManager = manager
	t.Cleanup(func() { statistic.DefaultManager = previousManager })

	generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultMixedName)
	metadata := &C.Metadata{}
	inbound.ApplyAdditions(metadata, defaultMixedAdditions(generation)...)
	tracker := &defaultListenerTestTracker{
		id:      "transient-default-mixed",
		info:    &statistic.TrackerInfo{Metadata: metadata},
		manager: manager,
	}
	manager.Join(tracker)

	invalidateDefaultMixedGeneration()

	current, ok := inbound.CurrentDefaultListenerGeneration(inbound.DefaultMixedName)
	if !ok {
		t.Fatal("default mixed generation is unavailable")
	}
	if current != generation+1 {
		t.Fatalf("default mixed generation = %d, want %d", current, generation+1)
	}
	if !tracker.closed.Load() {
		t.Fatal("transient default mixed tracker remained open")
	}
	if manager.Get(tracker.ID()) != nil {
		t.Fatal("transient default mixed tracker remained in manager")
	}
	if inbound.IsCurrentDefaultListenerGeneration(metadata) {
		t.Fatal("transient default mixed metadata remained current for a late join")
	}
}
