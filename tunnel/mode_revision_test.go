package tunnel

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type modeTestConn struct {
	N.ExtendedConn
	closed atomic.Bool
}

type earlyDataModeTestConn struct {
	*modeTestConn
	writes atomic.Int32
}

type blockingModeTracker struct {
	statistic.Tracker
	manager                *statistic.Manager
	closeStarted           chan struct{}
	concurrentCloseStarted chan struct{}
	releaseClose           <-chan struct{}
	closeCalls             atomic.Int32
	activeCloses           atomic.Int32
	maxConcurrentCloses    atomic.Int32
}

func (t *blockingModeTracker) Close() error {
	call := t.closeCalls.Add(1)
	active := t.activeCloses.Add(1)
	for {
		maximum := t.maxConcurrentCloses.Load()
		if active <= maximum || t.maxConcurrentCloses.CompareAndSwap(maximum, active) {
			break
		}
	}

	if call == 1 {
		close(t.closeStarted)
	} else if call == 2 {
		close(t.concurrentCloseStarted)
	}
	<-t.releaseClose
	t.activeCloses.Add(-1)

	if call != 1 {
		return nil
	}
	t.manager.Leave(t)
	return t.Tracker.Close()
}

func (c *modeTestConn) Close() error {
	c.closed.Store(true)
	return c.ExtendedConn.Close()
}

func (c *earlyDataModeTestConn) NeedHandshake() bool {
	return true
}

func (c *earlyDataModeTestConn) Write(payload []byte) (int, error) {
	c.writes.Add(1)
	return len(payload), nil
}

func (*modeTestConn) Chains() C.Chain {
	return C.Chain{"MODE-REVISION-TEST"}
}

func (*modeTestConn) ProviderChains() C.Chain {
	return nil
}

func (*modeTestConn) AppendToChains(C.ProxyAdapter) {}

func (*modeTestConn) RemoteDestination() string {
	return "127.0.0.1:0"
}

func alternateMode(mode TunnelMode) TunnelMode {
	if mode == Direct {
		return Rule
	}
	return Direct
}

func newModeTestConn() (*modeTestConn, net.Conn) {
	conn, peer := net.Pipe()
	return &modeTestConn{ExtendedConn: N.NewExtendedConn(conn)}, peer
}

func TestSetModeRevisionChangesOnlyWhenModeChanges(t *testing.T) {
	previousMode := Mode()
	t.Cleanup(func() { SetMode(previousMode) })

	SetMode(Rule)
	revision := captureModeRevision()
	SetMode(Rule)
	if current := captureModeRevision(); current != revision {
		t.Fatalf("revision after same mode = %d, want %d", current, revision)
	}

	SetMode(Direct)
	if current := captureModeRevision(); current != revision+1 {
		t.Fatalf("revision after mode change = %d, want %d", current, revision+1)
	}
}

func TestCaptureMetadataRouteStateTagsEveryConnection(t *testing.T) {
	for _, metadata := range []*C.Metadata{
		{},
		{SpecialProxy: "EXPLICIT"},
	} {
		state := captureMetadataRouteState(metadata)
		if !metadata.RouteRevisionSet || metadata.RouteRevision != state.revision {
			t.Fatalf("metadata route revision = (%d, %t), want (%d, true)",
				metadata.RouteRevision, metadata.RouteRevisionSet, state.revision)
		}
	}
}

func TestJoinModeTrackerRejectsChangeBeforeJoin(t *testing.T) {
	previousMode := Mode()
	t.Cleanup(func() { SetMode(previousMode) })

	metadata := &C.Metadata{}
	revision := captureMetadataRouteState(metadata).revision
	SetMode(alternateMode(Mode()))

	conn, peer := newModeTestConn()
	t.Cleanup(func() { _ = peer.Close() })
	joinCalled := false
	tracker, err := joinModeTracker[C.Conn](revision, metadata, conn, func(conn C.Conn) C.Conn {
		joinCalled = true
		return conn
	})
	if !errors.Is(err, errModeChanged) {
		t.Fatalf("join error = %v, want %v", err, errModeChanged)
	}
	if tracker != nil {
		t.Fatal("tracker returned after mode changed")
	}
	if joinCalled {
		t.Fatal("join called after revision had already changed")
	}
	if !conn.closed.Load() {
		t.Fatal("connection remained open after pre-join revision rejection")
	}
}

func TestTCPRouteChangeStopsRetryBeforeEarlyData(t *testing.T) {
	previousMode := Mode()
	t.Cleanup(func() { SetMode(previousMode) })

	tests := []struct {
		name       string
		invalidate func(*C.Metadata)
		wantErr    error
	}{
		{
			name: "mode revision",
			invalidate: func(*C.Metadata) {
				SetMode(alternateMode(Mode()))
			},
			wantErr: errModeChanged,
		},
		{
			name: "listener generation",
			invalidate: func(metadata *C.Metadata) {
				generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultTunName)
				inbound.ApplyAdditions(
					metadata,
					inbound.WithDefaultListenerGeneration(inbound.DefaultTunName, generation),
				)
				inbound.AdvanceDefaultListenerGeneration(inbound.DefaultTunName)
			},
			wantErr: errListenerGenerationChanged,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := &C.Metadata{}
			routeState := captureMetadataRouteState(metadata)
			test.invalidate(metadata)

			baseConn, peer := newModeTestConn()
			t.Cleanup(func() { _ = peer.Close() })
			remoteConn := &earlyDataModeTestConn{modeTestConn: baseConn}
			attempts := 0

			_, err := retry[C.Conn](context.Background(), func(context.Context) (C.Conn, error) {
				attempts++
				validatedConn, err := validateTCPRemoteConnRouteState(routeState.revision, metadata, remoteConn)
				if err != nil {
					return nil, err
				}
				if N.NeedHandshake(validatedConn) {
					_, err = validatedConn.Write([]byte("early data"))
				}
				return validatedConn, err
			}, nil)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("retry error = %v, want %v", err, test.wantErr)
			}
			if attempts != 1 {
				t.Fatalf("retry attempts = %d, want 1", attempts)
			}
			if writes := remoteConn.writes.Load(); writes != 0 {
				t.Fatalf("early data writes = %d, want 0", writes)
			}
			if !baseConn.closed.Load() {
				t.Fatal("stale remote connection remained open")
			}
		})
	}
}

func TestJoinModeTrackerLeavesChangeDuringJoin(t *testing.T) {
	previousMode := Mode()
	previousNotify := statistic.DefaultRequestNotify
	t.Cleanup(func() {
		SetMode(previousMode)
		statistic.DefaultRequestNotify = previousNotify
	})

	manager := &statistic.Manager{}
	joinStarted := make(chan statistic.Tracker, 1)
	allowJoin := make(chan struct{})
	statistic.DefaultRequestNotify = func(tracker statistic.Tracker) {
		joinStarted <- tracker
		<-allowJoin
	}

	conn, peer := newModeTestConn()
	t.Cleanup(func() { _ = peer.Close() })
	metadata := &C.Metadata{}
	revision := captureMetadataRouteState(metadata).revision
	type result struct {
		tracker C.Conn
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		tracker, err := joinModeTracker[C.Conn](revision, metadata, conn, func(conn C.Conn) C.Conn {
			return statistic.NewTCPTracker(conn, manager, metadata, nil, 0, 0, false)
		})
		resultCh <- result{tracker: tracker, err: err}
	}()

	pendingTracker := <-joinStarted
	if tracker := manager.Get(pendingTracker.ID()); tracker != nil {
		t.Fatal("tracker stored before request notification returned")
	}
	SetMode(alternateMode(Mode()))
	close(allowJoin)

	trackerResult := <-resultCh
	if !errors.Is(trackerResult.err, errModeChanged) {
		t.Fatalf("join error = %v, want %v", trackerResult.err, errModeChanged)
	}
	if trackerResult.tracker != nil {
		t.Fatal("tracker returned after mode changed during join")
	}
	if tracker := manager.Get(pendingTracker.ID()); tracker != nil {
		t.Fatal("tracker remained in manager after post-join revision check")
	}
	if !conn.closed.Load() {
		t.Fatal("connection remained open after post-join revision rejection")
	}
}

func TestJoinModeTrackerRejectsStaleListenerBeforeJoin(t *testing.T) {
	generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultMixedName)
	metadata := &C.Metadata{SpecialProxy: "EXPLICIT"}
	inbound.ApplyAdditions(
		metadata,
		inbound.WithDefaultListenerGeneration(inbound.DefaultMixedName, generation),
	)
	routeState := captureMetadataRouteState(metadata)
	inbound.AdvanceDefaultListenerGeneration(inbound.DefaultMixedName)

	conn, peer := newModeTestConn()
	t.Cleanup(func() { _ = peer.Close() })
	joinCalled := false
	tracker, err := joinModeTracker[C.Conn](routeState.revision, metadata, conn, func(conn C.Conn) C.Conn {
		joinCalled = true
		return conn
	})
	if !errors.Is(err, errListenerGenerationChanged) {
		t.Fatalf("join error = %v, want %v", err, errListenerGenerationChanged)
	}
	if tracker != nil {
		t.Fatal("tracker returned for a stale listener")
	}
	if joinCalled {
		t.Fatal("join called for a stale listener")
	}
	if !conn.closed.Load() {
		t.Fatal("connection remained open after stale listener rejection")
	}
}

func TestJoinModeTrackerRejectsListenerChangeDuringJoin(t *testing.T) {
	previousNotify := statistic.DefaultRequestNotify
	t.Cleanup(func() { statistic.DefaultRequestNotify = previousNotify })

	manager := &statistic.Manager{}
	joinStarted := make(chan statistic.Tracker, 1)
	allowJoin := make(chan struct{})
	statistic.DefaultRequestNotify = func(tracker statistic.Tracker) {
		joinStarted <- tracker
		<-allowJoin
	}

	generation := inbound.AdvanceDefaultListenerGeneration(inbound.DefaultTunName)
	metadata := &C.Metadata{}
	inbound.ApplyAdditions(
		metadata,
		inbound.WithDefaultListenerGeneration(inbound.DefaultTunName, generation),
	)
	routeState := captureMetadataRouteState(metadata)
	conn, peer := newModeTestConn()
	t.Cleanup(func() { _ = peer.Close() })

	type result struct {
		tracker C.Conn
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		tracker, err := joinModeTracker[C.Conn](routeState.revision, metadata, conn, func(conn C.Conn) C.Conn {
			return statistic.NewTCPTracker(conn, manager, metadata, nil, 0, 0, false)
		})
		resultCh <- result{tracker: tracker, err: err}
	}()

	pendingTracker := <-joinStarted
	inbound.AdvanceDefaultListenerGeneration(inbound.DefaultTunName)
	close(allowJoin)

	trackerResult := <-resultCh
	if !errors.Is(trackerResult.err, errListenerGenerationChanged) {
		t.Fatalf("join error = %v, want %v", trackerResult.err, errListenerGenerationChanged)
	}
	if trackerResult.tracker != nil {
		t.Fatal("tracker returned after listener changed during join")
	}
	if tracker := manager.Get(pendingTracker.ID()); tracker != nil {
		t.Fatal("stale listener tracker remained in manager")
	}
	if !conn.closed.Load() {
		t.Fatal("connection remained open after listener changed during join")
	}
}

func TestSetModeClosesOnlyOlderRouteRevision(t *testing.T) {
	previousMode := Mode()
	previousManager := statistic.DefaultManager
	manager := &statistic.Manager{}
	statistic.DefaultManager = manager
	t.Cleanup(func() {
		SetMode(previousMode)
		statistic.DefaultManager = previousManager
	})

	oldMetadata := &C.Metadata{}
	captureMetadataRouteState(oldMetadata)
	oldConn, oldPeer := newModeTestConn()
	t.Cleanup(func() { _ = oldPeer.Close() })
	oldTracker := statistic.NewTCPTracker(oldConn, manager, oldMetadata, nil, 0, 0, false)

	SetMode(alternateMode(Mode()))
	if !oldConn.closed.Load() {
		t.Fatal("older route revision remained open after mode change")
	}
	if manager.Get(oldTracker.ID()) != nil {
		t.Fatal("older route revision remained in manager")
	}

	currentMetadata := &C.Metadata{}
	captureMetadataRouteState(currentMetadata)
	currentConn, currentPeer := newModeTestConn()
	t.Cleanup(func() { _ = currentPeer.Close() })
	currentTracker := statistic.NewTCPTracker(currentConn, manager, currentMetadata, nil, 0, 0, false)
	SetMode(Mode())
	if currentConn.closed.Load() {
		t.Fatal("current route revision was closed without a mode change")
	}
	if manager.Get(currentTracker.ID()) == nil {
		t.Fatal("current route revision was removed from manager")
	}
	_ = currentTracker.Close()
}

func TestSetModeSerializesBlockingTrackerClose(t *testing.T) {
	previousMode := Mode()
	previousManager := statistic.DefaultManager
	manager := &statistic.Manager{}
	statistic.DefaultManager = manager
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		SetMode(previousMode)
		statistic.DefaultManager = previousManager
	})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseClose) })
	})

	metadata := &C.Metadata{}
	captureMetadataRouteState(metadata)
	conn, peer := newModeTestConn()
	t.Cleanup(func() { _ = peer.Close() })
	baseTracker := statistic.NewTCPTracker(conn, manager, metadata, nil, 0, 0, false)
	tracker := &blockingModeTracker{
		Tracker:                baseTracker,
		manager:                manager,
		closeStarted:           make(chan struct{}),
		concurrentCloseStarted: make(chan struct{}),
		releaseClose:           releaseClose,
	}
	manager.Join(tracker)

	firstMode := alternateMode(Mode())
	firstDone := make(chan bool, 1)
	go func() {
		firstDone <- SetMode(firstMode)
	}()

	select {
	case <-tracker.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("first mode switch did not reach the blocking tracker")
	}

	if modeSwitchMu.TryLock() {
		modeSwitchMu.Unlock()
		t.Error("mode switch mutex was released before the stale tracker closed")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan bool, 1)
	secondMode := alternateMode(firstMode)
	go func() {
		close(secondStarted)
		secondDone <- SetMode(secondMode)
	}()
	<-secondStarted

	secondFinished := false
	secondChanged := false
	select {
	case <-tracker.concurrentCloseStarted:
		t.Error("the same tracker was closed concurrently by two mode switches")
	case secondChanged = <-secondDone:
		secondFinished = true
		t.Error("second mode switch completed before the first tracker close returned")
	case <-time.After(50 * time.Millisecond):
	}
	if current := Mode(); current != firstMode {
		t.Errorf("mode advanced to %v before the first tracker close returned, want %v", current, firstMode)
	}

	releaseOnce.Do(func() { close(releaseClose) })
	select {
	case changed := <-firstDone:
		if !changed {
			t.Error("first mode switch reported no change")
		}
	case <-time.After(time.Second):
		t.Fatal("first mode switch did not complete after tracker close was released")
	}
	if !secondFinished {
		select {
		case secondChanged = <-secondDone:
		case <-time.After(time.Second):
			t.Fatal("second mode switch did not complete after tracker close was released")
		}
	}
	if !secondChanged {
		t.Error("second mode switch reported no change")
	}
	if current := Mode(); current != secondMode {
		t.Errorf("mode after serialized switches = %v, want %v", current, secondMode)
	}

	if calls := tracker.closeCalls.Load(); calls != 1 {
		t.Errorf("tracker Close calls = %d, want 1", calls)
	}
	if maximum := tracker.maxConcurrentCloses.Load(); maximum != 1 {
		t.Errorf("maximum concurrent tracker Close calls = %d, want 1", maximum)
	}
	if !conn.closed.Load() {
		t.Error("stale tracker connection remained open")
	}
	if manager.Get(tracker.ID()) != nil {
		t.Error("stale tracker remained in manager")
	}
}

func TestModeConcurrentAccess(t *testing.T) {
	previousMode := Mode()
	previousManager := statistic.DefaultManager
	statistic.DefaultManager = &statistic.Manager{}
	t.Cleanup(func() {
		SetMode(previousMode)
		statistic.DefaultManager = previousManager
	})

	const (
		writers    = 4
		readers    = 8
		iterations = 2000
	)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	var invalid atomic.Bool

	waitGroup.Add(writers)
	for writer := 0; writer < writers; writer++ {
		writer := writer
		go func() {
			defer waitGroup.Done()
			<-start
			modes := [...]TunnelMode{Rule, Global, Direct}
			for index := 0; index < iterations; index++ {
				SetMode(modes[(writer+index)%len(modes)])
			}
		}()
	}

	waitGroup.Add(readers)
	for reader := 0; reader < readers; reader++ {
		go func() {
			defer waitGroup.Done()
			<-start
			lastRevision := captureModeState().revision
			for index := 0; index < iterations; index++ {
				snapshot := captureModeState()
				if snapshot.mode != Rule && snapshot.mode != Global && snapshot.mode != Direct {
					invalid.Store(true)
				}
				if snapshot.revision < lastRevision {
					invalid.Store(true)
				}
				lastRevision = snapshot.revision
			}
		}()
	}

	close(start)
	waitGroup.Wait()
	if invalid.Load() {
		t.Fatal("concurrent mode reads observed an invalid mode or decreasing revision")
	}
}
