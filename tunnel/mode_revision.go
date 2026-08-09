package tunnel

import (
	"errors"
	"sync"

	"github.com/metacubex/mihomo/adapter/inbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

var errModeChanged = errors.New("tunnel mode changed during connection setup")
var errListenerGenerationChanged = errors.New("inbound listener changed during connection setup")

var modeSwitchMu sync.Mutex

type tunnelModeState struct {
	mode     TunnelMode
	revision uint64
}

type modeTrackable interface {
	Close() error
}

func captureModeRevision() uint64 {
	return captureModeState().revision
}

func captureModeState() tunnelModeState {
	return modeState.Load()
}

func captureMetadataRouteState(metadata *C.Metadata) tunnelModeState {
	state := captureModeState()
	metadata.RouteRevision = state.revision
	metadata.RouteRevisionSet = true
	return state
}

func routeStateError(revision uint64, metadata *C.Metadata) error {
	if captureModeRevision() != revision {
		return errModeChanged
	}
	if !inbound.IsCurrentDefaultListenerGeneration(metadata) {
		return errListenerGenerationChanged
	}
	return nil
}

func validateTCPRemoteConnRouteState(revision uint64, metadata *C.Metadata, conn C.Conn) (C.Conn, error) {
	if err := routeStateError(revision, metadata); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func joinModeTracker[T modeTrackable](revision uint64, metadata *C.Metadata, conn T, join func(T) T) (T, error) {
	if err := routeStateError(revision, metadata); err != nil {
		_ = conn.Close()
		var zero T
		return zero, err
	}

	tracker := join(conn)
	if err := routeStateError(revision, metadata); err != nil {
		_ = tracker.Close()
		var zero T
		return zero, err
	}

	return tracker, nil
}

func connectionsBeforeModeRevision(revision uint64) []statistic.Tracker {
	connections := make([]statistic.Tracker, 0)
	statistic.DefaultManager.Range(func(connection statistic.Tracker) bool {
		info := connection.Info()
		if info != nil && info.Metadata != nil &&
			info.Metadata.RouteRevisionSet && info.Metadata.RouteRevision != revision {
			connections = append(connections, connection)
		}
		return true
	})
	return connections
}
