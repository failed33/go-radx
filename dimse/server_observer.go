package dimse

import (
	"context"
	"net"

	"github.com/failed33/go-radx/dimse/dul"
)

// ConnectionEventKind identifies admission and termination of inbound TCP connections.
// Admission precedes TLS and DICOM association negotiation.
type ConnectionEventKind string

const (
	ConnectionAccepted        ConnectionEventKind = "accepted"
	ConnectionCapacityRefused ConnectionEventKind = "capacity_refused"
	ConnectionClosed          ConnectionEventKind = "closed"
)

// ConnectionCloseReason identifies how an admitted connection ended. A peer abort
// or orderly EOF is distinct from a graceful DICOM release even when Err is nil.
type ConnectionCloseReason string

const (
	ConnectionReleased      ConnectionCloseReason = "released"
	ConnectionPeerAborted   ConnectionCloseReason = "peer_aborted"
	ConnectionPeerClosed    ConnectionCloseReason = "peer_closed"
	ConnectionServerStopped ConnectionCloseReason = "server_stopped"
	ConnectionFailed        ConnectionCloseReason = "failed"
)

// ConnectionOperation identifies the protocol boundary returning a connection
// error, not the server's last progress marker. It is empty for a clean close or
// cancellation detected between operations. ResponseSendEvent reports individual
// response-send errors within ConnectionProcess.
type ConnectionOperation string

const (
	ConnectionEstablish ConnectionOperation = "establish"
	ConnectionReceive   ConnectionOperation = "receive"
	ConnectionProcess   ConnectionOperation = "process_dimse"
	ConnectionRelease   ConnectionOperation = "release"
)

// ConnectionEvent reports one inbound connection's lifecycle. Reason, Operation
// and Err describe ConnectionClosed only. ServerStopped means the server context
// was done when termination was observed; Err remains the original error and may
// retain private information. Observers must sanitize it before exporting.
// RemoteAddr identifies the peer, including before negotiation; it may be private.
// ConnectionID is opaque, unique within this Server, and shared with handler contexts.
type ConnectionEvent struct {
	Kind         ConnectionEventKind
	ConnectionID uint64
	RemoteAddr   net.Addr
	Reason       ConnectionCloseReason
	Operation    ConnectionOperation
	Err          error
}

// ResponseSendEvent reports completion of one local DIMSE response send on an
// inbound Server connection. All supported C- and N-service responses, including
// pending, final and unsupported-operation replies, pass through this boundary.
// Err includes command/dataset encoding and transport failures, unchanged. A nil
// Err means the entire local send completed; it does not prove peer receipt.
// No dataset, SOP Instance UID or private peer address is included. Err is opaque
// and must be sanitized before exporting.
type ResponseSendEvent struct {
	ConnectionID              uint64
	PresentationID            uint8
	CommandField              CommandField
	MessageIDBeingRespondedTo uint16
	StatusCode                uint16
	Err                       error
}

// WithConnectionObserver observes inbound TCP admission/refusal and termination.
// Calls are synchronous on the accept or connection goroutine, may be concurrent,
// and hold no Server lock. Keep the callback fast: do not block, panic, or call a
// blocking Server method such as Shutdown from it. Nil disables this hook.
func WithConnectionObserver(observer func(ConnectionEvent)) ServerOption {
	return func(c *serverConfig) { c.connectionObserver = observer }
}

// WithResponseSendObserver observes whole local DIMSE response sends, including
// datasets, on inbound Server connections. Requests and sends on other connections
// are excluded even when they inherit a handler context. Calls are synchronous on
// the sending goroutine, may be concurrent, and hold no Server lock. Keep the
// callback fast and nonblocking; it must not panic or call blocking Server methods.
// Nil disables this hook. Neither hook creates goroutines or exports telemetry.
func WithResponseSendObserver(observer func(ResponseSendEvent)) ServerOption {
	return func(c *serverConfig) { c.responseObserver = observer }
}

type serverConnectionContextKey struct{}

type serverObservation struct {
	id       uint64
	conn     *dul.Conn
	response func(ResponseSendEvent)
}

// ServerConnectionID returns the opaque ID shared by handler contexts and this
// Server's observer events. It is zero outside an inbound Server handler context.
func ServerConnectionID(ctx context.Context) uint64 {
	if observation, ok := ctx.Value(serverConnectionContextKey{}).(*serverObservation); ok {
		return observation.id
	}
	return 0
}

func responseObservation(ctx context.Context, conn *dul.Conn, cmd CommandSet) *serverObservation {
	observation, _ := ctx.Value(serverConnectionContextKey{}).(*serverObservation)
	if observation == nil || observation.response == nil || observation.conn != conn || !cmd.IsResponse() {
		return nil
	}
	return observation
}

func (s *Server) observeConnection(event ConnectionEvent) {
	if s.cfg.connectionObserver != nil {
		s.cfg.connectionObserver(event)
	}
}

type connectionEnd struct {
	reason    ConnectionCloseReason
	operation ConnectionOperation
	err       error
}

func connectionFailure(ctx context.Context, operation ConnectionOperation, err error) connectionEnd {
	reason := ConnectionFailed
	if ctx.Err() != nil {
		reason = ConnectionServerStopped
	}
	return connectionEnd{reason: reason, operation: operation, err: err}
}
