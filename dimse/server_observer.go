package dimse

import (
	"context"
	"net"
)

// ServerEventKind identifies inbound connection and response-send observations.
type ServerEventKind string

const (
	ServerCapacityRefused    ServerEventKind = "capacity_refused"
	ServerConnectionAccepted ServerEventKind = "connection_accepted"
	ServerConnectionClosed   ServerEventKind = "connection_closed"
	ServerStoreResponse      ServerEventKind = "store_response"
)

// ServerStage identifies the operation running when a connection ended.
type ServerStage string

const (
	ServerStageTLS             ServerStage = "tls"
	ServerStageAssociation     ServerStage = "association"
	ServerStageInbound         ServerStage = "inbound"
	ServerStageMessageDecode   ServerStage = "message_decode"
	ServerStageDispatch        ServerStage = "dispatch"
	ServerStageStoreValidation ServerStage = "store_validation"
	ServerStageStoreResponse   ServerStage = "store_response"
	ServerStageRelease         ServerStage = "release"
)

// ServerEvent carries protocol/resource diagnostics without dataset values.
// ConnectionAccepted means TCP admission, not successful DICOM negotiation.
// Err on ConnectionClosed retains negotiation, protocol, decode and transport
// faults, including those before a StoreHandler can run. Err remains opaque;
// observers must sanitize it before exporting. Active is a concurrent
// snapshot after the event; another connection may change it immediately.
type ServerEvent struct {
	Kind         ServerEventKind
	ConnectionID uint64
	Peer         net.Addr
	Active       int
	Capacity     int
	Err          error
	Stage        ServerStage
	// These fields are populated only for ServerStoreResponse. Err is the
	// local send result; nil does not prove the peer received the response.
	PresentationID uint8
	MessageID      uint16
	StatusCode     uint16
}

// WithServerObserver reports admission and connection termination, including
// failures before any DIMSE handler runs. Calls may be concurrent and must not
// block. No server lock is held while the observer runs. The observer must not
// panic. Nil disables reporting; the wire and shutdown behavior are unchanged.
func WithServerObserver(observer func(ServerEvent)) ServerOption {
	return func(c *serverConfig) { c.observer = observer }
}

type serverConnectionContextKey struct{}

type serverObservation struct {
	id      uint64
	stage   ServerStage // owned by the connection's dispatch goroutine
	observe func(ServerEvent)
}

// ServerConnectionID returns the opaque connection ID shared by handler calls
// and ServerEvents on this server. It is zero outside a Server handler context.
func ServerConnectionID(ctx context.Context) uint64 {
	if observation, ok := ctx.Value(serverConnectionContextKey{}).(*serverObservation); ok {
		return observation.id
	}
	return 0
}

func serverStage(ctx context.Context, stage ServerStage) {
	if observation, ok := ctx.Value(serverConnectionContextKey{}).(*serverObservation); ok {
		observation.stage = stage
	}
}

func observeStoreResponse(ctx context.Context, info OpInfo, status Status, err error) {
	if observation, ok := ctx.Value(serverConnectionContextKey{}).(*serverObservation); ok {
		observation.observe(ServerEvent{
			Kind: ServerStoreResponse, ConnectionID: observation.id,
			Stage: ServerStageStoreResponse, PresentationID: info.PresentationID,
			MessageID: info.MessageID, StatusCode: status.Code, Err: err,
		})
	}
}

func (s *Server) observe(event ServerEvent) {
	if s.cfg.observer != nil {
		event.Active = len(s.sem)
		event.Capacity = cap(s.sem)
		s.cfg.observer(event)
	}
}
