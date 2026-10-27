package dimse

import (
	"fmt"
	"time"
)

// AssociationErrorKind classifies a failed or refused association.
type AssociationErrorKind uint8

const (
	// AssociationRejected is an A-ASSOCIATE-RJ: the peer refused the association.
	AssociationRejected AssociationErrorKind = iota
	// AssociationAborted is an A-ABORT during establishment (service-user source).
	AssociationAborted
	// AssociationProviderAborted is an A-P-ABORT during establishment (provider source).
	AssociationProviderAborted
	// AssociationTimeout is a negotiation that exceeded its deadline.
	AssociationTimeout
	// AssociationNotEstablished is an operation attempted on an association that was never
	// established or has already been released/aborted (Codex DIMSE-017).
	AssociationNotEstablished
)

var associationErrorKindNames = map[AssociationErrorKind]string{
	AssociationRejected:        "rejected",
	AssociationAborted:         "aborted",
	AssociationProviderAborted: "provider-aborted",
	AssociationTimeout:         "timeout",
	AssociationNotEstablished:  "not established",
}

// String renders the association-error kind by name (PRD §8.2).
func (k AssociationErrorKind) String() string {
	if name, ok := associationErrorKindNames[k]; ok {
		return name
	}
	return "unknown"
}

// AssociationError reports a failed or refused association: an A-ASSOCIATE-RJ (with source
// and reason), an A-ABORT during establishment, a negotiation timeout, or an operation on an
// unestablished/released association (Codex DIMSE-017). It names the kind, source, and reason
// without PHI; it never carries a patient value.
type AssociationError struct {
	Kind   AssociationErrorKind
	Result uint8 // PS3.8 rejection result: permanent or transient (0 when not applicable)
	Source uint8 // PS3.8 rejection/abort source (0 when not applicable)
	Reason uint8 // PS3.8 reason (0 when not applicable)
	// Detail names the violated constraint for kinds without a wire source/reason.
	Detail string
	Cause  error
}

func (e *AssociationError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("dimse: association %s: %s", e.Kind, e.Detail)
	}
	return fmt.Sprintf("dimse: association %s (result %d, source %d, reason %d)",
		e.Kind, e.Result, e.Source, e.Reason)
}

// Unwrap preserves the transport or context failure that prevented association establishment.
func (e *AssociationError) Unwrap() error { return e.Cause }

// TransportError reports a socket read or write failure while an association is established.
// Cause preserves the concrete net or io error for errors.Is/errors.As classification.
type TransportError struct {
	Op    string
	Cause error
}

func (e *TransportError) Error() string {
	if e.Op == "" {
		return "dimse: transport: " + e.Cause.Error()
	}
	return fmt.Sprintf("dimse: transport %s: %v", e.Op, e.Cause)
}

func (e *TransportError) Unwrap() error { return e.Cause }

// PeerReleaseError reports that the peer gracefully released the association while a DIMSE
// operation was awaiting its response. The release handshake and socket close have completed.
type PeerReleaseError struct{}

func (e *PeerReleaseError) Error() string {
	return "dimse: peer released the association during the operation"
}

// Is lets callers classify every PeerReleaseError through one sentinel value.
func (e *PeerReleaseError) Is(target error) bool {
	_, ok := target.(*PeerReleaseError)
	return ok
}

// AbortError reports an A-ABORT (user) or A-P-ABORT (provider) on an established association.
type AbortError struct {
	Provider bool // true => A-P-ABORT (provider-initiated)
	Source   uint8
	Reason   uint8
}

func (e *AbortError) Error() string {
	kind := "A-ABORT"
	if e.Provider {
		kind = "A-P-ABORT"
	}
	return fmt.Sprintf("dimse: %s (source %d, reason %d)", kind, e.Source, e.Reason)
}

// TimeoutError reports that the association's DIMSE timeout expired mid-conversation: no
// PDU-level transport progress with the peer for Idle (the configured WithDIMSETimeout),
// with the caller's ctx still live. The bound re-arms on every PDU in both directions, so
// it can only fire on true peer silence — never on a long operation or a large dataset that
// is still flowing. It is distinct from the caller's own cancellation or deadline (the ctx
// cancellation contract) and from a transport fault (AbortError, ProtocolError); it
// deliberately does not unwrap to context.DeadlineExceeded, because conflating the two is
// what forces callers into provenance guesswork. The interrupted PDU is half-framed, so the
// association is no longer usable and must be discarded, not reused.
type TimeoutError struct {
	Idle time.Duration // the configured maximum silence that elapsed
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("dimse: no PDU exchanged with the peer within %s", e.Idle)
}

// ProtocolError reports a malformed PDU, a length-limit violation, or an unexpected PDU for
// the current DUL state. It names the state and the violated constraint without PHI.
type ProtocolError struct {
	State State
	// Detail names the violated constraint without any patient value.
	Detail string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("dimse: protocol error in %s: %s", e.State, e.Detail)
}

// ValidationError reports an invalid value-type construction at the API boundary: an
// AE Title outside the 1..16 length or default character repertoire, a max-PDU length
// too small to carry a PDV, or a malformed presentation context. It is a fault the
// caller fixes (PRD §8.2, §9.2), distinct from the protocol/association faults that
// arise once a conversation is under way. It names the violated constraint without PHI.
type ValidationError struct {
	// Detail names the violated constraint without any patient value.
	Detail string
}

func (e *ValidationError) Error() string { return "dimse: " + e.Detail }
