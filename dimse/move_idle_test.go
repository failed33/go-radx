package dimse

import (
	"context"
	"errors"
	"testing"
	"time"
)

// dialMoveSCUWithTimeout mirrors dialMoveSCU but with a caller-chosen DIMSE timeout, so the
// idle-semantics tests can place the timeout between the inter-response gap and the
// operation's total duration.
func dialMoveSCUWithTimeout(t *testing.T, addr string, dimseTimeout time.Duration) (*Association, context.Context, context.CancelFunc) {
	t.Helper()
	ae, err := NewAE(AETitle("MOVESCU"), WithDIMSETimeout(dimseTimeout))
	if err != nil {
		t.Fatalf("NewAE: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	assoc, err := ae.Associate(ctx, addr, AETitle("MOVESCP"), QueryRetrieveContexts())
	if err != nil {
		cancel()
		t.Fatalf("Associate: %v", err)
	}
	return assoc, ctx, cancel
}

// A C-MOVE whose total duration exceeds the DIMSE timeout must still succeed as long as every
// inter-response gap stays under it: the timeout bounds peer silence, never operation duration
// (the pynetdicom/DCMTK dimse-timeout semantic). Four pendings spaced 200ms apart plus the
// terminal make the operation ~1s long against a 500ms timeout.
func TestMoveOutlivesDIMSETimeoutWhileResponsesFlow(t *testing.T) {
	responses := []moveResponse{
		{status: 0xFF00, remaining: 4, delay: 200 * time.Millisecond},
		{status: 0xFF00, remaining: 3, completed: 1, delay: 200 * time.Millisecond},
		{status: 0xFF00, remaining: 2, completed: 2, delay: 200 * time.Millisecond},
		{status: 0xFF00, remaining: 1, completed: 3, delay: 200 * time.Millisecond},
		{status: 0x0000, remaining: 0, completed: 4, delay: 200 * time.Millisecond},
	}
	addr, _ := startMoveSCP(t, responses)
	assoc, ctx, cancel := dialMoveSCUWithTimeout(t, addr, 500*time.Millisecond)
	defer cancel()

	var terminal Status
	var pendings int
	for status := range assoc.Move(ctx, nil, QueryLevelStudy, AETitle("STORESCP")) {
		if status.IsPending() {
			pendings++
			continue
		}
		terminal = status
	}
	if err := assoc.LastError(); err != nil {
		t.Fatalf("LastError() = %v; a streaming C-MOVE longer than the DIMSE timeout must not fault", err)
	}
	if !terminal.IsSuccess() {
		t.Errorf("terminal status = %v, want Success", terminal)
	}
	if pendings != 4 {
		t.Errorf("pending responses seen = %d, want 4", pendings)
	}
}

// A peer that goes silent mid-stream (pendings, then nothing) must surface a typed
// *TimeoutError naming the operation and the configured idle bound — not a bare
// context.DeadlineExceeded (which callers cannot tell from their own deadline), and not a
// laundered Success.
func TestMoveStalledPeerSurfacesTimeoutError(t *testing.T) {
	const idle = 300 * time.Millisecond
	responses := []moveResponse{
		{status: 0xFF00, remaining: 2, delay: 50 * time.Millisecond},
		{status: 0xFF00, remaining: 1, completed: 1, delay: 50 * time.Millisecond},
		// No terminal: the SCP stays silent (its ServeRelease keeps the conn open).
	}
	addr, _ := startMoveSCP(t, responses)
	assoc, ctx, cancel := dialMoveSCUWithTimeout(t, addr, idle)
	defer cancel()

	var sawSuccess bool
	var terminalSeen bool
	for status, _ := range assoc.Move(ctx, nil, QueryLevelStudy, AETitle("STORESCP")) {
		if status.IsPending() {
			continue
		}
		terminalSeen = true
		if status.IsSuccess() {
			sawSuccess = true
		}
	}
	if sawSuccess {
		t.Error("a stalled peer was laundered into a Success status")
	}
	if !terminalSeen {
		t.Error("a stalled C-MOVE ended without yielding any terminal status")
	}
	var te *TimeoutError
	if !errors.As(assoc.LastError(), &te) {
		t.Fatalf("LastError() = %v (%T), want *TimeoutError", assoc.LastError(), assoc.LastError())
	}
	if te.Op != "C-MOVE" {
		t.Errorf("TimeoutError.Op = %q, want %q", te.Op, "C-MOVE")
	}
	if te.Idle != idle {
		t.Errorf("TimeoutError.Idle = %v, want %v", te.Idle, idle)
	}
	if errors.Is(assoc.LastError(), context.DeadlineExceeded) {
		t.Error("TimeoutError must not unwrap to context.DeadlineExceeded; that conflation is the bug")
	}
}
