package dimse

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/failed33/go-radx/dicom"
	"net"
	"testing"
	"time"
)

type observedEcho struct{ ids chan uint64 }

func (h observedEcho) Echo(ctx context.Context, _ OpInfo) Status {
	h.ids <- ServerConnectionID(ctx)
	return StatusEchoSuccess
}

func awaitConnectionEvent(t *testing.T, events <-chan ConnectionEvent, kind ConnectionEventKind) ConnectionEvent {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Kind == kind {
				return event
			}
		case <-timer.C:
			t.Fatalf("no %s event", kind)
		}
	}
}

func awaitResponseSend(t *testing.T, events <-chan ResponseSendEvent) ResponseSendEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("no response-send completion")
		return ResponseSendEvent{}
	}
}

func TestServerObserverReportsCapacityAndHandlerConnection(t *testing.T) {
	// Buffer the admission/refusal/close sequence so observer callbacks never
	// block behind assertions on the test goroutine.
	events := make(chan ConnectionEvent, 3)
	ids := make(chan uint64, 1)
	srv, _ := startServer(t, observedEcho{ids: ids}, WithMaxAssociations(1), WithConnectionObserver(func(e ConnectionEvent) { events <- e }))
	scu, err := NewAE("OBSERVER-SCU")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	assoc, err := scu.Associate(ctx, srv.Addr().String(), "RADX-SCP", VerificationContexts())
	if err != nil {
		t.Fatal(err)
	}
	admitted := awaitConnectionEvent(t, events, ConnectionAccepted)
	if status, err := assoc.Echo(ctx); err != nil || !status.IsSuccess() {
		t.Fatalf("Echo=%v,%v", status, err)
	}
	if id := <-ids; id == 0 || id != admitted.ConnectionID {
		t.Fatalf("handler connection ID=%d, admission=%d", id, admitted.ConnectionID)
	}
	extra, err := scu.Associate(ctx, srv.Addr().String(), "RADX-SCP", VerificationContexts())
	if err == nil {
		_ = extra.Release(ctx)
		t.Fatal("overflow association admitted")
	}
	refused := awaitConnectionEvent(t, events, ConnectionCapacityRefused)
	if refused.ConnectionID == 0 || refused.ConnectionID == admitted.ConnectionID || refused.Err != nil {
		t.Fatalf("wrong refusal event: %+v", refused)
	}
	if err := assoc.Release(ctx); err != nil {
		t.Fatal(err)
	}
	closed := awaitConnectionEvent(t, events, ConnectionClosed)
	if closed.ConnectionID != admitted.ConnectionID || closed.Err != nil || closed.Reason != ConnectionReleased {
		t.Fatalf("wrong close event: %+v", closed)
	}
}

func TestServerObserverReportsFaultBeforeHandler(t *testing.T) {
	// Both admission and timeout can occur before the test resumes reading.
	events := make(chan ConnectionEvent, 2)
	ae, err := NewAE("RADX-SCP", WithACSETimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(ae, VerificationContexts(), observedEcho{ids: make(chan uint64, 1)}, WithConnectionObserver(func(e ConnectionEvent) { events <- e }))
	go func() { _ = srv.ListenAndServe(context.Background(), "127.0.0.1:0") }()
	waitForAddr(t, srv)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	admitted := awaitConnectionEvent(t, events, ConnectionAccepted)
	// The TCP peer sends no A-ASSOCIATE-RQ. No Echo/Store handler is entered,
	// but the failed negotiation must still be observable.
	closed := awaitConnectionEvent(t, events, ConnectionClosed)
	if closed.Err == nil || closed.ConnectionID != admitted.ConnectionID || closed.Operation != ConnectionEstablish || closed.Reason != ConnectionFailed {
		t.Fatalf("missing pre-handler fault: %+v", closed)
	}
}

func TestServerObserverReportsLocalStoreResponseSend(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		name := "local send succeeds"
		if failWrite {
			name = "connection closes after handler entry"
		}
		t.Run(name, func(t *testing.T) {
			// Admission and close may precede an assertion.
			events := make(chan ConnectionEvent, 2)
			responses := make(chan ResponseSendEvent, 1)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			h := &funcStoreHandler{fn: func(ctx context.Context, _ *dicom.DataSet, info OpInfo) Status {
				if ServerConnectionID(ctx) == 0 {
					t.Error("missing handler connection ID")
				}
				close(entered)
				<-release
				return StatusStoreSuccess
			}}
			srv, _ := startServer(t, h, WithConnectionObserver(func(e ConnectionEvent) { events <- e }),
				WithResponseSendObserver(func(e ResponseSendEvent) { responses <- e }))
			scu, err := NewAE("OBSERVER-SCU")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			assoc, err := scu.Associate(ctx, srv.Addr().String(), "RADX-SCP", echoAndStorageContexts())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = assoc.Abort(ctx) }()
			admitted := awaitConnectionEvent(t, events, ConnectionAccepted)
			file, err := dicom.ReadFile(filepath.Join("..", "testdata", "dicom", "liver.dcm"))
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				status, err := assoc.Store(ctx, file.DataSet)
				if err == nil && !status.IsSuccess() {
					err = errors.New("unexpected store status")
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if failWrite {
				// Shutdown closes the connection while the handler is held. Its
				// join cannot finish until release, so this deterministically makes
				// the subsequent response send fail without changing the wire path.
				stopCtx, stop := context.WithTimeout(ctx, 10*time.Millisecond)
				err := srv.Shutdown(stopCtx)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("blocked handler shutdown=%v", err)
				}
			}
			unblock()
			response := awaitResponseSend(t, responses)
			if response.ConnectionID != admitted.ConnectionID || response.MessageIDBeingRespondedTo == 0 ||
				response.PresentationID == 0 || response.StatusCode != 0 ||
				response.CommandField != CommandCStoreRSP || (response.Err != nil) != failWrite {
				t.Fatalf("wrong local response-send result: %+v", response)
			}
			if err := <-result; (err != nil) != failWrite {
				t.Fatalf("Store error=%v, failWrite=%v", err, failWrite)
			}
			if !failWrite {
				if err := assoc.Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
			closed := awaitConnectionEvent(t, events, ConnectionClosed)
			if failWrite && (closed.Err != response.Err || closed.Operation != ConnectionProcess) {
				t.Fatalf("response error identity or operation lost: response=%+v closed=%+v", response, closed)
			}
		})
	}
}

func TestServerObserverDistinguishesReleaseAbortAndEOF(t *testing.T) {
	for _, reason := range []ConnectionCloseReason{ConnectionReleased, ConnectionPeerAborted, ConnectionPeerClosed} {
		t.Run(string(reason), func(t *testing.T) {
			// Admission and close can both precede assertions.
			events := make(chan ConnectionEvent, 2)
			srv, _ := startServer(t, observedEcho{ids: make(chan uint64, 1)}, WithConnectionObserver(func(e ConnectionEvent) { events <- e }))
			scu, err := NewAE("OBSERVER-SCU")
			if err != nil {
				t.Fatal(err)
			}
			assoc, err := scu.Associate(t.Context(), srv.Addr().String(), "RADX-SCP", VerificationContexts())
			if err != nil {
				t.Fatal(err)
			}
			admitted := awaitConnectionEvent(t, events, ConnectionAccepted)
			switch reason {
			case ConnectionReleased:
				err = assoc.Release(t.Context())
			case ConnectionPeerAborted:
				err = assoc.Abort(t.Context())
			case ConnectionPeerClosed:
				err = assoc.requestor.Conn().Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			closed := awaitConnectionEvent(t, events, ConnectionClosed)
			if closed.ConnectionID != admitted.ConnectionID || closed.Reason != reason || closed.Err != nil ||
				closed.Operation != "" || admitted.RemoteAddr == nil || closed.RemoteAddr.String() != admitted.RemoteAddr.String() {
				t.Fatalf("wrong connection termination: admitted=%+v closed=%+v", admitted, closed)
			}
		})
	}
}

func TestServerObserverCancellationAfterSuccessfulResponseHasNoStaleOperation(t *testing.T) {
	ctx, cancelServer := context.WithCancel(t.Context())
	defer cancelServer()
	// The server admits one connection, sends one response, then closes.
	events := make(chan ConnectionEvent, 2)
	responses := make(chan ResponseSendEvent, 1)
	ae, err := NewAE("RADX-SCP")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(ae, VerificationContexts(), &echoOnlyHandler{status: StatusEchoSuccess},
		WithConnectionObserver(func(e ConnectionEvent) { events <- e }),
		WithResponseSendObserver(func(e ResponseSendEvent) {
			responses <- e
			cancelServer()
		}))
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe(ctx, "127.0.0.1:0") }()
	waitForAddr(t, srv)
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(stop)
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	scu, err := NewAE("OBSERVER-SCU")
	if err != nil {
		t.Fatal(err)
	}
	assoc, err := scu.Associate(t.Context(), srv.Addr().String(), "RADX-SCP", VerificationContexts())
	if err != nil {
		t.Fatal(err)
	}
	if status, err := assoc.Echo(t.Context()); err != nil || !status.IsSuccess() {
		t.Fatalf("response changed by observer cancellation: %v,%v", status, err)
	}
	response := awaitResponseSend(t, responses)
	closed := awaitConnectionEvent(t, events, ConnectionClosed)
	if response.Err != nil || closed.Reason != ConnectionServerStopped || closed.Operation != "" || !errors.Is(closed.Err, context.Canceled) {
		t.Fatalf("stale operation after successful response: response=%+v closed=%+v", response, closed)
	}
}
