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

func awaitServerEvent(t *testing.T, events <-chan ServerEvent, kind ServerEventKind) ServerEvent {
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

func TestServerObserverReportsCapacityAndHandlerConnection(t *testing.T) {
	// Buffer the admission/refusal/close sequence so observer callbacks never
	// block behind assertions on the test goroutine.
	events := make(chan ServerEvent, 3)
	ids := make(chan uint64, 1)
	srv, _ := startServer(t, observedEcho{ids: ids}, WithMaxAssociations(1), WithServerObserver(func(e ServerEvent) { events <- e }))
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
	admitted := awaitServerEvent(t, events, ServerConnectionAccepted)
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
	refused := awaitServerEvent(t, events, ServerCapacityRefused)
	if refused.ConnectionID == 0 || refused.ConnectionID == admitted.ConnectionID || refused.Active != 1 || refused.Capacity != 1 || refused.Err != nil {
		t.Fatalf("wrong refusal event: %+v", refused)
	}
	if err := assoc.Release(ctx); err != nil {
		t.Fatal(err)
	}
	closed := awaitServerEvent(t, events, ServerConnectionClosed)
	if closed.ConnectionID != admitted.ConnectionID || closed.Active != 0 || closed.Err != nil {
		t.Fatalf("wrong close event: %+v", closed)
	}
}

func TestServerObserverReportsFaultBeforeHandler(t *testing.T) {
	// Both admission and timeout can occur before the test resumes reading.
	events := make(chan ServerEvent, 2)
	ae, err := NewAE("RADX-SCP", WithACSETimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(ae, VerificationContexts(), observedEcho{ids: make(chan uint64, 1)}, WithServerObserver(func(e ServerEvent) { events <- e }))
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
	admitted := awaitServerEvent(t, events, ServerConnectionAccepted)
	// The TCP peer sends no A-ASSOCIATE-RQ. No Echo/Store handler is entered,
	// but the failed negotiation must still be observable.
	closed := awaitServerEvent(t, events, ServerConnectionClosed)
	if closed.Err == nil || closed.ConnectionID != admitted.ConnectionID || closed.Active != 0 || closed.Stage != ServerStageAssociation {
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
			// Admission, response result and close may all precede an assertion.
			events := make(chan ServerEvent, 3)
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
			srv, _ := startServer(t, h, WithServerObserver(func(e ServerEvent) { events <- e }))
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
			admitted := awaitServerEvent(t, events, ServerConnectionAccepted)
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
			response := awaitServerEvent(t, events, ServerStoreResponse)
			if response.ConnectionID != admitted.ConnectionID || response.MessageID == 0 ||
				response.PresentationID == 0 || response.StatusCode != 0 ||
				response.Stage != ServerStageStoreResponse || (response.Err != nil) != failWrite {
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
			closed := awaitServerEvent(t, events, ServerConnectionClosed)
			if failWrite && (closed.Err != response.Err || closed.Stage != ServerStageStoreResponse) {
				t.Fatalf("response error identity or stage lost: response=%+v closed=%+v", response, closed)
			}
		})
	}
}
