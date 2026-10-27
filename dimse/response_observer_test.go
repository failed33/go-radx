package dimse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/failed33/go-radx/dicom"
)

func TestServerResponseObserverCoversEveryUnsupportedReply(t *testing.T) {
	responses := make(chan ResponseSendEvent, 1)
	srv, _ := startServer(t, struct{}{}, WithResponseSendObserver(func(e ResponseSendEvent) { responses <- e }))
	scu, err := NewAE("OBSERVER-SCU")
	if err != nil {
		t.Fatal(err)
	}
	assoc, err := scu.Associate(t.Context(), srv.Addr().String(), "RADX-SCP", VerificationContexts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = assoc.Release(t.Context()) }()
	for i, field := range []CommandField{
		CommandCEchoRQ, CommandCStoreRQ, CommandCFindRQ, CommandCMoveRQ, CommandCGetRQ,
		CommandNCreateRQ, CommandNSetRQ, CommandNActionRQ, CommandNEventReportRQ, CommandNGetRQ, CommandNDeleteRQ,
	} {
		t.Run(fmt.Sprintf("0x%04X", field), func(t *testing.T) {
			cmd := CommandSet{
				CommandField: field, MessageID: uint16(i + 1),
				AffectedSOPClassUID: dicom.UID(verificationSOPClass), RequestedSOPClassUID: dicom.UID(verificationSOPClass),
				AffectedSOPInstanceUID: "1.2.3", RequestedSOPInstanceUID: "1.2.3",
				CommandDataSetType: CommandDataSetNotPresent,
			}
			if err := sendCommand(t.Context(), assoc.requestor.Conn(), assoc.requestor.Machine(), 1, cmd); err != nil {
				t.Fatal(err)
			}
			rsp, _, err := receiveCommand(t.Context(), assoc.requestor.Conn(), assoc.requestor.Machine())
			if err != nil {
				t.Fatal(err)
			}
			event := awaitResponseSend(t, responses)
			if event.ConnectionID == 0 || event.CommandField != rsp.CommandField ||
				event.CommandField != field|CommandField(commandResponseBit) || event.MessageIDBeingRespondedTo != cmd.MessageID ||
				event.PresentationID != 1 || event.StatusCode != StatusSOPClassNotSupported.Code || event.Err != nil {
				t.Fatalf("wrong unsupported-response event: %+v, wire=%+v", event, rsp)
			}
		})
	}
}

func TestResponseObserverIncludesPendingFinalAndNormalizedDataSets(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		name := "C-FIND pending and final"
		if normalized {
			name = "N-GET attributes"
		}
		t.Run(name, func(t *testing.T) {
			ctx, req, acc := messageTestAssociation(t)
			// C-FIND emits pending and final before assertions resume.
			events := make(chan ResponseSendEvent, 2)
			ctx = context.WithValue(ctx, serverConnectionContextKey{}, &serverObservation{id: 17, conn: acc.Conn(), response: func(e ResponseSendEvent) { events <- e }})
			attrs := dicom.NewDataSet()
			attrs.SetString(dicom.TagManufacturer, "RADX")
			count := 2
			field := CommandCFindRSP
			if normalized {
				count, field = 1, CommandNGetRSP
			}
			done := make(chan error, 1)
			go func() {
				for i := range count {
					cmd, ds, _, err := receiveMessage(ctx, req.Conn(), req.Machine(), newMessageReassembler(dicom.ImplicitVRLittleEndian))
					if err != nil {
						done <- err
						return
					}
					if cmd.CommandField != field || (i == 0 && ds == nil) || (i > 0 && ds != nil) {
						done <- errors.New("wrong response family or dataset presence")
						return
					}
					if i == 0 {
						if value, _ := ds.GetString(dicom.TagManufacturer); value != "RADX" {
							done <- errors.New("response dataset was not fully delivered")
							return
						}
					}
				}
				done <- nil
			}()
			cmd := CommandSet{MessageID: 7, AffectedSOPClassUID: dicom.UID(verificationSOPClass), RequestedSOPClassUID: dicom.UID(verificationSOPClass)}
			if normalized {
				err := serveNGetMessage(ctx, acc, &nServiceHandler{getStatus: StatusNSuccess, getAttrs: attrs}, cmd, nil, 1, OpInfo{})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err := sendFindResponse(ctx, acc, cmd, 1, StatusFindPending, attrs); err != nil {
					t.Fatal(err)
				}
				if err := sendFindResponse(ctx, acc, cmd, 1, StatusFindSuccess, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			for i := range count {
				event := awaitResponseSend(t, events)
				wantStatus := uint16(0)
				if !normalized && i == 0 {
					wantStatus = StatusFindPending.Code
				}
				if event.ConnectionID != 17 || event.CommandField != field || event.MessageIDBeingRespondedTo != 7 || event.StatusCode != wantStatus || event.Err != nil {
					t.Fatalf("wrong response completion: %+v", event)
				}
			}
		})
	}
}

func TestResponseObserverRetainsFailureAfterCommandBeforeDataSet(t *testing.T) {
	ctx, req, acc := messageTestAssociation(t)
	events := make(chan ResponseSendEvent, 1)
	ctx = context.WithValue(ctx, serverConnectionContextKey{}, &serverObservation{id: 17, conn: acc.Conn(), response: func(e ResponseSendEvent) { events <- e }})
	ds := dicom.NewDataSet()
	ds.Set(dicom.Element{Tag: dicom.TagPixelData, VR: dicom.VROB, Value: dicom.NewBytes(dicom.VROB, bytes.Repeat([]byte{1}, 32768))})
	readHeader := make(chan error, 1)
	go func() {
		_, err := req.Conn().ReadPDU(ctx)
		if err == nil {
			// The command has arrived, but none of the response dataset has.
			select {
			case <-events:
				err = errors.New("response completion emitted before dataset send")
			default:
			}
		}
		_ = req.Conn().Close()
		readHeader <- err
	}()
	cmd := CommandSet{CommandField: CommandNGetRSP, MessageIDBeingRespondedTo: 9, HasStatus: true, CommandDataSetType: CommandDataSetPresent}
	err := sendMessage(ctx, acc.Conn(), acc.Machine(), 1, cmd, ds, dicom.ImplicitVRLittleEndian, 16382)
	if headerErr := <-readHeader; headerErr != nil {
		t.Fatal(headerErr)
	}
	event := awaitResponseSend(t, events)
	if err == nil || event.Err != err || event.CommandField != CommandNGetRSP || event.MessageIDBeingRespondedTo != 9 {
		t.Fatalf("dataset failure or original error identity lost: event=%+v err=%v", event, err)
	}
}

func TestResponseObserverRetainsDataSetEncodingFailure(t *testing.T) {
	ctx, _, acc := messageTestAssociation(t)
	var event ResponseSendEvent
	ctx = context.WithValue(ctx, serverConnectionContextKey{}, &serverObservation{id: 17, conn: acc.Conn(), response: func(e ResponseSendEvent) { event = e }})
	cmd := CommandSet{CommandField: CommandNGetRSP, MessageIDBeingRespondedTo: 9, HasStatus: true, CommandDataSetType: CommandDataSetPresent}
	// Dataset encoding fails before transport I/O; the command itself is valid.
	err := sendMessage(ctx, acc.Conn(), acc.Machine(), 1, cmd, dicom.NewDataSet(), "unsupported", 16382)
	if err == nil || event.Err != err || event.ConnectionID != 17 || event.CommandField != CommandNGetRSP {
		t.Fatalf("encoding failure or original error identity lost: event=%+v err=%v", event, err)
	}
}

func TestResponseObserverExcludesRequestsAndOtherConnections(t *testing.T) {
	ctx, req, acc := messageTestAssociation(t)
	observed := 0
	ctx = context.WithValue(ctx, serverConnectionContextKey{}, &serverObservation{id: 17, conn: acc.Conn(), response: func(ResponseSendEvent) { observed++ }})
	// A handler context may be inherited by a C-MOVE destination connection.
	// Even a response on that other connection must not inherit its origin ID.
	for _, tc := range []struct {
		field CommandField
		other bool
	}{{CommandCEchoRQ, false}, {CommandCEchoRSP, true}} {
		sender, receiver := acc.Conn(), req.Conn()
		machine := acc.Machine()
		if tc.other {
			sender, receiver, machine = req.Conn(), acc.Conn(), req.Machine()
		}
		done := make(chan error, 1)
		go func() { _, err := receiver.ReadPDU(ctx); done <- err }()
		if err := sendCommand(ctx, sender, machine, 1, CommandSet{CommandField: tc.field, CommandDataSetType: CommandDataSetNotPresent}); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if observed != 0 {
		t.Fatalf("observed %d requests or unrelated sends", observed)
	}
}
