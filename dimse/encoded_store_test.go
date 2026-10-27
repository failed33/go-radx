package dimse

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/failed33/go-radx/dicom"
)

// encodedStoreRecorder implements ONLY EncodedStoreHandler and records what it received.
type encodedStoreRecorder struct {
	mu    sync.Mutex
	calls int
	ds    EncodedDataSet
	info  OpInfo
}

func (h *encodedStoreRecorder) StoreEncoded(_ context.Context, ds EncodedDataSet, info OpInfo) Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.ds = ds
	h.info = info
	return StatusStoreSuccess
}

func readLiver(t *testing.T) *dicom.DataSet {
	t.Helper()
	src, err := dicom.ReadFile(filepath.Join("..", "testdata", "dicom", "liver.dcm"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return src.DataSet
}

func associateWithServer(t *testing.T, srv *Server) (context.Context, *Association) {
	t.Helper()
	scu, err := NewAE(AETitle("RADX-SCU"))
	if err != nil {
		t.Fatalf("NewAE: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	assoc, err := scu.Associate(ctx, srv.Addr().String(), AETitle("RADX-SCP"), echoAndStorageContexts())
	if err != nil {
		t.Fatalf("Associate: %v", err)
	}
	return ctx, assoc
}

// TestServerEncodedStoreIsBitPreserving proves an EncodedStoreHandler receives the dataset bytes
// exactly as the SCU encoded them across many PDVs, a prefix decoded only through Series Instance
// UID, and that WriteFileMeta plus those bytes is a Part 10 file whose main dataset is them verbatim.
func TestServerEncodedStoreIsBitPreserving(t *testing.T) {
	sent := readLiver(t)
	h := &encodedStoreRecorder{}
	srv, _ := startServer(t, h)
	ctx, assoc := associateWithServer(t, srv)

	status, err := assoc.Store(ctx, sent)
	if err != nil {
		t.Fatalf("Store transport error: %v", err)
	}
	if !status.IsSuccess() {
		t.Fatalf("Store status = %s, want success", status)
	}
	if err := assoc.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.calls != 1 {
		t.Fatalf("StoreEncoded calls = %d, want 1", h.calls)
	}
	var wire bytes.Buffer
	if err := dicom.EncodeDataSet(&wire, sent, h.info.TransferSyntax); err != nil {
		t.Fatalf("EncodeDataSet: %v", err)
	}
	if wire.Len() <= int(defaultMaxPDULength) {
		t.Fatalf("fixture dataset is %d bytes; the test needs one spanning several PDVs", wire.Len())
	}
	if !bytes.Equal(h.ds.Bytes, wire.Bytes()) {
		t.Fatalf("received %d dataset bytes, want the %d bytes the SCU encoded, unchanged", len(h.ds.Bytes), wire.Len())
	}

	wantSeries, _ := sent.GetString(dicom.TagSeriesInstanceUID)
	if got, _ := h.ds.Prefix.GetString(dicom.TagSeriesInstanceUID); got != wantSeries {
		t.Errorf("prefix Series Instance UID = %q, want %q", got, wantSeries)
	}
	if _, ok := h.ds.Prefix.Get(dicom.TagPixelData); ok {
		t.Error("prefix decoded Pixel Data; the partial decode must stop after Series Instance UID")
	}

	const implementationUID = dicom.UID("2.25.329800735698586629295641978511506172918")
	var header bytes.Buffer
	if err := h.ds.WriteFileMeta(&header, h.info, implementationUID); err != nil {
		t.Fatalf("WriteFileMeta: %v", err)
	}
	headerLen := header.Len()
	header.Write(h.ds.Bytes)
	part10 := header.Bytes()
	if !bytes.Equal(part10[headerLen:], wire.Bytes()) {
		t.Fatal("Part 10 bytes after the file meta header differ from the received dataset bytes")
	}

	file, err := dicom.Read(bytes.NewReader(part10))
	if err != nil {
		t.Fatalf("Read Part 10: %v", err)
	}
	wantInstance, _ := sent.GetString(dicom.TagSOPInstanceUID)
	if file.Meta.MediaStorageSOPClassUID != h.info.SOPClassUID ||
		string(file.Meta.MediaStorageSOPInstanceUID) != wantInstance ||
		file.Meta.TransferSyntaxUID != h.info.TransferSyntax ||
		file.Meta.ImplementationClassUID != implementationUID {
		t.Errorf("file meta = %+v, want the C-STORE identity, negotiated syntax and implementation UID", file.Meta)
	}
	var defaultHeader bytes.Buffer
	if err := h.ds.WriteFileMeta(&defaultHeader, h.info, ""); err != nil {
		t.Fatalf("WriteFileMeta (default UID): %v", err)
	}
	defaultFile, err := dicom.Read(bytes.NewReader(append(defaultHeader.Bytes(), h.ds.Bytes...)))
	if err != nil {
		t.Fatalf("Read Part 10 (default UID): %v", err)
	}
	if got := defaultFile.Meta.ImplementationClassUID; got != dicom.ImplementationClassUID {
		t.Errorf("empty implementation UID wrote (0002,0012) = %q, want %q", got, dicom.ImplementationClassUID)
	}
	var reencoded bytes.Buffer
	if err := dicom.EncodeDataSet(&reencoded, file.DataSet, file.Meta.TransferSyntaxUID); err != nil {
		t.Fatalf("EncodeDataSet(read): %v", err)
	}
	if !bytes.Equal(reencoded.Bytes(), wire.Bytes()) {
		t.Error("main dataset read back from the Part 10 file does not match the stored instance")
	}
}

// TestServerEncodedStoreKeepsMandatoryValidation proves the encoded path still fails closed on a
// command/context or command/dataset identity mismatch: the association faults and the handler is
// never called.
func TestServerEncodedStoreKeepsMandatoryValidation(t *testing.T) {
	sent := readLiver(t)
	sopClass, _ := sent.GetString(dicom.TagSOPClassUID)
	sopInstance, _ := sent.GetString(dicom.TagSOPInstanceUID)

	tests := []struct {
		name     string
		sopClass dicom.UID
		instance dicom.UID
	}{
		{name: "instance mismatch", sopClass: dicom.UID(sopClass), instance: "1.2.3.999"},
		{name: "context mismatch", sopClass: dicom.UID(verificationSOPClass), instance: dicom.UID(sopInstance)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &encodedStoreRecorder{}
			srv, _ := startServer(t, h)
			ctx, assoc := associateWithServer(t, srv)

			pcID, ts, ok := assoc.contextForStorage(dicom.SOPClassUID(sopClass))
			if !ok {
				t.Fatal("no accepted storage context for the fixture SOP Class")
			}
			rq := CommandSet{
				CommandField:           CommandCStoreRQ,
				MessageID:              storeMessageID,
				AffectedSOPClassUID:    tt.sopClass,
				AffectedSOPInstanceUID: tt.instance,
				HasPriority:            true,
				Priority:               PriorityMedium,
				CommandDataSetType:     CommandDataSetPresent,
			}
			conn, m := assoc.requestor.Conn(), assoc.requestor.Machine()
			if err := sendMessage(ctx, conn, m, pcID, rq, sent, ts, assoc.sendCap()); err != nil {
				t.Fatalf("sendMessage: %v", err)
			}
			if _, _, err := receiveCommand(ctx, conn, m); err == nil {
				t.Fatal("SCP answered a mismatched C-STORE-RQ; want the association to fault")
			}

			h.mu.Lock()
			defer h.mu.Unlock()
			if h.calls != 0 {
				t.Errorf("StoreEncoded calls = %d, want 0", h.calls)
			}
		})
	}
}
