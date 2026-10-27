package dimse

import (
	"context"
	"io"

	"github.com/failed33/go-radx/dicom"
)

// encodedPrefixStop ends the dispatcher's partial decode of an encoded C-STORE dataset right after
// Series Instance UID (0020,000E), so the prefix holds the SOP Common and Patient/Study/Series
// identifying attributes but none of the instance-level bulk that follows.
const encodedPrefixStop = dicom.TagSeriesInstanceUID + 1

// EncodedDataSet is a C-STORE dataset exactly as it arrived on the wire.
type EncodedDataSet struct {
	// Bytes is the dataset PDV payload in arrival order, encoded in the negotiated transfer syntax
	// (OpInfo.TransferSyntax). The dispatcher allocates it per message and never reuses it, so the
	// handler may retain it.
	Bytes []byte
	// Prefix holds the top-level elements decoded from Bytes up to and including Series Instance UID
	// (0020,000E). Later elements were neither decoded nor validated.
	Prefix *dicom.DataSet
}

// EncodedStoreHandler receives a C-STORE dataset without the dispatcher fully decoding it, for an
// SCP that persists instances bit-preserved in their negotiated transfer syntax (pynetdicom's
// event.encoded_dataset, DCMTK storescp). The dispatcher still validates the presentation context
// and the command/dataset SOP Instance UID agreement before calling it. When a handler implements
// both, the SCP dispatcher prefers EncodedStoreHandler over StoreHandler. The C-GET sub-operation
// sink (WithStoreHandler) is unaffected.
type EncodedStoreHandler interface {
	// StoreEncoded receives one dataset (C-STORE). Return StatusStoreSuccess only after persisting it.
	StoreEncoded(ctx context.Context, ds EncodedDataSet, info OpInfo) Status
}

// WriteFileMeta writes the PS3.10 header for a received dataset: a zero preamble, the DICM prefix,
// and the File Meta Information built from the C-STORE (SOP Class and Instance UID, negotiated
// transfer syntax) and implementationClassUID; an empty implementationClassUID writes go-radx's
// identity (dicom.ImplementationClassUID and dicom.ImplementationVersionName). The Part 10 file is
// that header followed by ds.Bytes unchanged; the header is small, so a caller can buffer it to
// learn the file size.
func (ds EncodedDataSet) WriteFileMeta(w io.Writer, info OpInfo, implementationClassUID dicom.UID) error {
	instance, _ := ds.Prefix.GetString(dicom.TagSOPInstanceUID)
	return dicom.WriteFileMeta(w, [128]byte{}, &dicom.FileMeta{
		MediaStorageSOPClassUID:    info.SOPClassUID,
		MediaStorageSOPInstanceUID: dicom.SOPInstanceUID(instance),
		TransferSyntaxUID:          info.TransferSyntax,
		ImplementationClassUID:     implementationClassUID,
	})
}

// encodedStoreAdapter lets serveStoreMessage drive an EncodedStoreHandler, so both store
// capabilities share one validation and response path. Its Store receives the decoded prefix.
type encodedStoreAdapter struct {
	h       EncodedStoreHandler
	encoded []byte
}

func (s encodedStoreAdapter) Store(ctx context.Context, prefix *dicom.DataSet, info OpInfo) Status {
	return s.h.StoreEncoded(ctx, EncodedDataSet{Bytes: s.encoded, Prefix: prefix}, info)
}
