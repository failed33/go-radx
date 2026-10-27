package dicom

import (
	"bytes"
	"testing"
)

func TestWriteFileMetaRoundTrip(t *testing.T) {
	meta := &FileMeta{
		MediaStorageSOPClassUID:    "1.2.840.10008.5.1.4.1.1.7",
		MediaStorageSOPInstanceUID: "1.2.3.4.5.6.7.8.9",
		TransferSyntaxUID:          ExplicitVRLittleEndian,
		ImplementationClassUID:     "1.2.3.4.5",
	}
	var buf bytes.Buffer
	if err := WriteFileMeta(&buf, [128]byte{}, meta); err != nil {
		t.Fatalf("WriteFileMeta: %v", err)
	}

	br := newBoundedReader(bytes.NewReader(buf.Bytes()), defaultMaxElementLen)
	if _, err := readPreamble(br); err != nil {
		t.Fatalf("readPreamble: %v", err)
	}
	got, err := readFileMeta(br)
	if err != nil {
		t.Fatalf("readFileMeta: %v", err)
	}
	if got.TransferSyntaxUID != meta.TransferSyntaxUID {
		t.Errorf("TransferSyntaxUID = %q, want %q", got.TransferSyntaxUID, meta.TransferSyntaxUID)
	}
	if got.MediaStorageSOPClassUID != meta.MediaStorageSOPClassUID {
		t.Errorf("MediaStorageSOPClassUID = %q, want %q", got.MediaStorageSOPClassUID, meta.MediaStorageSOPClassUID)
	}
	if got.MediaStorageSOPInstanceUID != meta.MediaStorageSOPInstanceUID {
		t.Errorf("MediaStorageSOPInstanceUID = %q, want %q", got.MediaStorageSOPInstanceUID, meta.MediaStorageSOPInstanceUID)
	}
}

// DCM-001: the written (0002,0000) group length equals the exact byte count of the
// group-0002 elements that follow it. Read it back and verify against the actual
// trailing bytes.
func TestWriteFileMetaGroupLengthIsExact(t *testing.T) {
	meta := &FileMeta{
		MediaStorageSOPClassUID:    "1.2.840.10008.5.1.4.1.1.7",
		MediaStorageSOPInstanceUID: "1.2.3.4.5.6.7.8.9",
		TransferSyntaxUID:          ExplicitVRLittleEndian,
		ImplementationClassUID:     "1.2.3.4.5",
	}
	var buf bytes.Buffer
	if err := WriteFileMeta(&buf, [128]byte{}, meta); err != nil {
		t.Fatalf("WriteFileMeta: %v", err)
	}
	raw := buf.Bytes()

	// Parse the (0002,0000) group length value directly.
	br := newBoundedReader(bytes.NewReader(raw), defaultMaxElementLen)
	if _, err := readPreamble(br); err != nil {
		t.Fatalf("readPreamble: %v", err)
	}
	h, err := readElementHeader(br, ExplicitVRLittleEndian)
	if err != nil {
		t.Fatalf("read group-length header: %v", err)
	}
	if h.tag != tagFileMetaGroupLength {
		t.Fatalf("first element is %s, want %s (group length written first)", h.tag, tagFileMetaGroupLength)
	}
	v, err := decodeValue(br, h, encodingFor(ExplicitVRLittleEndian), nil)
	if err != nil {
		t.Fatalf("decode group length: %v", err)
	}
	declared := v.(*Ints).Ints()[0]

	// Bytes after the group-length element through end of buffer (= the group-0002
	// elements) is exactly offset-consumed-so-far to len(raw).
	actualTrailing := int64(len(raw)) - br.offset()
	if declared != actualTrailing {
		t.Errorf("declared group length %d != actual trailing group-0002 bytes %d (DCM-001)", declared, actualTrailing)
	}
}

// An existing (0002,0000) in the FileMeta.Elements must be ignored and recomputed,
// never trusted or double-written.
func TestWriteFileMetaRecomputesStaleGroupLength(t *testing.T) {
	elems := NewDataSet()
	elems.Set(Element{Tag: tagFileMetaGroupLength, VR: VRUL, Value: NewInts(VRUL, 999999)}) // stale
	elems.Set(Element{Tag: tagMediaStorageSOPClass, VR: VRUI, Value: NewStrings(VRUI, "1.2.840.10008.5.1.4.1.1.7")})
	meta := &FileMeta{
		MediaStorageSOPClassUID: "1.2.840.10008.5.1.4.1.1.7",
		TransferSyntaxUID:       ExplicitVRLittleEndian,
		Elements:                elems,
	}
	var buf bytes.Buffer
	if err := WriteFileMeta(&buf, [128]byte{}, meta); err != nil {
		t.Fatalf("WriteFileMeta: %v", err)
	}
	br := newBoundedReader(bytes.NewReader(buf.Bytes()), defaultMaxElementLen)
	if _, err := readPreamble(br); err != nil {
		t.Fatalf("readPreamble: %v", err)
	}
	h, _ := readElementHeader(br, ExplicitVRLittleEndian)
	v, _ := decodeValue(br, h, encodingFor(ExplicitVRLittleEndian), nil)
	if got := v.(*Ints).Ints()[0]; got == 999999 {
		t.Error("stale group length 999999 was trusted instead of recomputed (DCM-001)")
	}
}

// (0002,0012) is Type 1 and must identify a real implementation (PS3.10 §7.1): an unset
// Implementation Class UID writes go-radx's identity, and (0002,0013) accompanies only that
// default, never a caller's UID or in place of a caller's version name.
func TestWriteFileMetaImplementationIdentity(t *testing.T) {
	if _, err := ParseUID(string(ImplementationClassUID)); err != nil || ImplementationClassUID == "2.25.0" {
		t.Fatalf("ImplementationClassUID %q is not a usable UID: %v", ImplementationClassUID, err)
	}
	if n := len(ImplementationVersionName); n == 0 || n > 16 {
		t.Fatalf("ImplementationVersionName %q has length %d, want 1..16 (SH)", ImplementationVersionName, n)
	}
	callerVersion := NewDataSet()
	callerVersion.Set(Element{Tag: TagImplementationVersionName, VR: VRSH, Value: NewStrings(VRSH, "OTHER_1")})

	tests := []struct {
		name        string
		class       UID
		elements    *DataSet
		wantClass   UID
		wantVersion string // "" = (0002,0013) absent
	}{
		{"unset", "", nil, ImplementationClassUID, ImplementationVersionName},
		{"caller UID", "1.2.3.4.5", nil, "1.2.3.4.5", ""},
		{"caller version name", "", callerVersion, ImplementationClassUID, "OTHER_1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &FileMeta{
				MediaStorageSOPClassUID:    "1.2.840.10008.5.1.4.1.1.7",
				MediaStorageSOPInstanceUID: "1.2.3.4.5.6.7.8.9",
				TransferSyntaxUID:          ExplicitVRLittleEndian,
				ImplementationClassUID:     tt.class,
				Elements:                   tt.elements,
			}
			var buf bytes.Buffer
			if err := WriteFileMeta(&buf, [128]byte{}, meta); err != nil {
				t.Fatalf("WriteFileMeta: %v", err)
			}
			br := newBoundedReader(bytes.NewReader(buf.Bytes()), defaultMaxElementLen)
			if _, err := readPreamble(br); err != nil {
				t.Fatalf("readPreamble: %v", err)
			}
			got, err := readFileMeta(br)
			if err != nil {
				t.Fatalf("readFileMeta: %v", err)
			}
			if got.ImplementationClassUID != tt.wantClass {
				t.Errorf("(0002,0012) = %q, want %q", got.ImplementationClassUID, tt.wantClass)
			}
			version, ok := got.Elements.GetString(TagImplementationVersionName)
			if tt.wantVersion == "" && ok {
				t.Errorf("(0002,0013) = %q, want absent", version)
			}
			if tt.wantVersion != "" && version != tt.wantVersion {
				t.Errorf("(0002,0013) = %q (present %v), want %q", version, ok, tt.wantVersion)
			}
		})
	}
}
