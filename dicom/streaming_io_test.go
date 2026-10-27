package dicom

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

type outputObservedValue struct{ started *bool }

func (*outputObservedValue) VR() VR { return VROB }
func (v *outputObservedValue) EncodedLen(binary.ByteOrder) uint32 {
	if !*v.started {
		return 2 // Unknown values encode zero bytes, so this fails length validation.
	}
	return 0
}

type observedOutput struct{ started bool }

func (w *observedOutput) Write(p []byte) (int, error) {
	w.started = true
	return len(p), nil
}

func TestWriteStartsOutputBeforeEncodingMainDataSet(t *testing.T) {
	for _, ts := range []TransferSyntax{ExplicitVRLittleEndian, DeflatedExplicitVRLittleEndian} {
		t.Run(ts.Name(), func(t *testing.T) {
			out := &observedOutput{}
			file := sampleFile(ts)
			file.DataSet.Set(Element{Tag: NewTag(0x7777, 0x1010), VR: VROB, Value: &outputObservedValue{started: &out.started}})
			if err := Write(out, file); err != nil {
				t.Fatalf("main dataset was encoded before output started: %v", err)
			}
		})
	}
}

func TestWriteReportsLateDataSetFailure(t *testing.T) {
	file := sampleFile(ExplicitVRLittleEndian)
	file.DataSet.Set(Element{Tag: NewTag(0x7777, 0x1010), VR: VROB})
	var out bytes.Buffer
	err := Write(&out, file)
	var invalid *ValueError
	if !errors.As(err, &invalid) || out.Len() == 0 {
		t.Fatalf("streaming write must report the late error despite partial output: bytes=%d error=%v", out.Len(), err)
	}
}

func TestWriteMatchesBufferedPart10(t *testing.T) {
	files := []*File{
		sampleFile(ImplicitVRLittleEndian), sampleFile(ExplicitVRLittleEndian),
		sampleFile(ExplicitVRBigEndian), sampleFile(DeflatedExplicitVRLittleEndian),
	}
	pixels := make([]byte, 128<<10)
	for i := range pixels {
		pixels[i] = byte(i % 251)
	}
	for _, file := range files {
		file.DataSet.Set(Element{Tag: TagPixelData, VR: VROB, Value: NewBytes(VROB, pixels)})
	}
	encapsulated, err := ReadFile(filepath.Join("..", "testdata", "dicom", "liver_rle.dcm"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, encapsulated)
	for _, file := range files {
		t.Run(file.Meta.TransferSyntaxUID.Name(), func(t *testing.T) {
			var main, previous, actual bytes.Buffer
			if err := writeDataSet(&main, file.DataSet, file.Meta.TransferSyntaxUID); err != nil {
				t.Fatal(err)
			}
			if err := WriteFileMeta(&previous, file.Preamble, file.Meta); err != nil {
				t.Fatal(err)
			}
			if file.Meta.TransferSyntaxUID.IsDeflated() {
				compressed, err := flate.NewWriter(&previous, flate.DefaultCompression)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := compressed.Write(main.Bytes()); err != nil {
					t.Fatal(err)
				}
				if err := compressed.Close(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := previous.Write(main.Bytes()); err != nil {
				t.Fatal(err)
			}
			if err := Write(&actual, file); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual.Bytes(), previous.Bytes()) {
				t.Fatal("streaming output differs from the buffered Part 10 encoding")
			}
		})
	}
}

type failingOutput struct {
	remaining int
	err       error
}

func (w *failingOutput) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	w.remaining -= n
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}

func TestWritePreservesLateSinkError(t *testing.T) {
	for _, ts := range []TransferSyntax{ExplicitVRLittleEndian, DeflatedExplicitVRLittleEndian} {
		t.Run(ts.Name(), func(t *testing.T) {
			file := sampleFile(ts)
			var header bytes.Buffer
			if err := WriteFileMeta(&header, file.Preamble, file.Meta); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("synthetic sink failure")
			out := &failingOutput{remaining: header.Len() + 8, err: failure}
			if err := Write(out, file); !errors.Is(err, failure) {
				t.Fatalf("late sink failure lost its cause: %v", err)
			}
		})
	}
}

func TestBinaryCodecAvoidsRedundantOwnedCopies(t *testing.T) {
	for _, size := range []int{128 << 10, 256 << 10} {
		t.Run(fmt.Sprintf("%dKiB", size>>10), func(t *testing.T) {
			wire := make([]byte, size)
			value := NewBytes(VROB, wire).(*Bytes)
			encoded := testing.Benchmark(func(b *testing.B) {
				for b.Loop() {
					if n, err := encodeValue(io.Discard, value, encodingFor(ExplicitVRLittleEndian)); err != nil || n != uint32(size) {
						b.Fatalf("binary encode: bytes=%d error=%v", n, err)
					}
				}
			})
			if n := encoded.AllocedBytesPerOp(); n > int64(size/2) {
				t.Errorf("internal encoding copied an owned value: %d B/op for %d bytes", n, size)
			}
			decoded := testing.Benchmark(func(b *testing.B) {
				for b.Loop() {
					reader := newBoundedReader(bytes.NewReader(wire), defaultMaxElementLen)
					value, err := decodeValue(reader, elementHeader{vr: VROB, length: uint32(size)}, encodingFor(ExplicitVRLittleEndian), nil)
					if err != nil || value.EncodedLen(binary.LittleEndian) != uint32(size) {
						b.Fatalf("binary decode: %v", err)
					}
				}
			})
			if n := decoded.AllocedBytesPerOp(); n > int64(size+size/2) {
				t.Errorf("internal decoding copied its newly owned read buffer: %d B/op for %d bytes", n, size)
			}
		})
	}
}

func TestBinaryOwnershipStaysPrivateAtPublicBoundaries(t *testing.T) {
	source := []byte{1, 2, 3, 4}
	constructed := NewBytes(VROB, source).(*Bytes)
	source[0] = 9
	if constructed.Bytes()[0] != 1 {
		t.Fatal("NewBytes retained the caller's mutable input")
	}
	reader := newBoundedReader(bytes.NewReader(source), defaultMaxElementLen)
	decoded, err := decodeValue(reader, elementHeader{vr: VROB, length: uint32(len(source))}, encodingFor(ExplicitVRLittleEndian), nil)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 8
	returned := decoded.(*Bytes).Bytes()
	returned[0] = 7
	if decoded.(*Bytes).Bytes()[0] != 9 {
		t.Fatal("decoded bytes alias the reader's source or a public getter result")
	}
}
