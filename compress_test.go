// Copyright 2026 The go-erofs Authors.
//
// SPDX-License-Identifier: Apache-2.0

package erofs

import (
	"bytes"
	"compress/flate"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz/lzma"
)

// compressedSample is a payload that compresses well and spans several
// clusters, so partial and full decodes exercise the same input shape.
func compressedSample() []byte {
	return bytes.Repeat([]byte("go-erofs compressed cluster payload\n"), 512)
}

// encodeDeflate returns a raw DEFLATE stream (no zlib/gzip wrapper), matching
// what EROFS stores.
func encodeDeflate(t *testing.T, source []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer, err := flate.NewWriter(&buffer, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(source); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// encodeZstd returns a standard Zstandard frame, matching what EROFS stores.
func encodeZstd(t *testing.T, source []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(source); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// encodeLZ4 returns a raw LZ4 block (no frame), matching what EROFS stores.
func encodeLZ4(t *testing.T, source []byte) []byte {
	t.Helper()
	buffer := make([]byte, lz4.CompressBlockBound(len(source)))
	written, err := lz4.CompressBlock(source, buffer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if written == 0 {
		t.Skip("lz4 did not compress the sample")
	}
	return buffer[:written]
}

// encodeMicroLZMA returns a microLZMA stream: the inverted LZMA1 properties
// byte followed by the range-coder stream without the classic .lzma header.
func encodeMicroLZMA(t *testing.T, source []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer, err := lzma.NewWriter(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(source); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	alone := buffer.Bytes()
	// alone: 1 byte properties, 4 bytes dictionary size, 8 bytes size,
	// then a dummy zero byte and the range coder stream.
	stream := alone[lzma.HeaderLen+1:]
	micro := make([]byte, 0, len(stream)+1)
	micro = append(micro, ^alone[0])
	micro = append(micro, stream...)
	return micro
}

// TestCompressionAlgorithmNames checks the algorithm ID mapping.
func TestCompressionAlgorithmNames(t *testing.T) {
	cases := []struct {
		id       uint8
		expected string
	}{
		{0, "lz4"},
		{1, "lzma"},
		{2, "deflate"},
		{3, "zstd"},
	}
	for _, testCase := range cases {
		algorithm, err := compressionAlgorithmFromID(testCase.id)
		if err != nil {
			t.Fatalf("compressionAlgorithmFromID(%d): %v", testCase.id, err)
		}
		if got := algorithm.String(); got != testCase.expected {
			t.Errorf("compressionAlgorithmFromID(%d).String() = %q, want %q", testCase.id, got, testCase.expected)
		}
	}
	if _, err := compressionAlgorithmFromID(4); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("compressionAlgorithmFromID(4) error = %v, want ErrNotImplemented", err)
	}
}

// TestTrimLeadingZeros checks the EROFS right-alignment padding removal.
func TestTrimLeadingZeros(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []byte
	}{
		{nil, nil},
		{[]byte{}, []byte{}},
		{[]byte{0, 0, 0, 1, 2}, []byte{1, 2}},
		{[]byte{1, 0, 2}, []byte{1, 0, 2}},
		{[]byte{0, 0}, []byte{}},
	}
	for _, testCase := range cases {
		if got := trimLeadingZeros(testCase.input); !bytes.Equal(got, testCase.expected) {
			t.Errorf("trimLeadingZeros(%v) = %v, want %v", testCase.input, got, testCase.expected)
		}
	}
}

// TestDecodeCompressionCluster round-trips every codec through the decoder.
func TestDecodeCompressionCluster(t *testing.T) {
	source := compressedSample()
	config := defaultLZMAConfig()
	cases := []struct {
		name      string
		algorithm compressionAlgorithm
		input     []byte
	}{
		{"lz4", compressionLZ4, encodeLZ4(t, source)},
		{"lzma", compressionLZMA, encodeMicroLZMA(t, source)},
		{"deflate", compressionDeflate, encodeDeflate(t, source)},
		{"zstd", compressionZstd, encodeZstd(t, source)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// Simulate EROFS right-alignment with leading zero padding.
			input := append([]byte{0, 0, 0}, testCase.input...)
			output := make([]byte, len(source))
			if err := decodeCompressionCluster(testCase.algorithm, config, input, output, false); err != nil {
				t.Fatalf("decodeCompressionCluster: %v", err)
			}
			if !bytes.Equal(output, source) {
				t.Fatalf("decoded content mismatch")
			}
			// A zero-sized destination is a no-op.
			if err := decodeCompressionCluster(testCase.algorithm, config, input, nil, false); err != nil {
				t.Fatalf("empty decode: %v", err)
			}
		})
	}
}

// TestDecodeCompressionClusterErrors checks the failure paths.
func TestDecodeCompressionClusterErrors(t *testing.T) {
	config := defaultLZMAConfig()
	output := make([]byte, 64)
	if err := decodeCompressionCluster(compressionLZ4, config, []byte{0, 0, 0}, output, false); !errors.Is(err, ErrInvalid) {
		t.Errorf("all-zero input error = %v, want ErrInvalid", err)
	}
	if err := decodeCompressionCluster(compressionLZMA, config, []byte{0x5d, 0x00}, output, false); !errors.Is(err, ErrInvalid) {
		t.Errorf("short lzma error = %v, want ErrInvalid", err)
	}
	if err := decodeCompressionCluster(compressionAlgorithm(9), config, []byte{1}, output, false); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("unknown algorithm error = %v, want ErrNotImplemented", err)
	}
	if err := decodeCompressionCluster(compressionDeflate, config, []byte{0xff}, output, false); err == nil {
		t.Error("invalid deflate input decoded without error")
	}
}

// TestDecodeCompressionClusterPartial decodes only a prefix of a pcluster, as
// required by Z_EROFS_LI_PARTIAL_REF references (image deduplication).
func TestDecodeCompressionClusterPartial(t *testing.T) {
	source := compressedSample()
	const prefix = 1000
	cases := []struct {
		name      string
		algorithm compressionAlgorithm
		input     []byte
	}{
		{"lz4", compressionLZ4, encodeLZ4(t, source)},
		{"lzma", compressionLZMA, encodeMicroLZMA(t, source)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			output := make([]byte, prefix)
			if err := decodeCompressionCluster(testCase.algorithm, defaultLZMAConfig(), testCase.input, output, true); err != nil {
				t.Fatalf("partial decode: %v", err)
			}
			if !bytes.Equal(output, source[:prefix]) {
				t.Fatalf("partial decoded content mismatch")
			}
		})
	}
}
