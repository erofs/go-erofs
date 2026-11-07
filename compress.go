// Copyright 2026 The go-erofs Authors.
//
// The compressed-cluster decoding logic in this file is derived from the
// MIT-licensed am-fs-erofs project (https://github.com/antimatter-studios/rust-fs-erofs),
// Copyright (c) 2026 Antimatter Studios. See that project's LICENSE for the
// full MIT license text.

package erofs

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz/lzma"

	"github.com/erofs/go-erofs/internal/disk"
)

// compressionAlgorithm is an EROFS compression algorithm ID as stored in
// z_erofs_map_header.h_algorithmtype (a low nibble for HEAD1 lclusters and a
// high nibble for HEAD2 lclusters).
type compressionAlgorithm uint8

const (
	compressionLZ4     compressionAlgorithm = disk.CompressionLZ4
	compressionLZMA    compressionAlgorithm = disk.CompressionLZMA
	compressionDeflate compressionAlgorithm = disk.CompressionDeflate
	compressionZstd    compressionAlgorithm = disk.CompressionZstd
)

func (a compressionAlgorithm) String() string {
	switch a {
	case compressionLZ4:
		return "lz4"
	case compressionLZMA:
		return "lzma"
	case compressionDeflate:
		return "deflate"
	case compressionZstd:
		return "zstd"
	default:
		return fmt.Sprintf("unknown compression algorithm %d", uint8(a))
	}
}

func compressionAlgorithmFromID(id uint8) (compressionAlgorithm, error) {
	if id > disk.CompressionZstd {
		return 0, fmt.Errorf("unsupported compression algorithm %d: %w", id, ErrNotImplemented)
	}
	return compressionAlgorithm(id), nil
}

// lzmaConfig mirrors the reader-relevant fields of z_erofs_lzma_cfgs. EROFS
// stores the LZMA1 dictionary size out of band; lc/lp/pb are fixed at the
// LZMA1 defaults by erofs-utils and the kernel.
type lzmaConfig struct {
	dictSize uint32
	lc       uint8
	lp       uint8
	pb       uint8
}

func defaultLZMAConfig() lzmaConfig {
	// The kernel's defaults for EROFS microLZMA streams.
	return lzmaConfig{dictSize: 1 << 20, lc: 3, lp: 0, pb: 2}
}

// decodeCompressionCluster decompresses one physical cluster. input is the
// raw on-disk pcluster (including any leading zero padding added by EROFS);
// output must be sized to the exact number of decompressed source bytes.
//
// partial requests a partial-reference decode: only the first len(output)
// bytes of the pcluster's decompressed payload are produced, and the payload
// may be longer. Image deduplication (-E dedupe) truncates a reference to a
// previously written pcluster and marks it Z_EROFS_LI_PARTIAL_REF; the
// pcluster itself still holds the untruncated stream.
func decodeCompressionCluster(algo compressionAlgorithm, cfg lzmaConfig, input, output []byte, partial bool) error {
	if len(output) == 0 {
		return nil
	}
	// EROFS right-aligns all codec payloads within the physical cluster:
	// the leading bytes of the pcluster are zero padding. This is mandatory
	// for LZMA/DEFLATE/ZSTD and is selected by
	// EROFS_FEATURE_INCOMPAT_LZ4_0PADDING for LZ4.
	input = trimLeadingZeros(input)
	if len(input) == 0 {
		return fmt.Errorf("compressed cluster is all zero padding: %w", ErrInvalid)
	}

	switch algo {
	case compressionLZ4:
		if partial {
			return decodeLZ4Partial(input, output)
		}
		return decodeLZ4(input, output)
	case compressionLZMA:
		return decodeLZMA(input, output, cfg, partial)
	case compressionDeflate:
		return decodeDeflate(input, output)
	case compressionZstd:
		return decodeZstd(input, output)
	default:
		return fmt.Errorf("%s: %w", algo, ErrNotImplemented)
	}
}

func trimLeadingZeros(b []byte) []byte {
	i := 0
	for i < len(b) && b[i] == 0 {
		i++
	}
	return b[i:]
}

// decodeLZ4Partial decodes the first len(output) bytes of an LZ4 block while
// tolerating more stream beyond them. The shared library only supports a
// destination sized for the whole block, so walk the block format directly
// and stop once the requested prefix has been produced. This is the LZ4
// counterpart of the kernel's LZ4_decompress_safe_partial().
func decodeLZ4Partial(input, output []byte) error {
	sp, dp := 0, 0

	for dp < len(output) {
		if sp >= len(input) {
			return fmt.Errorf("lz4: truncated block: %w", ErrInvalid)
		}
		token := input[sp]
		sp++

		lit := int(token >> 4)
		if lit == 15 {
			for {
				if sp >= len(input) {
					return fmt.Errorf("lz4: truncated literal length: %w", ErrInvalid)
				}
				b := input[sp]
				sp++
				lit += int(b)
				if b != 255 {
					break
				}
			}
		}
		if lit > len(input)-sp {
			return fmt.Errorf("lz4: literal run exceeds input: %w", ErrInvalid)
		}
		if n := min(lit, len(output)-dp); n > 0 {
			copy(output[dp:], input[sp:sp+n])
			sp += n
			dp += n
		}
		if dp == len(output) {
			// The requested prefix ended inside a literal run.
			break
		}
		if sp == len(input) {
			break
		}
		if sp+2 > len(input) {
			return fmt.Errorf("lz4: truncated match offset: %w", ErrInvalid)
		}
		offset := int(binary.LittleEndian.Uint16(input[sp:]))
		sp += 2
		if offset == 0 || offset > dp {
			return fmt.Errorf("lz4: invalid match offset %d: %w", offset, ErrInvalid)
		}

		match := int(token&0x0f) + 4
		if token&0x0f == 15 {
			for {
				if sp >= len(input) {
					return fmt.Errorf("lz4: truncated match length: %w", ErrInvalid)
				}
				b := input[sp]
				sp++
				match += int(b)
				if b != 255 {
					break
				}
			}
		}
		for range match {
			if dp >= len(output) {
				return nil
			}
			output[dp] = output[dp-offset]
			dp++
		}
	}
	if dp != len(output) {
		return fmt.Errorf("lz4: decompressed %d bytes, want %d: %w", dp, len(output), ErrInvalid)
	}
	return nil
}

func decodeLZ4(input, output []byte) error {
	// A raw LZ4 block does not carry its compressed length. Images without
	// LZ4 zero-padding left-align the payload and pad the pcluster with
	// zeros, while zero-padded images right-align it. Walk the block grammar
	// to find the exact end and hide the padding from the decoder.
	size, err := lz4BlockLength(input, len(output))
	if err != nil {
		return fmt.Errorf("lz4: %w", err)
	}
	n, err := lz4.UncompressBlock(input[:size], output)
	if err != nil {
		return fmt.Errorf("lz4: %w: %v", ErrInvalid, err)
	}
	if n != len(output) {
		return fmt.Errorf("lz4: decompressed %d bytes, want %d: %w", n, len(output), ErrInvalid)
	}
	return nil
}

// lz4BlockLength walks the LZ4 block format until the expected number of
// output bytes has been produced and returns the number of compressed bytes
// consumed. It accepts trailing zero padding, which EROFS uses when the
// LZ4_0PADDING feature is not enabled.
func lz4BlockLength(src []byte, outputLen int) (int, error) {
	sp, dp := 0, 0
	for {
		if sp >= len(src) {
			return 0, fmt.Errorf("truncated block: %w", ErrInvalid)
		}
		token := src[sp]
		sp++

		lit := int(token >> 4)
		if lit == 15 {
			for {
				if sp >= len(src) {
					return 0, fmt.Errorf("truncated literal length: %w", ErrInvalid)
				}
				b := src[sp]
				sp++
				lit += int(b)
				if b != 255 {
					break
				}
			}
		}
		if sp+lit > len(src) {
			return 0, fmt.Errorf("literal run exceeds input: %w", ErrInvalid)
		}
		sp += lit
		dp += lit
		if dp == outputLen {
			// Valid LZ4 blocks end with a literal-only sequence.
			return sp, nil
		}
		if dp > outputLen {
			return 0, fmt.Errorf("literal run exceeds output: %w", ErrInvalid)
		}

		if sp+2 > len(src) {
			return 0, fmt.Errorf("truncated match offset: %w", ErrInvalid)
		}
		offset := int(binary.LittleEndian.Uint16(src[sp:]))
		sp += 2
		if offset == 0 || offset > dp {
			return 0, fmt.Errorf("invalid match offset %d: %w", offset, ErrInvalid)
		}

		match := int(token & 0x0f)
		if match == 15 {
			for {
				if sp >= len(src) {
					return 0, fmt.Errorf("truncated match length: %w", ErrInvalid)
				}
				b := src[sp]
				sp++
				match += int(b)
				if b != 255 {
					break
				}
			}
		}
		dp += match + 4
		if dp > outputLen {
			return 0, fmt.Errorf("match run exceeds output: %w", ErrInvalid)
		}
	}
}

func decodeLZMA(input, output []byte, cfg lzmaConfig, partial bool) error {
	if len(input) < 5 {
		return fmt.Errorf("lzma: microLZMA stream is %d bytes: %w", len(input), ErrInvalid)
	}
	if cfg.dictSize == 0 {
		cfg.dictSize = defaultLZMAConfig().dictSize
	}

	// EROFS uses the Linux microLZMA dialect. Its first byte carries the
	// inverted LZMA1 properties byte (lc/lp/pb), followed by the range
	// coder's four initialization bytes. The classic LZMA1 format instead
	// has a 13-byte header followed by a dummy zero byte and the same four
	// initialization bytes. Convert one to the other here.
	props := ^input[0]
	if _, err := lzma.PropertiesForCode(props); err != nil {
		return fmt.Errorf("lzma: %w: %v", ErrInvalid, err)
	}

	// The classic header carries the uncompressed size. A partial
	// reference does not know the pcluster's full length, so declare it
	// unknown (all ones) and stop after the requested prefix instead.
	uncompressedSize := uint64(len(output))
	if partial {
		uncompressedSize = ^uint64(0)
	}
	var header [lzma.HeaderLen]byte
	header[0] = props
	binary.LittleEndian.PutUint32(header[1:5], cfg.dictSize)
	binary.LittleEndian.PutUint64(header[5:13], uncompressedSize)

	framed := make([]byte, 0, len(header)+len(input))
	framed = append(framed, header[:]...)
	// The microLZMA properties byte is ignored by the range decoder, just
	// like the classic format's leading zero byte. The remaining four bytes
	// are the actual range coder state and must be preserved verbatim.
	framed = append(framed, 0)
	framed = append(framed, input[1:]...)

	r, err := lzma.ReaderConfig{DictCap: int(cfg.dictSize)}.NewReader(bytes.NewReader(framed))
	if err != nil {
		return fmt.Errorf("lzma: %w: %v", ErrInvalid, err)
	}
	n, err := io.ReadFull(r, output)
	if err != nil {
		return fmt.Errorf("lzma: decompressed %d bytes, want %d: %w: %v", n, len(output), ErrInvalid, err)
	}
	return nil
}

func decodeDeflate(input, output []byte) error {
	r := flate.NewReader(bytes.NewReader(input))
	defer r.Close()
	n, err := io.ReadFull(r, output)
	if err != nil {
		return fmt.Errorf("deflate: decompressed %d bytes, want %d: %w: %v", n, len(output), ErrInvalid, err)
	}
	return nil
}

func decodeZstd(input, output []byte) error {
	r, err := zstd.NewReader(bytes.NewReader(input), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("zstd: %w: %v", ErrInvalid, err)
	}
	defer r.Close()
	n, err := io.ReadFull(r, output)
	if err != nil {
		return fmt.Errorf("zstd: decompressed %d bytes, want %d: %w: %v", n, len(output), ErrInvalid, err)
	}
	return nil
}
