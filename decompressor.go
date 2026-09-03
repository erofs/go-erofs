package erofs

import (
	"fmt"

	"github.com/pierrec/lz4/v4"

	"github.com/erofs/go-erofs/internal/disk"
)

// decompressor decompresses a single pcluster's worth of bytes.
type decompressor interface {
	// decompress reads up to len(dst) bytes of decompressed output from src
	// into dst, returning the number of bytes written. src is the raw on-disk
	// pcluster payload (already stripped of any leading 0-padding for LZ4).
	decompress(dst, src []byte) (int, error)
}

type lz4Decompressor struct{}

func (lz4Decompressor) decompress(dst, src []byte) (int, error) {
	// EROFS pclusters zero-pad their LZ4 payload to a full physical block,
	// so the trailing src bytes are not part of the LZ4 stream. The kernel
	// uses LZ4_decompress_safe_partial which stops when dst is full and
	// tolerates trailing input. pierrec/lz4 instead errors on leftover
	// input, so we use an in-tree decoder modelled on the reference
	// implementation.
	n, err := lz4UncompressPartial(dst, src)
	if err != nil {
		return 0, fmt.Errorf("lz4 decompress: %w", err)
	}
	return n, nil
}

// lz4UncompressPartial decodes an LZ4 block into dst, stopping when dst is
// full. It matches kernel `LZ4_decompress_safe_partial` semantics: trailing
// bytes in src past the natural end of the LZ4 stream are ignored.
func lz4UncompressPartial(dst, src []byte) (int, error) {
	const minMatch = 4
	var si, di int
	for si < len(src) {
		if di >= len(dst) {
			return di, nil
		}
		token := int(src[si])
		si++

		// Literal length: high nibble, with 0xF prefix for extended length.
		lLen := token >> 4
		if lLen == 0xF {
			for {
				if si >= len(src) {
					return 0, fmt.Errorf("lz4: truncated literal length: %w", ErrInvalid)
				}
				x := int(src[si])
				si++
				lLen += x
				if x != 0xFF {
					break
				}
			}
		}

		if lLen > 0 {
			if di+lLen > len(dst) {
				// LZ4 partial: clip the trailing literal copy to dst.
				lLen = len(dst) - di
			}
			if si+lLen > len(src) {
				return 0, fmt.Errorf("lz4: truncated literal data: %w", ErrInvalid)
			}
			copy(dst[di:di+lLen], src[si:si+lLen])
			si += lLen
			di += lLen
		}

		if di >= len(dst) {
			return di, nil
		}
		// End-of-block: a clean LZ4 stream ends right after a literal
		// sequence with mLen=0 — i.e. when si has reached the natural end
		// of the encoder's output. Pclusters pad past this with zeros, so
		// detect end-of-block by looking at the token's match nibble.
		if si >= len(src) {
			return di, nil
		}

		// 16-bit little-endian match offset.
		if si+2 > len(src) {
			return 0, fmt.Errorf("lz4: truncated match offset: %w", ErrInvalid)
		}
		offset := int(src[si]) | int(src[si+1])<<8
		si += 2
		if offset == 0 {
			// Offset 0 is invalid in real LZ4 data and is what trailing
			// zero padding decodes to once we've stepped past the natural
			// end of the LZ4 stream. Treat it as end-of-stream only if we
			// have already produced the full requested output — otherwise
			// it's a truncated stream that happens to have zero bytes
			// where the offset field would be, and silently returning a
			// short decode would mask real corruption.
			if di < len(dst) {
				return 0, fmt.Errorf("lz4: zero match offset before end of output (%d/%d bytes decoded): %w", di, len(dst), ErrInvalid)
			}
			return di, nil
		}
		if offset > di {
			return 0, fmt.Errorf("lz4: match offset %d > written %d: %w", offset, di, ErrInvalid)
		}

		mLen := token & 0xF
		if mLen == 0xF {
			for {
				if si >= len(src) {
					return 0, fmt.Errorf("lz4: truncated match length: %w", ErrInvalid)
				}
				x := int(src[si])
				si++
				mLen += x
				if x != 0xFF {
					break
				}
			}
		}
		mLen += minMatch

		for mLen > 0 {
			n := offset
			if n > mLen {
				n = mLen
			}
			if di+n > len(dst) {
				n = len(dst) - di
			}
			if n <= 0 {
				return di, nil
			}
			copy(dst[di:di+n], dst[di-offset:di-offset+n])
			di += n
			mLen -= n
		}
	}
	return di, nil
}

// pickDecompressor returns the decompressor for the given on-disk algorithm
// identifier, or an error if the algorithm is unsupported.
func pickDecompressor(algo uint8) (decompressor, error) {
	if algo == disk.ZErofsCompressionLZ4 {
		return lz4Decompressor{}, nil
	}
	return nil, fmt.Errorf("compression algorithm %d: %w", algo, ErrNotImplemented)
}

// compressor produces a compressed block of bytes. compressBlock reads from
// src and writes the LZ4-compressed encoding into dst, returning the number
// of bytes written. A return value of 0 indicates the input was
// incompressible (the caller should emit the input uncompressed as a PLAIN
// lcluster). Argument order is (src, dst), matching the underlying
// pierrec/lz4 CompressBlock — both []byte, so the type system can't catch
// a swap; the matching order at least makes the impl pass-through.
//
// algorithmID reports the on-disk Z_EROFS_COMPRESSION_* identifier recorded
// in the z_erofs map header and the superblock's compression bitmap.
type compressor interface {
	compressBlock(src, dst []byte) (int, error)
	algorithmID() uint8
}

type lz4Compressor struct{}

func (lz4Compressor) compressBlock(src, dst []byte) (int, error) {
	var c lz4.Compressor
	n, err := c.CompressBlock(src, dst)
	if err != nil {
		return 0, fmt.Errorf("lz4 compress: %w", err)
	}
	return n, nil
}

func (lz4Compressor) algorithmID() uint8 { return disk.ZErofsCompressionLZ4 }

// pickCompressor returns a compressor for the given algorithm. LZ4 is the
// only supported writer target.
func pickCompressor(c Compression) (compressor, error) {
	if c == CompressionLZ4 {
		return lz4Compressor{}, nil
	}
	return nil, fmt.Errorf("compression %d: %w", c, ErrNotImplemented)
}
