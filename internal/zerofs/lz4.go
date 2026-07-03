package zerofs

import (
	"errors"
)

// uncompressLZ4Partial decodes an LZ4 block from src into dst, stopping
// when len(dst) bytes have been produced. It is the equivalent of the
// upstream LZ4_decompress_safe_partial function used by the Linux EROFS
// driver. The 0-padding compression variant places valid LZ4 data at the
// start of one or more physical blocks and pads the remainder with zero
// bytes — those padding bytes must never be consumed as LZ4 sequences
// (they would decode as match-offset-0 or otherwise corrupt output).
//
// Returns the number of bytes written to dst (== len(dst) on success).
//
// LZ4 block format reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
func uncompressLZ4Partial(dst, src []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(src) == 0 {
		return 0, errLZ4ShortSrc
	}

	var si, di int
	for di < len(dst) {
		if si >= len(src) {
			return 0, errLZ4ShortSrc
		}
		token := src[si]
		si++

		// Literal length: high nibble of token, with extension bytes if 15.
		lLen := int(token >> 4)
		if lLen == 15 {
			for {
				if si >= len(src) {
					return 0, errLZ4ShortSrc
				}
				b := src[si]
				si++
				lLen += int(b)
				if b != 255 {
					break
				}
				if lLen > len(dst) {
					return 0, errLZ4BadLength
				}
			}
		}

		// Copy literals.
		if lLen > 0 {
			if si+lLen > len(src) {
				return 0, errLZ4ShortSrc
			}
			n := lLen
			if di+n > len(dst) {
				n = len(dst) - di
			}
			copy(dst[di:di+n], src[si:si+n])
			si += lLen
			di += n
			if di >= len(dst) {
				return di, nil
			}
		}

		// Match offset (2 bytes, little-endian).
		if si+2 > len(src) {
			return 0, errLZ4ShortSrc
		}
		mOff := int(src[si]) | int(src[si+1])<<8
		si += 2
		if mOff == 0 || mOff > di {
			return 0, errLZ4BadMatch
		}

		// Match length: low nibble of token + 4, with extension bytes if low nibble is 15.
		mLen := int(token&0x0F) + 4
		if int(token&0x0F) == 15 {
			for {
				if si >= len(src) {
					return 0, errLZ4ShortSrc
				}
				b := src[si]
				si++
				mLen += int(b)
				if b != 255 {
					break
				}
				if mLen > len(dst) {
					return 0, errLZ4BadLength
				}
			}
		}

		// Back-copy from earlier in dst. May overlap (LZ4 allows mOff < mLen);
		// must copy byte-by-byte to support the standard RLE-style overlap
		// pattern.
		n := mLen
		if di+n > len(dst) {
			n = len(dst) - di
		}
		for i := 0; i < n; i++ {
			dst[di+i] = dst[di+i-mOff]
		}
		di += n
	}
	return di, nil
}

var (
	errLZ4ShortSrc  = errors.New("lz4: unexpected end of compressed input")
	errLZ4BadMatch  = errors.New("lz4: invalid match offset")
	errLZ4BadLength = errors.New("lz4: implausible length extension")
)
