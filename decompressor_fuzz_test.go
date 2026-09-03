package erofs

import (
	"testing"
)

// FuzzLz4UncompressPartial drives lz4UncompressPartial with arbitrary src
// bytes and dst sizes. The decoder is on the read path for every compressed
// EROFS image, so it must not panic, infinite-loop, or write past the
// caller-provided destination buffer for any attacker-controlled input.
//
// The invariants checked are intentionally weak — fuzzing isn't trying to
// catch decoded-output mismatches (we have round-trip tests for that), just
// that the function returns within reasonable bounds without crashing.
func FuzzLz4UncompressPartial(f *testing.F) {
	// Seed with a few hand-crafted cases that exercise distinct branches
	// in the decoder: empty, literal-only, all-zero (offset==0 path),
	// token-with-extended-length, etc. The fuzzer will mutate from here.
	f.Add([]byte{}, uint16(0))
	f.Add([]byte{}, uint16(4096))
	f.Add([]byte{0x00, 0x00, 0x00}, uint16(16))                                       // token 0x00 + zeros (offset==0 path)
	f.Add([]byte{0x10, 0xAA}, uint16(16))                                             // 1-byte literal
	f.Add([]byte{0x50, 'h', 'e', 'l', 'l', 'o', 0x05, 0x00}, uint16(32))              // literal + match
	f.Add([]byte{0xF0, 0x00, 0xAA}, uint16(16))                                       // extended literal length=15
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, uint16(8192)) // extension runaway

	f.Fuzz(func(t *testing.T, src []byte, dstSize uint16) {
		// Cap dst to a sane size so the fuzzer doesn't burn time on huge
		// allocations; 64 KiB covers everything our reader will request
		// (max blockSize * max pcluster lclusters).
		const maxDst = 64 * 1024
		size := int(dstSize)
		if size > maxDst {
			size = maxDst
		}
		dst := make([]byte, size)

		n, err := lz4UncompressPartial(dst, src)

		// Contract: on error n must be 0; on success 0 <= n <= len(dst).
		if err != nil {
			if n != 0 {
				t.Errorf("error path returned n=%d (expected 0): %v", n, err)
			}
			return
		}
		if n < 0 || n > len(dst) {
			t.Errorf("n=%d out of range [0, %d]", n, len(dst))
		}
	})
}
