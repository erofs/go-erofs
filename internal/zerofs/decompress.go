package zerofs

import (
	"fmt"
	"io"

	"github.com/erofs/go-erofs/internal/disk"
)

// Decompress reads the pcluster's compressed bytes via meta (which is the
// io.ReaderAt providing the same physical address space the lcluster
// BlkAddr values point into) and writes its decompressed output into out.
//
// out must be at least p.LogLen bytes long. The function returns the number
// of decompressed bytes written; for well-formed images this always equals
// p.LogLen.
func Decompress(meta io.ReaderAt, blkSizeBits uint8, p Pcluster, out []byte) (int, error) {
	blkSize := int(1) << blkSizeBits
	if int64(len(out)) < p.LogLen {
		return 0, fmt.Errorf("decompress output buffer too small: %d < %d", len(out), p.LogLen)
	}
	physOff := int64(p.HeadBlk) << blkSizeBits

	switch p.HeadType {
	case disk.ZLclusterTypePlain:
		n, err := readFull(meta, out[:p.LogLen], physOff)
		if err != nil {
			return 0, fmt.Errorf("plain pcluster head=%d read: %w", p.HeadLcn, err)
		}
		return n, nil
	case disk.ZLclusterTypeHead1:
		src := make([]byte, int(p.NPhysBlks)*blkSize)
		if _, err := readFull(meta, src, physOff); err != nil {
			return 0, fmt.Errorf("lz4 pcluster head=%d compressed read: %w", p.HeadLcn, err)
		}
		// EROFS LZ4_0Padding mode places the LZ4 stream at the END of the
		// physical block(s) and pads leading bytes with zeros. Skip the
		// leading zero margin to find the actual LZ4 stream start, matching
		// the kernel's inputmargin scan in z_erofs_lz4_decompress_mem.
		inputMargin := 0
		for inputMargin < len(src) && src[inputMargin] == 0 {
			inputMargin++
		}
		if inputMargin >= len(src) {
			return 0, fmt.Errorf("lz4 pcluster head=%d: physical block is all zeros: %w",
				p.HeadLcn, ErrCorrupt)
		}
		n, err := uncompressLZ4Partial(out[:p.LogLen], src[inputMargin:])
		if err != nil {
			return 0, fmt.Errorf("lz4 decompress head=%d (src=%d margin=%d, dst=%d): %w",
				p.HeadLcn, len(src), inputMargin, p.LogLen, err)
		}
		if int64(n) != p.LogLen {
			return 0, fmt.Errorf("lz4 decompress head=%d: got %d bytes, want %d: %w",
				p.HeadLcn, n, p.LogLen, ErrCorrupt)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported head type %d: %w", p.HeadType, ErrNotImplemented)
	}
}

func readFull(r io.ReaderAt, dst []byte, off int64) (int, error) {
	total := 0
	for total < len(dst) {
		n, err := r.ReadAt(dst[total:], off+int64(total))
		total += n
		if err != nil {
			if total == len(dst) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}
