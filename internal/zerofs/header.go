// Package zerofs implements the z_erofs (compressed EROFS) read path:
// parsing the per-inode map header, decoding the LayoutCompressedFull
// lcluster index, and decompressing physical clusters with LZ4.
//
// Features outside this set -- LayoutCompressedCompact, ztailpacking,
// fragments, interlaced pclusters, head2, and non-LZ4 algorithms --
// return ErrNotImplemented from ParseHeader or the Decoder so the
// caller can surface the limitation to the user rather than silently
// returning wrong data.
package zerofs

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erofs/go-erofs/internal/disk"
)

// ErrNotImplemented is returned when a parsable but unsupported feature is
// encountered. It is wrapped by the package's error messages and re-exported
// by callers as erofs.ErrNotImplemented (matching by errors.Is).
var ErrNotImplemented = errors.New("not implemented")

// ErrCorrupt is returned for inconsistencies in the compressed metadata —
// values that should never appear in a well-formed image.
var ErrCorrupt = errors.New("corrupt compressed metadata")

// ParseHeader decodes the 8-byte z_erofs_map_header and rejects features
// that this package does not implement.
func ParseHeader(buf []byte) (disk.ZMapHeader, error) {
	var h disk.ZMapHeader
	if len(buf) < disk.SizeZMapHeader {
		return h, fmt.Errorf("z_erofs_map_header: short buffer (%d bytes)", len(buf))
	}
	if _, err := binary.Decode(buf[:disk.SizeZMapHeader], binary.LittleEndian, &h); err != nil {
		return h, fmt.Errorf("z_erofs_map_header: %w", err)
	}
	if h.IdataSize != 0 {
		return h, fmt.Errorf("inline pcluster data (idata_size=%d): %w", h.IdataSize, ErrNotImplemented)
	}
	if h.Advise&disk.ZAdviseInlinePcluster != 0 {
		return h, fmt.Errorf("ztailpacking not supported: %w", ErrNotImplemented)
	}
	if h.Advise&disk.ZAdviseFragmentPcluster != 0 {
		return h, fmt.Errorf("fragments not supported: %w", ErrNotImplemented)
	}
	if h.Advise&disk.ZAdviseInterlacedPcluster != 0 {
		return h, fmt.Errorf("interlaced pcluster not supported: %w", ErrNotImplemented)
	}
	if h.Advise&disk.ZAdviseBigPcluster2 != 0 {
		return h, fmt.Errorf("big pcluster (head2) not supported: %w", ErrNotImplemented)
	}
	if h.AlgorithmType>>4 != 0 {
		return h, fmt.Errorf("second compression algorithm (head2=%d) not supported: %w",
			h.AlgorithmType>>4, ErrNotImplemented)
	}
	if h.AlgorithmType&0xF != 0 {
		return h, fmt.Errorf("compression algorithm %d not supported: %w",
			h.AlgorithmType&0xF, ErrNotImplemented)
	}
	return h, nil
}
