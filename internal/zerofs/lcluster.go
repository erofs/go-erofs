package zerofs

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/erofs/go-erofs/internal/disk"
)

// LclusterEntry is the decoded form of a single logical cluster entry. Only
// the fields valid for the entry's Type are populated.
type LclusterEntry struct {
	Type       uint8  // disk.ZLclusterType{Plain,Head1,Nonhead,Head2}
	ClusterOfs uint16 // valid for PLAIN/HEAD1: byte offset within decompressed data where this lcluster begins
	BlkAddr    uint32 // valid for PLAIN/HEAD1: physical block address of the (start of the) pcluster
	DeltaPrev  uint16 // valid for NONHEAD: lcluster distance back to the head
	DeltaNext  uint16 // valid for NONHEAD: lcluster distance forward to the next head (0 if unknown)
	// CBlkCnt is non-zero only for big-pcluster NONHEAD entries: it overrides
	// the pcluster's physical block count.
	CBlkCnt uint16
}

// Decoder reads lcluster index entries for one compressed inode.
type Decoder struct {
	// Meta provides random access to the image's metadata bytes.
	Meta io.ReaderAt
	// IndexBase is the absolute byte offset where the lcluster index array
	// starts (immediately after the 8-byte ZMapHeader).
	IndexBase int64
	// Layout is one of disk.LayoutCompressedFull or disk.LayoutCompressedCompact.
	Layout uint8
	// BlkSizeBits is the image's log2 block size.
	BlkSizeBits uint8
	// LclusterBits is BlkSizeBits + (ZMapHeader.ClusterBits & 7).
	LclusterBits uint8
	// Header is the parsed map header for this inode.
	Header disk.ZMapHeader
	// Size is the logical size of the inode in bytes.
	Size int64
	// NLclusters is ceil(Size / (1<<LclusterBits)).
	NLclusters int
}

// LclusterSize returns the byte size of one logical cluster.
func (d *Decoder) LclusterSize() int64 {
	return int64(1) << d.LclusterBits
}

// Entry returns the decoded lcluster index entry at idx.
func (d *Decoder) Entry(idx int) (LclusterEntry, error) {
	if idx < 0 || idx >= d.NLclusters {
		return LclusterEntry{}, fmt.Errorf("lcluster index %d out of range [0,%d)", idx, d.NLclusters)
	}
	switch d.Layout {
	case disk.LayoutCompressedFull:
		return d.decodeFull(idx)
	case disk.LayoutCompressedCompact:
		return LclusterEntry{}, fmt.Errorf("compact lcluster layout not supported: %w", ErrNotImplemented)
	default:
		return LclusterEntry{}, fmt.Errorf("unknown compressed layout %d", d.Layout)
	}
}

func (d *Decoder) decodeFull(idx int) (LclusterEntry, error) {
	var buf [disk.SizeLclusterIndex]byte
	off := d.IndexBase + int64(idx)*disk.SizeLclusterIndex
	if _, err := d.Meta.ReadAt(buf[:], off); err != nil {
		return LclusterEntry{}, fmt.Errorf("read lcluster %d at %d: %w", idx, off, err)
	}
	var li disk.LclusterIndex
	if _, err := binary.Decode(buf[:], binary.LittleEndian, &li); err != nil {
		return LclusterEntry{}, fmt.Errorf("decode lcluster %d: %w", idx, err)
	}
	e := LclusterEntry{Type: uint8(li.Advise & disk.ZLclusterTypeMask)}
	switch e.Type {
	case disk.ZLclusterTypePlain, disk.ZLclusterTypeHead1:
		e.ClusterOfs = li.ClusterOfs
		e.BlkAddr = li.U
	case disk.ZLclusterTypeNonhead:
		delta0 := uint16(li.U & 0xFFFF)
		delta1 := uint16(li.U >> 16)
		if delta0&disk.ZLclusterD0CBlkCnt != 0 {
			e.CBlkCnt = delta0 &^ disk.ZLclusterD0CBlkCnt
			e.DeltaPrev = 1
		} else {
			e.DeltaPrev = delta0
		}
		e.DeltaNext = delta1
	case disk.ZLclusterTypeHead2:
		return LclusterEntry{}, fmt.Errorf("HEAD2 lcluster (lcn=%d): %w", idx, ErrNotImplemented)
	}
	return e, nil
}
