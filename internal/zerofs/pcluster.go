package zerofs

import (
	"fmt"

	"github.com/erofs/go-erofs/internal/disk"
)

// Pcluster describes one physical (compressed) cluster: where its compressed
// bytes live, how much decompressed output it produces, and at which logical
// file offset that decompressed output begins.
type Pcluster struct {
	HeadType  uint8  // disk.ZLclusterType{Plain,Head1}
	HeadLcn   int    // lcluster index of the head
	HeadOfs   uint16 // ClusterOfs of the head lcluster
	HeadBlk   uint32 // physical block address of the (start of the) compressed data
	NPhysBlks uint32 // physical (compressed) block count
	LogStart  int64  // logical byte offset where the pcluster's decompressed output begins
	LogLen    int64  // length of the pcluster's decompressed output in bytes
}

// FindPclusterForOffset returns the pcluster containing logical file offset
// pos. The caller must ensure 0 <= pos < d.Size.
func (d *Decoder) FindPclusterForOffset(pos int64) (Pcluster, error) {
	lcsize := d.LclusterSize()
	lcn := int(pos >> d.LclusterBits)
	endoff := uint16(pos & (lcsize - 1))

	e, err := d.Entry(lcn)
	if err != nil {
		return Pcluster{}, err
	}

	headLcn := lcn
	switch e.Type {
	case disk.ZLclusterTypePlain, disk.ZLclusterTypeHead1:
		// If pos sits in the "preroll" region of this lcluster (the bytes
		// before ClusterOfs belong to the previous pcluster), look back.
		if endoff < e.ClusterOfs {
			// Treat as if we were a NONHEAD with delta=1.
			if lcn == 0 {
				return Pcluster{}, fmt.Errorf("preroll on lcn 0 (clusterofs=%d, endoff=%d): %w",
					e.ClusterOfs, endoff, ErrCorrupt)
			}
			headLcn, e, err = d.lookbackToHead(lcn, 1)
			if err != nil {
				return Pcluster{}, err
			}
		}
	case disk.ZLclusterTypeNonhead:
		if e.DeltaPrev == 0 {
			return Pcluster{}, fmt.Errorf("NONHEAD with delta=0 at lcn=%d: %w", lcn, ErrCorrupt)
		}
		headLcn, e, err = d.lookbackToHead(lcn, int(e.DeltaPrev))
		if err != nil {
			return Pcluster{}, err
		}
	default:
		return Pcluster{}, fmt.Errorf("unexpected lcluster type %d at lcn=%d", e.Type, lcn)
	}

	logStart := int64(headLcn)<<d.LclusterBits + int64(e.ClusterOfs)

	// Walk forward to find the next head's start (which is this pcluster's end).
	logEnd, cblkcntFromTail, err := d.findNextHeadStart(headLcn)
	if err != nil {
		return Pcluster{}, err
	}
	if logEnd > d.Size {
		logEnd = d.Size
	}
	if logEnd <= logStart {
		return Pcluster{}, fmt.Errorf("pcluster log range empty: start=%d end=%d (head=%d clusterofs=%d): %w",
			logStart, logEnd, headLcn, e.ClusterOfs, ErrCorrupt)
	}
	logLen := logEnd - logStart

	pc := Pcluster{
		HeadType: e.Type,
		HeadLcn:  headLcn,
		HeadOfs:  e.ClusterOfs,
		HeadBlk:  e.BlkAddr,
		LogStart: logStart,
		LogLen:   logLen,
	}

	blkSize := int64(1) << d.BlkSizeBits
	switch e.Type {
	case disk.ZLclusterTypePlain:
		pc.NPhysBlks = uint32((logLen + blkSize - 1) / blkSize)
	case disk.ZLclusterTypeHead1:
		if d.Header.Advise&disk.ZAdviseBigPcluster1 == 0 {
			pc.NPhysBlks = 1
		} else {
			if cblkcntFromTail == 0 {
				return Pcluster{}, fmt.Errorf("big pcluster head=%d missing CBlkCnt marker: %w",
					headLcn, ErrCorrupt)
			}
			pc.NPhysBlks = uint32(cblkcntFromTail)
		}
	}
	if pc.NPhysBlks == 0 {
		return Pcluster{}, fmt.Errorf("pcluster head=%d resolved to 0 physical blocks: %w", headLcn, ErrCorrupt)
	}
	return pc, nil
}

// lookbackToHead walks back delta lclusters from lcn until it reaches a
// HEAD or PLAIN entry. The kernel semantics: the head pointed at by delta
// must be HEAD/PLAIN; if not we error out as corrupt.
func (d *Decoder) lookbackToHead(lcn, delta int) (int, LclusterEntry, error) {
	if delta < 1 || lcn-delta < 0 {
		return 0, LclusterEntry{}, fmt.Errorf("invalid lookback from lcn=%d by delta=%d: %w",
			lcn, delta, ErrCorrupt)
	}
	headLcn := lcn - delta
	for {
		e, err := d.Entry(headLcn)
		if err != nil {
			return 0, LclusterEntry{}, err
		}
		switch e.Type {
		case disk.ZLclusterTypePlain, disk.ZLclusterTypeHead1:
			return headLcn, e, nil
		case disk.ZLclusterTypeNonhead:
			// Some images chain NONHEAD->NONHEAD with cumulative deltas.
			if e.DeltaPrev == 0 {
				return 0, LclusterEntry{}, fmt.Errorf("NONHEAD chain hit delta=0 at lcn=%d: %w",
					headLcn, ErrCorrupt)
			}
			if headLcn-int(e.DeltaPrev) < 0 {
				return 0, LclusterEntry{}, fmt.Errorf("NONHEAD chain underflow at lcn=%d delta=%d: %w",
					headLcn, e.DeltaPrev, ErrCorrupt)
			}
			headLcn -= int(e.DeltaPrev)
		default:
			return 0, LclusterEntry{}, fmt.Errorf("lookback hit type %d at lcn=%d: %w",
				e.Type, headLcn, ErrCorrupt)
		}
	}
}

// findNextHeadStart scans lclusters after headLcn looking for the start of
// the next pcluster (a HEAD/PLAIN entry). Returns the logical byte offset at
// which the next pcluster begins (= end of the current pcluster) and the
// big-pcluster CBlkCnt marker if observed in a NONHEAD entry along the way.
// If the end of the file is reached without finding another head, returns
// d.Size as the end.
func (d *Decoder) findNextHeadStart(headLcn int) (int64, uint16, error) {
	var cblkcnt uint16
	for i := headLcn + 1; i < d.NLclusters; i++ {
		e, err := d.Entry(i)
		if err != nil {
			return 0, 0, err
		}
		switch e.Type {
		case disk.ZLclusterTypePlain, disk.ZLclusterTypeHead1:
			return int64(i)<<d.LclusterBits + int64(e.ClusterOfs), cblkcnt, nil
		case disk.ZLclusterTypeNonhead:
			if e.CBlkCnt != 0 {
				cblkcnt = e.CBlkCnt
			}
		default:
			return 0, 0, fmt.Errorf("unexpected lcluster type %d at lcn=%d in forward walk: %w",
				e.Type, i, ErrCorrupt)
		}
	}
	// Reached end of file. The pcluster runs to the file's logical end.
	return d.Size, cblkcnt, nil
}
