package zerofs

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/erofs/go-erofs/internal/disk"
)

// fullLayoutImage builds a synthetic lcluster index for full layout. entries
// is the sequence of (type, clusterofs, u) tuples to encode. Returns an
// io.ReaderAt whose offset 0 is the start of the index.
func buildFullIndex(t *testing.T, entries []disk.LclusterIndex) *bytes.Reader {
	t.Helper()
	buf := make([]byte, len(entries)*disk.SizeLclusterIndex)
	for i, e := range entries {
		off := i * disk.SizeLclusterIndex
		binary.LittleEndian.PutUint16(buf[off:off+2], e.Advise)
		binary.LittleEndian.PutUint16(buf[off+2:off+4], e.ClusterOfs)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], e.U)
	}
	return bytes.NewReader(buf)
}

// TestFullDecodeHeadAndNonhead exercises the most common case: a HEAD1
// followed by N NONHEAD entries.
func TestFullDecodeHeadAndNonhead(t *testing.T) {
	entries := []disk.LclusterIndex{
		// lcn 0: HEAD1, clusterofs=0, pcluster blkaddr=10
		{Advise: disk.ZLclusterTypeHead1, ClusterOfs: 0, U: 10},
		// lcn 1: NONHEAD, delta[0]=1 (back 1 lcluster to head)
		{Advise: disk.ZLclusterTypeNonhead, ClusterOfs: 0, U: 1},
		// lcn 2: NONHEAD, delta[0]=2
		{Advise: disk.ZLclusterTypeNonhead, ClusterOfs: 0, U: 2},
		// lcn 3: HEAD1, clusterofs=0, pcluster blkaddr=20 (next pcluster)
		{Advise: disk.ZLclusterTypeHead1, ClusterOfs: 0, U: 20},
	}
	r := buildFullIndex(t, entries)
	d := &Decoder{
		Meta:         r,
		IndexBase:    0,
		Layout:       disk.LayoutCompressedFull,
		BlkSizeBits:  12,
		LclusterBits: 12,
		Size:         4 * 4096, // 4 lclusters worth
		NLclusters:   4,
	}
	for i, want := range entries {
		got, err := d.Entry(i)
		if err != nil {
			t.Fatalf("Entry(%d): %v", i, err)
		}
		wantType := uint8(want.Advise & disk.ZLclusterTypeMask)
		if got.Type != wantType {
			t.Errorf("lcn %d: type got %d want %d", i, got.Type, wantType)
		}
		if wantType == disk.ZLclusterTypeHead1 {
			if got.BlkAddr != want.U {
				t.Errorf("lcn %d: blkaddr got %d want %d", i, got.BlkAddr, want.U)
			}
		}
		if wantType == disk.ZLclusterTypeNonhead {
			if uint32(got.DeltaPrev) != want.U {
				t.Errorf("lcn %d: delta_prev got %d want %d", i, got.DeltaPrev, want.U)
			}
		}
	}
}

// TestFindPclusterForOffset checks pcluster boundary resolution across the
// canonical HEAD..NONHEAD..NONHEAD..HEAD pattern.
func TestFindPclusterForOffset(t *testing.T) {
	entries := []disk.LclusterIndex{
		{Advise: disk.ZLclusterTypeHead1, ClusterOfs: 0, U: 100},
		{Advise: disk.ZLclusterTypeNonhead, ClusterOfs: 0, U: 1},
		{Advise: disk.ZLclusterTypeNonhead, ClusterOfs: 0, U: 2},
		{Advise: disk.ZLclusterTypeHead1, ClusterOfs: 0, U: 200},
		{Advise: disk.ZLclusterTypeNonhead, ClusterOfs: 0, U: 1},
	}
	r := buildFullIndex(t, entries)
	d := &Decoder{
		Meta:         r,
		IndexBase:    0,
		Layout:       disk.LayoutCompressedFull,
		BlkSizeBits:  12,
		LclusterBits: 12,
		Size:         5 * 4096,
		NLclusters:   5,
	}

	// Offset 0 is in pcluster A (head=0, blkaddr=100, spans lcns 0..2).
	pc, err := d.FindPclusterForOffset(0)
	if err != nil {
		t.Fatalf("FindPcluster(0): %v", err)
	}
	if pc.HeadLcn != 0 || pc.HeadBlk != 100 {
		t.Errorf("offset 0: head=%d blk=%d want head=0 blk=100", pc.HeadLcn, pc.HeadBlk)
	}
	if pc.LogStart != 0 || pc.LogLen != 3*4096 {
		t.Errorf("offset 0: logStart=%d logLen=%d want 0/%d", pc.LogStart, pc.LogLen, 3*4096)
	}

	// Offset in the middle of pcluster A (lcn=1).
	pc, err = d.FindPclusterForOffset(4096 + 100)
	if err != nil {
		t.Fatalf("FindPcluster(4196): %v", err)
	}
	if pc.HeadLcn != 0 || pc.HeadBlk != 100 {
		t.Errorf("offset 4196: head=%d blk=%d want head=0 blk=100", pc.HeadLcn, pc.HeadBlk)
	}

	// Offset at lcn=3, the head of pcluster B (blkaddr=200, spans lcns 3..4).
	pc, err = d.FindPclusterForOffset(3 * 4096)
	if err != nil {
		t.Fatalf("FindPcluster(lcn=3): %v", err)
	}
	if pc.HeadLcn != 3 || pc.HeadBlk != 200 {
		t.Errorf("lcn 3: head=%d blk=%d want head=3 blk=200", pc.HeadLcn, pc.HeadBlk)
	}
	if pc.LogStart != 3*4096 || pc.LogLen != 2*4096 {
		t.Errorf("lcn 3: logStart=%d logLen=%d want %d/%d", pc.LogStart, pc.LogLen, 3*4096, 2*4096)
	}
}

func TestFindPclusterCompactRejected(t *testing.T) {
	d := &Decoder{Layout: disk.LayoutCompressedCompact, NLclusters: 1, Size: 4096, LclusterBits: 12, BlkSizeBits: 12}
	_, err := d.Entry(0)
	if err == nil {
		t.Fatal("compact layout should return error")
	}
}
