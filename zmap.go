// Copyright 2026 The go-erofs Authors.
//
// The compressed-cluster index decoder is derived from the MIT-licensed
// am-fs-erofs project (https://github.com/antimatter-studios/rust-fs-erofs),
// Copyright (c) 2026 Antimatter Studios, and follows the public EROFS
// on-disk format. See that project's LICENSE for the full MIT license text.

package erofs

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/erofs/go-erofs/internal/disk"
)

// On-disk z_erofs_map_header advise bits.
const (
	// zErofsAdviseCompacted2B enables the compacted-2B middle region for
	// EROFS_INODE_COMPRESSED_COMPACT inodes. For full-index inodes the same
	// bit means "extent metadata" (zErofsAdviseExtents).
	zErofsAdviseCompacted2B = 0x0001
	zErofsAdviseExtents     = 0x0001

	zErofsAdviseBigPcluster1 = 0x0002
	zErofsAdviseBigPcluster2 = 0x0004
	zErofsAdviseInline       = 0x0008
	zErofsAdviseInterlaced   = 0x0010
	zErofsAdviseFragment     = 0x0020
)

// zErofsFragmentInodeBit is the high bit of h_clusterbits. When set the whole
// inode lives in the superblock's packed inode at h_fragmentoff.
const zErofsFragmentInodeBit = 0x80

// Lcluster types.
const (
	zErofsLclusterTypePlain   = 0
	zErofsLclusterTypeHead1   = 1
	zErofsLclusterTypeNonhead = 2
	zErofsLclusterTypeHead2   = 3
)

const (
	// zErofsLID0Cblkcnt marks a NONHEAD entry whose low bits carry the
	// compressed-block count of a BIG_PCLUSTER physical cluster.
	zErofsLID0Cblkcnt = 1 << 11
	zErofsLID0ValMask = zErofsLID0Cblkcnt - 1

	// zErofsLIPartialRef and zErofsLIHole are only present in the legacy
	// 8-byte index format.
	zErofsLIPartialRef = 1 << 15
	zErofsLIHole       = 1 << 14
)

const (
	// zErofsFullIndexGap is the 8-byte reserved gap between the map header
	// and the first legacy lcluster entry (Z_EROFS_FULL_INDEX_START).
	zErofsFullIndexGap = 8

	// zErofsLclusterIndexSize is sizeof(struct z_erofs_lcluster_index).
	zErofsLclusterIndexSize = 8
)

type zmapHeader struct {
	fragmentOffOrIData uint32 // union of h_fragmentoff / h_idata_size
	advise             uint16
	algorithmType      uint8
	clusterBits        uint8
}

func (h zmapHeader) idataSize() uint16 { return uint16(h.fragmentOffOrIData >> 16) }

type zmapIndexFormat uint8

const (
	zmapIndexLegacy zmapIndexFormat = iota
	zmapIndexCompact
)

// zclusterEntry is a format-independent decoded lcluster index entry.
type zclusterEntry struct {
	typ        uint8
	clusterofs uint16
	pblk       uint32
	delta0     uint16
	delta1     uint16
	cblkcnt    uint32
	hasCblkcnt bool
	partialRef bool
	hole       bool
}

func (e zclusterEntry) isHeadOrPlain() bool {
	return e.typ == zErofsLclusterTypePlain ||
		e.typ == zErofsLclusterTypeHead1 ||
		e.typ == zErofsLclusterTypeHead2
}

// compactPack describes one compacted-index pack.
type compactPack struct {
	packBytes  int64
	vcnt       int
	encodeBits int
	lobits     int
}

// zmap decodes the compressed-cluster index of one inode.
type zmap struct {
	img  *image
	nid  uint64
	size int64

	inodeSize int
	xattrSize int
	layout    uint8

	header   zmapHeader
	format   zmapIndexFormat
	ebase    int64
	indexOff int64

	lclusterBits  uint8 // low nibble of h_clusterbits
	zLclusterBits uint8 // blkSizeBits + lclusterBits
	lclusterSize  int64
	totalIdx      uint64

	compact4bInitial uint64
	compact2b        uint64

	bigPcluster1 bool
	bigPcluster2 bool

	algoHead1 uint8
	algoHead2 uint8

	hasInline    bool
	idataSize    int
	hasFragment  bool
	fullFragment bool
	fragmentOff  uint64
	interlaced   bool

	tailComputed  bool
	tailHeadLCN   uint64
	tailInlineOff int64
	tailFragStart int64
	tailErr       error
}

type tailInfo struct {
	headLCN   uint64
	inlineOff int64
	fragStart int64
}

func openZmap(img *image, ino *inode) (*zmap, error) {
	bodyEnd := img.metaStartPos() + int64(ino.nid)*disk.SizeInodeCompact +
		int64(ino.icsize) + int64(ino.xsize)
	headerOff := (bodyEnd + 7) &^ 7

	var raw [8]byte
	if _, err := img.meta.ReadAt(raw[:], headerOff); err != nil {
		return nil, fmt.Errorf("reading compressed map header for nid %d: %w", ino.nid, err)
	}

	z := &zmap{
		img:       img,
		nid:       ino.nid,
		size:      ino.size,
		inodeSize: int(ino.icsize),
		xattrSize: ino.xsize,
		layout:    ino.inodeLayout,
	}

	// The highest bit of the 8-byte map header means the whole file is
	// stored in the packed inode; the remaining bits carry h_fragmentoff.
	if raw[7]&zErofsFragmentInodeBit != 0 {
		fragOff := binary.LittleEndian.Uint64(raw[:]) ^ (uint64(1) << 63)
		z.fullFragment = true
		z.hasFragment = true
		z.header = zmapHeader{
			advise:             zErofsAdviseFragment,
			fragmentOffOrIData: uint32(fragOff),
		}
		z.fragmentOff = fragOff
		z.format = zmapIndexLegacy
		switch ino.inodeLayout {
		case disk.LayoutCompressedCompact:
			z.format = zmapIndexCompact
		case disk.LayoutCompressedFull:
			z.format = zmapIndexLegacy
		default:
			return nil, fmt.Errorf("nid %d: invalid compressed layout %d: %w", ino.nid, ino.inodeLayout, ErrInvalid)
		}
		// Geometry is still needed for consistency checks; the fragment
		// path does not read the lcluster index.
		z.lclusterBits = raw[7] & 0x0f
		z.zLclusterBits = img.sb.BlkSizeBits + z.lclusterBits
		z.lclusterSize = int64(1) << z.zLclusterBits
		if z.lclusterSize <= 0 {
			return nil, fmt.Errorf("nid %d: invalid lcluster size: %w", ino.nid, ErrInvalid)
		}
		if z.size > 0 {
			z.totalIdx = uint64((z.size + z.lclusterSize - 1) / z.lclusterSize)
		}
		return z, nil
	}

	z.header = zmapHeader{
		fragmentOffOrIData: binary.LittleEndian.Uint32(raw[0:4]),
		advise:             binary.LittleEndian.Uint16(raw[4:6]),
		algorithmType:      raw[6],
		clusterBits:        raw[7],
	}
	z.lclusterBits = z.header.clusterBits & 0x0f
	z.zLclusterBits = img.sb.BlkSizeBits + z.lclusterBits
	if z.zLclusterBits >= 31 {
		return nil, fmt.Errorf("nid %d: invalid lcluster bits %d: %w", ino.nid, z.zLclusterBits, ErrInvalid)
	}
	z.lclusterSize = int64(1) << z.zLclusterBits

	switch ino.inodeLayout {
	case disk.LayoutCompressedFull:
		z.format = zmapIndexLegacy
		if z.header.advise&zErofsAdviseExtents != 0 {
			return nil, fmt.Errorf("nid %d: compressed extent metadata is not supported: %w", ino.nid, ErrNotImplemented)
		}
	case disk.LayoutCompressedCompact:
		z.format = zmapIndexCompact
	default:
		return nil, fmt.Errorf("nid %d: invalid compressed layout %d: %w", ino.nid, ino.inodeLayout, ErrInvalid)
	}

	z.bigPcluster1 = z.header.advise&zErofsAdviseBigPcluster1 != 0
	z.bigPcluster2 = z.header.advise&zErofsAdviseBigPcluster2 != 0
	z.algoHead1 = z.header.algorithmType & 0x0f
	z.algoHead2 = (z.header.algorithmType >> 4) & 0x0f

	z.hasFragment = z.header.advise&zErofsAdviseFragment != 0
	z.interlaced = z.header.advise&zErofsAdviseInterlaced != 0
	// The inline-tail idata size shares the h_fragmentoff union slot with
	// the fragment offset; fragments take precedence.
	if !z.hasFragment && z.header.advise&zErofsAdviseInline != 0 {
		z.idataSize = int(z.header.idataSize())
		z.hasInline = z.idataSize > 0
	}

	if z.size > 0 {
		z.totalIdx = uint64((z.size + z.lclusterSize - 1) / z.lclusterSize)
	}
	z.ebase = headerOff + 8
	switch z.format {
	case zmapIndexLegacy:
		z.indexOff = z.ebase + zErofsFullIndexGap
	case zmapIndexCompact:
		z.indexOff = z.ebase
		initial := ((32 - (z.ebase % 32)) / 4) & 7
		z.compact4bInitial = uint64(initial)
		if z.header.advise&zErofsAdviseCompacted2B != 0 && z.compact4bInitial < z.totalIdx {
			z.compact2b = ((z.totalIdx - z.compact4bInitial) / 16) * 16
		}
	}
	return z, nil
}

// fragmentOff stores the full 64-bit fragment offset for the whole-inode
// fragment case. For the ordinary 32-bit h_fragmentoff case it stays zero and
// the low 32 bits are read from the header directly.
var _ = math.MaxUint32

func (z *zmap) lclusterCount() uint64 { return z.totalIdx }

func (z *zmap) readLcluster(i uint64) (zclusterEntry, error) {
	if i >= z.totalIdx {
		return zclusterEntry{}, fmt.Errorf("lcluster %d out of range (count %d): %w", i, z.totalIdx, ErrInvalid)
	}
	if z.format == zmapIndexLegacy {
		return z.readLegacyLcluster(i)
	}
	return z.readCompactLcluster(i)
}

func (z *zmap) readLegacyLcluster(i uint64) (zclusterEntry, error) {
	var buf [zErofsLclusterIndexSize]byte
	off := z.indexOff + int64(i)*zErofsLclusterIndexSize
	if _, err := z.img.meta.ReadAt(buf[:], off); err != nil {
		return zclusterEntry{}, fmt.Errorf("reading lcluster %d at %d: %w", i, off, err)
	}
	advise := binary.LittleEndian.Uint16(buf[0:2])
	clusterofs := binary.LittleEndian.Uint16(buf[2:4])
	u := binary.LittleEndian.Uint32(buf[4:8])

	e := zclusterEntry{
		typ:        uint8(advise & 0x3),
		clusterofs: clusterofs,
	}
	switch e.typ {
	case zErofsLclusterTypePlain, zErofsLclusterTypeHead1, zErofsLclusterTypeHead2:
		e.pblk = u
		e.partialRef = advise&zErofsLIPartialRef != 0
		e.hole = advise&zErofsLIHole != 0
		if int64(e.clusterofs) >= z.lclusterSize {
			return zclusterEntry{}, fmt.Errorf("lcluster %d: clusterofs %d >= lcluster size %d: %w",
				i, e.clusterofs, z.lclusterSize, ErrInvalid)
		}
	case zErofsLclusterTypeNonhead:
		d0 := uint32(u & 0xffff)
		if d0&zErofsLID0Cblkcnt != 0 {
			if !z.bigPcluster1 && !z.bigPcluster2 {
				return zclusterEntry{}, fmt.Errorf("lcluster %d: CBLKCNT without BIG_PCLUSTER: %w", i, ErrInvalid)
			}
			e.hasCblkcnt = true
			e.cblkcnt = d0 &^ zErofsLID0Cblkcnt
			e.delta0 = 1
		} else {
			e.delta0 = uint16(d0)
		}
		e.delta1 = uint16(u >> 16)
	default:
		return zclusterEntry{}, fmt.Errorf("lcluster %d: invalid cluster type %d: %w", i, e.typ, ErrInvalid)
	}
	return e, nil
}

func (z *zmap) compactPack(amortizedShift int) (compactPack, error) {
	lobits := int(z.zLclusterBits)
	if lobits < 12 {
		lobits = 12
	}
	if lobits >= 32 {
		return compactPack{}, fmt.Errorf("invalid compact lobits %d: %w", lobits, ErrInvalid)
	}
	switch amortizedShift {
	case 2:
		if z.zLclusterBits > 14 {
			return compactPack{}, fmt.Errorf("lcluster bits %d too large for compacted-4B: %w", z.zLclusterBits, ErrNotImplemented)
		}
		return compactPack{packBytes: 8, vcnt: 2, encodeBits: 16, lobits: lobits}, nil
	case 1:
		if z.zLclusterBits > 12 {
			return compactPack{}, fmt.Errorf("lcluster bits %d too large for compacted-2B: %w", z.zLclusterBits, ErrNotImplemented)
		}
		return compactPack{packBytes: 32, vcnt: 16, encodeBits: 14, lobits: lobits}, nil
	default:
		return compactPack{}, fmt.Errorf("invalid compact amortized shift %d: %w", amortizedShift, ErrInvalid)
	}
}

// compactLocation returns the on-disk byte offset of the pack containing
// lcluster lcn, that pack's geometry, and the intra-pack index.
func (z *zmap) compactLocation(lcn uint64) (int64, compactPack, int, error) {
	pos := z.ebase
	l := lcn
	amortizedShift := 2
	if l >= z.compact4bInitial {
		pos += int64(z.compact4bInitial * 4)
		l -= z.compact4bInitial
		if l < z.compact2b {
			amortizedShift = 1
		} else {
			pos += int64(z.compact2b * 2)
			l -= z.compact2b
		}
	}
	pos += int64(l) << amortizedShift

	p, err := z.compactPack(amortizedShift)
	if err != nil {
		return 0, compactPack{}, 0, err
	}
	packStart := pos - pos%p.packBytes
	idx := int((pos - packStart) >> amortizedShift)
	return packStart, p, idx, nil
}

func decodeCompactEntry(pack []byte, p compactPack, idx int) (uint8, uint32, error) {
	if idx < 0 || idx >= p.vcnt {
		return 0, 0, fmt.Errorf("compact entry index %d out of range %d: %w", idx, p.vcnt, ErrInvalid)
	}
	bitPos := p.encodeBits * idx
	bytePos := bitPos / 8
	shift := bitPos % 8
	if bytePos+4 > len(pack) {
		return 0, 0, fmt.Errorf("compact bit window %d out of range: %w", bytePos, ErrInvalid)
	}
	v := binary.LittleEndian.Uint32(pack[bytePos:bytePos+4]) >> shift
	lo := v & ((1 << p.lobits) - 1)
	typ := uint8((v >> p.lobits) & 0x3)
	return typ, lo, nil
}

func (z *zmap) readCompactPack(packStart int64, p compactPack) ([]byte, uint32, error) {
	buf := make([]byte, p.packBytes)
	if _, err := z.img.meta.ReadAt(buf, packStart); err != nil {
		return nil, 0, fmt.Errorf("reading compact pack at %d: %w", packStart, err)
	}
	base := binary.LittleEndian.Uint32(buf[p.packBytes-4:])
	return buf, base, nil
}

func (z *zmap) readCompactLcluster(i uint64) (zclusterEntry, error) {
	packStart, p, idx, err := z.compactLocation(i)
	if err != nil {
		return zclusterEntry{}, err
	}
	pack, base, err := z.readCompactPack(packStart, p)
	if err != nil {
		return zclusterEntry{}, err
	}
	typ, lo, err := decodeCompactEntry(pack, p, idx)
	if err != nil {
		return zclusterEntry{}, err
	}
	e := zclusterEntry{typ: typ}
	switch typ {
	case zErofsLclusterTypePlain, zErofsLclusterTypeHead1, zErofsLclusterTypeHead2:
		e.clusterofs = uint16(lo)
		nblk, err := z.compactNblk(pack, p, idx)
		if err != nil {
			return zclusterEntry{}, err
		}
		if uint64(base)+uint64(nblk) > math.MaxUint32 {
			return zclusterEntry{}, fmt.Errorf("lcluster %d: compact pcluster address overflow: %w", i, ErrInvalid)
		}
		e.pblk = base + nblk
	case zErofsLclusterTypeNonhead:
		if lo&zErofsLID0Cblkcnt != 0 {
			if !z.bigPcluster1 {
				return zclusterEntry{}, fmt.Errorf("lcluster %d: CBLKCNT without BIG_PCLUSTER_1: %w", i, ErrInvalid)
			}
			e.hasCblkcnt = true
			e.cblkcnt = lo &^ zErofsLID0Cblkcnt
			e.delta0 = 1
			return e, nil
		}
		if idx+1 != p.vcnt {
			e.delta0 = uint16(lo)
			return e, nil
		}
		// The last entry of a pack stores delta[1] in lo rather than
		// delta[0]. Derive delta[0] from the previous entry.
		if idx == 0 {
			return zclusterEntry{}, fmt.Errorf("lcluster %d: compact pack has no previous entry: %w", i, ErrInvalid)
		}
		prevType, prevLo, err := decodeCompactEntry(pack, p, idx-1)
		if err != nil {
			return zclusterEntry{}, err
		}
		derived := prevLo
		if prevType != zErofsLclusterTypeNonhead {
			derived = 0
		} else if prevLo&zErofsLID0Cblkcnt != 0 {
			derived = 1
		}
		if derived == math.MaxUint32 {
			return zclusterEntry{}, fmt.Errorf("lcluster %d: compact delta0 overflow: %w", i, ErrInvalid)
		}
		e.delta0 = uint16(derived + 1)
	default:
		return zclusterEntry{}, fmt.Errorf("lcluster %d: invalid compact cluster type %d: %w", i, typ, ErrInvalid)
	}
	return e, nil
}

// compactNblk implements the running nblk arithmetic used to derive a
// HEAD/PLAIN pcluster's block address from its pack base.
func (z *zmap) compactNblk(pack []byte, p compactPack, idx int) (uint32, error) {
	if !z.bigPcluster1 {
		nblk := uint32(1)
		i := idx
		for i > 0 {
			i--
			typ, lo, err := decodeCompactEntry(pack, p, i)
			if err != nil {
				return 0, err
			}
			if typ == zErofsLclusterTypeNonhead {
				if lo&zErofsLID0Cblkcnt != 0 {
					return 0, fmt.Errorf("CBLKCNT marker without BIG_PCLUSTER: %w", ErrInvalid)
				}
				i -= int(lo)
			}
			if i >= 0 {
				nblk++
			}
		}
		return nblk, nil
	}

	nblk := uint32(0)
	i := idx
	for i > 0 {
		i--
		typ, lo, err := decodeCompactEntry(pack, p, i)
		if err != nil {
			return 0, err
		}
		if typ == zErofsLclusterTypeNonhead {
			if lo&zErofsLID0Cblkcnt != 0 {
				i--
				nblk += lo &^ zErofsLID0Cblkcnt
				continue
			}
			if lo <= 1 {
				return 0, fmt.Errorf("big-pcluster NONHEAD with delta0 %d: %w", lo, ErrInvalid)
			}
			i -= int(lo - 2)
			continue
		}
		nblk++
	}
	return nblk, nil
}

// nextPackOffset returns the byte offset immediately following the pack that
// contains lcluster i. It is the start of inline tail-packing data.
func (z *zmap) nextPackOffset(i uint64) (int64, error) {
	if z.format == zmapIndexLegacy {
		return z.indexOff + int64(i)*zErofsLclusterIndexSize + zErofsLclusterIndexSize, nil
	}
	packStart, p, _, err := z.compactLocation(i)
	if err != nil {
		return 0, err
	}
	return packStart + p.packBytes, nil
}

// resolveExtent maps a file offset to the physical cluster that owns it,
// without applying inline-tail or fragment redirection.
func (z *zmap) resolveExtent(off int64) (compressedExtent, error) {
	if off < 0 || off >= z.size {
		return compressedExtent{}, fmt.Errorf("file offset %d out of range [0,%d): %w", off, z.size, ErrInvalid)
	}
	lcn := uint64(off) >> z.zLclusterBits
	endoff := uint64(off) & uint64(z.lclusterSize-1)
	e, err := z.readLcluster(lcn)
	if err != nil {
		return compressedExtent{}, err
	}

	var head zclusterEntry
	var headLCN uint64
	if e.typ != zErofsLclusterTypeNonhead && endoff >= uint64(e.clusterofs) {
		head = e
		headLCN = lcn
	} else {
		if e.typ != zErofsLclusterTypeNonhead {
			e.delta0 = 1
		}
		headLCN, head, err = z.lookback(lcn, e)
		if err != nil {
			return compressedExtent{}, err
		}
	}
	start := int64(headLCN<<z.zLclusterBits) + int64(head.clusterofs)
	end, err := z.extentSourceEnd(headLCN)
	if err != nil {
		return compressedExtent{}, err
	}
	if start > off || off >= end {
		return compressedExtent{}, fmt.Errorf("nid %d: extent [%d,%d) does not contain offset %d: %w",
			z.nid, start, end, off, ErrInvalid)
	}
	if !head.isHeadOrPlain() {
		return compressedExtent{}, fmt.Errorf("nid %d: lcluster %d resolves to invalid head type %d: %w",
			z.nid, headLCN, head.typ, ErrInvalid)
	}

	ext := compressedExtent{
		pblk:           head.pblk,
		sourceStart:    start,
		sourceEnd:      end,
		headLcluster:   headLCN,
		clusterType:    head.typ,
		headClusterofs: head.clusterofs,
		partialRef:     head.partialRef,
		hole:           head.hole,
	}

	switch head.typ {
	case zErofsLclusterTypePlain:
		ext.plain = true
		ext.interlaced = z.interlaced
	case zErofsLclusterTypeHead1:
		algo, err := compressionAlgorithmFromID(z.algoHead1)
		if err != nil {
			return compressedExtent{}, maybeNotImplemented(err)
		}
		ext.algo = algo
	case zErofsLclusterTypeHead2:
		algo, err := compressionAlgorithmFromID(z.algoHead2)
		if err != nil {
			return compressedExtent{}, maybeNotImplemented(err)
		}
		ext.algo = algo
	}

	if ext.hole {
		return ext, nil
	}

	blocks, err := z.pclusterBlocks(headLCN, head)
	if err != nil {
		return compressedExtent{}, err
	}
	if blocks == 0 {
		ext.hole = true
		return ext, nil
	}
	ext.pclusterBlocks = blocks
	return ext, nil
}

// extentSourceEnd walks forward from the head lcluster to the next
// HEAD/PLAIN entry (or EOF) to find where this physical cluster's source
// bytes end. One pcluster can cover many logical clusters.
func (z *zmap) extentSourceEnd(headLCN uint64) (int64, error) {
	end := z.size
	for i := headLCN + 1; i < z.totalIdx; i++ {
		e, err := z.readLcluster(i)
		if err != nil {
			return 0, err
		}
		if e.isHeadOrPlain() {
			end = int64(i<<z.zLclusterBits) + int64(e.clusterofs)
			break
		}
	}
	if end > z.size {
		end = z.size
	}
	return end, nil
}

func (z *zmap) lookback(lcn uint64, e zclusterEntry) (uint64, zclusterEntry, error) {
	dist := e.delta0
	if dist == 0 {
		return 0, zclusterEntry{}, fmt.Errorf("nid %d: NONHEAD lcluster %d has zero delta0: %w", z.nid, lcn, ErrInvalid)
	}
	cur := lcn
	for cur >= uint64(dist) {
		cur -= uint64(dist)
		he, err := z.readLcluster(cur)
		if err != nil {
			return 0, zclusterEntry{}, err
		}
		if he.typ == zErofsLclusterTypeNonhead {
			if he.delta0 == 0 {
				return 0, zclusterEntry{}, fmt.Errorf("nid %d: NONHEAD lcluster %d has zero delta0: %w", z.nid, cur, ErrInvalid)
			}
			dist = he.delta0
			continue
		}
		return cur, he, nil
	}
	return 0, zclusterEntry{}, fmt.Errorf("nid %d: NONHEAD lcluster %d delta0 %d crosses lcluster 0: %w", z.nid, lcn, dist, ErrInvalid)
}

// pclusterBlocks returns the on-disk size of the physical cluster in blocks.
// A zero return means the BIG_PCLUSTER marker explicitly described a hole.
func (z *zmap) pclusterBlocks(headLCN uint64, head zclusterEntry) (uint64, error) {
	useBig := false
	switch head.typ {
	case zErofsLclusterTypeHead1:
		useBig = z.bigPcluster1
	case zErofsLclusterTypeHead2, zErofsLclusterTypePlain:
		useBig = z.bigPcluster2
	}
	if !useBig {
		return 1, nil
	}
	nextLCN := headLCN + 1
	if int64(nextLCN<<z.zLclusterBits) >= z.size {
		return 1, nil
	}
	next, err := z.readLcluster(nextLCN)
	if err != nil {
		return 0, err
	}
	if next.typ != zErofsLclusterTypeNonhead {
		return 1, nil
	}
	if next.delta0 != 1 || !next.hasCblkcnt {
		return 0, fmt.Errorf("nid %d: invalid BIG_PCLUSTER marker at lcluster %d: %w", z.nid, nextLCN, ErrInvalid)
	}
	return uint64(next.cblkcnt), nil
}

func (z *zmap) headerAlgorithm(typ uint8) (compressionAlgorithm, error) {
	switch typ {
	case zErofsLclusterTypeHead1:
		return compressionAlgorithmFromID(z.algoHead1)
	case zErofsLclusterTypeHead2:
		return compressionAlgorithmFromID(z.algoHead2)
	default:
		return 0, fmt.Errorf("lcluster type %d has no compression algorithm: %w", typ, ErrInvalid)
	}
}

func maybeNotImplemented(err error) error {
	return fmt.Errorf("%w", err)
}

// compressedExtent describes one physical cluster's mapping into the
// decompressed logical file.
type compressedExtent struct {
	nid            uint64
	pblk           uint32
	pclusterBlocks uint64
	sourceStart    int64
	sourceEnd      int64
	headLcluster   uint64
	clusterType    uint8
	headClusterofs uint16
	algo           compressionAlgorithm
	plain          bool
	hole           bool
	partialRef     bool
	inline         bool
	inlineOffset   int64
	inlineSize     int
	fragment       bool
	fragmentOffset int64
	interlaced     bool
}
