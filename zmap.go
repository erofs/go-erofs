package erofs

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/erofs/go-erofs/internal/disk"
)

// zmapState is the per-inode compression metadata, lazily populated on the
// first compressed read. It mirrors the subset of struct erofs_inode (the
// kernel's z_erofs_fill_inode_lazy output) needed to resolve a logical
// offset to a physical pcluster.
type zmapState struct {
	once         sync.Once
	err          error
	advise       uint16
	lclusterBits uint8
	algoType     [2]uint8 // [0]=HEAD1 algorithm, [1]=HEAD2 algorithm
	// indexStart is the byte offset (in img.meta) of the first lcluster index.
	// For COMPRESSED_FULL this is Z_EROFS_FULL_INDEX_START(end);
	// for COMPRESSED_COMPACT it is round_up(end,8) + sizeof(map_header).
	indexStart int64
	// totalLcn is the total number of logical clusters for this inode.
	totalLcn uint32

	// Single-extent cache for sequential reads. cachedBuf holds the
	// decompressed bytes for the extent at cachedExt; cachedValid is true
	// when populated. Guarded by cacheMu.
	cacheMu     sync.Mutex
	cachedExt   zextent
	cachedBuf   []byte
	cachedValid bool
}

// zextent describes a single contiguous decompression extent backing a
// logical region of the file.
type zextent struct {
	logicalStart  int64 // start of decompressed extent (lcn << lclusterBits + clusterofs)
	logicalSize   int64 // decompressed size in bytes
	physicalStart int64 // physical byte address (block * blocksize)
	physicalSize  int64 // compressed size in bytes
	algorithm     uint8 // disk.ZErofsCompressionLZ4 etc.
	plain         bool  // PLAIN lcluster type — copy bytes through, no decompression
}

// maprec mirrors the kernel's z_erofs_maprecorder for a single lcluster.
type maprec struct {
	lcn        uint32
	typ        uint8 // Z_EROFS_LCLUSTER_TYPE_*
	clusterOfs uint16
	pblk       uint32
	delta      [2]uint16
	// compressedBlks is set when CBLKCNT is decoded on a NONHEAD lcluster;
	// it carries the on-disk block count of the preceding pcluster.
	compressedBlks uint32
	partialRef     bool
	headType       uint8 // set after a successful lookback walk
}

// zmapInit reads the inode's z_erofs_map_header and validates that the
// inode uses only features we currently support.
func (img *image) zmapInit(fi *inode) *zmapState {
	if fi.zmap == nil {
		fi.zmap = &zmapState{}
	}
	z := fi.zmap
	z.once.Do(func() {
		z.err = img.zmapInitLocked(fi, z)
	})
	return z
}

func (img *image) zmapInitLocked(fi *inode, z *zmapState) error {
	inodeStart := img.metaStartPos() + int64(fi.nid*disk.SizeInodeCompact)
	hdrPos := alignUp8(inodeStart + fi.flatDataOffset())

	var hdrBuf [disk.SizeZErofsMapHeader]byte
	if _, err := img.meta.ReadAt(hdrBuf[:], hdrPos); err != nil {
		return fmt.Errorf("read z_erofs_map_header at %d: %w", hdrPos, err)
	}
	var h disk.ZErofsMapHeader
	if _, err := binary.Decode(hdrBuf[:], binary.LittleEndian, &h); err != nil {
		return fmt.Errorf("decode z_erofs_map_header: %w", err)
	}

	// Reject features this implementation does not support.
	//
	// Compacted2B is the layout hint for the compact lcluster index — it
	// just says the index uses a 2-byte-per-entry run in addition to the
	// 4-byte run, and the compact decoder consults it directly.
	// BigPcluster1 enables multi-block pclusters for HEAD1 lclusters,
	// signalled per-pcluster via the CBLKCNT marker on the first NONHEAD.
	const acceptedAdvise = uint16(disk.ZErofsAdviseCompacted2B | disk.ZErofsAdviseBigPcluster1)
	if unsupported := h.HAdvise &^ acceptedAdvise; unsupported != 0 {
		// Map specific bits to clearer messages.
		switch {
		case h.HAdvise&disk.ZErofsAdviseFragmentPcluster != 0:
			return fmt.Errorf("fragment pcluster: %w", ErrNotImplemented)
		case h.HAdvise&disk.ZErofsAdviseInlinePcluster != 0:
			return fmt.Errorf("inline pcluster (tail-packing): %w", ErrNotImplemented)
		case h.HAdvise&disk.ZErofsAdviseBigPcluster2 != 0:
			return fmt.Errorf("big pcluster 2 (HEAD2): %w", ErrNotImplemented)
		case h.HAdvise&disk.ZErofsAdviseInterlacedPcluster != 0:
			return fmt.Errorf("interlaced pcluster: %w", ErrNotImplemented)
		}
		return fmt.Errorf("unsupported h_advise bits 0x%x: %w", unsupported, ErrNotImplemented)
	}
	// Mirror the kernel's consistency check (fs/erofs/zmap.c): BIG_PCLUSTER_1
	// in h_advise must agree with the superblock's BIG_PCLUSTER feature bit.
	// An image that sets the per-inode bit without the sb feature is rejected
	// by the kernel as -EFSCORRUPTED; reject it here too so writers that
	// forget the sb bit fail loudly rather than silently producing
	// kernel-unreadable images.
	if h.HAdvise&disk.ZErofsAdviseBigPcluster1 != 0 &&
		img.sb.FeatureIncompat&disk.FeatureIncompatBigPcluster == 0 {
		return fmt.Errorf("BIG_PCLUSTER_1 advise without sb feature bit for nid %d: %w",
			fi.nid, ErrInvalid)
	}
	// COMPRESSED_COMPACT + BIG_PCLUSTER_1 needs a separate pblk loop the
	// compact decoder doesn't yet implement (see fs/erofs/zmap.c:212-233).
	// Reject the combination up front rather than failing mid-read.
	if fi.inodeLayout == disk.LayoutCompressedCompact &&
		h.HAdvise&disk.ZErofsAdviseBigPcluster1 != 0 {
		return fmt.Errorf("big pcluster in compact layout: %w", ErrNotImplemented)
	}

	// h_clusterbits high bit signals whole-file fragment storage; reject.
	if h.ClusterBits>>7 != 0 {
		return fmt.Errorf("packed-inode fragment: %w", ErrNotImplemented)
	}

	z.advise = h.HAdvise
	z.lclusterBits = img.sb.BlkSizeBits + (h.ClusterBits & 0xf)
	z.algoType[0] = h.AlgorithmType & 0xf
	z.algoType[1] = h.AlgorithmType >> 4
	if z.algoType[0] >= disk.ZErofsCompressionMax || z.algoType[1] >= disk.ZErofsCompressionMax {
		return fmt.Errorf("algorithm types %d/%d: %w", z.algoType[0], z.algoType[1], ErrNotImplemented)
	}

	switch fi.inodeLayout {
	case disk.LayoutCompressedFull:
		// Lcluster index starts at MAP_HEADER_END(end) + 8 = round_up(end,8)+16.
		z.indexStart = hdrPos + disk.SizeZErofsMapHeader + 8
	case disk.LayoutCompressedCompact:
		// Compact bit-packed entries start right after the map header.
		z.indexStart = hdrPos + disk.SizeZErofsMapHeader
		// lclusterbits is bounded by the spec; > 14 is invalid encoding,
		// not a feature gap.
		if z.lclusterBits > 14 {
			return fmt.Errorf("compact lclusterbits %d > 14: %w", z.lclusterBits, ErrInvalid)
		}
	default:
		return fmt.Errorf("inode layout %d is not compressed: %w", fi.inodeLayout, ErrInvalid)
	}

	z.totalLcn = uint32((fi.size + (1 << z.lclusterBits) - 1) >> z.lclusterBits)
	return nil
}

// loadLcluster reads the lcn-th lcluster index entry for fi into m. lookahead
// requests that compact mode also fill m.delta[1] for the extent's end.
func (img *image) loadLcluster(fi *inode, z *zmapState, lcn uint32, lookahead bool, m *maprec) error {
	if lcn >= z.totalLcn {
		return fmt.Errorf("lcn %d out of range (total %d): %w", lcn, z.totalLcn, ErrInvalid)
	}
	switch fi.inodeLayout {
	case disk.LayoutCompressedFull:
		return img.loadFullLcluster(z, lcn, m)
	case disk.LayoutCompressedCompact:
		return img.loadCompactLcluster(z, lcn, lookahead, m)
	}
	return fmt.Errorf("inode layout %d: %w", fi.inodeLayout, ErrInvalid)
}

func (img *image) loadFullLcluster(z *zmapState, lcn uint32, m *maprec) error {
	pos := z.indexStart + int64(lcn)*disk.SizeZErofsLclusterIndex
	var buf [disk.SizeZErofsLclusterIndex]byte
	if _, err := img.meta.ReadAt(buf[:], pos); err != nil {
		return fmt.Errorf("read lcluster %d at %d: %w", lcn, pos, err)
	}
	var li disk.ZErofsLclusterIndex
	if _, err := binary.Decode(buf[:], binary.LittleEndian, &li); err != nil {
		return fmt.Errorf("decode lcluster %d: %w", lcn, err)
	}

	m.lcn = lcn
	m.compressedBlks = 0
	m.partialRef = false
	advise := li.DiAdvise
	m.typ = uint8(advise & disk.ZErofsLclusterTypeMask)
	if m.typ == disk.ZErofsLclusterTypeNonhead {
		m.clusterOfs = uint16(1 << z.lclusterBits)
		m.delta[0] = uint16(li.DiU & 0xFFFF)
		m.delta[1] = uint16(li.DiU >> 16)
		if m.delta[0]&disk.ZErofsLiD0CblkCnt != 0 {
			// Big-pcluster CBLKCNT: delta[0] carries the on-disk block
			// count of the preceding pcluster (low 11 bits) and the
			// implicit lookback distance to the HEAD is 1.
			if z.advise&disk.ZErofsAdviseBigPcluster1 == 0 {
				return fmt.Errorf("CBLKCNT without BIG_PCLUSTER_1 advise: %w", ErrInvalid)
			}
			m.compressedBlks = uint32(m.delta[0]) &^ disk.ZErofsLiD0CblkCnt
			m.delta[0] = 1
		}
	} else {
		m.partialRef = advise&disk.ZErofsLiPartialRef != 0
		m.clusterOfs = li.DiClusterOfs
		m.pblk = li.DiU
		m.delta[0] = 0
		m.delta[1] = 0
		if m.clusterOfs >= 1<<z.lclusterBits {
			return fmt.Errorf("lcn %d: clusterofs %d > lcluster size: %w", lcn, m.clusterOfs, ErrInvalid)
		}
	}
	return nil
}

// loadCompactLcluster mirrors z_erofs_load_compact_lcluster in erofs-utils.
// Compact mode bit-packs lcluster entries: 4-byte initial alignment slots
// (compact_4b) followed by an optional run of 2-byte entries (compact_2b),
// then continuing compact_4b. Each pack has a trailing 32-bit field that
// stores the base pblk for HEAD lclusters within the pack.
func (img *image) loadCompactLcluster(z *zmapState, lcn uint32, lookahead bool, m *maprec) error {
	ebase := z.indexStart
	lclusterBits := z.lclusterBits

	m.lcn = lcn
	m.compressedBlks = 0
	m.partialRef = false

	// compacted_4b_initial aligns the start of the compact_2b run (if any)
	// to a 32-byte boundary measured from disk position 0.
	compacted4bInitial := ((32 - uint32(ebase%32)) / 4) & 7
	var compacted2b uint32
	if z.advise&disk.ZErofsAdviseCompacted2B != 0 && compacted4bInitial < z.totalLcn {
		compacted2b = (z.totalLcn - compacted4bInitial) &^ 15
	}

	pos := ebase
	amortShift := uint8(2) // log2(byte size of one packed entry) — start at 4B
	if lcn >= compacted4bInitial {
		pos += int64(compacted4bInitial) * 4
		lcn -= compacted4bInitial
		if lcn < compacted2b {
			amortShift = 1
		} else {
			pos += int64(compacted2b) * 2
			lcn -= compacted2b
		}
	}
	pos += int64(lcn) << amortShift

	var vcnt uint32
	switch {
	case amortShift == 2 && lclusterBits <= 14:
		vcnt = 2
	case amortShift == 1 && lclusterBits <= 12:
		vcnt = 16
	default:
		return fmt.Errorf("compact layout vcnt for amortShift=%d lclusterBits=%d: %w",
			amortShift, lclusterBits, ErrNotImplemented)
	}

	packBytes := vcnt << amortShift
	packStart := pos - (pos & int64(packBytes-1))
	pack := make([]byte, packBytes)
	if _, err := img.meta.ReadAt(pack, packStart); err != nil {
		return fmt.Errorf("read compact pack at %d: %w", packStart, err)
	}

	lobits := uint(lclusterBits)
	if cblkBits := uint(12); lobits < cblkBits {
		lobits = cblkBits // ilog2(Z_EROFS_LI_D0_CBLKCNT)+1 = ilog2(2048)+1 = 12
	}
	encodebits := ((uint(packBytes) - 4) * 8) / uint(vcnt)
	i := uint32(pos-packStart) >> amortShift

	lo, typ := decodeCompactedBits(lobits, pack, encodebits*uint(i))
	m.typ = typ
	if typ == disk.ZErofsLclusterTypeNonhead {
		m.clusterOfs = uint16(1 << lclusterBits)
		if lookahead {
			m.delta[1] = uint16(getCompactedLaDistance(lobits, encodebits, vcnt, pack, i))
		}
		if lo&disk.ZErofsLiD0CblkCnt != 0 {
			return fmt.Errorf("big-pcluster CBLKCNT: %w", ErrNotImplemented)
		}
		if i+1 != vcnt {
			m.delta[0] = uint16(lo)
			return nil
		}
		// Last entry in the pack: lo encodes delta[1]. Walk back one step.
		lo2, typ2 := decodeCompactedBits(lobits, pack, encodebits*uint(i-1))
		if typ2 != disk.ZErofsLclusterTypeNonhead {
			lo2 = 0
		} else if lo2&disk.ZErofsLiD0CblkCnt != 0 {
			lo2 = 1
		}
		m.delta[0] = uint16(lo2 + 1)
		return nil
	}

	m.clusterOfs = uint16(lo)
	m.delta[0] = 0
	if m.clusterOfs >= 1<<lclusterBits {
		return fmt.Errorf("compact lcn: clusterofs %d > lcluster size: %w", m.clusterOfs, ErrInvalid)
	}

	// Compute the HEAD pblk for entry i within the pack.
	//
	// Mirrors the non-big-pcluster branch of kernel z_erofs_load_compact_lcluster
	// (fs/erofs/zmap.c:200-211). The pack's trailing 32-bit field stores
	// `first_HEAD_pblk - 1`, not the first HEAD's pblk directly — i.e. the
	// formula is `pblk_for_entry_i = base + nblk` where nblk starts at 1
	// and increments once per HEAD-lcluster predecessor (NONHEADs jump
	// back to the HEAD they reference). For i == 0 (we are the first HEAD)
	// the loop never runs, nblk stays at 1, and we recover the first HEAD's
	// real pblk = base + 1.
	var nblk uint32 = 1
	for j := int(i) - 1; j >= 0; j-- {
		lo2, typ2 := decodeCompactedBits(lobits, pack, encodebits*uint(j))
		if typ2 == disk.ZErofsLclusterTypeNonhead {
			j -= int(lo2)
		}
		if j >= 0 {
			nblk++
		}
	}
	pblk := binary.LittleEndian.Uint32(pack[packBytes-4:])
	m.pblk = pblk + nblk
	return nil
}

func decodeCompactedBits(lobits uint, in []byte, pos uint) (uint32, uint8) {
	byteOff := pos / 8
	// Read up to 4 bytes from byteOff and shift. Use a wider read to handle
	// alignment, but guard the slice length.
	var v uint32
	switch {
	case byteOff+4 <= uint(len(in)):
		v = binary.LittleEndian.Uint32(in[byteOff:])
	case byteOff+3 <= uint(len(in)):
		v = uint32(in[byteOff]) | uint32(in[byteOff+1])<<8 | uint32(in[byteOff+2])<<16
	case byteOff+2 <= uint(len(in)):
		v = uint32(in[byteOff]) | uint32(in[byteOff+1])<<8
	case byteOff+1 <= uint(len(in)):
		v = uint32(in[byteOff])
	}
	v >>= pos & 7
	lo := v & ((1 << lobits) - 1)
	typ := uint8((v >> lobits) & 3)
	return lo, typ
}

func getCompactedLaDistance(lobits, encodebits uint, vcnt uint32, in []byte, i uint32) uint32 {
	var d1 uint32
	var lo uint32
	for j := i; j < vcnt; j++ {
		l, typ := decodeCompactedBits(lobits, in, encodebits*uint(j))
		lo = l
		if typ != disk.ZErofsLclusterTypeNonhead {
			return d1
		}
		d1++
	}
	if lo&disk.ZErofsLiD0CblkCnt == 0 {
		d1 += lo - 1
	}
	return d1
}

// extentLookback walks back through NONHEAD lclusters until a HEAD is found.
// On success m points at the HEAD lcluster and m.headType is set.
func (img *image) extentLookback(fi *inode, z *zmapState, m *maprec, distance uint16) error {
	for distance > 0 && m.lcn >= uint32(distance) {
		lcn := m.lcn - uint32(distance)
		if err := img.loadLcluster(fi, z, lcn, false, m); err != nil {
			return err
		}
		switch m.typ {
		case disk.ZErofsLclusterTypeNonhead:
			distance = m.delta[0]
			if distance == 0 {
				return fmt.Errorf("zero lookback at lcn %d: %w", lcn, ErrInvalid)
			}
		case disk.ZErofsLclusterTypePlain,
			disk.ZErofsLclusterTypeHead1,
			disk.ZErofsLclusterTypeHead2:
			m.headType = m.typ
			return nil
		default:
			return fmt.Errorf("unknown lcluster type %d at lcn %d: %w", m.typ, lcn, ErrInvalid)
		}
	}
	return fmt.Errorf("bad lookback distance %d at lcn %d: %w", distance, m.lcn, ErrInvalid)
}

// zmapLookup resolves a logical file offset to its containing pcluster.
// The returned extent's logical range covers a single HEAD pcluster's worth
// of decompressed data.
func (img *image) zmapLookup(fi *inode, ofs int64) (zextent, error) {
	z := img.zmapInit(fi)
	if z.err != nil {
		return zextent{}, z.err
	}
	if ofs < 0 || ofs >= fi.size {
		return zextent{}, fmt.Errorf("offset %d out of range: %w", ofs, ErrInvalid)
	}

	lclusterBits := z.lclusterBits
	initialLcn := uint32(ofs >> lclusterBits)
	endoff := uint32(ofs & ((1 << lclusterBits) - 1))

	var m maprec
	if err := img.loadLcluster(fi, z, initialLcn, false, &m); err != nil {
		return zextent{}, err
	}

	switch m.typ {
	case disk.ZErofsLclusterTypePlain,
		disk.ZErofsLclusterTypeHead1,
		disk.ZErofsLclusterTypeHead2:
		if endoff >= uint32(m.clusterOfs) {
			m.headType = m.typ
			break
		}
		if m.lcn == 0 {
			return zextent{}, fmt.Errorf("invalid lcluster 0 at nid %d: %w", fi.nid, ErrInvalid)
		}
		m.delta[0] = 1
		fallthrough
	case disk.ZErofsLclusterTypeNonhead:
		if err := img.extentLookback(fi, z, &m, m.delta[0]); err != nil {
			return zextent{}, err
		}
	default:
		return zextent{}, fmt.Errorf("unknown lcluster type %d: %w", m.typ, ErrInvalid)
	}

	headLcn := m.lcn
	headPblk := m.pblk
	clusterOfs := m.clusterOfs
	logicalStart := (int64(headLcn) << lclusterBits) | int64(clusterOfs)

	compressedBlks, err := img.extentCompressedLen(fi, z, &m)
	if err != nil {
		return zextent{}, err
	}

	// Walk forward via delta[1] to find the next HEAD/PLAIN (or EOF) — that
	// terminator's clusterofs marks the partial-tail end of this extent.
	// Single-lcluster pclusters get exactly one lcluster's worth; multi-
	// lcluster extents (big-pcluster or dedup'd) cover several.
	end, err := img.extentDecompressedEnd(fi, z, headLcn)
	if err != nil {
		return zextent{}, err
	}

	blockSize := int64(1) << img.sb.BlkSizeBits
	ext := zextent{
		logicalStart:  logicalStart,
		logicalSize:   end - logicalStart,
		physicalStart: int64(headPblk) << img.sb.BlkSizeBits,
		physicalSize:  int64(compressedBlks) * blockSize,
		plain:         m.headType == disk.ZErofsLclusterTypePlain,
	}
	if m.headType == disk.ZErofsLclusterTypeHead2 {
		ext.algorithm = z.algoType[1]
	} else {
		ext.algorithm = z.algoType[0]
	}
	// Clamp logicalSize to the file size — the last pcluster may produce
	// less than a full lcluster's worth of decompressed bytes.
	if ext.logicalStart+ext.logicalSize > fi.size {
		ext.logicalSize = fi.size - ext.logicalStart
	}
	return ext, nil
}

// extentDecompressedEnd walks the lcluster index forward from headLcn using
// delta[1] until it lands on a new HEAD/PLAIN (or runs off the end of the
// file). Returns the logical end byte offset of the current extent —
// i.e., (next_HEAD_lcn << lclusterBits) + next_HEAD.clusterofs. The
// terminating HEAD/PLAIN may carry a non-zero clusterofs (signalling the
// previous extent's partial tail), so we can't just round to a whole
// lcluster.
//
// Mirrors z_erofs_get_extent_decompressedlen in fs/erofs/zmap.c. The walk
// starts at headLcn+1 — zmapLookup has already loaded and classified the
// HEAD at headLcn, so re-reading it would be wasted I/O.
func (img *image) extentDecompressedEnd(fi *inode, z *zmapState, headLcn uint32) (int64, error) {
	lcn := headLcn + 1
	for {
		if int64(lcn)<<z.lclusterBits >= fi.size {
			return fi.size, nil
		}
		if lcn >= z.totalLcn {
			return int64(z.totalLcn) << z.lclusterBits, nil
		}
		var m maprec
		if err := img.loadLcluster(fi, z, lcn, true, &m); err != nil {
			return 0, err
		}
		switch m.typ {
		case disk.ZErofsLclusterTypeNonhead:
			d1 := m.delta[1]
			if d1 == 0 {
				d1 = 1 // workaround for older mkfs.erofs that wrote d1=0
			}
			lcn += uint32(d1)
		default:
			return int64(lcn)<<z.lclusterBits + int64(m.clusterOfs), nil
		}
	}
}

// extentCompressedLen returns the on-disk block count of the pcluster anchored
// at the HEAD lcluster in m. Mirrors z_erofs_get_extent_compressedlen in
// fs/erofs/zmap.c — for big-pcluster images the count is recovered from the
// CBLKCNT marker on the first NONHEAD lcluster that follows m.
func (img *image) extentCompressedLen(fi *inode, z *zmapState, m *maprec) (uint32, error) {
	// HEAD2 / PLAIN big-pcluster (BIG_PCLUSTER_2) isn't implemented in the
	// writer, and the reader rejects the advise bit, so anything other than
	// a HEAD1 anchored pcluster in big-pcluster mode is unambiguously 1 block.
	if z.advise&disk.ZErofsAdviseBigPcluster1 == 0 ||
		m.headType != disk.ZErofsLclusterTypeHead1 {
		return 1, nil
	}
	nextLcn := m.lcn + 1
	if int64(nextLcn)<<z.lclusterBits >= fi.size {
		// HEAD is the last lcluster of the file — exactly one block.
		return 1, nil
	}

	// Re-use the caller's maprec for the next lcluster; the caller doesn't
	// reuse m after this point in the lookup path.
	var next maprec
	if err := img.loadLcluster(fi, z, nextLcn, false, &next); err != nil {
		return 0, err
	}
	if next.typ == disk.ZErofsLclusterTypeNonhead && next.compressedBlks > 0 {
		return next.compressedBlks, nil
	}
	// The next lcluster is either a fresh HEAD/PLAIN (new pcluster) or a
	// NONHEAD without CBLKCNT — either way, the current pcluster is one
	// block. Matches the kernel's "if (m->type != NONHEAD || !compressedblks)"
	// fallback.
	return 1, nil
}

func alignUp8(v int64) int64 {
	return (v + 7) &^ 7
}

// readCompressed reads up to blockSize bytes of decompressed data starting at
// the given logical file position. It uses the per-inode pcluster cache so
// sequential reads pay decompression cost once per pcluster.
//
// The returned slice points into the inode's cache buffer; callers must not
// retain it across subsequent reads of the same inode.
func (img *image) readCompressed(fi *inode, pos int64) ([]byte, error) {
	z := img.zmapInit(fi)
	if z.err != nil {
		return nil, z.err
	}

	z.cacheMu.Lock()
	defer z.cacheMu.Unlock()

	if !z.cachedValid || pos < z.cachedExt.logicalStart ||
		pos >= z.cachedExt.logicalStart+z.cachedExt.logicalSize {
		ext, err := img.zmapLookup(fi, pos)
		if err != nil {
			return nil, err
		}
		if err := img.fillExtent(fi, &ext, z); err != nil {
			return nil, err
		}
	}

	offsetInExt := pos - z.cachedExt.logicalStart
	blockSize := int64(1) << img.sb.BlkSizeBits
	end := offsetInExt + blockSize
	if end > int64(len(z.cachedBuf)) {
		end = int64(len(z.cachedBuf))
	}
	return z.cachedBuf[offsetInExt:end], nil
}

// fillExtent reads the on-disk pcluster for ext and decompresses it into
// z.cachedBuf. Called with z.cacheMu held.
func (img *image) fillExtent(fi *inode, ext *zextent, z *zmapState) error {
	// Resolve physical address through the device table (extra device
	// support comes for free this way).
	reader, addr, err := img.mapDev(0, ext.physicalStart)
	if err != nil {
		return fmt.Errorf("map device for nid %d: %w", fi.nid, err)
	}

	if ext.physicalSize <= 0 || ext.physicalSize > disk.MaxPclusterSize {
		return fmt.Errorf("pcluster size %d out of range (max %d) for nid %d: %w",
			ext.physicalSize, disk.MaxPclusterSize, fi.nid, ErrInvalid)
	}
	src := make([]byte, ext.physicalSize)
	if _, err := reader.ReadAt(src, addr); err != nil {
		return fmt.Errorf("read pcluster at %d for nid %d: %w", addr, fi.nid, err)
	}

	// Trim LZ4_0Padding zero bytes when the feature flag is set. The
	// encoder may insert leading zero bytes within a pcluster block to
	// align the compressed payload; the LZ4 block decoder treats a
	// 0-prefix as no-op (length=0 literal/match), but trimming it
	// keeps decoded sizes predictable.
	if img.sb.FeatureIncompat&disk.FeatureIncompatLZ4_0Padding != 0 && !ext.plain {
		for len(src) > 0 && src[0] == 0 {
			src = src[1:]
		}
	}

	if cap(z.cachedBuf) < int(ext.logicalSize) {
		z.cachedBuf = make([]byte, ext.logicalSize)
	} else {
		z.cachedBuf = z.cachedBuf[:ext.logicalSize]
	}

	if ext.plain {
		// PLAIN lcluster type: the on-disk bytes ARE the logical bytes.
		copy(z.cachedBuf, src)
	} else {
		d, err := pickDecompressor(ext.algorithm)
		if err != nil {
			return err
		}
		n, err := d.decompress(z.cachedBuf, src)
		if err != nil {
			return fmt.Errorf("decompress nid %d at logical %d: %w", fi.nid, ext.logicalStart, err)
		}
		if n < len(z.cachedBuf) {
			// Last pcluster of file may produce fewer bytes than a full lcluster.
			z.cachedBuf = z.cachedBuf[:n]
		}
	}

	z.cachedExt = *ext
	z.cachedExt.logicalSize = int64(len(z.cachedBuf))
	z.cachedValid = true
	return nil
}
