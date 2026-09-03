package erofs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/erofs/go-erofs/internal/builder"
	"github.com/erofs/go-erofs/internal/disk"
)

// maxBlockSize is the largest block size we support. EROFS images with
// larger block sizes are unmountable on common platforms (aarch64 caps
// page size at 64 KiB) and the reader rejects BlkSizeBits > 16.
const maxBlockSize = 1 << 16

// onlyWriter wraps an io.Writer to hide io.ReaderFrom so that
// io.CopyBuffer uses the caller-provided buffer instead of
// the destination's ReadFrom (which allocates its own).
type onlyWriter struct{ io.Writer }

// erofsWriter serializes EROFS metadata to an io.Writer.
type erofsWriter struct {
	entries     []*erofsEntry // all entries in NID order
	rootNid     uint64
	metaBlkAddr uint32
	totalInodes uint64
	buildTime   uint64
	buildTimeNs uint32
	devices     []uint64 // per-device block counts (one slot per entry)
	blockSize   int
	chunkBits   uint8                        // log2(chunkSize / blockSize); chunkSize = blockSize << chunkBits
	copyBuf     []byte                       // reusable buffer for io.CopyBuffer
	zeroBuf     []byte                       // blockSize-length zero buffer for padding
	inodeBuf    [disk.SizeInodeExtended]byte // scratch buffer for writeInode
	compression Compression                  // compression algorithm for regular file data

	// Compressor cache, resolved once when the writer needs to compress its
	// first entry. comp is nil and algoID is 0 when compression == CompressionNone.
	compResolved bool
	algoID       uint8
	comp         compressor

	// cspool holds the compressed bytes for every LayoutCompressedFull entry,
	// appended sequentially during compressEntries and copied back to the
	// output during writeDataBlocks. Using a tempfile (rather than a per-
	// entry []byte) caps peak memory at O(scratch buffers) regardless of
	// image size. Created lazily on the first compressed entry; the underlying
	// file is unlinked immediately after open so it'll be reclaimed when the
	// fd closes.
	cspool    *os.File
	cspoolOff int64
	tempDir   string // mirror of fsys.tempDir for the cspool
}

// inodeSize returns the on-disk inode header size for e.
func inodeCoreSize(e *erofsEntry) int {
	if e.compact {
		return disk.SizeInodeCompact
	}
	return disk.SizeInodeExtended
}

// entryChunkBits returns the chunk bits for a specific entry.
// Contiguous entries use a larger chunk size to minimize chunk indexes.
func (w *erofsWriter) entryChunkBits(e *erofsEntry) uint8 {
	if e.chunkBits > 0 {
		return e.chunkBits
	}
	return w.chunkBits
}

// entryChunkSize returns the chunk size in bytes for a specific entry.
func (w *erofsWriter) entryChunkSize(e *erofsEntry) int {
	return w.blockSize << w.entryChunkBits(e)
}

// minChunkBits returns the minimum chunkBits such that file size fits in
// one chunk (chunkSize >= size). Capped at 31 (LayoutChunkFormatBits max).
func (w *erofsWriter) minChunkBits(size uint64) uint8 {
	bits := w.chunkBits
	for uint64(w.blockSize)<<bits < size && bits < 31 {
		bits++
	}
	return bits
}

// resolveCompressor populates w.algoID and w.comp from w.compression on the
// first call. Subsequent calls are no-ops. Callers that don't actually need
// the compressor (e.g., when there are no compressed entries) never invoke
// it. Returns an error only for unsupported algorithms.
func (w *erofsWriter) resolveCompressor() error {
	if w.compResolved {
		return nil
	}
	comp, err := pickCompressor(w.compression)
	if err != nil {
		return err
	}
	w.algoID = comp.algorithmID()
	w.comp = comp
	w.compResolved = true
	return nil
}

func (w *erofsWriter) write(out io.WriteSeeker) error {
	w.copyBuf = make([]byte, 256*1024) // shared io.CopyBuffer buffer
	defer w.closeCspool()
	return w.writeSeekable(out)
}

// writeSeekable uses a data-first on-disk layout: block0 (placeholder),
// data blocks, metadata. After everything is written, it seeks back to
// write the real superblock. This matches how mkfs.erofs lays out
// streaming sources — data is written as it arrives, metadata last.
func (w *erofsWriter) writeSeekable(out io.WriteSeeker) error {
	// Data-first layout: sbArea, data blocks, metadata.
	// Set metaBlkAddr to a sentinel so assignDataBlocks uses data-first.
	w.metaBlkAddr = 0xFFFFFFFF
	w.assignDataBlocks()

	// Write placeholder superblock area.
	if _, err := out.Write(make([]byte, w.sbAreaSize())); err != nil {
		return err
	}

	// Stream data blocks directly to output.
	if err := w.writeDataBlocks(out); err != nil {
		return err
	}

	// Buffer and write metadata.
	meta := w.newMetaBuffer()
	if err := w.writeMetadataInodes(meta); err != nil {
		return err
	}
	if _, err := meta.WriteTo(out); err != nil {
		return err
	}

	// Seek back and write the real block 0 (superblock).
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.writeBlock0(out)
}

// newMetaBuffer returns a pre-sized bytes.Buffer for metadata serialization.
func (w *erofsWriter) newMetaBuffer() *bytes.Buffer {
	totalMetaBytes := 0
	for _, e := range w.entries {
		if e.hardLinkPrimary != nil {
			continue // secondary entries share the primary's on-disk inode
		}
		isz := disk.SizeInodeExtended
		if e.compact {
			isz = disk.SizeInodeCompact
		}
		sz := isz + e.xattrSize + e.chunkPad + e.trailingSize
		if sz%32 != 0 {
			sz = (sz + 31) & ^31
		}
		totalMetaBytes += sz
	}
	// SB area + metadata padded to block boundary.
	capacity := w.blockSize + ((totalMetaBytes + w.blockSize - 1) & ^(w.blockSize - 1))
	buf := bytes.NewBuffer(make([]byte, 0, capacity))
	return buf
}

// assignDataBlocks assigns data block addresses to flat-plain entries.
// For metadata-first layout, data follows metadata.
// For data-first layout, data starts after the superblock area.
func (w *erofsWriter) assignDataBlocks() {
	sbBlks := w.sbAreaBlocks()
	if w.metaBlkAddr == uint32(sbBlks) {
		// Metadata-first: data blocks come after metadata.
		totalMetaBytes := 0
		for _, e := range w.entries {
			if e.hardLinkPrimary != nil {
				continue
			}
			expectedOff := int(e.nid) * 32
			sz := inodeCoreSize(e) + e.xattrSize + e.chunkPad + e.trailingSize
			if sz%32 != 0 {
				sz = (sz + 31) & ^31
			}
			end := expectedOff + sz
			if end > totalMetaBytes {
				totalMetaBytes = end
			}
		}
		metaBlocks := (totalMetaBytes + w.blockSize - 1) / w.blockSize
		addr := uint32(w.sbAreaBlocks() + metaBlocks)
		for _, e := range w.entries {
			if nblk := w.entryDataBlocks(e); nblk > 0 {
				e.dataBlkAddr = addr
				addr += uint32(nblk)
			}
		}
	} else {
		// Data-first: data starts after superblock area.
		addr := uint32(w.sbAreaBlocks())
		for _, e := range w.entries {
			if nblk := w.entryDataBlocks(e); nblk > 0 {
				e.dataBlkAddr = addr
				addr += uint32(nblk)
			}
		}
		w.metaBlkAddr = addr // metadata follows data
	}
}

// sbAreaSize returns the number of bytes needed for the superblock area
// (blocks before metadata): 1024-byte pad + superblock + device slots,
// rounded up to block boundary.
func (w *erofsWriter) sbAreaSize() int {
	n := disk.SuperBlockOffset + disk.SizeSuperBlock
	if len(w.devices) > 0 {
		n += len(w.devices) * disk.SizeDeviceSlot
	}
	return ((n + w.blockSize - 1) / w.blockSize) * w.blockSize
}

// sbAreaBlocks returns the number of blocks occupied by the superblock area.
func (w *erofsWriter) sbAreaBlocks() int {
	return w.sbAreaSize() / w.blockSize
}

// metadataBytes computes the total size of the metadata area, including
// any zero-padding inserted to reach each inode's expected offset (NID * 32)
// and rounding each entry up to a 32-byte boundary.
func (w *erofsWriter) metadataBytes() int {
	curOff := 0
	for _, e := range w.entries {
		if e.hardLinkPrimary != nil {
			continue // secondary entries have no on-disk inode
		}
		expectedOff := int(e.nid) * 32
		if curOff < expectedOff {
			curOff = expectedOff
		}
		sz := inodeCoreSize(e) + e.xattrSize + e.chunkPad + e.trailingSize
		if rem := sz % 32; rem != 0 {
			sz += 32 - rem
		}
		curOff += sz
	}
	return curOff
}

func (w *erofsWriter) writeBlock0(buf io.Writer) error {
	sbArea := make([]byte, w.sbAreaSize())

	totalMetaBytes := w.metadataBytes()
	metaBlocks := (totalMetaBytes + w.blockSize - 1) / w.blockSize

	// Count data blocks (flat-plain plus compressed pclusters).
	dataBlocks := 0
	for _, e := range w.entries {
		dataBlocks += w.entryDataBlocks(e)
	}
	totalBlocks := w.sbAreaBlocks() + metaBlocks + dataBlocks

	var featureIncompat uint32
	var extraDevices uint16
	var devtSlotOff uint16

	if len(w.devices) > 0 {
		featureIncompat |= disk.FeatureIncompatDeviceTable
		extraDevices = uint16(len(w.devices))
		devtSlotOff = uint16(disk.SizeSuperBlock / 16)
	}
	var comprAlgs uint16
	for _, e := range w.entries {
		if len(e.chunks) > 0 {
			featureIncompat |= disk.FeatureIncompatChunkedFile
		}
		if e.layout == disk.LayoutCompressedFull {
			featureIncompat |= disk.FeatureIncompatLZ4_0Padding
			comprAlgs |= 1 << w.algoID
			// The kernel rejects images that use BIG_PCLUSTER_1 advise
			// without the matching superblock feature bit ("per-inode
			// big pcluster without sb feature" → -EFSCORRUPTED, see
			// fs/erofs/zmap.c). Set FeatureIncompatBigPcluster as soon
			// as any pcluster in the entry actually spans more than one
			// block. The bit shares value 0x2 with COMPR_CFGS.
			if e.hasBigPcluster {
				featureIncompat |= disk.FeatureIncompatBigPcluster
			}
		}
	}

	sb := disk.SuperBlock{
		MagicNumber:     disk.MagicNumber,
		BlkSizeBits:     blkBits(w.blockSize),
		RootNid:         uint16(w.rootNid),
		Inos:            w.totalInodes,
		BuildTime:       w.buildTime,
		BuildTimeNs:     w.buildTimeNs,
		Blocks:          uint32(totalBlocks),
		MetaBlkAddr:     w.metaBlkAddr,
		FeatureIncompat: featureIncompat,
		ComprAlgs:       comprAlgs,
		ExtraDevices:    extraDevices,
		DevtSlotOff:     devtSlotOff,
	}

	sbBuf := &bytes.Buffer{}
	if err := binary.Write(sbBuf, binary.LittleEndian, &sb); err != nil {
		return fmt.Errorf("write superblock: %w", err)
	}
	copy(sbArea[disk.SuperBlockOffset:], sbBuf.Bytes())

	// Write device slots right after superblock.
	for i, blocks := range w.devices {
		if blocks > math.MaxUint32 {
			return fmt.Errorf("device %d block count %d exceeds 32-bit limit", i+1, blocks)
		}
		devSlot := disk.DeviceSlot{
			Blocks: uint32(blocks),
		}
		devBuf := &bytes.Buffer{}
		if err := binary.Write(devBuf, binary.LittleEndian, &devSlot); err != nil {
			return fmt.Errorf("write device slot: %w", err)
		}
		off := disk.SuperBlockOffset + disk.SizeSuperBlock + i*disk.SizeDeviceSlot
		copy(sbArea[off:], devBuf.Bytes())
	}

	_, err := buf.Write(sbArea)
	return err
}

// writeMetadataInodes writes inode metadata. Data block addresses must
// already be assigned on each entry before calling this method.
func (w *erofsWriter) writeMetadataInodes(buf io.Writer) error {
	metaStart := 0
	for _, e := range w.entries {
		// Secondary hard-link entries share the primary's on-disk inode.
		if e.hardLinkPrimary != nil {
			continue
		}

		expectedOff := int(e.nid) * 32
		if expectedOff > metaStart {
			if _, err := buf.Write(w.zeroBuf[:expectedOff-metaStart]); err != nil {
				return err
			}
			metaStart = expectedOff
		}

		if err := w.writeInode(buf, e); err != nil {
			return fmt.Errorf("write inode for %s: %w", e.path, err)
		}
		if e.compact {
			metaStart += disk.SizeInodeCompact
		} else {
			metaStart += disk.SizeInodeExtended
		}

		// Write xattr area
		if e.xattrSize > 0 {
			if err := w.writeXattrs(buf, e); err != nil {
				return fmt.Errorf("write xattrs for %s: %w", e.path, err)
			}
			metaStart += e.xattrSize
		}

		// Write trailing data
		switch e.mode & disk.StatTypeMask {
		case disk.StatTypeReg:
			switch {
			case e.layout == disk.LayoutChunkBased && (e.size > 0 || len(e.chunks) > 0):
				// Align the chunk-index map to the chunk-index unit.
				if e.chunkPad > 0 {
					if _, err := buf.Write(w.zeroBuf[:e.chunkPad]); err != nil {
						return err
					}
					metaStart += e.chunkPad
				}
				if err := w.writeChunkIndexes(buf, e); err != nil {
					return fmt.Errorf("write chunks for %s: %w", e.path, err)
				}
				metaStart += e.trailingSize
			case e.layout == disk.LayoutCompressedFull:
				if err := w.writeCompressedTrailing(buf, e); err != nil {
					return fmt.Errorf("write compressed metadata for %s: %w", e.path, err)
				}
				metaStart += e.trailingSize
			case e.layout == disk.LayoutFlatInline && e.size > 0 && e.data != nil:
				// e.data may be an unbounded reader (e.g. directData from CopyFrom);
				// limit to e.size bytes to prevent overwriting subsequent metadata.
				expected := int64(e.size)
				n, err := io.CopyBuffer(onlyWriter{buf}, io.LimitReader(e.data, expected), w.copyBuf)
				if c, ok := e.data.(io.Closer); ok {
					_ = c.Close()
				}
				if err != nil {
					return fmt.Errorf("write inline data for %s: %w", e.path, err)
				}
				if n != expected {
					return fmt.Errorf("write inline data for %s: short read: got %d bytes, expected %d", e.path, n, expected)
				}
				metaStart += int(n)
			}
		case disk.StatTypeDir:
			if e.layout == disk.LayoutFlatInline {
				n, err := w.writeDirents(buf, e)
				if err != nil {
					return fmt.Errorf("write dirents for %s: %w", e.path, err)
				}
				metaStart += n
			}
		case disk.StatTypeSymlink:
			if e.layout == disk.LayoutFlatInline {
				if _, err := io.WriteString(buf, e.symTarget); err != nil {
					return fmt.Errorf("write symlink for %s: %w", e.path, err)
				}
				metaStart += len(e.symTarget)
			}
		}

		// Pad to 32-byte boundary
		inodeSize := disk.SizeInodeExtended
		if e.compact {
			inodeSize = disk.SizeInodeCompact
		}
		totalWritten := inodeSize + e.xattrSize + e.chunkPad + e.trailingSize
		if totalWritten%32 != 0 {
			padSize := 32 - (totalWritten % 32)
			if _, err := buf.Write(w.zeroBuf[:padSize]); err != nil {
				return err
			}
			metaStart += padSize
		}
	}

	// Pad metadata to full block boundary
	if metaStart%w.blockSize != 0 {
		padSize := w.blockSize - (metaStart % w.blockSize)
		if _, err := buf.Write(w.zeroBuf[:padSize]); err != nil {
			return err
		}
	}

	return nil
}

func (w *erofsWriter) writeInode(buf io.Writer, e *erofsEntry) error {
	var inodeData uint32

	switch e.mode & disk.StatTypeMask {
	case disk.StatTypeReg:
		if e.layout == disk.LayoutChunkBased {
			inodeData = disk.LayoutChunkFormatIndexes | uint32(w.entryChunkBits(e))
		} else if e.layout == disk.LayoutFlatPlain && e.size > 0 {
			inodeData = e.dataBlkAddr
		}
		// LayoutCompressedFull leaves inodeData=0; the algorithm and
		// block addresses live in the trailing map header + lcluster index.
	case disk.StatTypeDir, disk.StatTypeSymlink:
		if e.layout == disk.LayoutFlatPlain {
			inodeData = e.dataBlkAddr
		}
	case disk.StatTypeChrdev, disk.StatTypeBlkdev, disk.StatTypeFifo, disk.StatTypeSock:
		inodeData = e.rdev
	}

	fileSize := e.size
	switch e.mode & disk.StatTypeMask {
	case disk.StatTypeDir:
		fileSize = uint64(w.direntDataSize(e))
	case disk.StatTypeSymlink:
		fileSize = uint64(len(e.symTarget))
	}

	b := &w.inodeBuf
	clear(b[:])

	if e.compact {
		binary.LittleEndian.PutUint16(b[0:2], inodeFormat(e.layout, true))
		binary.LittleEndian.PutUint16(b[2:4], xattrCount(e.xattrSize))
		binary.LittleEndian.PutUint16(b[4:6], e.mode)
		binary.LittleEndian.PutUint16(b[6:8], uint16(e.nlink))
		binary.LittleEndian.PutUint32(b[8:12], uint32(fileSize))
		binary.LittleEndian.PutUint32(b[16:20], inodeData)
		binary.LittleEndian.PutUint16(b[24:26], uint16(e.uid))
		binary.LittleEndian.PutUint16(b[26:28], uint16(e.gid))
		_, err := buf.Write(b[:disk.SizeInodeCompact])
		return err
	}

	binary.LittleEndian.PutUint16(b[0:2], inodeFormat(e.layout, false))
	binary.LittleEndian.PutUint16(b[2:4], xattrCount(e.xattrSize))
	binary.LittleEndian.PutUint16(b[4:6], e.mode)
	binary.LittleEndian.PutUint64(b[8:16], fileSize)
	binary.LittleEndian.PutUint32(b[16:20], inodeData)
	binary.LittleEndian.PutUint32(b[24:28], e.uid)
	binary.LittleEndian.PutUint32(b[28:32], e.gid)
	binary.LittleEndian.PutUint64(b[32:40], e.mtime)
	binary.LittleEndian.PutUint32(b[40:44], e.mtimeNs)
	binary.LittleEndian.PutUint32(b[44:48], e.nlink)
	_, err := buf.Write(b[:disk.SizeInodeExtended])
	return err
}

func (w *erofsWriter) writeXattrs(buf io.Writer, e *erofsEntry) error {
	// XattrHeader: 4-byte name filter + 1-byte shared count + 7 reserved = 12 bytes
	var xhdr [12]byte
	binary.LittleEndian.PutUint32(xhdr[0:4], 0xFFFFFFFF) // name filter unused
	if _, err := buf.Write(xhdr[:]); err != nil {
		return err
	}

	for _, name := range sortedXattrKeys(e.xattrs) {
		value := e.xattrs[name]
		nameIndex, suffix := xattrSplit(name)

		var xent [disk.SizeXattrEntry]byte
		xent[0] = uint8(len(suffix))
		xent[1] = nameIndex
		binary.LittleEndian.PutUint16(xent[2:4], uint16(len(value)))
		if _, err := buf.Write(xent[:]); err != nil {
			return err
		}
		if _, err := io.WriteString(buf, suffix); err != nil {
			return err
		}
		if _, err := io.WriteString(buf, value); err != nil {
			return err
		}

		// Pad to 4-byte boundary
		entryLen := disk.SizeXattrEntry + len(suffix) + len(value)
		if entryLen%4 != 0 {
			if _, err := buf.Write(w.zeroBuf[:4-entryLen%4]); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeCompressedTrailing emits the on-disk trailing area for a
// LayoutCompressedFull inode: alignment padding, the z_erofs_map_header,
// the reserved 8-byte slot before the lcluster index, and one
// z_erofs_lcluster_index entry per logical cluster.
//
// Entries come from e.lclusterEntries which compressEntry populated.
func (w *erofsWriter) writeCompressedTrailing(buf io.Writer, e *erofsEntry) error {
	headerSize := inodeCoreSize(e) + e.xattrSize
	alignPad := (8 - (headerSize % 8)) % 8
	if alignPad > 0 {
		if _, err := buf.Write(w.zeroBuf[:alignPad]); err != nil {
			return err
		}
	}

	// z_erofs_map_header: clusterbits=0 (lcluster=blocksize),
	// algorithmtype in the low nibble, h_advise carries BIG_PCLUSTER_1 if
	// any pcluster spans more than one block. The compressor was resolved
	// during compressEntries so w.algoID is already set here.
	var hAdvise uint16
	if e.hasBigPcluster {
		hAdvise |= disk.ZErofsAdviseBigPcluster1
	}
	var hdr [disk.SizeZErofsMapHeader]byte
	binary.LittleEndian.PutUint16(hdr[4:6], hAdvise)
	hdr[6] = w.algoID // h_algorithmtype: low nibble = HEAD1 algo
	hdr[7] = 0        // h_clusterbits: lcluster_bits = blockSize_bits + 0
	if _, err := buf.Write(hdr[:]); err != nil {
		return err
	}

	// 8 bytes reserved gap (Z_EROFS_FULL_INDEX_START = MAP_HEADER_END + 8).
	if _, err := buf.Write(w.zeroBuf[:8]); err != nil {
		return err
	}

	// One z_erofs_lcluster_index per lcluster.
	var li [disk.SizeZErofsLclusterIndex]byte
	for _, le := range e.lclusterEntries {
		binary.LittleEndian.PutUint16(li[0:2], uint16(le.typ))
		if le.typ == disk.ZErofsLclusterTypeNonhead {
			binary.LittleEndian.PutUint16(li[2:4], 0)
			// NONHEAD packs delta[0] and delta[1] into di_u.
			binary.LittleEndian.PutUint32(li[4:8],
				uint32(le.delta0)|(uint32(le.delta1)<<16))
		} else {
			binary.LittleEndian.PutUint16(li[2:4], 0) // di_clusterofs = 0
			binary.LittleEndian.PutUint32(li[4:8], le.pblk)
		}
		if _, err := buf.Write(li[:]); err != nil {
			return err
		}
	}
	return nil
}

// writeChunkIndexes writes chunk index entries for a regular file.
// Each index entry covers one logical chunk (chunkSize bytes).
func (w *erofsWriter) writeChunkIndexes(buf io.Writer, e *erofsEntry) error {
	cs := w.entryChunkSize(e)
	blocksPerChunk := cs / w.blockSize
	nchunks := (int(e.size) + cs - 1) / cs

	// Null chunk index (no mapping): StartBlkHi=0xFFFF, DeviceID=0, StartBlkLo=NullAddr.
	var nullIdx [disk.SizeChunkIndex]byte
	binary.LittleEndian.PutUint16(nullIdx[0:2], 0xFFFF)
	binary.LittleEndian.PutUint32(nullIdx[4:8], nullAddr)

	if len(e.chunks) > 0 {
		// Walk source chunks and emit one index per logical chunk.
		// Source chunks use block-granularity counts; we step by blocksPerChunk.
		// A chunk with PhysicalBlock == builder.NullPhysicalBlock is a hole:
		// emit nullIdx entries for its block span.
		var scratch [disk.SizeChunkIndex]byte
		ci := 0   // index into source chunks
		coff := 0 // block offset within current source chunk
		for n := 0; n < nchunks; n++ {
			if ci >= len(e.chunks) {
				if _, err := buf.Write(nullIdx[:]); err != nil {
					return err
				}
				continue
			}
			c := e.chunks[ci]
			if c.PhysicalBlock == builder.NullPhysicalBlock {
				// Hole chunk: emit a null index entry.
				if _, err := buf.Write(nullIdx[:]); err != nil {
					return err
				}
			} else {
				phys := c.PhysicalBlock + uint64(coff)
				binary.LittleEndian.PutUint16(scratch[0:2], uint16(phys>>32))
				binary.LittleEndian.PutUint16(scratch[2:4], c.DeviceID)
				binary.LittleEndian.PutUint32(scratch[4:8], uint32(phys))
				if _, err := buf.Write(scratch[:]); err != nil {
					return err
				}
			}
			coff += blocksPerChunk
			for ci < len(e.chunks) && coff >= int(e.chunks[ci].Count) {
				coff -= int(e.chunks[ci].Count)
				ci++
			}
		}
	} else {
		for n := 0; n < nchunks; n++ {
			if _, err := buf.Write(nullIdx[:]); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeDirents writes EROFS directory entries packed into block-sized chunks.
func (w *erofsWriter) writeDirents(buf io.Writer, e *erofsEntry) (int, error) {
	type direntInfo struct {
		name     string
		nid      uint64
		fileType uint8
	}

	// Build the full entry list including "." and ".." then sort
	// alphabetically. EROFS requires dirents to be sorted within each block;
	// "." and ".." are not guaranteed to sort first (filenames with ASCII
	// values below '.' such as '-' or ',' sort before them).
	allEnts := make([]direntInfo, 0, len(e.children)+2)
	allEnts = append(allEnts,
		direntInfo{".", e.nid, disk.FileTypeDir},
		direntInfo{"..", e.parentNid, disk.FileTypeDir},
	)
	for _, c := range e.children {
		allEnts = append(allEnts, direntInfo{
			name:     c.name,
			nid:      c.nid,
			fileType: c.erofsFileType,
		})
	}
	sort.Slice(allEnts, func(i, j int) bool {
		return allEnts[i].name < allEnts[j].name
	})

	totalWritten := 0
	i := 0
	for i < len(allEnts) {
		// Determine how many entries fit in this block
		start := i
		blockUsed := 0
		nameSize := 0
		for j := i; j < len(allEnts); j++ {
			headerSize := (j - start + 1) * disk.SizeDirent
			nameSize += len(allEnts[j].name)
			needed := headerSize + nameSize
			if needed > w.blockSize {
				break
			}
			blockUsed = needed
			i = j + 1
		}
		if i == start {
			// Single entry too large for a block (shouldn't happen)
			blockUsed = disk.SizeDirent + len(allEnts[i].name)
			i++
		}

		blockEnts := allEnts[start:i]
		blockHeaderSize := len(blockEnts) * disk.SizeDirent

		// Write dirent headers
		var scratch [disk.SizeDirent]byte
		nameOff := uint16(blockHeaderSize)
		for j, de := range blockEnts {
			if j > 0 {
				nameOff += uint16(len(blockEnts[j-1].name))
			}
			binary.LittleEndian.PutUint64(scratch[0:8], de.nid)
			binary.LittleEndian.PutUint16(scratch[8:10], nameOff)
			scratch[10] = de.fileType
			scratch[11] = 0
			if _, err := buf.Write(scratch[:]); err != nil {
				return totalWritten, err
			}
			totalWritten += disk.SizeDirent
		}

		// Write names
		for _, de := range blockEnts {
			n, err := io.WriteString(buf, de.name)
			if err != nil {
				return totalWritten, err
			}
			totalWritten += n
		}

		// Pad to block boundary if there are more entries
		if i < len(allEnts) && blockUsed%w.blockSize != 0 {
			padSize := w.blockSize - (blockUsed % w.blockSize)
			if _, err := buf.Write(w.zeroBuf[:padSize]); err != nil {
				return totalWritten, err
			}
			totalWritten += padSize
		}
	}

	return totalWritten, nil
}

// writeDataBlocks writes data blocks for flat-plain entries directly to out.
func (w *erofsWriter) writeDataBlocks(out io.Writer) error {
	for _, e := range w.entries {
		if e.hardLinkPrimary != nil {
			continue // secondary hard-link entries share the primary's data blocks
		}
		if e.layout == disk.LayoutCompressedFull {
			if err := w.writeCompressedData(out, e); err != nil {
				return err
			}
			continue
		}
		ds := w.flatPlainDataSize(e)
		if ds == 0 {
			continue
		}

		var n int
		switch e.mode & disk.StatTypeMask {
		case disk.StatTypeReg:
			expected := int64(ds)
			var written int64
			var err error
			limited := io.LimitReader(e.data, expected)
			// Use io.Copy for *os.File sources to enable copy_file_range.
			if _, ok := e.data.(*os.File); ok {
				written, err = io.Copy(out, limited)
			} else {
				written, err = io.CopyBuffer(onlyWriter{out}, limited, w.copyBuf)
			}
			if c, ok := e.data.(io.Closer); ok {
				_ = c.Close()
			}
			if err != nil {
				return fmt.Errorf("write data for %s: %w", e.path, err)
			}
			if written != expected {
				return fmt.Errorf("write data for %s: short read: got %d bytes, expected %d", e.path, written, expected)
			}
			n = int(written)
		case disk.StatTypeDir:
			written, err := w.writeDirents(out, e)
			if err != nil {
				return fmt.Errorf("write dirents for %s: %w", e.path, err)
			}
			n = written
		case disk.StatTypeSymlink:
			written, err := io.WriteString(out, e.symTarget)
			if err != nil {
				return fmt.Errorf("write symlink data for %s: %w", e.path, err)
			}
			n = written
		}

		if n%w.blockSize != 0 {
			padSize := w.blockSize - (n % w.blockSize)
			if _, err := out.Write(w.zeroBuf[:padSize]); err != nil {
				return fmt.Errorf("write padding for %s: %w", e.path, err)
			}
		}
	}
	return nil
}

// flatPlainDataSize returns the data size for a flat-plain entry, or 0.
func (w *erofsWriter) flatPlainDataSize(e *erofsEntry) int {
	if e.layout != disk.LayoutFlatPlain {
		return 0
	}
	switch e.mode & disk.StatTypeMask {
	case disk.StatTypeReg:
		if e.size > 0 && e.data != nil {
			return int(e.size)
		}
	case disk.StatTypeDir:
		return w.direntDataSize(e)
	case disk.StatTypeSymlink:
		return len(e.symTarget)
	}
	return 0
}

// maxPclusterLclusters is the maximum number of logical clusters the writer
// will group into a single physical cluster. mkfs.erofs's default is 1; we
// pick 4 so a typical lz4-compressed text/code file actually shrinks. The
// kernel caps this at Z_EROFS_PCLUSTER_MAX_SIZE / blockSize (256 for a 4 KiB
// block) and surfaces the choice via the lz4_cfgs.max_pclusterblks field.
const maxPclusterLclusters = 4

// Compile-time guard: the on-disk CBLKCNT marker encodes the pcluster's
// physical block count in 11 bits of NONHEAD di_u.delta[0] (the high bit at
// ZErofsLiD0CblkCnt = 0x800 is the marker itself, the low 11 bits hold the
// value). If maxPclusterLclusters ever exceeds 0x7FF (= ZErofsLiD0CblkCnt-1)
// the OR in compressEntry would clobber the marker bit and emit a corrupt
// index entry. Make the conversion below fail at build time before that
// happens; if you legitimately need a larger value, widen the encoding.
var _ [int(disk.ZErofsLiD0CblkCnt) - maxPclusterLclusters]struct{}

// compressEntries walks all entries and pre-compresses any LayoutCompressedFull
// regular files into the shared cspool tempfile. After this returns, each
// compressed entry has its cspoolOff (start offset in the spool), nPblks
// (physical block count, also = len(spool slice) / blockSize), and
// lclusterEntries populated, so assignDataBlocks and entryDataBlocks can use
// the real on-disk size. The cspool itself is unlinked on creation and
// freed automatically when the writer closes it via closeCspool().
func (w *erofsWriter) compressEntries() error {
	// Resolve the compressor once. If no entry is compressed we skip the
	// resolve and the writer's algoID/comp stay zero — that's fine because
	// writeBlock0/writeCompressedTrailing only read them when at least one
	// entry has LayoutCompressedFull.
	hasCompressed := false
	for _, e := range w.entries {
		if e.layout == disk.LayoutCompressedFull {
			hasCompressed = true
			break
		}
	}
	if !hasCompressed {
		return nil
	}
	if err := w.resolveCompressor(); err != nil {
		return fmt.Errorf("resolve compressor: %w", err)
	}
	for _, e := range w.entries {
		if e.layout != disk.LayoutCompressedFull {
			continue
		}
		if err := w.compressEntry(e); err != nil {
			return err
		}
	}
	return nil
}

// ensureCspool lazily opens the shared compressed-data spool. The tempfile
// is unlinked immediately so it disappears when the fd closes, matching the
// raw-data spool in Writer.ensureSpool.
func (w *erofsWriter) ensureCspool() error {
	if w.cspool != nil {
		return nil
	}
	tmp, err := os.CreateTemp(w.tempDir, "erofs-cdata-*")
	if err != nil {
		return fmt.Errorf("create compressed-data spool: %w", err)
	}
	_ = os.Remove(tmp.Name())
	w.cspool = tmp
	return nil
}

// closeCspool releases the spool fd. Safe to call multiple times.
func (w *erofsWriter) closeCspool() {
	if w.cspool != nil {
		_ = w.cspool.Close()
		w.cspool = nil
	}
}

// cspoolWrite appends p to the cspool and advances cspoolOff.
func (w *erofsWriter) cspoolWrite(p []byte) error {
	n, err := w.cspool.Write(p)
	w.cspoolOff += int64(n)
	if err != nil {
		return fmt.Errorf("write compressed-data spool: %w", err)
	}
	return nil
}

// compressEntry compresses one regular file's data, choosing per pcluster
// between three encodings:
//   - big-pcluster: K logical clusters share M < K physical blocks via a
//     HEAD1 + (K-1) NONHEAD lcluster group, with CBLKCNT=M on the first
//     NONHEAD;
//   - HEAD1: one logical cluster, one physical block, LZ4 output < blockSize;
//   - PLAIN: one logical cluster, one physical block, raw bytes (used when
//     LZ4 doesn't shrink the input).
//
// Decisions are local to each batch of up to maxPclusterLclusters lclusters.
// Empty / nil data files keep their planLayout layout and produce no data.
func (w *erofsWriter) compressEntry(e *erofsEntry) error {
	if e.size == 0 || e.data == nil {
		return nil
	}
	defer func() {
		if c, ok := e.data.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	if err := w.ensureCspool(); err != nil {
		return err
	}
	e.cspoolOff = w.cspoolOff

	bs := w.blockSize
	nlcn := int(e.nLclusters)
	e.lclusterEntries = make([]lclusterEntry, nlcn)

	// Scratch buffers sized for the biggest batch. These live on the stack
	// frame and are released when compressEntry returns.
	rawBatch := make([]byte, maxPclusterLclusters*bs)
	compressedBatch := make([]byte, maxPclusterLclusters*bs)
	// Per-block scratch used by the single-lcluster fallback.
	compressed := make([]byte, bs)

	remaining := int64(e.size)
	pblk := uint32(0) // physical block index within the entry
	i := 0
	for i < nlcn {
		k := maxPclusterLclusters
		if i+k > nlcn {
			k = nlcn - i
		}

		// Read k blocks worth into rawBatch. The last batch's final lcluster
		// may be partial; zero-pad the tail so compression sees a deterministic
		// k*blockSize input.
		batchBytes := int64(k) * int64(bs)
		actual := remaining
		if actual > batchBytes {
			actual = batchBytes
		}
		if actual > 0 {
			if _, err := io.ReadFull(e.data, rawBatch[:actual]); err != nil {
				return fmt.Errorf("read data for %s: %w", e.path, err)
			}
		}
		clear(rawBatch[actual:batchBytes])
		remaining -= actual

		// First try big-pcluster: compress the whole batch as one LZ4 stream.
		// If the result fits in fewer than k blocks, emit the multi-lcluster
		// pcluster. Skip when k == 1 (degenerates to the per-lcluster path).
		if k > 1 {
			n, err := w.comp.compressBlock(rawBatch[:batchBytes], compressedBatch)
			if err != nil {
				return fmt.Errorf("compress batch for %s: %w", e.path, err)
			}
			if n > 0 && n < int(batchBytes) {
				m := (n + bs - 1) / bs
				if m < k {
					// Emit the HEAD1 + NONHEADs.
					e.lclusterEntries[i] = lclusterEntry{
						typ:  disk.ZErofsLclusterTypeHead1,
						pblk: pblk,
					}
					// First NONHEAD carries CBLKCNT = m.
					e.lclusterEntries[i+1] = lclusterEntry{
						typ:    disk.ZErofsLclusterTypeNonhead,
						delta0: uint16(m) | disk.ZErofsLiD0CblkCnt,
						delta1: uint16(k - 1),
					}
					for j := 2; j < k; j++ {
						e.lclusterEntries[i+j] = lclusterEntry{
							typ:    disk.ZErofsLclusterTypeNonhead,
							delta0: uint16(j),
							delta1: uint16(k - j),
						}
					}
					// Append m blocks of compressed data + zero pad to the spool.
					if err := w.cspoolWrite(compressedBatch[:n]); err != nil {
						return err
					}
					if pad := m*bs - n; pad > 0 {
						if err := w.cspoolWrite(w.zeroBuf[:pad]); err != nil {
							return err
						}
					}
					e.hasBigPcluster = true
					pblk += uint32(m)
					i += k
					continue
				}
			}
		}

		// Per-lcluster fallback: the K-block batch above either didn't
		// compress (n == 0) or didn't compress small enough to save a
		// physical block (m >= k). Re-compress each lcluster on its own so
		// individual blocks can still pick HEAD1 or PLAIN, even though the
		// batch as a whole couldn't share a pcluster. This costs roughly 2x
		// compression CPU on incompressible inputs — the batch attempt is
		// wasted — which is the trade-off for cheap big-pcluster opt-in
		// without a separate scan phase.
		for j := 0; j < k; j++ {
			block := rawBatch[j*bs : (j+1)*bs]
			n, err := w.comp.compressBlock(block, compressed)
			if err != nil {
				return fmt.Errorf("compress block for %s: %w", e.path, err)
			}
			if n <= 0 || n >= bs {
				e.lclusterEntries[i+j] = lclusterEntry{
					typ:  disk.ZErofsLclusterTypePlain,
					pblk: pblk,
				}
				if err := w.cspoolWrite(block); err != nil {
					return err
				}
			} else {
				e.lclusterEntries[i+j] = lclusterEntry{
					typ:  disk.ZErofsLclusterTypeHead1,
					pblk: pblk,
				}
				if err := w.cspoolWrite(compressed[:n]); err != nil {
					return err
				}
				if pad := bs - n; pad > 0 {
					if err := w.cspoolWrite(w.zeroBuf[:pad]); err != nil {
						return err
					}
				}
			}
			pblk++
		}
		i += k
	}

	e.nPblks = pblk
	if w.cspoolOff-e.cspoolOff != int64(pblk)*int64(bs) {
		return fmt.Errorf("internal: %s emitted %d spool bytes for %d pblks (blockSize=%d)",
			e.path, w.cspoolOff-e.cspoolOff, pblk, bs)
	}
	return nil
}

// writeCompressedData streams an entry's pre-compressed bytes from the spool
// to the output. Block addresses inside e.lclusterEntries are relative to
// the start of the entry's data area; here we rewrite them with the
// absolute dataBlkAddr so writeCompressedTrailing can emit them as-is.
func (w *erofsWriter) writeCompressedData(out io.Writer, e *erofsEntry) error {
	if e.nPblks == 0 {
		return nil
	}
	for idx := range e.lclusterEntries {
		le := &e.lclusterEntries[idx]
		if le.typ != disk.ZErofsLclusterTypeNonhead {
			le.pblk += e.dataBlkAddr
		}
	}
	size := int64(e.nPblks) * int64(w.blockSize)
	src := io.NewSectionReader(w.cspool, e.cspoolOff, size)
	n, err := io.CopyBuffer(onlyWriter{out}, src, w.copyBuf)
	if err != nil {
		return fmt.Errorf("copy compressed data for %s: %w", e.path, err)
	}
	if n != size {
		return fmt.Errorf("short copy of compressed data for %s: got %d, want %d", e.path, n, size)
	}
	return nil
}

// entryDataBlocks returns the number of physical data blocks an entry will
// occupy outside the metadata area. Used to size the on-disk data section
// for block-address assignment and superblock accounting.
func (w *erofsWriter) entryDataBlocks(e *erofsEntry) int {
	if e.hardLinkPrimary != nil {
		return 0 // secondary entries share the primary's data blocks
	}
	switch e.layout {
	case disk.LayoutFlatPlain:
		ds := w.flatPlainDataSize(e)
		if ds == 0 {
			return 0
		}
		return (ds + w.blockSize - 1) / w.blockSize
	case disk.LayoutCompressedFull:
		// compressEntry populates nPblks from the actual on-disk size, which
		// may be less than nLclusters when big-pcluster grouping wins.
		return int(e.nPblks)
	}
	return 0
}
