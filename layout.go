package erofs

import (
	"sort"

	"github.com/erofs/go-erofs/internal/disk"
)

// planLayout assigns NIDs and determines trailing data sizes for all entries.
func (w *erofsWriter) planLayout(root *erofsEntry) {
	// Collect all entries in a deterministic order (DFS, pre-order).
	// DFS keeps directory contents close to their parent inode,
	// improving locality for operations like find and ls -lR.
	w.entries = nil
	var walk func(e *erofsEntry)
	walk = func(e *erofsEntry) {
		w.entries = append(w.entries, e)
		if e.mode&disk.StatTypeMask == disk.StatTypeDir {
			sort.Slice(e.children, func(i, j int) bool {
				return e.children[i].name < e.children[j].name
			})
			for _, c := range e.children {
				walk(c)
			}
		}
	}
	walk(root)

	// Secondary hard-link entries share the primary's inode; count only primaries.
	var inodeCount uint64
	for _, e := range w.entries {
		if e.hardLinkPrimary == nil {
			inodeCount++
		}
	}
	w.totalInodes = inodeCount

	// Block 0 holds: 1024-byte pad + 128-byte superblock + device slot(s) + padding
	// MetaBlkAddr is set later by write() depending on the on-disk layout.

	// Assign NIDs sequentially. NID = byte offset from metaStartPos / 32.
	// Secondary hard-link entries are skipped here; a second pass copies the
	// primary's finalized NID to them.
	currentOff := 0 // byte offset from metaStartPos
	for _, e := range w.entries {
		if e.hardLinkPrimary != nil {
			continue // skip; NID assigned in second pass below
		}

		e.xattrSize = calcXattrSize(e)

		// Decide compact (32B) vs extended (64B) inode.
		_, mtimeOK := w.compactMtime(e)
		e.compact = e.uid <= 0xFFFF && e.gid <= 0xFFFF &&
			e.nlink <= 0xFFFF && e.size <= 0xFFFFFFFF && mtimeOK

		// The inode header region is inode core + xattr area.
		// Trailing data (dirents, chunk indexes, inline data) follows.
		headerSize := inodeCoreSize(e) + e.xattrSize

		e.layout = w.baseLayout(e)
		inline := -1
		if e.layout == disk.LayoutFlatPlain {
			inline = w.inlineSize(e, headerSize)
		}
		currentOff = w.placeEntry(e, currentOff, headerSize, inline)
		e.nid = uint64(currentOff / 32)

		e.trailingSize = w.calcTrailingSize(e)
		e.chunkPad = chunkIndexPad(e)
		currentOff += roundUp32(headerSize + e.chunkPad + e.trailingSize)
	}

	// Second pass: copy the primary's finalized NID to secondary entries.
	for _, e := range w.entries {
		if e.hardLinkPrimary != nil {
			e.nid = e.hardLinkPrimary.nid
		}
	}

	w.rootNid = root.nid
}

// baseLayout returns the layout of e before placement: chunk-based for
// chunked or metadata-only files, flat-plain otherwise. placeEntry may
// change a flat-plain layout to flat-inline.
func (w *erofsWriter) baseLayout(e *erofsEntry) uint8 {
	if e.mode&disk.StatTypeMask == disk.StatTypeReg && (len(e.chunks) > 0 || e.metadataOnly) {
		if e.contiguous {
			e.chunkBits = w.minChunkBits(e.size)
		}
		return disk.LayoutChunkBased
	}
	return disk.LayoutFlatPlain
}

// inlineSize returns the number of bytes e would store after its inode with
// the flat-inline layout, or -1 if e cannot use it. For regular files this
// is the part after the last full block. Inline data must not cross a block
// boundary, so header and data must fit in one block.
func (w *erofsWriter) inlineSize(e *erofsEntry, headerSize int) int {
	n := -1
	switch e.mode & disk.StatTypeMask {
	case disk.StatTypeReg:
		if !w.noInlineData && e.data != nil {
			n = regInlineSize(e.size, w.blockSize)
		}
	case disk.StatTypeDir:
		n = w.direntDataSize(e)
	case disk.StatTypeSymlink:
		n = len(e.symTarget)
	}
	if n <= 0 || headerSize+n > w.blockSize {
		return -1
	}
	return n
}

// placeEntry returns the metadata offset for e, which needs headerSize bytes
// plus inline bytes of inline data (-1 if none), and sets e.layout to
// flat-inline when the data is stored inline. The current block is used if
// everything fits. Otherwise the data is stored inline at the start of the
// next block when the space left in the current block is no more than the
// space a data block would waste, and in a data block of its own if not.
// The inode core never crosses a block boundary.
func (w *erofsWriter) placeEntry(e *erofsEntry, off, headerSize, inline int) int {
	left := w.blockSize - off%w.blockSize
	if inline >= 0 {
		if headerSize+inline <= left {
			e.layout = disk.LayoutFlatInline
			return off
		}
		if left <= w.blockSize-inline {
			e.layout = disk.LayoutFlatInline
			return off + left
		}
	}
	if inodeCoreSize(e) > left {
		return off + left
	}
	return off
}

// regInlineSize returns the bytes of a regular file of the given size that
// the flat-inline layout stores after the inode: the part after the last
// full block. The full blocks are stored in data blocks.
func regInlineSize(size uint64, blockSize int) int {
	return int(size % uint64(blockSize))
}

// roundUp32 rounds n up to a multiple of 32, the inode slot size.
func roundUp32(n int) int {
	return (n + 31) &^ 31
}

// chunkIndexPad returns the padding inserted between an inode's xattr area and
// its chunk-index map so the map begins aligned to the chunk-index unit, as the
// kernel and reader both expect:
//
//	pos = ALIGN(iloc + inode_isize + xattr_isize, unit) + unit*chunknr
//
// (see fs/erofs/data.c and loadBlock's LayoutChunkBased case). inodeCoreSize is
// always a multiple of the unit, so only the xattr area can push the map off
// alignment. Non-chunk layouts (inline data) are read without this alignment
// and so must not be padded.
func chunkIndexPad(e *erofsEntry) int {
	if e.layout != disk.LayoutChunkBased || e.trailingSize == 0 {
		return 0
	}
	if r := (inodeCoreSize(e) + e.xattrSize) % disk.SizeChunkIndex; r != 0 {
		return disk.SizeChunkIndex - r
	}
	return 0
}

// calcTrailingSize returns the number of bytes following the 64-byte inode.
func (w *erofsWriter) calcTrailingSize(e *erofsEntry) int {
	switch e.mode & disk.StatTypeMask {
	case disk.StatTypeReg:
		if e.layout == disk.LayoutChunkBased {
			if e.size == 0 && len(e.chunks) == 0 {
				return 0
			}
			cs := w.entryChunkSize(e)
			nchunks := (int(e.size) + cs - 1) / cs
			return nchunks * disk.SizeChunkIndex
		}
		if e.layout == disk.LayoutFlatInline {
			return regInlineSize(e.size, w.blockSize)
		}
		return 0
	case disk.StatTypeDir:
		if e.layout == disk.LayoutFlatInline {
			return w.direntDataSize(e)
		}
		return 0
	case disk.StatTypeSymlink:
		if e.layout == disk.LayoutFlatInline {
			return len(e.symTarget)
		}
		return 0
	default:
		return 0
	}
}

// direntDataSize calculates the serialized EROFS dirent data size for a directory.
// For multi-block directories, this includes inter-block padding.
func (w *erofsWriter) direntDataSize(e *erofsEntry) int {
	if len(e.children) == 0 {
		// Empty dir still needs "." and ".." entries.
		return 2*disk.SizeDirent + 1 + 2
	}

	// Build sorted name lengths matching the order writeDirents produces.
	// "." and ".." are not guaranteed to sort before all real names (any
	// name with an ASCII value below '.' such as '-' sorts before them).
	names := make([]string, 0, len(e.children)+2)
	names = append(names, ".", "..")
	for _, c := range e.children {
		names = append(names, c.name)
	}
	sort.Strings(names)

	nEntries := len(names)
	totalSize := 0
	i := 0
	for i < nEntries {
		blockUsed := 0
		start := i
		nameSize := 0
		for j := i; j < nEntries; j++ {
			headerSize := (j - start + 1) * disk.SizeDirent
			nameSize += len(names[j])
			needed := headerSize + nameSize
			if needed > w.blockSize {
				break
			}
			blockUsed = needed
			i = j + 1
		}
		if i == start {
			blockUsed = disk.SizeDirent + len(names[i])
			i++
		}
		// Pad non-final blocks to block boundary.
		if i < nEntries && blockUsed%w.blockSize != 0 {
			blockUsed = (blockUsed + w.blockSize - 1) & ^(w.blockSize - 1)
		}
		totalSize += blockUsed
	}

	return totalSize
}
