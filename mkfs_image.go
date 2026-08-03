package erofs

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"path"

	"github.com/forkcloser/erofs/internal/builder"
	"github.com/forkcloser/erofs/internal/disk"
)

// maxEagerMetaBytes caps the metadata region copyFromImage will read into
// memory in one shot when the source's true size cannot be determined.
// Images whose declared metadata exceeds this are rejected rather than
// allowed to drive an unbounded allocation.
const maxEagerMetaBytes = 1 << 30 // 1 GiB

// maxQueuePrealloc caps the BFS queue's initial capacity. This is only a
// sizing hint — the queue still grows to whatever the image legitimately
// needs — so capping it costs nothing and keeps a corrupt inode count from
// requesting an enormous allocation up front.
const maxQueuePrealloc = 1 << 16

// imageSize reports the readable size of an image, when the io.ReaderAt can
// say. *bytes.Reader, *io.SectionReader and *os.File all can, which covers
// every way this package is realistically used. Callers must treat a false
// result as "unknown", not as "empty".
func imageSize(ra io.ReaderAt) (int64, bool) {
	switch v := ra.(type) {
	case interface{ Size() int64 }:
		return v.Size(), true
	case interface{ Stat() (fs.FileInfo, error) }:
		fi, err := v.Stat()
		if err != nil {
			return 0, false
		}
		return fi.Size(), true
	}

	return 0, false
}

// newMetaReader returns an at() function backed by an eagerly-read
// metadata buffer plus an on-demand block cache for data blocks
// outside the metadata region.
//
// The caller is responsible for validating metaStart and totalBytes against
// the real size of ra before calling: both derive from superblock fields and
// would otherwise size the allocation below directly from untrusted input.
func newMetaReader(ra io.ReaderAt, metaStart, totalBytes int64, blockSize int) func(int64) []byte {
	metaSize := totalBytes - metaStart
	if metaSize <= 0 {
		return func(int64) []byte { return nil }
	}
	metaBuf := make([]byte, metaSize)
	if n, err := ra.ReadAt(metaBuf, metaStart); err != nil || int64(n) != metaSize {
		return func(int64) []byte { return nil }
	}

	cache := make(map[int64][]byte)

	return func(off int64) []byte {
		// Fast path: offset in metadata region.
		if off >= metaStart {
			o := off - metaStart
			if o >= int64(len(metaBuf)) {
				return nil
			}
			return metaBuf[o:]
		}
		// Outside metadata — flat-plain data block. Load on demand.
		if off < 0 || off >= totalBytes {
			return nil
		}
		blkAddr := off - off%int64(blockSize)
		if cached, ok := cache[blkAddr]; ok {
			return cached[off-blkAddr:]
		}
		sz := int64(blockSize)
		if blkAddr+sz > totalBytes {
			sz = totalBytes - blkAddr
		}
		buf := make([]byte, sz)
		if n, err := ra.ReadAt(buf, blkAddr); err != nil || int64(n) != sz {
			return nil
		}
		cache[blkAddr] = buf
		return buf[off-blkAddr:]
	}
}

// imgQEntry is a BFS queue entry for the image metadata walk.
type imgQEntry struct {
	nid  uint64
	path string
}

// copyFromImage is a fast path for CopyFrom when the source is an *image.
// Instead of walking via the fs.FS interface (which does per-inode ReadAt
// syscalls), it reads the entire metadata area into memory and parses
// inodes, directory entries, xattrs, and chunk indexes directly from the
// buffer. This reduces thousands of syscalls to a single ReadAt.
func (fsys *Writer) copyFromImage(img *image) error {
	metaStart := img.metaStartPos()
	totalBytes := int64(img.sb.Blocks) << img.sb.BlkSizeBits
	if totalBytes <= 0 {
		return nil
	}

	// sb.Blocks, sb.MetaBlkAddr and sb.Inos are read verbatim from the image
	// and must never size an allocation on their own: a handful of tampered
	// bytes would otherwise request a terabyte-scale buffer, which fails as
	// an unrecoverable runtime OOM rather than a returned error.
	if actual, ok := imageSize(img.meta); ok {
		if totalBytes > actual {
			return fmt.Errorf("superblock declares %d bytes but image is %d bytes: %w",
				totalBytes, actual, ErrInvalid)
		}
	} else if totalBytes-metaStart > maxEagerMetaBytes {
		return fmt.Errorf("metadata region of %d bytes exceeds the %d byte limit for a source of unknown size: %w",
			totalBytes-metaStart, int64(maxEagerMetaBytes), ErrInvalid)
	}
	if metaStart < 0 || metaStart >= totalBytes {
		return fmt.Errorf("metadata start %d outside image of %d bytes: %w",
			metaStart, totalBytes, ErrInvalid)
	}

	blkBits := img.sb.BlkSizeBits
	buildTime := img.sb.BuildTime
	buildTimeNs := img.sb.BuildTimeNs

	blockSize := int(1 << blkBits)

	// Get an accessor for image data. Reads the metadata region eagerly
	// and loads flat-plain data blocks on demand.
	at := newMetaReader(img.meta, metaStart, totalBytes, blockSize)

	// Shared xattr block address (if present). The at() function
	// will load the block on demand when xattrs are parsed.
	var sharedXattrOff int64
	if img.sb.XattrBlkAddr > 0 {
		sharedXattrOff = int64(img.sb.XattrBlkAddr) << blkBits
	}

	// Pre-allocate based on the inode count from the superblock, bounded by
	// what the metadata region can physically hold (one inode per 32-byte
	// slot at minimum) and by maxQueuePrealloc.
	maxInodes := (totalBytes - metaStart) / disk.SizeInodeCompact
	inodeCount := int64(img.sb.Inos)
	if inodeCount <= 0 || inodeCount > maxInodes {
		inodeCount = maxInodes
	}
	if inodeCount > maxQueuePrealloc {
		inodeCount = maxQueuePrealloc
	}
	if inodeCount < 64 {
		inodeCount = 64
	}
	queue := make([]imgQEntry, 0, inodeCount)
	queue = append(queue, imgQEntry{nid: uint64(img.sb.RootNid), path: "/"})

	// Directory nids are expanded at most once. EROFS, like POSIX, has no
	// directory hardlinks, so a directory reachable by two paths means the
	// dirent graph is not a tree. Following it would revisit the same
	// subtree forever, growing both the queue and the path strings without
	// bound. Non-directory nids are deliberately not tracked: a file nid
	// legitimately appears under several names when the source has
	// hardlinks, and each name needs its own entry here.
	expanded := make(map[uint64]struct{})

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		// Merge mode: process whiteout markers.
		if fsys.copyMerge && cur.path != "/" {
			base := path.Base(cur.path)
			if len(base) > len(whiteoutPrefix) && base[:len(whiteoutPrefix)] == whiteoutPrefix {
				if base == opaqueWhiteout {
					fsys.removeChildren(path.Dir(cur.path))
				} else {
					target := path.Dir(cur.path) + "/" + base[len(whiteoutPrefix):]
					if path.Dir(cur.path) == "/" {
						target = "/" + base[len(whiteoutPrefix):]
					}
					fsys.remove(target)
				}
				continue
			}
		}

		inodeAddr := metaStart + int64(cur.nid*disk.SizeInodeCompact)
		buf := at(inodeAddr)
		if len(buf) < disk.SizeInodeCompact {
			return fmt.Errorf("inode %d out of range", cur.nid)
		}

		format := binary.LittleEndian.Uint16(buf[:2])
		layout := uint8((format & 0x0E) >> 1)
		compact := format&0x01 == 0

		if compact && len(buf) < disk.SizeInodeCompact {
			return fmt.Errorf("compact inode %d out of range", cur.nid)
		}
		if !compact && len(buf) < disk.SizeInodeExtended {
			return fmt.Errorf("extended inode %d out of range", cur.nid)
		}

		var (
			mode    uint16
			uid     uint32
			gid     uint32
			nlink   uint32
			size    uint64
			idata   uint32
			mtime   uint64
			mtimeNs uint32
			xcnt    uint16
			icSize  int
		)

		if compact {
			var ino disk.InodeCompact
			ino.Unmarshal(buf)
			mode = ino.Mode
			uid = uint32(ino.UID)
			gid = uint32(ino.GID)
			nlink = uint32(ino.Nlink)
			size = uint64(ino.Size)
			idata = ino.InodeData
			mtime = buildTime
			mtimeNs = buildTimeNs
			xcnt = ino.XattrCount
			icSize = disk.SizeInodeCompact
		} else {
			var ino disk.InodeExtended
			ino.Unmarshal(buf)
			mode = ino.Mode
			uid = ino.UID
			gid = ino.GID
			nlink = ino.Nlink
			size = ino.Size
			idata = ino.InodeData
			mtime = ino.Mtime
			mtimeNs = ino.MtimeNs
			xcnt = ino.XattrCount
			icSize = disk.SizeInodeExtended
		}

		// Parse xattr area.
		xattrSize := 0
		if xcnt > 0 {
			xattrSize = int(xcnt-1)*disk.SizeXattrEntry + disk.SizeXattrBodyHeader
		}
		var xattrs map[string]string
		if xattrSize > 0 {
			xattrAddr := inodeAddr + int64(icSize)
			xb := at(xattrAddr)
			if len(xb) >= xattrSize {
				xattrs = parseXattrsFromBuf(xb[:xattrSize], at, sharedXattrOff, img.getLongPrefix)
			}
		}

		trailingAddr := inodeAddr + int64(icSize) + int64(xattrSize)
		typ := mode & disk.StatTypeMask

		// Build fsEntry directly, bypassing builder.Entry + add() overhead.
		fe := &fsEntry{
			path:    cur.path,
			mode:    mode,
			uid:     uid,
			gid:     gid,
			mtime:   mtime,
			mtimeNs: mtimeNs,
			size:    size,
			xattrs:  xattrs,
		}
		if nlink > 0 {
			fe.nlink = nlink
			fe.nlinkSet = true
		}
		fe.fileClosed = true
		if fsys.copyMetadataOnly {
			fe.metadataOnly = true
		}

		// Sizes come straight off disk. For the types whose content this
		// function materializes, the declared size must fit inside the image
		// before it is used as an allocation length — or converted to int,
		// where a value past 2^63 overflows to a negative length.
		switch typ {
		case disk.StatTypeDir, disk.StatTypeSymlink:
			if size > uint64(totalBytes) {
				return fmt.Errorf("nid %d declares %d bytes, larger than the %d byte image: %w",
					cur.nid, size, totalBytes, ErrInvalid)
			}
		}

		switch typ {
		case disk.StatTypeDir:
			// A directory reachable twice means the dirent graph is not a
			// tree; expanding it again would loop forever.
			if _, seen := expanded[cur.nid]; seen {
				return fmt.Errorf("directory nid %d is reachable more than once (at %s): %w",
					cur.nid, cur.path, ErrInvalid)
			}
			expanded[cur.nid] = struct{}{}

			dirSize := int(size)
			if dirSize > 0 {
				var dirData []byte
				switch layout {
				case disk.LayoutFlatPlain:
					dataAddr := int64(idata) << blkBits
					d := at(dataAddr)
					if d != nil && len(d) >= dirSize {
						dirData = d[:dirSize]
					} else {
						dirData = make([]byte, dirSize)
						if _, err := img.meta.ReadAt(dirData, dataAddr); err != nil {
							return fmt.Errorf("read dir data for nid %d: %w", cur.nid, err)
						}
					}
				case disk.LayoutFlatInline:
					d := at(trailingAddr)
					if d != nil && len(d) >= dirSize {
						dirData = d[:dirSize]
					}
				}
				if dirData != nil {
					fsys.parseDirBlock(dirData, dirSize, blockSize, cur.path, &queue)
				}
			}

		case disk.StatTypeSymlink:
			if size > 0 {
				var linkData []byte
				if layout == disk.LayoutFlatPlain {
					linkData = make([]byte, size)
					if _, err := img.meta.ReadAt(linkData, int64(idata)<<blkBits); err != nil {
						return fmt.Errorf("read symlink data for nid %d: %w", cur.nid, err)
					}
				} else {
					linkData = at(trailingAddr)
				}
				if linkData != nil && int(size) <= len(linkData) {
					fe.linkTarget = string(linkData[:size])
				}
			}

		case disk.StatTypeReg:
			if layout == disk.LayoutChunkBased && size > 0 {
				chunkFmt := uint16(idata)
				indexed := chunkFmt&disk.LayoutChunkFormatIndexes != 0
				chunkAddr := trailingAddr
				unit := int64(4)
				if indexed {
					unit = disk.SizeChunkIndex
					if chunkAddr%8 != 0 {
						chunkAddr = (chunkAddr + 7) & ^int64(7)
					}
				}

				// The declared size must be backed by a complete chunk-index
				// map inside the image. Without this an inode can claim a
				// petabyte-scale file, and the size flows through
				// calcTrailingSize into the in-memory metadata buffer, which
				// then grows without bound. Both chunk formats are checked:
				// only the indexed one yields chunks, but an unindexed inode
				// carries the same untrusted size.
				need, err := chunkMapBytes(chunkFmt, size, blkBits, unit)
				if err != nil {
					return fmt.Errorf("nid %d: %w", cur.nid, err)
				}
				if avail := int64(len(at(chunkAddr))); avail < need {
					return fmt.Errorf("nid %d: chunk index truncated: have %d bytes, need %d for a %d byte file: %w",
						cur.nid, avail, need, size, ErrInvalid)
				}

				if indexed {
					chunks, err := fsys.parseChunks(at(chunkAddr), chunkFmt, size, blkBits, img.deviceIDMask)
					if err != nil {
						return fmt.Errorf("chunk index for nid %d: %w", cur.nid, err)
					}
					fe.chunks = chunks
					// Only a single non-hole extent is contiguous. Claiming
					// it for a fragmented or sparse file makes planLayout
					// pick a chunk size spanning the whole file, collapsing
					// every extent into one mapping.
					fe.contiguous = len(chunks) == 1 &&
						chunks[0].PhysicalBlock != builder.NullPhysicalBlock
				}
			}

		case disk.StatTypeChrdev, disk.StatTypeBlkdev:
			fe.rdev = disk.RdevFromMode(mode, idata)
		}

		// Remap chunk DeviceIDs for metadata-only sources.
		if fsys.copyMetadataOnly && fsys.copyDeviceID > 0 {
			offset := fsys.copyDeviceID - 1
			for i := range fe.chunks {
				fe.chunks[i].DeviceID += offset
			}
		}

		// Register in the tree.
		if cur.path == "/" {
			// Update root metadata.
			fsys.root.mode = fe.mode
			fsys.root.uid = fe.uid
			fsys.root.gid = fe.gid
			fsys.root.mtime = fe.mtime
			fsys.root.mtimeNs = fe.mtimeNs
			fsys.root.nlink = fe.nlink
			fsys.root.nlinkSet = fe.nlinkSet
			fsys.root.xattrs = fe.xattrs
		} else if existing, ok := fsys.byPath[cur.path]; ok {
			// Merge overwrites: preserve tree linkage.
			savedParent := existing.parent
			savedChildren := existing.children
			*existing = *fe
			existing.parent = savedParent
			existing.children = savedChildren
		} else {
			fsys.addChild(fe)
		}
	}
	return nil
}

// parseDirBlock extracts directory entries from dirent data and enqueues
// child inodes for BFS traversal.
func (fsys *Writer) parseDirBlock(data []byte, dirSize, blockSize int, parentPath string, queue *[]imgQEntry) {
	pos := 0
	for pos < dirSize {
		blockEnd := min(pos+blockSize, dirSize)
		blk := data[pos:blockEnd]
		if len(blk) < disk.SizeDirent {
			break
		}

		firstNameOff := binary.LittleEndian.Uint16(blk[8:10])
		nEntries := int(firstNameOff / disk.SizeDirent)
		if nEntries == 0 || nEntries*disk.SizeDirent > len(blk) {
			break
		}

		for i := range nEntries {
			off := i * disk.SizeDirent
			nid := binary.LittleEndian.Uint64(blk[off : off+8])
			nameOff := int(binary.LittleEndian.Uint16(blk[off+8 : off+10]))

			var nameEnd int
			if i < nEntries-1 {
				nameEnd = int(binary.LittleEndian.Uint16(blk[(i+1)*disk.SizeDirent+8 : (i+1)*disk.SizeDirent+10]))
			} else {
				nameEnd = len(blk)
			}
			if nameOff >= len(blk) || nameEnd > len(blk) || nameOff >= nameEnd {
				continue
			}

			// Extract name, trimming trailing NUL padding.
			nameBytes := blk[nameOff:nameEnd]
			for len(nameBytes) > 0 && nameBytes[len(nameBytes)-1] == 0 {
				nameBytes = nameBytes[:len(nameBytes)-1]
			}
			name := string(nameBytes)
			if name == "." || name == ".." || name == "" {
				continue
			}

			childPath := parentPath + "/" + name
			if parentPath == "/" {
				childPath = "/" + name
			}
			*queue = append(*queue, imgQEntry{nid: nid, path: childPath})
		}

		pos = blockEnd
	}
}

// chunkMapBytes returns the on-disk size of the chunk-index map an inode of
// the given size and format declares, in entries of unit bytes each.
//
// The arithmetic is done in uint64: fileSize comes straight off disk and the
// obvious int conversion overflows to a negative count for sizes past 2^63.
func chunkMapBytes(chunkFmt uint16, fileSize uint64, blkBits uint8, unit int64) (int64, error) {
	chunkBits := blkBits + uint8(chunkFmt&disk.LayoutChunkFormatBits)
	if chunkBits >= 64 {
		return 0, fmt.Errorf("chunk size of 2^%d bytes is out of range: %w", chunkBits, ErrInvalid)
	}
	cs := uint64(1) << chunkBits
	nchunks := fileSize / cs
	if fileSize%cs != 0 {
		nchunks++
	}
	if nchunks > uint64(maxChunkIndexBytes/unit) {
		return 0, fmt.Errorf("chunk index for a %d byte file exceeds the %d byte limit: %w",
			fileSize, int64(maxChunkIndexBytes), ErrInvalid)
	}

	return int64(nchunks) * unit, nil
}

// parseChunks extracts chunk index entries from an in-memory buffer.
//
// An error is returned when the inode's declared size implies a chunk-index
// map that is larger than the data actually present, or larger than the
// reader will ever parse back (maxChunkIndexBytes). Both mean the size field
// cannot be trusted, and carrying it forward would let a corrupt source drive
// the writer's own allocations — the entry's trailing size is derived from it.
func (fsys *Writer) parseChunks(data []byte, chunkFmt uint16, fileSize uint64, blkBits uint8, deviceIDMask uint16) ([]builder.Chunk, error) {
	chunkBits := blkBits + uint8(chunkFmt&disk.LayoutChunkFormatBits)
	nchunks := int((fileSize-1)>>chunkBits) + 1
	blocksPerChunk := 1 << (chunkBits - blkBits)

	// builder.Chunk counts blocks in a uint16, so a chunk spanning more
	// blocks than that cannot be represented at all.
	if blocksPerChunk > 65535 {
		return nil, fmt.Errorf("chunk size of %d blocks is not representable: %w", blocksPerChunk, ErrInvalid)
	}

	// Align to 8 bytes for index entries.
	needed := int64(nchunks) * disk.SizeChunkIndex
	if needed > maxChunkIndexBytes {
		return nil, fmt.Errorf("chunk index of %d bytes for a %d byte file exceeds the %d byte limit: %w",
			needed, fileSize, int64(maxChunkIndexBytes), ErrInvalid)
	}
	if int64(len(data)) < needed {
		return nil, fmt.Errorf("chunk index truncated: have %d bytes, need %d: %w",
			len(data), needed, ErrInvalid)
	}

	chunks := make([]builder.Chunk, 0, nchunks)
	for i := range nchunks {
		off := i * disk.SizeChunkIndex
		startBlkLo := binary.LittleEndian.Uint32(data[off+4 : off+8])
		if ^startBlkLo == 0 {
			// A hole. Its logical span has to be carried over: chunks
			// describe the file's layout positionally, so dropping a hole
			// slides every later extent down into the wrong logical offset
			// and the file reads back unrelated device bytes where it should
			// read zeros.
			if len(chunks) > 0 {
				prev := &chunks[len(chunks)-1]
				if prev.PhysicalBlock == builder.NullPhysicalBlock &&
					int(prev.Count)+blocksPerChunk <= 65535 {
					prev.Count += uint16(blocksPerChunk)

					continue
				}
			}
			chunks = append(chunks, builder.Chunk{
				PhysicalBlock: builder.NullPhysicalBlock,
				Count:         uint16(blocksPerChunk),
			})

			continue
		}
		startBlkHi := binary.LittleEndian.Uint16(data[off : off+2])
		deviceID := binary.LittleEndian.Uint16(data[off+2:off+4]) & deviceIDMask
		physBlock := (uint64(startBlkHi) << 32) | uint64(startBlkLo)

		if len(chunks) > 0 {
			prev := &chunks[len(chunks)-1]
			if prev.PhysicalBlock != builder.NullPhysicalBlock &&
				prev.DeviceID == deviceID &&
				prev.PhysicalBlock+uint64(prev.Count) == physBlock &&
				int(prev.Count)+blocksPerChunk <= 65535 {
				prev.Count += uint16(blocksPerChunk)
				continue
			}
		}
		chunks = append(chunks, builder.Chunk{
			PhysicalBlock: physBlock,
			Count:         uint16(blocksPerChunk),
			DeviceID:      deviceID,
		})
	}

	return chunks, nil
}

// parseXattrsFromBuf parses xattr entries from an in-memory buffer.
// at provides on-demand access to the shared xattr block at sharedOff.
// longPrefix resolves long xattr prefix indexes (NameIndex with high bit set).
func parseXattrsFromBuf(buf []byte, at func(int64) []byte, sharedOff int64, longPrefix func(uint8) (string, error)) map[string]string {
	if len(buf) < disk.SizeXattrBodyHeader {
		return nil
	}

	var xh disk.XattrHeader
	xh.Unmarshal(buf)
	pos := disk.SizeXattrBodyHeader

	xattrs := make(map[string]string)

	// Resolve shared xattr references.
	for i := 0; i < int(xh.SharedCount) && pos+4 <= len(buf); i++ {
		idx := binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4

		if sharedOff == 0 {
			continue
		}
		sharedBlock := at(sharedOff + int64(idx)*4)
		if sharedBlock == nil || len(sharedBlock) < disk.SizeXattrEntry {
			continue
		}
		var xe disk.XattrEntry
		xe.Unmarshal(sharedBlock)
		entryLen := int(xe.NameLen) + int(xe.ValueLen)
		if disk.SizeXattrEntry+entryLen > len(sharedBlock) {
			continue
		}
		sb := sharedBlock[disk.SizeXattrEntry:]
		name := xattrName(xe, sb[:xe.NameLen], longPrefix)
		value := string(sb[xe.NameLen : int(xe.NameLen)+int(xe.ValueLen)])
		xattrs[name] = value
	}

	// Parse inline xattr entries.
	for pos+disk.SizeXattrEntry <= len(buf) {
		var xe disk.XattrEntry
		xe.Unmarshal(buf[pos:])
		pos += disk.SizeXattrEntry

		entryLen := int(xe.NameLen) + int(xe.ValueLen)
		if pos+entryLen > len(buf) {
			break
		}

		name := xattrName(xe, buf[pos:pos+int(xe.NameLen)], longPrefix)
		pos += int(xe.NameLen)
		value := string(buf[pos : pos+int(xe.ValueLen)])
		pos += int(xe.ValueLen)

		xattrs[name] = value

		// Round up to 4-byte boundary.
		if rem := pos % 4; rem != 0 {
			pos += 4 - rem
		}
	}
	if len(xattrs) == 0 {
		return nil
	}
	return xattrs
}

// xattrName builds the full xattr name from an entry and its raw name bytes.
// longPrefix resolves long prefix indexes when the high bit of NameIndex is set.
func xattrName(xe disk.XattrEntry, rawName []byte, longPrefix func(uint8) (string, error)) string {
	var prefix string
	if xe.NameIndex&0x80 != 0 {
		// Long prefix: high bit set, low 7 bits index the prefix table.
		if longPrefix != nil {
			if p, err := longPrefix(xe.NameIndex & 0x7F); err == nil {
				prefix = p
			}
		}
	} else if xe.NameIndex != 0 {
		prefix = xattrIndex(xe.NameIndex).String()
	}
	return prefix + string(rawName)
}
