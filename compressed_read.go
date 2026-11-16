// Copyright 2026 The go-erofs Authors.
//
// The compressed read path is derived from the MIT-licensed am-fs-erofs
// project (https://github.com/antimatter-studios/rust-fs-erofs), Copyright
// (c) 2026 Antimatter Studios. See that project's LICENSE for the full MIT
// license text.

package erofs

import (
	"container/list"
	"fmt"
	"io"
	"sync"
)

// zmapFor returns the cached compressed-cluster map for an inode.
func (img *image) zmapFor(ino *inode) (*zmap, error) {
	if ino.zmap != nil {
		return ino.zmap, nil
	}
	if ino.zmapErr != nil {
		return nil, ino.zmapErr
	}
	z, err := openZmap(img, ino)
	if err != nil {
		ino.zmapErr = fmt.Errorf("nid %d: %w", ino.nid, err)
		return nil, ino.zmapErr
	}
	ino.zmap = z
	return z, nil
}

// mapExtent resolves a file offset to a compressed extent, applying the
// inline-tail and fragment redirections advertised by the map header.
func (z *zmap) mapExtent(off int64) (compressedExtent, error) {
	if z.size == 0 || off < 0 || off >= z.size {
		return compressedExtent{}, fmt.Errorf("nid %d: file offset %d out of range: %w", z.nid, off, ErrInvalid)
	}
	if z.fullFragment {
		return compressedExtent{
			nid:            z.nid,
			fragment:       true,
			fragmentOffset: int64(z.fragmentOff),
			sourceStart:    0,
			sourceEnd:      z.size,
		}, nil
	}

	ext, err := z.resolveExtent(off)
	if err != nil {
		return compressedExtent{}, err
	}
	ext.nid = z.nid

	if z.hasFragment {
		ti, err := z.tailInfo()
		if err != nil {
			return compressedExtent{}, err
		}
		if off >= ti.fragStart {
			fragOff := uint64(z.header.fragmentOffOrIData)
			if z.format == zmapIndexLegacy {
				// Legacy partial fragments store the high 32 bits of the
				// packed-inode offset in the tail pcluster's blkaddr.
				fragOff |= uint64(ext.pblk) << 32
			}
			return compressedExtent{
				nid:            z.nid,
				fragment:       true,
				fragmentOffset: int64(fragOff),
				sourceStart:    ti.fragStart,
				sourceEnd:      z.size,
			}, nil
		}
	}

	if z.hasInline {
		ti, err := z.tailInfo()
		if err != nil {
			return compressedExtent{}, err
		}
		if ext.headLcluster == ti.headLCN {
			ext.inline = true
			ext.inlineOffset = ti.inlineOff
			ext.inlineSize = z.idataSize
		}
	}
	return ext, nil
}

type ztailInfo struct {
	headLCN   uint64
	inlineOff int64
	fragStart int64
}

func (z *zmap) tailInfo() (ztailInfo, error) {
	if z.tailComputed {
		return ztailInfo{headLCN: z.tailHeadLCN, inlineOff: z.tailInlineOff, fragStart: z.tailFragStart}, z.tailErr
	}
	z.tailComputed = true
	if z.size <= 0 {
		return ztailInfo{}, nil
	}
	lastLCN := uint64(z.size-1) >> z.zLclusterBits
	// Resolve the last byte's extent without applying redirections.
	ext, err := z.resolveExtent(z.size - 1)
	if err != nil {
		z.tailErr = err
		return ztailInfo{}, err
	}
	inlineOff, err := z.nextPackOffset(lastLCN)
	if err != nil {
		z.tailErr = err
		return ztailInfo{}, err
	}
	z.tailHeadLCN = ext.headLcluster
	z.tailInlineOff = inlineOff
	z.tailFragStart = ext.sourceStart
	return ztailInfo{headLCN: z.tailHeadLCN, inlineOff: z.tailInlineOff, fragStart: z.tailFragStart}, nil
}

// compressedCacheKey identifies a decompressed physical cluster. Including the
// logical source range keeps deduplicated but shifted references separate.
type compressedCacheKey struct {
	nid         uint64
	pblk        uint32
	sourceStart int64
	sourceEnd   int64
	algo        uint8
	inline      bool
	inlineOff   int64
	partial     bool
}

type compressedCacheEntry struct {
	key  compressedCacheKey
	data []byte
}

type compressedCache struct {
	mu       sync.Mutex
	ll       *list.List
	items    map[compressedCacheKey]*list.Element
	bytes    int64
	maxBytes int64
	maxItems int
}

const (
	defaultCompressedCacheBytes = 32 << 20 // 32 MiB
	defaultCompressedCacheItems = 64
)

func (c *compressedCache) get(key compressedCacheKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		return nil
	}
	el, ok := c.items[key]
	if !ok {
		return nil
	}
	c.ll.MoveToFront(el)
	return el.Value.(*compressedCacheEntry).data
}

func (c *compressedCache) put(key compressedCacheKey, data []byte) {
	maxBytes := c.maxBytes
	if maxBytes == 0 {
		maxBytes = defaultCompressedCacheBytes
	}
	if int64(len(data)) > maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ll == nil {
		c.ll = list.New()
		c.items = make(map[compressedCacheKey]*list.Element)
	}
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&compressedCacheEntry{key: key, data: data})
	c.items[key] = el
	c.bytes += int64(len(data))
	maxItems := c.maxItems
	if maxItems == 0 {
		maxItems = defaultCompressedCacheItems
	}
	for (c.bytes > maxBytes || c.ll.Len() > maxItems) && c.ll.Len() > 1 {
		last := c.ll.Back()
		if last == nil {
			break
		}
		ent := last.Value.(*compressedCacheEntry)
		c.ll.Remove(last)
		delete(c.items, ent.key)
		c.bytes -= int64(len(ent.data))
	}
}

// readExtentData returns the decompressed bytes for the entire extent
// [ext.sourceStart, ext.sourceEnd).
func (img *image) readExtentData(ext compressedExtent) ([]byte, error) {
	length := ext.sourceEnd - ext.sourceStart
	if length < 0 {
		return nil, fmt.Errorf("nid %d: negative compressed extent length: %w", ext.nid, ErrInvalid)
	}
	if length == 0 {
		return nil, nil
	}
	if ext.hole {
		return make([]byte, length), nil
	}

	blockSize := int64(img.blockSize())

	if ext.fragment {
		if img.sb.PackedNid == 0 {
			return nil, fmt.Errorf("nid %d: fragment inode without a packed inode: %w", ext.nid, ErrInvalid)
		}
		data := make([]byte, length)
		if err := img.readPackedInodeAt(data, ext.fragmentOffset); err != nil {
			return nil, fmt.Errorf("nid %d: reading fragment from packed inode: %w", ext.nid, err)
		}
		return data, nil
	}

	if ext.plain {
		if uint64(length) > ext.pclusterBlocks*uint64(blockSize) {
			return nil, fmt.Errorf("nid %d: PLAIN extent length %d exceeds %d cluster blocks: %w",
				ext.nid, length, ext.pclusterBlocks, ErrInvalid)
		}
		off := int64(ext.pblk) * blockSize
		if !ext.interlaced {
			data := make([]byte, length)
			if _, err := img.meta.ReadAt(data, off); err != nil {
				return nil, fmt.Errorf("nid %d: reading PLAIN cluster at %d: %w", ext.nid, off, err)
			}
			return data, nil
		}

		// Interlaced PLAIN clusters store the source range rotated within
		// its block: on-disk bytes [cofs:] followed by [0:cofs] form the
		// source bytes beginning at sourceStart.
		input := make([]byte, ext.pclusterBlocks*uint64(blockSize))
		if _, err := img.meta.ReadAt(input, off); err != nil {
			return nil, fmt.Errorf("nid %d: reading interlaced PLAIN cluster at %d: %w", ext.nid, off, err)
		}
		rot := int(ext.sourceStart & (blockSize - 1))
		if rot >= len(input) {
			return nil, fmt.Errorf("nid %d: interlaced rotation %d exceeds cluster size %d: %w",
				ext.nid, rot, len(input), ErrInvalid)
		}
		data := make([]byte, length)
		for i := int64(0); i < length; i++ {
			data[i] = input[(int64(rot)+i)%int64(len(input))]
		}
		return data, nil
	}

	// A partial reference (-E dedupe) reuses a pcluster written for a
	// longer extent; only its leading bytes are referenced.
	partial := ext.partialRef && !ext.plain

	key := compressedCacheKey{
		nid:         ext.nid,
		pblk:        ext.pblk,
		sourceStart: ext.sourceStart,
		sourceEnd:   ext.sourceEnd,
		algo:        uint8(ext.algo),
		inline:      ext.inline,
		inlineOff:   ext.inlineOffset,
		partial:     partial,
	}
	if data := img.comprCache.get(key); data != nil {
		return data, nil
	}

	var input []byte
	if ext.inline {
		switch {
		case ext.inlineSize <= 0:
			return nil, fmt.Errorf("nid %d: inline tail pcluster has no data: %w", ext.nid, ErrInvalid)
		case zmapInlineCrossesBlock(img, ext):
			return nil, fmt.Errorf("nid %d: inline tail pcluster crosses a block boundary: %w", ext.nid, ErrInvalid)
		}
		input = make([]byte, ext.inlineSize)
		if _, err := img.meta.ReadAt(input, ext.inlineOffset); err != nil {
			return nil, fmt.Errorf("nid %d: reading inline tail at %d: %w", ext.nid, ext.inlineOffset, err)
		}
	} else {
		if ext.pclusterBlocks == 0 {
			return nil, fmt.Errorf("nid %d: compressed pcluster has zero blocks: %w", ext.nid, ErrInvalid)
		}
		input = make([]byte, ext.pclusterBlocks*uint64(blockSize))
		off := int64(ext.pblk) * blockSize
		if _, err := img.meta.ReadAt(input, off); err != nil {
			return nil, fmt.Errorf("nid %d: reading compressed cluster at %d: %w", ext.nid, off, err)
		}
	}

	output := make([]byte, length)
	if err := decodeCompressionCluster(ext.algo, img.comprCfg.lzma, input, output, partial); err != nil {
		return nil, fmt.Errorf("nid %d: %w", ext.nid, err)
	}
	img.comprCache.put(key, output)
	return output, nil
}

func (img *image) readPackedInodeAt(dst []byte, off int64) error {
	f := &file{img: img, nid: img.sb.PackedNid}
	fi, err := f.readInfo()
	if err != nil {
		return err
	}
	if off < 0 || off+int64(len(dst)) > fi.size {
		return fmt.Errorf("packed inode range [%d,%d) exceeds size %d: %w",
			off, off+int64(len(dst)), fi.size, ErrInvalid)
	}
	f.offset = off
	n, err := io.ReadFull(f, dst)
	if err != nil {
		return fmt.Errorf("read %d bytes at %d: %w", n, off, err)
	}
	return nil
}

func zmapInlineCrossesBlock(img *image, ext compressedExtent) bool {
	blockSize := int64(img.blockSize())
	off := ext.inlineOffset & (blockSize - 1)
	return off+int64(ext.inlineSize) > blockSize
}

// loadCompressedBlock loads one logical filesystem block of a compressed
// inode. It may span multiple compressed extents, so it copies each extent's
// bytes into the requested block range.
func (img *image) loadCompressedBlock(fi *inode, pos int64) (*block, error) {
	z, err := img.zmapFor(fi)
	if err != nil {
		return nil, err
	}
	blockSize := int64(img.blockSize())
	bn := pos >> img.sb.BlkSizeBits
	blockOffset := pos & (blockSize - 1)
	blockEnd := blockSize
	if bn == int64(calculateBlocks(img.sb.BlkSizeBits, fi.size)-1) {
		blockEnd = fi.size - bn*blockSize
	}
	if blockEnd < blockOffset {
		return nil, fmt.Errorf("nid %d: invalid compressed block bounds [%d:%d]: %w", fi.nid, blockOffset, blockEnd, ErrInvalid)
	}

	b := img.getBlock()
	filePos := pos
	fileEnd := bn*blockSize + blockEnd
	dstOff := int(blockOffset)
	for filePos < fileEnd {
		ext, err := z.mapExtent(filePos)
		if err != nil {
			img.putBlock(b)
			return nil, err
		}
		data, err := img.readExtentData(ext)
		if err != nil {
			img.putBlock(b)
			return nil, err
		}
		within := filePos - ext.sourceStart
		if within < 0 || within >= int64(len(data)) {
			img.putBlock(b)
			return nil, fmt.Errorf("nid %d: extent data does not contain offset %d: %w", fi.nid, filePos, ErrInvalid)
		}
		n := min(fileEnd-filePos, int64(len(data))-within)
		copy(b.buf[dstOff:dstOff+int(n)], data[within:within+n])
		filePos += n
		dstOff += int(n)
	}
	b.offset = int32(blockOffset)
	b.end = int32(blockEnd)
	return b, nil
}
