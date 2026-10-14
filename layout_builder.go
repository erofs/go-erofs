package erofs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"strings"
	"unsafe"

	"github.com/erofs/go-erofs/internal/builder"
	"github.com/erofs/go-erofs/internal/disk"
)

// NodeID identifies an entry in a LayoutBuilder. The root is always zero.
type NodeID uint32

// HoleBlock denotes a logical block with no physical backing.
const HoleBlock = builder.NullPhysicalBlock

// Node describes an inode without opening its content. Size applies to regular
// files; the size of a symlink is derived from LinkTarget. Link counts are
// computed from the entries added to the builder. Xattrs are optional.
type Node struct {
	Mode       fs.FileMode
	UID, GID   uint32
	Size       uint64
	Mtime      uint64
	MtimeNS    uint32
	Rdev       uint32
	LinkTarget string
	Xattrs     map[string]string
}

// LayoutOptions configure a single-device, uncompressed layout. MemoryLimit
// and MetadataLimit must be nonzero. MemoryLimit bounds owned data structures
// and reserved serialization workspace, including capacity-growth overlap. It
// is not a process RSS limit: Go allocator and GC overhead remain process-wide.
// MetadataLimit bounds all image bytes outside the caller-supplied data area.
type LayoutOptions struct {
	BlockSize     int // zero selects 4096; otherwise a power of two from 512 to 65536
	BuildTime     uint64
	BuildTimeNS   uint32
	MemoryLimit   uint64
	MetadataLimit uint64
}

type layoutMemory struct{ used, peak, limit uint64 }

func (m *layoutMemory) reserve(n uint64) error {
	if n > m.limit-m.used {
		return fmt.Errorf("erofs: layout memory budget exceeded")
	}
	m.used += n
	m.peak = max(m.peak, m.used)
	return nil
}
func (m *layoutMemory) release(n uint64) { m.used -= n }

func growLayoutSlice[T any](m *layoutMemory, old []T) ([]T, error) {
	if len(old) < cap(old) {
		return old, nil
	}
	var item T
	size := uint64(unsafe.Sizeof(item))
	n := max(8, cap(old)*2)
	if n < cap(old) || uint64(n) > (math.MaxUint64-64)/size {
		return nil, fmt.Errorf("erofs: layout capacity overflow")
	}
	if err := m.reserve(uint64(n)*size + 64); err != nil {
		return nil, err
	}
	next := make([]T, len(old), n)
	copy(next, old)
	if cap(old) != 0 {
		m.release(uint64(cap(old))*size + 64)
	}
	return next, nil
}

type layoutNode struct {
	entry  *erofsEntry
	parent NodeID
	blocks uint64
}

// LayoutBuilder collects metadata and references to a caller-managed data area.
// Children of each directory must be added in strictly increasing bytewise name
// order. Parents and hard-link targets must already exist. This permits bounded
// construction without a second pathname map or a source filesystem scan.
//
// Block numbers passed to AddBlocks are relative to the data area, not DataRange
// source-device identifiers. The builder never opens data files or creates a
// spool. It owns copies of node metadata and chunk references. After any error,
// Close the builder; after Prepare, the returned Layout owns its resources.
// Methods are not safe for concurrent use.
type LayoutBuilder struct {
	options                               LayoutOptions
	memory                                layoutMemory
	nodes                                 []layoutNode
	err                                   error
	closed                                bool
	maxChildren, maxXattrBytes, maxXattrs uint64
}

// NewLayoutBuilder creates a builder with the supplied root directory metadata.
func NewLayoutBuilder(root Node, options LayoutOptions) (*LayoutBuilder, error) {
	if options.BlockSize == 0 {
		options.BlockSize = 4096
	}
	if options.BlockSize < 512 || options.BlockSize > maxBlockSize || options.BlockSize&(options.BlockSize-1) != 0 || options.BuildTimeNS >= 1e9 || options.MemoryLimit == 0 || options.MetadataLimit == 0 {
		return nil, fmt.Errorf("erofs: invalid layout options")
	}
	if root.Mode.Type() != fs.ModeDir {
		return nil, fmt.Errorf("erofs: layout root must be a directory")
	}
	b := &LayoutBuilder{options: options, memory: layoutMemory{limit: options.MemoryLimit}}
	if err := b.memory.reserve(4096); err != nil {
		return nil, err
	}
	e, err := b.makeEntry("/", root)
	if err != nil {
		return nil, err
	}
	b.nodes, err = growLayoutSlice(&b.memory, b.nodes)
	if err != nil {
		return nil, err
	}
	b.nodes = append(b.nodes, layoutNode{entry: e})
	return b, nil
}

func (b *LayoutBuilder) check() error {
	if b.closed {
		return fs.ErrClosed
	}
	return b.err
}

func (b *LayoutBuilder) makeEntry(name string, n Node) (*erofsEntry, error) {
	mode := goModeToUnixMode(n.Mode)
	if modeToFileType(mode) == 0 || n.MtimeNS >= 1e9 || n.Size > uint64(math.MaxInt-b.options.BlockSize) {
		return nil, fmt.Errorf("erofs: invalid node metadata")
	}
	if n.Mode.Type() != 0 && n.Size != 0 {
		return nil, fmt.Errorf("erofs: size is only valid for regular files")
	}
	if n.Mode.Type() != fs.ModeSymlink && n.LinkTarget != "" {
		return nil, fmt.Errorf("erofs: link target on a non-symlink")
	}
	if n.Mode.Type() == fs.ModeSymlink && (len(n.LinkTarget) == 0 || len(n.LinkTarget) > maxSymlinkSize || strings.ContainsRune(n.LinkTarget, 0)) {
		return nil, fmt.Errorf("erofs: invalid symlink target")
	}
	// Charge owned strings, the entry and maps, and conservative allocator
	// rounding before cloning anything supplied by the caller.
	if uint64(len(name))+uint64(len(n.LinkTarget)) > b.memory.limit/2 {
		return nil, fmt.Errorf("erofs: node strings exceed memory budget")
	}
	charge := uint64(unsafe.Sizeof(erofsEntry{})) + 128 + 2*(uint64(len(name))+uint64(len(n.LinkTarget)))
	var xbytes uint64
	for key, value := range n.Xattrs {
		_, suffix := xattrSplit(key)
		if key == "" || strings.ContainsRune(key, 0) || len(suffix) > 255 || len(value) > 65535 {
			return nil, fmt.Errorf("erofs: xattr is not representable")
		}
		xbytes += uint64(xattrEntrySize(key, value))
		if xbytes > uint64(math.MaxUint16-1)*4 {
			return nil, fmt.Errorf("erofs: xattr area is not representable")
		}
		charge += uint64(len(key)+len(value))*2 + 256
	}
	if err := b.memory.reserve(charge); err != nil {
		return nil, err
	}
	e := &erofsEntry{name: strings.Clone(name), mode: mode, uid: n.UID, gid: n.GID, size: n.Size, mtime: n.Mtime, mtimeNs: n.MtimeNS, rdev: n.Rdev, nlink: 1, erofsFileType: modeToFileType(mode)}
	e.path = e.name
	if n.Mode.Type() == fs.ModeDir {
		e.nlink = 2
	}
	if n.Mode.Type() == 0 && n.Size > 0 {
		e.metadataOnly = true
	}
	if n.Mode.Type() == fs.ModeSymlink {
		e.symTarget = strings.Clone(n.LinkTarget)
		e.size = uint64(len(e.symTarget))
	}
	if len(n.Xattrs) > 0 {
		e.xattrs = make(map[string]string, len(n.Xattrs))
		for key, value := range n.Xattrs {
			e.xattrs[strings.Clone(key)] = strings.Clone(value)
		}
	}
	b.maxXattrBytes = max(b.maxXattrBytes, xbytes)
	b.maxXattrs = max(b.maxXattrs, uint64(len(n.Xattrs)))
	return e, nil
}

func (b *LayoutBuilder) parent(parent NodeID, name string) (*erofsEntry, error) {
	if uint64(parent) >= uint64(len(b.nodes)) {
		return nil, fmt.Errorf("erofs: missing parent")
	}
	p := b.nodes[parent].entry
	if p.mode&disk.StatTypeMask != disk.StatTypeDir {
		return nil, fmt.Errorf("erofs: parent is not a directory")
	}
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
		return nil, fmt.Errorf("erofs: invalid entry name")
	}
	if len(p.children) > 0 && p.children[len(p.children)-1].name >= name {
		return nil, fmt.Errorf("erofs: children must be added in sorted unique order")
	}
	if uint64(len(b.nodes)) >= math.MaxUint32 {
		return nil, fmt.Errorf("erofs: too many layout nodes")
	}
	return p, nil
}

func (b *LayoutBuilder) attach(parent NodeID, p, e *erofsEntry) (NodeID, error) {
	var err error
	b.nodes, err = growLayoutSlice(&b.memory, b.nodes)
	if err != nil {
		return 0, err
	}
	p.children, err = growLayoutSlice(&b.memory, p.children)
	if err != nil {
		return 0, err
	}
	if e.mode&disk.StatTypeMask == disk.StatTypeDir {
		if p.nlink == math.MaxUint32 {
			return 0, fmt.Errorf("erofs: directory link count overflow")
		}
		p.nlink++
	}
	id := NodeID(len(b.nodes))
	b.nodes = append(b.nodes, layoutNode{entry: e, parent: parent})
	p.children = append(p.children, e)
	b.maxChildren = max(b.maxChildren, uint64(len(p.children)))
	return id, nil
}

// Add adds one node below parent. The returned ID is stable until Close.
func (b *LayoutBuilder) Add(parent NodeID, name string, node Node) (_ NodeID, retErr error) {
	if err := b.check(); err != nil {
		return 0, err
	}
	defer func() {
		if retErr != nil {
			b.err = retErr
		}
	}()
	p, err := b.parent(parent, name)
	if err != nil {
		return 0, err
	}
	e, err := b.makeEntry(name, node)
	if err != nil {
		return 0, err
	}
	return b.attach(parent, p, e)
}

// Link adds another name for an existing non-directory inode. Its metadata and
// chunk references are shared; the selected link count is updated automatically.
func (b *LayoutBuilder) Link(parent NodeID, name string, target NodeID) (_ NodeID, retErr error) {
	if err := b.check(); err != nil {
		return 0, err
	}
	defer func() {
		if retErr != nil {
			b.err = retErr
		}
	}()
	p, err := b.parent(parent, name)
	if err != nil {
		return 0, err
	}
	if uint64(target) >= uint64(len(b.nodes)) {
		return 0, fmt.Errorf("erofs: missing hard-link target")
	}
	t := b.nodes[target].entry
	if t.hardLinkPrimary != nil {
		t = t.hardLinkPrimary
	}
	if t.mode&disk.StatTypeMask == disk.StatTypeDir || t.nlink == math.MaxUint32 {
		return 0, fmt.Errorf("erofs: invalid hard-link target")
	}
	if err = b.memory.reserve(uint64(unsafe.Sizeof(erofsEntry{})) + 128 + 2*uint64(len(name))); err != nil {
		return 0, err
	}
	e := &erofsEntry{name: strings.Clone(name), mode: t.mode, erofsFileType: t.erofsFileType, hardLinkPrimary: t}
	e.path = e.name
	id, err := b.attach(parent, p, e)
	if err != nil {
		return 0, err
	}
	t.nlink++
	return id, nil
}

// AddBlocks appends count logical blocks to a regular file, referencing a
// consecutive range in the data area. block is zero based; HoleBlock supplies
// zeros instead. Every file must have exactly ceil(Size/BlockSize) references.
func (b *LayoutBuilder) AddBlocks(id NodeID, block uint64, count uint16) (retErr error) {
	if err := b.check(); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			b.err = retErr
		}
	}()
	if uint64(id) >= uint64(len(b.nodes)) || count == 0 {
		return fmt.Errorf("erofs: invalid block reference")
	}
	n := &b.nodes[id]
	e := n.entry
	if e.hardLinkPrimary != nil || e.mode&disk.StatTypeMask != disk.StatTypeReg {
		return fmt.Errorf("erofs: block reference requires a primary regular file")
	}
	needed := (e.size + uint64(b.options.BlockSize) - 1) / uint64(b.options.BlockSize)
	if uint64(count) > needed-n.blocks {
		return fmt.Errorf("erofs: excess logical blocks")
	}
	if block != HoleBlock {
		w := erofsWriter{blockSize: b.options.BlockSize}
		start := uint64(w.sbAreaBlocks())
		if block >= uint64(nullAddr)-start || uint64(count) > uint64(nullAddr)-start-block {
			return fmt.Errorf("erofs: block reference exceeds compact address range")
		}
		block += start
	}
	if len(e.chunks) > 0 {
		last := &e.chunks[len(e.chunks)-1]
		contiguous := last.PhysicalBlock == HoleBlock && block == HoleBlock || last.PhysicalBlock != HoleBlock && block == last.PhysicalBlock+uint64(last.Count)
		if contiguous && uint32(last.Count)+uint32(count) <= math.MaxUint16 {
			last.Count += count
			n.blocks += uint64(count)
			return nil
		}
	}
	var err error
	e.chunks, err = growLayoutSlice(&b.memory, e.chunks)
	if err != nil {
		return err
	}
	e.chunks = append(e.chunks, builder.Chunk{PhysicalBlock: block, Count: count})
	n.blocks += uint64(count)
	return nil
}

// MemoryUsage returns owned/reserved bytes and the largest reservation seen.
func (b *LayoutBuilder) MemoryUsage() (used, peak uint64) { return b.memory.used, b.memory.peak }

// Close releases builder references. It does not close caller-owned resources.
func (b *LayoutBuilder) Close() error {
	b.nodes = nil
	b.closed = true
	b.memory.used = 0
	return nil
}

// Layout holds a complete immutable placement of metadata and a data area.
// WriteMetadata does not read or write the payload. The caller supplies those
// blocks separately and owns all output files. Close releases construction
// indexes; it does not force a GC or close output writers. Methods are not safe
// for concurrent use, and WriteMetadata may be repeated until Close.
type Layout struct {
	w                          *erofsWriter
	size, dataOffset, dataSize uint64
	memory                     layoutMemory
}

// Prepare assigns inode numbers and block addresses using the same layout
// algorithm as Writer. dataBlocks is the length of the caller-managed data
// area. On success, ownership transfers to Layout and the builder is closed.
func (b *LayoutBuilder) Prepare(ctx context.Context, dataBlocks uint64) (_ *Layout, retErr error) {
	if err := b.check(); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			b.err = retErr
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := &erofsWriter{blockSize: b.options.BlockSize, buildTime: b.options.BuildTime, buildTimeNs: b.options.BuildTimeNS, compactIndexes: true}
	headerBlocks := uint64(w.sbAreaBlocks())
	if dataBlocks >= uint64(nullAddr)-headerBlocks {
		return nil, fmt.Errorf("erofs: data area exceeds block address range")
	}
	// Reserve traversal arrays, sorting/dirent and xattr scratch, output
	// buffering and block padding before the shared layout code allocates them.
	workspace := uint64(len(b.nodes))*16 + 128 + b.maxChildren*128 + 1024 + b.maxXattrBytes*4 + b.maxXattrs*128 + uint64(w.blockSize)*4 + (64 << 10)
	if err := b.memory.reserve(workspace); err != nil {
		return nil, err
	}
	w.zeroBuf = make([]byte, w.blockSize)
	w.entries = make([]*erofsEntry, 0, len(b.nodes))
	stack := make([]*erofsEntry, 1, len(b.nodes))
	stack[0] = b.nodes[0].entry
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		w.entries = append(w.entries, e)
		for i := len(e.children) - 1; i >= 0; i-- {
			stack = append(stack, e.children[i])
		}
	}
	for _, n := range b.nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e := n.entry
		if e.hardLinkPrimary == nil && e.mode&disk.StatTypeMask == disk.StatTypeReg {
			if n.blocks != (e.size+uint64(w.blockSize)-1)/uint64(w.blockSize) {
				return nil, fmt.Errorf("erofs: missing logical blocks for %s", e.name)
			}
			for i, c := range e.chunks {
				if i%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				if c.PhysicalBlock != HoleBlock && (c.PhysicalBlock < headerBlocks || c.PhysicalBlock+uint64(c.Count) > headerBlocks+dataBlocks) {
					return nil, fmt.Errorf("erofs: chunk outside data area")
				}
			}
		}
		if uint64(len(e.children))+2 > uint64(math.MaxInt)/(uint64(w.blockSize)+256) {
			return nil, fmt.Errorf("erofs: directory size overflow")
		}
	}
	if err := w.planEntries(ctx, b.nodes[0].entry, b.options.MetadataLimit); err != nil {
		return nil, err
	}
	for _, n := range b.nodes {
		n.entry.parentNid = b.nodes[n.parent].entry.nid
	}
	addr := headerBlocks + dataBlocks
	for _, e := range w.entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.hardLinkPrimary != nil {
			continue
		}
		size := w.flatPlainDataSize(e)
		if size == 0 {
			continue
		}
		blocks := (uint64(size) + uint64(w.blockSize) - 1) / uint64(w.blockSize)
		if addr >= uint64(nullAddr) || blocks > uint64(nullAddr)-addr {
			return nil, fmt.Errorf("erofs: structure address overflow")
		}
		e.dataBlkAddr = uint32(addr)
		addr += blocks
	}
	w.metaBlkAddr = uint32(addr)
	w.reservedDataBlocks = uint32(dataBlocks)
	metaBlocks := (uint64(w.metadataBytes()) + uint64(w.blockSize) - 1) / uint64(w.blockSize)
	if metaBlocks > uint64(nullAddr)-addr {
		return nil, fmt.Errorf("erofs: image size overflow")
	}
	size := (addr + metaBlocks) * uint64(w.blockSize)
	dataSize := dataBlocks * uint64(w.blockSize)
	if size > math.MaxInt64 || size-dataSize > b.options.MetadataLimit {
		return nil, fmt.Errorf("erofs: metadata byte budget exceeded")
	}
	// The node lookup and traversal stack are no longer retained. Serialization
	// scratch stays reserved in Layout even between writes.
	b.memory.release(uint64(cap(b.nodes))*uint64(unsafe.Sizeof(layoutNode{})) + 64)
	b.memory.release(uint64(len(b.nodes))*8 + 64)
	l := &Layout{w: w, size: size, dataOffset: headerBlocks * uint64(w.blockSize), dataSize: dataSize, memory: b.memory}
	b.nodes = nil
	b.closed = true
	b.memory.used = 0
	return l, nil
}

func (l *Layout) Size() uint64                     { return l.size }
func (l *Layout) DataOffset() uint64               { return l.dataOffset }
func (l *Layout) DataSize() uint64                 { return l.dataSize }
func (l *Layout) MemoryUsage() (used, peak uint64) { return l.memory.used, l.memory.peak }
func (l *Layout) Close() error                     { l.w = nil; l.memory.used = 0; return nil }

type layoutOutput struct {
	ctx         context.Context
	out         io.WriterAt
	offset, end uint64
}

func (w *layoutOutput) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(len(p)) > w.end-w.offset {
		return 0, fmt.Errorf("erofs: metadata write outside layout")
	}
	n, err := w.out.WriteAt(p, int64(w.offset))
	if n < 0 || n > len(p) {
		return 0, fmt.Errorf("erofs: invalid writer count")
	}
	w.offset += uint64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

// WriteMetadata writes only the superblock, directories, symlinks and inode
// metadata at their planned offsets. The data area is untouched; an absent
// payload is not a filesystem hole. An output error leaves a partial layout.
func (l *Layout) WriteMetadata(ctx context.Context, out io.WriterAt) error {
	if l.w == nil {
		return fs.ErrClosed
	}
	if out == nil {
		return fmt.Errorf("erofs: nil metadata writer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w := l.w
	header := &layoutOutput{ctx: ctx, out: out, end: l.dataOffset}
	if err := w.writeBlock0(header); err != nil {
		return err
	}
	if header.offset != header.end {
		return fmt.Errorf("erofs: incomplete superblock area")
	}
	tail := &layoutOutput{ctx: ctx, out: out, offset: l.dataOffset + l.dataSize, end: l.size}
	buf := bufio.NewWriterSize(tail, 64<<10)
	for _, e := range w.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.hardLinkPrimary != nil {
			continue
		}
		size := w.flatPlainDataSize(e)
		if size == 0 {
			continue
		}
		switch e.mode & disk.StatTypeMask {
		case disk.StatTypeDir:
			if _, err := w.writeDirents(buf, e); err != nil {
				return err
			}
		case disk.StatTypeSymlink:
			if _, err := io.WriteString(buf, e.symTarget); err != nil {
				return err
			}
		default:
			return fmt.Errorf("erofs: unexpected materialized payload")
		}
		if rem := size % w.blockSize; rem != 0 {
			if _, err := buf.Write(w.zeroBuf[:w.blockSize-rem]); err != nil {
				return err
			}
		}
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	if tail.offset != uint64(w.metaBlkAddr)*uint64(w.blockSize) {
		return fmt.Errorf("erofs: structure size mismatch")
	}
	if err := w.writeMetadataInodes(buf); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	if tail.offset != tail.end {
		return fmt.Errorf("erofs: incomplete inode metadata")
	}
	return ctx.Err()
}
