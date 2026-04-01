// Package tar provides an [fs.FS] view over tar archives for use with
// [erofs.Writer.CopyFrom].
package tar

import (
	archivetar "archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/erofs/go-erofs/internal/builder"
)

const (
	whiteoutPrefix     = ".wh."
	opaqueWhiteout     = ".wh..wh..opq"
	overlayOpaqueXattr = "trusted.overlay.opaque"
)

// Option configures the tar source.
type Option func(*config)

type config struct {
	convertWhiteouts bool
}

// ConvertWhiteouts enables AUFS-to-overlayfs whiteout conversion.
// AUFS .wh.<name> files become overlayfs character device 0/0 whiteouts,
// and .wh..wh..opq markers become trusted.overlay.opaque xattrs.
func ConvertWhiteouts() Option {
	return func(c *config) {
		c.convertWhiteouts = true
	}
}

// FS implements fs.FS, fs.StatFS, and fs.ReadDirFS over a parsed tar.
// FileInfo.Sys() returns *builder.Entry with extended metadata.
// When the input is not an io.ReaderAt, file data is spooled to a
// temporary file; call Close to release it.
type FS struct {
	root    *node
	nodes   map[string]*node
	tarSize int64    // total bytes consumed from the tar stream
	tmp     *os.File // temp spool file, nil when source is io.ReaderAt
}

// Close releases resources held by the FS. If file data was spooled to a
// temporary file (non-ReaderAt input), the file descriptor is closed.
// Close is safe to call multiple times.
func (f *FS) Close() error {
	if f.tmp != nil {
		err := f.tmp.Close()
		f.tmp = nil
		return err
	}
	return nil
}

// BlockSize returns 512 — the tar archive alignment.
func (f *FS) BlockSize() uint32 { return 512 }

// BuildTime returns the most common mtime across all entries.
// Entries matching this can use compact inodes.
func (f *FS) BuildTime() uint64 {
	counts := make(map[uint64]int)
	for _, n := range f.nodes {
		if n.info != nil && n.info.be != nil {
			counts[n.info.be.Mtime]++
		}
	}
	var best uint64
	var bestCount int
	for mt, c := range counts {
		if c > bestCount {
			best = mt
			bestCount = c
		}
	}
	return best
}

// DeviceBlocks returns the total number of 512-byte blocks in the tar.
// Used to set the device slot size for metadata-only mode.
func (f *FS) DeviceBlocks() uint64 { return (uint64(f.tarSize) + 511) / 512 }

type node struct {
	name     string
	path     string // cleaned absolute path (e.g. "/usr/bin/foo")
	info     *entryInfo
	children []*node
}

// entryInfo implements fs.FileInfo; Sys() returns *builder.Entry.
type entryInfo struct {
	name string
	size int64
	mode fs.FileMode
	mt   time.Time
	be   *builder.Entry
}

func (i *entryInfo) Name() string       { return i.name }
func (i *entryInfo) Size() int64        { return i.size }
func (i *entryInfo) Mode() fs.FileMode  { return i.mode }
func (i *entryInfo) ModTime() time.Time { return i.mt }
func (i *entryInfo) IsDir() bool        { return i.mode.IsDir() }
func (i *entryInfo) Sys() any           { return i.be }

// Open parses a tar stream and returns an fs.FS. The entire tar is consumed
// eagerly. If r implements [io.ReaderAt], file data references the source
// directly; otherwise data is written to a temporary file.
func Open(r io.Reader, opts ...Option) (*FS, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	ds, err := newDataStore(r)
	if err != nil {
		return nil, err
	}

	// Use a position tracker to record where each file's data begins.
	// For seekable sources, query position via Seek; otherwise wrap in countingReader.
	var pr posReader
	if s, ok := r.(io.ReadSeeker); ok {
		pr = &seekPosReader{rs: s}
	} else {
		cr := &countingReader{r: r}
		pr = cr
		r = cr
	}
	tr := archivetar.NewReader(r)

	var entries []parsed
	var dirsByPath map[string]int
	var pendingOpaque map[string]bool // dirs with opaque marker seen before dir entry
	if cfg.convertWhiteouts {
		dirsByPath = make(map[string]int)
		pendingOpaque = make(map[string]bool)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}

		p := path.Clean("/" + hdr.Name)

		if cfg.convertWhiteouts {
			base := path.Base(p)
			if strings.HasPrefix(base, whiteoutPrefix) {
				if base == opaqueWhiteout {
					dir := path.Dir(p)
					if idx, ok := dirsByPath[dir]; ok {
						be := entries[idx].info.be
						if be.Xattrs == nil {
							be.Xattrs = make(map[string]string)
						}
						be.Xattrs[overlayOpaqueXattr] = "y"
					} else {
						pendingOpaque[dir] = true
					}
					continue
				}
				origName := base[len(whiteoutPrefix):]
				wp := path.Join(path.Dir(p), origName)
				entries = append(entries, parsed{
					path: wp,
					info: makeWhiteoutInfo(origName, hdr),
				})
				continue
			}
		}

		dataPos := pr.pos()
		info, ok, err := makeEntryInfo(ds, tr, hdr, p, dataPos)
		if err != nil {
			return nil, fmt.Errorf("tar: %s: %w", p, err)
		}
		if !ok {
			continue
		}

		entries = append(entries, parsed{path: p, info: info})

		if cfg.convertWhiteouts && info.mode.IsDir() {
			dirsByPath[p] = len(entries) - 1
			if pendingOpaque[p] {
				be := info.be
				if be.Xattrs == nil {
					be.Xattrs = make(map[string]string)
				}
				be.Xattrs[overlayOpaqueXattr] = "y"
				delete(pendingOpaque, p)
			}
		}
	}

	f := buildFS(entries)
	f.tarSize = pr.pos()
	f.tmp = ds.tmp
	return f, nil
}

type parsed struct {
	path string
	info *entryInfo
}

// buildFS constructs an in-memory FS tree from parsed entries.
func buildFS(entries []parsed) *FS {
	f := &FS{nodes: make(map[string]*node)}

	// Ensure root exists.
	f.root = &node{
		name: "/",
		path: "/",
		info: &entryInfo{
			name: "/",
			mode: fs.ModeDir | 0o755,
			be:   &builder.Entry{Nlink: 2},
		},
	}
	f.nodes["/"] = f.root

	for _, e := range entries {
		n := &node{
			name: path.Base(e.path),
			path: e.path,
			info: e.info,
		}

		if e.path == "/" {
			// Update root metadata.
			f.root.info = e.info
			continue
		}

		// Ensure parent dirs exist.
		f.ensureDir(path.Dir(e.path))

		// Handle duplicate paths (tar overwrite semantics).
		if existing, ok := f.nodes[e.path]; ok {
			existing.info = e.info
			existing.children = nil
			n = existing
		} else {
			parent := f.nodes[path.Dir(e.path)]
			parent.children = append(parent.children, n)
			f.nodes[e.path] = n
		}

		if e.info.mode.IsDir() {
			f.nodes[e.path] = n
		}
	}

	return f
}

func (f *FS) ensureDir(p string) {
	if p == "/" {
		return
	}
	if _, ok := f.nodes[p]; ok {
		return
	}
	f.ensureDir(path.Dir(p))

	n := &node{
		name: path.Base(p),
		path: p,
		info: &entryInfo{
			name: path.Base(p),
			mode: fs.ModeDir | 0o755,
			be:   &builder.Entry{Nlink: 2},
		},
	}
	parent := f.nodes[path.Dir(p)]
	parent.children = append(parent.children, n)
	f.nodes[p] = n
}

func cleanName(name string) string {
	if name == "." || name == "" {
		return "/"
	}
	if name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}

// Open opens the named file.
func (f *FS) Open(name string) (fs.File, error) {
	name = cleanName(name)
	n, ok := f.nodes[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if n.info.mode.IsDir() {
		return &dir{node: n}, nil
	}
	if n.info.mode.IsRegular() && n.info.be != nil && n.info.be.Data != nil {
		if sr, ok := n.info.be.Data.(*io.SectionReader); ok {
			// Return a fresh SectionReader so multiple Opens work.
			return &file{node: n, reader: io.NewSectionReader(sr, 0, sr.Size())}, nil
		}
	}
	return &file{node: n}, nil
}

// Stat returns a FileInfo for the named file.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	name = cleanName(name)
	n, ok := f.nodes[name]
	if !ok {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return n.info, nil
}

// ReadDir reads the named directory.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	name = cleanName(name)
	n, ok := f.nodes[name]
	if !ok {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	if !n.info.mode.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fmt.Errorf("not a directory")}
	}
	entries := make([]fs.DirEntry, len(n.children))
	for i, c := range n.children {
		entries[i] = &nodeDirEntry{n: c}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries, nil
}

// ReadLink reads the target of the named symbolic link.
func (f *FS) ReadLink(name string) (string, error) {
	name = cleanName(name)
	n, ok := f.nodes[name]
	if !ok {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrNotExist}
	}
	if n.info.mode.Type()&fs.ModeSymlink == 0 {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fmt.Errorf("not a symlink")}
	}
	return n.info.be.LinkTarget, nil
}

// nodeDirEntry implements fs.DirEntry.
type nodeDirEntry struct {
	n *node
}

func (de *nodeDirEntry) Name() string               { return de.n.name }
func (de *nodeDirEntry) IsDir() bool                { return de.n.info.mode.IsDir() }
func (de *nodeDirEntry) Type() fs.FileMode          { return de.n.info.mode.Type() }
func (de *nodeDirEntry) Info() (fs.FileInfo, error) { return de.n.info, nil }

// file implements fs.File.
type file struct {
	node   *node
	reader io.Reader
	closed bool
}

func (f *file) Stat() (fs.FileInfo, error) { return f.node.info, nil }
func (f *file) Read(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	if f.reader == nil {
		return 0, io.EOF
	}
	return f.reader.Read(p)
}
func (f *file) Close() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return nil
}

// dir implements fs.ReadDirFile.
type dir struct {
	node   *node
	offset int
	closed bool
}

func (d *dir) Stat() (fs.FileInfo, error) { return d.node.info, nil }
func (d *dir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.node.path, Err: fmt.Errorf("is a directory")}
}
func (d *dir) Close() error {
	if d.closed {
		return fs.ErrClosed
	}
	d.closed = true
	return nil
}
func (d *dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.closed {
		return nil, fs.ErrClosed
	}
	children := d.node.children
	// Sort for deterministic order.
	sort.Slice(children, func(i, j int) bool {
		return children[i].name < children[j].name
	})
	if n <= 0 {
		entries := make([]fs.DirEntry, len(children)-d.offset)
		for i, c := range children[d.offset:] {
			entries[i] = &nodeDirEntry{n: c}
		}
		d.offset = len(children)
		return entries, nil
	}

	remaining := children[d.offset:]
	if len(remaining) == 0 {
		return nil, io.EOF
	}
	if n > len(remaining) {
		n = len(remaining)
	}
	entries := make([]fs.DirEntry, n)
	for i, c := range remaining[:n] {
		entries[i] = &nodeDirEntry{n: c}
	}
	d.offset += n
	if d.offset >= len(children) {
		return entries, io.EOF
	}
	return entries, nil
}

func makeWhiteoutInfo(name string, hdr *archivetar.Header) *entryInfo {
	return &entryInfo{
		name: name,
		mode: fs.ModeDevice | fs.ModeCharDevice | 0o666,
		mt:   hdr.ModTime,
		be: &builder.Entry{
			UID:   uint32(hdr.Uid),
			GID:   uint32(hdr.Gid),
			Mtime: uint64(hdr.ModTime.Unix()),
			Nlink: 1,
		},
	}
}

func makeEntryInfo(ds *dataStore, tr *archivetar.Reader, hdr *archivetar.Header, p string, dataPos int64) (*entryInfo, bool, error) {
	be := &builder.Entry{
		UID:   uint32(hdr.Uid),
		GID:   uint32(hdr.Gid),
		Mtime: uint64(hdr.ModTime.Unix()),
		Nlink: 1,
	}

	xattrs := extractXattrs(hdr)
	if len(xattrs) > 0 {
		be.Xattrs = xattrs
	}

	var mode fs.FileMode
	var size int64

	switch hdr.Typeflag {
	case archivetar.TypeReg, archivetar.TypeRegA: //nolint:staticcheck // TypeRegA needed for old tar archives
		mode = fs.FileMode(hdr.Mode & 0o7777)
		size = hdr.Size
		be.Contiguous = true // tar data is always contiguous
		if hdr.Size > 0 {
			data, err := ds.store(tr, dataPos, hdr.Size)
			if err != nil {
				return nil, false, err
			}
			be.Data = data

			// Record chunk mapping into the original tar (512-byte blocks).
			startBlock := uint64(dataPos) / 512
			totalBlocks := (uint64(hdr.Size) + 511) / 512
			for totalBlocks > 0 {
				count := totalBlocks
				if count > 65535 {
					count = 65535
				}
				be.Chunks = append(be.Chunks, builder.Chunk{
					PhysicalBlock: startBlock,
					Count:         uint16(count),
					DeviceID:      1,
				})
				startBlock += count
				totalBlocks -= count
			}
		}
	case archivetar.TypeDir:
		mode = fs.ModeDir | fs.FileMode(hdr.Mode&0o7777)
		be.Nlink = 2
	case archivetar.TypeSymlink:
		mode = fs.ModeSymlink | fs.FileMode(hdr.Mode&0o7777)
		be.LinkTarget = hdr.Linkname
	case archivetar.TypeChar:
		mode = fs.ModeDevice | fs.ModeCharDevice | fs.FileMode(hdr.Mode&0o7777)
		be.Rdev = uint32(hdr.Devmajor<<8 | hdr.Devminor)
	case archivetar.TypeBlock:
		mode = fs.ModeDevice | fs.FileMode(hdr.Mode&0o7777)
		be.Rdev = uint32(hdr.Devmajor<<8 | hdr.Devminor)
	case archivetar.TypeFifo:
		mode = fs.ModeNamedPipe | fs.FileMode(hdr.Mode&0o7777)
	default:
		return nil, false, nil
	}

	return &entryInfo{
		name: path.Base(p),
		size: size,
		mode: mode,
		mt:   hdr.ModTime,
		be:   be,
	}, true, nil
}

// extractXattrs returns xattrs from PAX records.
func extractXattrs(hdr *archivetar.Header) map[string]string {
	const xattrPrefix = "SCHILY.xattr."
	xattrs := make(map[string]string)
	for k, v := range hdr.PAXRecords {
		if len(k) > len(xattrPrefix) && k[:len(xattrPrefix)] == xattrPrefix {
			xattrs[k[len(xattrPrefix):]] = v
		}
	}
	if len(xattrs) == 0 {
		return nil
	}
	return xattrs
}

// dataStore manages file data spooling. For ReaderAt-backed tars,
// it records offsets into the source. For streaming readers, it
// writes data to a temp file.
type dataStore struct {
	ra   io.ReaderAt
	tmp  *os.File
	toff int64
}

func newDataStore(r io.Reader) (*dataStore, error) {
	if ra, ok := r.(io.ReaderAt); ok {
		return &dataStore{ra: ra}, nil
	}
	tmp, err := os.CreateTemp("", "erofs-tar-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	_ = os.Remove(tmp.Name()) // unlink immediately; fd keeps data accessible
	return &dataStore{tmp: tmp}, nil
}

// store records file data and returns a reader for deferred access.
// For ReaderAt-backed sources, the data is not read — the returned
// SectionReader reads directly from the source at the recorded offset.
// The tar reader's unconsumed data is skipped automatically by Next().
func (ds *dataStore) store(tr *archivetar.Reader, pos int64, size int64) (io.Reader, error) {
	if ds.ra != nil {
		// Don't read the data — Next() will skip it.
		return io.NewSectionReader(ds.ra, pos, size), nil
	}
	off := ds.toff
	n, err := io.Copy(ds.tmp, tr)
	if err != nil {
		return nil, err
	}
	ds.toff += n
	return io.NewSectionReader(ds.tmp, off, size), nil
}

// posReader tracks the current byte position in the tar stream.
type posReader interface {
	pos() int64
}

// seekPosReader uses Seek to query position (avoids wrapping the reader).
type seekPosReader struct {
	rs io.ReadSeeker
}

func (s *seekPosReader) pos() int64 {
	off, _ := s.rs.Seek(0, io.SeekCurrent)
	return off
}

// countingReader wraps a non-seekable reader and tracks bytes consumed.
type countingReader struct {
	r    io.Reader
	offs int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.offs += int64(n)
	return n, err
}

func (cr *countingReader) pos() int64 {
	return cr.offs
}
