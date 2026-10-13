package erofs

import (
	"fmt"

	"github.com/erofs/go-erofs/internal/builder"
	"github.com/erofs/go-erofs/internal/disk"
)

// CreateHole creates a regular file of the given size backed entirely by holes:
// a chunk-based inode that carries the file's size but none of its data, so a
// read returns zeros and the image holds no bytes for it. The content is meant
// to come from elsewhere at mount time — an overlayfs metacopy redirect into a
// data layer — which is what a composefs-style metadata image is made of: every
// regular file's size and metadata, none of its bytes.
//
// It is the size that matters. A metadata inode written zero-length (Create
// then Close with nothing written) reports i_size 0, and overlayfs sizes a
// data-modifying copy-up by i_size — so a write onto such a file copies up zero
// bytes and silently drops the redirected content. A hole inode of the true
// size makes copy-up read and preserve it.
//
// Unlike Create there is no File to write and Close; a hole has no data. Set the
// mode, owner, mtime and xattrs afterward with Chmod, Chown, Chtimes and
// Setxattr, exactly as for a file created any other way.
func (fsys *Writer) CreateHole(name string, size int64) error {
	if fsys.wErr != nil {
		return fsys.wErr
	}
	name = cleanPath(name)
	if name == "/" {
		return fmt.Errorf("mkfs: cannot create file at root")
	}
	if size < 0 {
		return fmt.Errorf("mkfs: negative size %d for %q", size, name)
	}
	if err := fsys.checkPath(name); err != nil {
		return err
	}

	// A single hole range spanning the whole file; chunksFromRanges splits it
	// into the null chunks planLayout expects, at the same chunk size the layout
	// pass sizes the chunk index with.
	var chunks []builder.Chunk
	if size > 0 {
		c, err := fsys.chunksFromRanges([]DataRange{{Offset: holeOffset, Size: size}}, size)
		if err != nil {
			return fmt.Errorf("mkfs: hole chunks for %q: %w", name, err)
		}
		chunks = c
	}

	fsys.ensureParent(name)
	fsys.addChild(&fsEntry{path: name, ino: &fsInode{
		mode:       disk.StatTypeReg | 0o644,
		size:       uint64(size),
		chunks:     chunks,
		fileClosed: true,
	}})
	return nil
}
