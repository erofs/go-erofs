package erofs

import (
	"fmt"
	"io/fs"
	"maps"
	"path"
	"strings"

	"github.com/erofs/go-erofs/internal/builder"
)

func layerEntry(info fs.FileInfo) *builder.Entry {
	if st, ok := info.Sys().(*Stat); ok {
		return &builder.Entry{Rdev: st.Rdev, Xattrs: st.Xattrs}
	}
	return entryFromSys(info)
}

func isLayerWhiteout(p string, info fs.FileInfo) bool {
	if p != "/" && strings.HasPrefix(path.Base(p), whiteoutPrefix) {
		return true
	}
	e := layerEntry(info)
	if e == nil {
		return false
	}
	_, trustedWhiteout := e.Xattrs["trusted.overlay.whiteout"]
	_, userWhiteout := e.Xattrs["user.overlay.whiteout"]
	return info.Mode()&fs.ModeCharDevice != 0 && e.Rdev == 0 ||
		info.Mode().IsRegular() && (trustedWhiteout || userWhiteout)
}

// Remove entries from previous layers before importing this layer. Processing
// markers while walking would let an opaque marker delete an earlier sibling
// from the same layer (for example a name beginning with a dot).
func (w *Writer) removeLayerWhiteouts(src fs.FS) error {
	type removal struct {
		name     string
		children bool
	}
	var removals []removal
	err := walkLayerSource(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name := "/" + p
		if p == "." {
			name = "/"
		}
		base := path.Base(name)
		if base == opaqueWhiteout {
			removals = append(removals, removal{path.Dir(name), true})
		} else if strings.HasPrefix(base, whiteoutPrefix) {
			removals = append(removals, removal{path.Join(path.Dir(name), strings.TrimPrefix(base, whiteoutPrefix)), false})
		} else if isLayerWhiteout(name, info) {
			removals = append(removals, removal{name, false})
		}
		if e := layerEntry(info); e != nil && info.IsDir() &&
			(e.Xattrs["trusted.overlay.opaque"] == "y" || e.Xattrs["user.overlay.opaque"] == "y") {
			removals = append(removals, removal{name, true})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan layer whiteouts: %w", err)
	}
	for _, r := range removals {
		if r.children {
			w.removeChildren(r.name)
		} else {
			w.remove(r.name)
		}
	}
	return nil
}

func cloneWithoutOverlayXattrs(e *builder.Entry) *builder.Entry {
	c := *e
	c.Xattrs = maps.Clone(e.Xattrs)
	for name := range c.Xattrs {
		if strings.HasPrefix(name, "trusted.overlay.") || strings.HasPrefix(name, "user.overlay.") {
			delete(c.Xattrs, name)
		}
	}
	return &c
}

// Inline tails are not block aligned. Until mixed local/external extents are
// available, copy the complete file into the primary image instead of creating
// a chunk reference that points at unrelated bytes.
func (w *Writer) copyLocalImageFile(img *image, fpath, p string, info fs.FileInfo, e *builder.Entry, links map[hardlinkKey]*fsInode) error {
	f, err := img.Open(fpath)
	if err != nil {
		return err
	}
	c := *e
	c.Chunks = nil
	c.Data = f
	old := w.copyMetadataOnly
	w.copyMetadataOnly = false
	defer func() { w.copyMetadataOnly = old }()
	return w.add(p, &entryFileInfo{info: info, sys: &c}, links)
}

// EROFS directories cannot be hardlinked. Detect cycles before WalkDir can
// recurse indefinitely through a malformed image.
func walkLayerSource(src fs.FS, fn fs.WalkDirFunc) error {
	_, imageSource := src.(*image)
	visited := make(map[int64]bool)
	return fs.WalkDir(src, ".", func(p string, d fs.DirEntry, err error) error {
		if err == nil && imageSource && d.IsDir() {
			info, e := d.Info()
			if e != nil {
				return e
			}
			if st, ok := info.Sys().(*Stat); ok {
				if visited[st.Ino] {
					return fmt.Errorf("repeated directory inode %d: %w", st.Ino, ErrInvalid)
				}
				visited[st.Ino] = true
			}
		}
		return fn(p, d, err)
	})
}
