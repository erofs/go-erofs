package erofs

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"path"
	"strings"
	"time"

	"github.com/erofs/go-erofs/internal/disk"
)

// TarOpt configures CopyTar.
type TarOpt func(*tarOptions)
type tarOptions struct {
	aufs      bool
	timestamp *time.Time
}

// WithAUFSWhiteouts translates OCI/AUFS whiteout names to overlayfs metadata.
func WithAUFSWhiteouts() TarOpt { return func(o *tarOptions) { o.aufs = true } }

// WithTarTimestamp overrides all timestamps imported from the archive.
func WithTarTimestamp(t time.Time) TarOpt { return func(o *tarOptions) { o.timestamp = &t } }

func tarPath(name string) (string, error) {
	if strings.IndexByte(name, 0) >= 0 {
		return "", fmt.Errorf("tar path contains NUL")
	}
	name = strings.TrimLeft(name, "/")
	name = path.Clean(name)
	if name == ".." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("tar path escapes root: %q", name)
	}
	if name == "." {
		return "/", nil
	}
	return "/" + name, nil
}

func (w *Writer) tarParents(name string) error {
	for p := path.Dir(name); p != "/"; p = path.Dir(p) {
		if e, ok := w.byPath[p]; ok && e.ino.mode&disk.StatTypeMask != disk.StatTypeDir {
			return fmt.Errorf("tar parent %s: %w", p, ErrNotDirectory)
		}
	}
	w.ensureParent(name)
	return nil
}

// CopyTar imports an uncompressed tar stream without extracting it into a
// host directory. File data is spooled on disk. Forward hardlinks are resolved
// after the archive has been read; links through non-directories are rejected.
func (w *Writer) CopyTar(r io.Reader, opts ...TarOpt) error {
	var o tarOptions
	for _, opt := range opts {
		opt(&o)
	}
	type hardlink struct{ name, target string }
	var pending []hardlink
	pendingByName := make(map[string]int)
	tr := tar.NewReader(r)
	for {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name, err := tarPath(h.Name)
		if err != nil {
			return err
		}
		if h.Size < 0 || h.Uid < 0 || h.Gid < 0 || uint64(h.Uid) > math.MaxUint32 || uint64(h.Gid) > math.MaxUint32 {
			return fmt.Errorf("invalid tar metadata for %q", h.Name)
		}
		delete(pendingByName, name)
		if err = w.tarParents(name); err != nil {
			return err
		}
		base := path.Base(name)
		if o.aufs && strings.HasPrefix(base, whiteoutPrefix) {
			if base == opaqueWhiteout {
				dir := path.Dir(name)
				if err = w.Setxattr(dir, "trusted.overlay.opaque", "y"); err != nil {
					return err
				}
			} else {
				target := path.Join(path.Dir(name), strings.TrimPrefix(base, whiteoutPrefix))
				if target == "/" {
					return fmt.Errorf("invalid root whiteout")
				}
				delete(pendingByName, target)
				w.remove(target)
				if err = w.Mknod(target, disk.StatTypeChrdev, 0); err != nil {
					return err
				}
				if err = w.Setxattr(path.Dir(target), "trusted.overlay.origin", ""); err != nil {
					return err
				}
			}
			continue
		}
		if name == "/" && h.Typeflag != tar.TypeDir {
			return fmt.Errorf("tar root must be a directory")
		}
		if e, ok := w.byPath[name]; ok && name != "/" {
			if h.Typeflag != tar.TypeDir || e.ino.mode&disk.StatTypeMask != disk.StatTypeDir {
				w.remove(name)
			}
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeGNUSparse:
			f, err := w.Create(name)
			if err != nil {
				return err
			}
			n, err := f.ReadFrom(tr)
			if err != nil {
				return err
			}
			if n != h.Size {
				return io.ErrUnexpectedEOF
			}
			if err = f.Close(); err != nil {
				return err
			}
		case tar.TypeDir:
			if _, ok := w.byPath[name]; !ok || name == "/" {
				if err = w.Mkdir(name, h.FileInfo().Mode()); err != nil {
					return err
				}
			}
		case tar.TypeSymlink:
			if len(h.Linkname) > maxSymlinkSize {
				return fmt.Errorf("tar symlink %s is too long", name)
			}
			if err = w.Symlink(h.Linkname, name); err != nil {
				return err
			}
		case tar.TypeLink:
			target, err := tarPath(h.Linkname)
			if err != nil {
				return err
			}
			if err = w.tarParents(target); err != nil {
				return err
			}
			if e, ok := w.byPath[target]; ok {
				if e.ino.mode&disk.StatTypeMask == disk.StatTypeDir {
					return fmt.Errorf("tar hardlink %s targets directory", name)
				}
				if err = w.Link(target, name); err != nil {
					return err
				}
			} else {
				pendingByName[name] = len(pending)
				pending = append(pending, hardlink{name, target})
			}
			continue
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			var mode uint16
			switch h.Typeflag {
			case tar.TypeChar:
				mode = disk.StatTypeChrdev
			case tar.TypeBlock:
				mode = disk.StatTypeBlkdev
			case tar.TypeFifo:
				mode = disk.StatTypeFifo
			}
			if h.Devmajor < 0 || h.Devmajor > 0xfff || h.Devminor < 0 || h.Devminor > 0xfffff {
				return fmt.Errorf("invalid device number for %s", name)
			}
			dev := uint32(h.Devminor&0xff) | uint32(h.Devmajor)<<8 | uint32(h.Devminor&0xfff00)<<12
			if err = w.Mknod(name, mode|uint16(h.Mode&0o7777), dev); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported tar type %d for %s: %w", h.Typeflag, name, ErrNotImplemented)
		}
		if err = w.Chmod(name, h.FileInfo().Mode()); err != nil {
			return err
		}
		if err = w.Chown(name, h.Uid, h.Gid); err != nil {
			return err
		}
		mt := h.ModTime
		if o.timestamp != nil {
			mt = *o.timestamp
		}
		if err = w.Chtimes(name, mt, mt); err != nil {
			return err
		}
		for key, value := range h.Xattrs {
			if err = w.Setxattr(name, key, value); err != nil {
				return err
			}
		}
	}
	for len(pendingByName) > 0 {
		progress := false
		for i, l := range pending {
			if current, ok := pendingByName[l.name]; !ok || current != i {
				continue
			}
			if err := w.tarParents(l.target); err != nil {
				return err
			}
			e, ok := w.byPath[l.target]
			if !ok {
				continue
			}
			if e.ino.mode&disk.StatTypeMask == disk.StatTypeDir {
				return fmt.Errorf("tar hardlink %s targets directory", l.name)
			}
			if err := w.tarParents(l.name); err != nil {
				return err
			}
			if err := w.Link(l.target, l.name); err != nil {
				return err
			}
			delete(pendingByName, l.name)
			progress = true
		}
		if !progress {
			for _, i := range pendingByName {
				l := pending[i]
				return fmt.Errorf("unresolved tar hardlink %s -> %s: %w", l.name, l.target, fs.ErrNotExist)
			}
		}
	}
	return nil
}

// Abort releases temporary resources without serializing an image.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.spool != nil {
		return w.spool.Close()
	}
	return nil
}
