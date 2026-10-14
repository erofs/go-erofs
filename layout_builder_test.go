package erofs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
)

func layoutOptions() erofs.LayoutOptions {
	return erofs.LayoutOptions{MemoryLimit: 16 << 20, MetadataLimit: 16 << 20}
}

func newLayoutBuilder(t *testing.T, options erofs.LayoutOptions) *erofs.LayoutBuilder {
	t.Helper()
	b, err := erofs.NewLayoutBuilder(erofs.Node{Mode: fs.ModeDir | 0751, UID: 123, GID: 456}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

func addLayoutNode(t *testing.T, b *erofs.LayoutBuilder, parent erofs.NodeID, name string, n erofs.Node) erofs.NodeID {
	t.Helper()
	id, err := b.Add(parent, name, n)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func layoutImage(t *testing.T, l *erofs.Layout, payload []byte, blockSize int) (*os.File, fs.FS) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "layout-*.erofs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = f.Truncate(int64(l.Size())); err != nil {
		t.Fatal(err)
	}
	if uint64(len(payload)) != l.DataSize() {
		t.Fatal("payload length")
	}
	// Fill first: metadata output must not overwrite data-area bytes.
	if _, err = f.WriteAt(payload, int64(l.DataOffset())); err != nil {
		t.Fatal(err)
	}
	if err = l.WriteMetadata(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err = f.ReadAt(got, int64(l.DataOffset())); err != nil && len(got) != 0 {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("metadata overwrote payload")
	}
	// Native erofs-utils rejects blocks larger than the host page size.
	// Reader/content/determinism checks below still cover every block size.
	if blockSize <= os.Getpagesize() {
		erofstest.FsckErofs(t, f.Name())
	} else {
		t.Logf("native fsck unavailable for %d-byte blocks on a %d-byte-page host", blockSize, os.Getpagesize())
	}
	img, err := erofs.Open(f)
	if err != nil {
		t.Fatal(err)
	}
	return f, img
}

func TestLayoutMetadata(t *testing.T) {
	for _, blockSize := range []int{512, 1024, 4096, 65536} {
		for _, value := range []string{"", "a", "abcde"} {
			t.Run(fmt.Sprintf("block-%d-xattr-%d", blockSize, len(value)), func(t *testing.T) {
				opts := layoutOptions()
				opts.BlockSize = blockSize
				b := newLayoutBuilder(t, opts)
				id := addLayoutNode(t, b, 0, "00-file", erofs.Node{Mode: fs.ModeSetuid | 0660, UID: 65537, GID: 70000, Size: uint64(blockSize*2 + 3), Xattrs: map[string]string{"user.key": value}})
				for _, block := range []uint64{0, 0, 1} {
					if err := b.AddBlocks(id, block, 1); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := b.Link(0, "01-alias", id); err != nil {
					t.Fatal(err)
				}
				addLayoutNode(t, b, 0, "02-char", erofs.Node{Mode: fs.ModeDevice | fs.ModeCharDevice | 0600, Rdev: 0x103})
				dir := addLayoutNode(t, b, 0, "03-dir", erofs.Node{Mode: fs.ModeDir | fs.ModeSticky | 0755})
				for i := 0; i < 100; i++ {
					addLayoutNode(t, b, dir, fmt.Sprintf("%04d", i), erofs.Node{Mode: 0644})
				}
				addLayoutNode(t, b, 0, "04-fifo", erofs.Node{Mode: fs.ModeNamedPipe | 0600})
				hole := addLayoutNode(t, b, 0, "05-hole", erofs.Node{Mode: 0644, Size: uint64(blockSize * 3)})
				if err := b.AddBlocks(hole, erofs.HoleBlock, 3); err != nil {
					t.Fatal(err)
				}
				link := addLayoutNode(t, b, 0, "06-link", erofs.Node{Mode: fs.ModeSymlink | 0777, LinkTarget: "00-file"})
				if _, err := b.Link(0, "07-linkalias", link); err != nil {
					t.Fatal(err)
				}
				addLayoutNode(t, b, 0, "08-sock", erofs.Node{Mode: fs.ModeSocket | 0600})
				l, err := b.Prepare(context.Background(), 2)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := l.Close(); err != nil {
						t.Error(err)
					}
				}()
				payload := append(bytes.Repeat([]byte{'a'}, blockSize), bytes.Repeat([]byte{'b'}, blockSize)...)
				first, img := layoutImage(t, l, payload, blockSize)
				want := append(bytes.Repeat([]byte{'a'}, blockSize*2), []byte("bbb")...)
				for _, name := range []string{"00-file", "01-alias", "06-link", "07-linkalias"} {
					got, err := fs.ReadFile(img, name)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("%s: content err=%v", name, err)
					}
				}
				got, err := fs.ReadFile(img, "05-hole")
				if err != nil || !bytes.Equal(got, make([]byte, 3*blockSize)) {
					t.Fatalf("sparse content: %v", err)
				}
				a := erofstest.Lstat(t, img, "00-file")
				alias := erofstest.Lstat(t, img, "01-alias")
				if a.Ino != alias.Ino || a.Nlink != 2 || a.UID != 65537 || a.GID != 70000 || a.Mode != fs.ModeSetuid|0660 {
					t.Fatalf("file metadata: %+v alias=%+v", a, alias)
				}
				if a.Xattrs["user.key"] != value {
					t.Fatalf("xattrs=%v", a.Xattrs)
				}
				sym := erofstest.Lstat(t, img, "06-link")
				symalias := erofstest.Lstat(t, img, "07-linkalias")
				if sym.Ino != symalias.Ino || sym.Nlink != 2 {
					t.Fatal("symlink hard-link identity")
				}
				root := erofstest.Lstat(t, img, ".")
				if root.Mode != fs.ModeDir|0751 || root.UID != 123 || root.GID != 456 || root.Nlink != 3 {
					t.Fatalf("root metadata: %+v", root)
				}
				for name, mode := range map[string]fs.FileMode{"02-char": fs.ModeDevice | fs.ModeCharDevice, "04-fifo": fs.ModeNamedPipe, "08-sock": fs.ModeSocket} {
					if got := erofstest.Lstat(t, img, name); got.Mode.Type() != mode {
						t.Fatalf("%s: mode=%v", name, got.Mode)
					}
				}
				if got := erofstest.Lstat(t, img, "02-char"); got.Rdev != 0x103 {
					t.Fatalf("rdev=%#x", got.Rdev)
				}
				second, _ := layoutImage(t, l, payload, blockSize)
				x, err := os.ReadFile(first.Name())
				if err != nil {
					t.Fatal(err)
				}
				y, err := os.ReadFile(second.Name())
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(x, y) {
					t.Fatal("repeated metadata writes differ")
				}
			})
		}
	}
}

func TestLayoutEmpty(t *testing.T) {
	for _, emptyFile := range []bool{false, true} {
		b := newLayoutBuilder(t, layoutOptions())
		if emptyFile {
			addLayoutNode(t, b, 0, "empty", erofs.Node{Mode: 0644})
		}
		l, err := b.Prepare(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		_, img := layoutImage(t, l, nil, 4096)
		entries, err := fs.ReadDir(img, ".")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 && !emptyFile || len(entries) != 1 && emptyFile {
			t.Fatal(entries)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLayoutLimitsAndInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(*erofs.LayoutBuilder) error
	}{
		{"long-symlink", func(b *erofs.LayoutBuilder) error {
			_, err := b.Add(0, "a", erofs.Node{Mode: fs.ModeSymlink | 0777, LinkTarget: strings.Repeat("x", 4097)})
			return err
		}},
		{"parent", func(b *erofs.LayoutBuilder) error { _, e := b.Add(9, "a", erofs.Node{}); return e }},
		{"name", func(b *erofs.LayoutBuilder) error { _, e := b.Add(0, "../a", erofs.Node{}); return e }},
		{"order", func(b *erofs.LayoutBuilder) error {
			if _, e := b.Add(0, "b", erofs.Node{}); e != nil {
				return e
			}
			_, e := b.Add(0, "a", erofs.Node{})
			return e
		}},
		{"duplicate", func(b *erofs.LayoutBuilder) error {
			if _, e := b.Add(0, "a", erofs.Node{}); e != nil {
				return e
			}
			_, e := b.Add(0, "a", erofs.Node{})
			return e
		}},
		{"link-dir", func(b *erofs.LayoutBuilder) error { _, e := b.Link(0, "a", 0); return e }},
		{"xattr", func(b *erofs.LayoutBuilder) error {
			_, e := b.Add(0, "a", erofs.Node{Xattrs: map[string]string{"user.a": strings.Repeat("a", 65536)}})
			return e
		}},
		{"missing-block", func(b *erofs.LayoutBuilder) error {
			if _, e := b.Add(0, "a", erofs.Node{Size: 1}); e != nil {
				return e
			}
			_, e := b.Prepare(context.Background(), 1)
			return e
		}},
		{"excess-block", func(b *erofs.LayoutBuilder) error {
			id, e := b.Add(0, "a", erofs.Node{Size: 1})
			if e != nil {
				return e
			}
			return b.AddBlocks(id, 0, 2)
		}},
		{"address", func(b *erofs.LayoutBuilder) error {
			id, e := b.Add(0, "a", erofs.Node{Size: 1})
			if e != nil {
				return e
			}
			return b.AddBlocks(id, 0xffffffff, 1)
		}},
		{"missing-data", func(b *erofs.LayoutBuilder) error {
			id, e := b.Add(0, "a", erofs.Node{Size: 1})
			if e != nil {
				return e
			}
			if e = b.AddBlocks(id, 1, 1); e != nil {
				return e
			}
			_, e = b.Prepare(context.Background(), 1)
			return e
		}},
		{"area-overflow", func(b *erofs.LayoutBuilder) error { _, e := b.Prepare(context.Background(), 1<<32); return e }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newLayoutBuilder(t, layoutOptions())
			if err := tt.run(b); err == nil {
				t.Fatal("accepted invalid layout")
			}
		})
	}
	opts := layoutOptions()
	opts.MemoryLimit = 1
	if _, err := erofs.NewLayoutBuilder(erofs.Node{Mode: fs.ModeDir | 0755}, opts); err == nil {
		t.Fatal("ignored constructor budget")
	}
	opts.MemoryLimit = 64 << 10
	b := newLayoutBuilder(t, opts)
	for i := 0; ; i++ {
		_, err := b.Add(0, fmt.Sprintf("%08d", i), erofs.Node{Mode: 0644})
		used, peak := b.MemoryUsage()
		if used > opts.MemoryLimit || peak > opts.MemoryLimit {
			t.Fatal("allocation exceeded admitted budget")
		}
		if err != nil {
			break
		}
		if i > 1000 {
			t.Fatal("budget did not stop node growth")
		}
	}
	opts = layoutOptions()
	opts.MetadataLimit = 4096
	b = newLayoutBuilder(t, opts)
	if _, err := b.Prepare(context.Background(), 0); err == nil {
		t.Fatal("ignored metadata byte budget")
	}
}

type cancelLayoutWriter struct{ cancel context.CancelFunc }

func (w cancelLayoutWriter) WriteAt(p []byte, _ int64) (int, error) { w.cancel(); return len(p), nil }

type shortLayoutWriter struct{}

func (shortLayoutWriter) WriteAt(p []byte, _ int64) (int, error) { return len(p) - 1, nil }

func TestLayoutCancelCloseAndWriteErrors(t *testing.T) {
	b := newLayoutBuilder(t, layoutOptions())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Prepare(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b = newLayoutBuilder(t, layoutOptions())
	l, err := b.Prepare(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Add(0, "a", erofs.Node{}); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	if err = l.WriteMetadata(ctx, cancelLayoutWriter{cancel}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = l.WriteMetadata(context.Background(), shortLayoutWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "image"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = l.WriteMetadata(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.WriteMetadata(context.Background(), f); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = f.Stat(); err != nil {
		t.Fatal("layout closed caller output:", err)
	}
}

func TestLayoutTimestampsAndLongSymlink(t *testing.T) {
	opts := layoutOptions()
	opts.BuildTime = 42
	opts.BuildTimeNS = 123
	b := newLayoutBuilder(t, opts)
	addLayoutNode(t, b, 0, "a", erofs.Node{Mode: 0644, Mtime: 42})
	addLayoutNode(t, b, 0, "b", erofs.Node{Mode: 0644, Mtime: 42, MtimeNS: 123})
	target := strings.Repeat("x", 4095)
	addLayoutNode(t, b, 0, "c", erofs.Node{Mode: fs.ModeSymlink | 0777, LinkTarget: target})
	l, err := b.Prepare(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	}()
	_, img := layoutImage(t, l, nil, 4096)
	for name, ns := range map[string]int{"a": 0, "b": 123} {
		info, err := fs.Stat(img, name)
		if err != nil {
			t.Fatal(err)
		}
		if info.ModTime().Unix() != 42 || info.ModTime().Nanosecond() != ns {
			t.Fatalf("%s mtime=%s", name, info.ModTime())
		}
	}
	erofstest.CheckReadLink(t, img, "c", target)
}

func TestLayoutChunkGrowthBudget(t *testing.T) {
	opts := layoutOptions()
	opts.MemoryLimit = 64 << 10
	b := newLayoutBuilder(t, opts)
	id := addLayoutNode(t, b, 0, "a", erofs.Node{Mode: 0644, Size: 4096 * 10000})
	for i := 0; i < 10000; i++ {
		err := b.AddBlocks(id, uint64(i%2), 1)
		used, peak := b.MemoryUsage()
		if used > opts.MemoryLimit || peak > opts.MemoryLimit {
			t.Fatal("chunk growth exceeded admitted budget")
		}
		if err != nil {
			return
		}
	}
	t.Fatal("chunk budget did not stop growth")
}
