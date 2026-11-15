package erofs_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/disk"
	"github.com/erofs/go-erofs/internal/erofstest"
)

// layoutFile is a regular file or symlink to write in a layout test.
type layoutFile struct {
	name    string
	content string
	symlink bool
}

// buildLayoutImage writes files (and their parent directories) and returns
// the opened image. It runs fsck.erofs when available.
func buildLayoutImage(t *testing.T, files []layoutFile, opts ...erofs.CreateOpt) (fs.FS, []byte) {
	t.Helper()
	buf := &testBuffer{}
	w := erofs.Create(buf, opts...)
	for _, f := range files {
		if dir := f.name[:strings.LastIndex(f.name, "/")]; dir != "" {
			if _, err := w.Stat(dir); err != nil {
				if err := w.Mkdir(dir, 0o755); err != nil {
					t.Fatalf("Mkdir(%q): %v", dir, err)
				}
			}
		}
		if f.symlink {
			if err := w.Symlink(f.content, f.name); err != nil {
				t.Fatalf("Symlink(%q): %v", f.name, err)
			}
			continue
		}
		fw, err := w.Create(f.name)
		if err != nil {
			t.Fatalf("Create(%q): %v", f.name, err)
		}
		if _, err := fw.Write([]byte(f.content)); err != nil {
			t.Fatalf("Write(%q): %v", f.name, err)
		}
		if err := fw.Close(); err != nil {
			t.Fatalf("Close(%q): %v", f.name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, f := range files {
		name := f.name[1:]
		if f.symlink {
			got, err := efs.(interface {
				ReadLink(string) (string, error)
			}).ReadLink(name)
			if err != nil || got != f.content {
				t.Errorf("ReadLink(%q) = %q, %v; want %q", name, got, err, f.content)
			}
			continue
		}
		erofstest.CheckFile(t, efs, name, f.content)
	}
	return efs, buf.Bytes()
}

func layoutOf(t *testing.T, efs fs.FS, name string) uint8 {
	t.Helper()
	return erofstest.Stat(t, efs, name).InodeLayout
}

// TestLayoutInlineInNextBlock checks that data which does not fit in the
// rest of a metadata block is stored inline at the start of the next block
// when that wastes less than a data block of its own would.
func TestLayoutInlineInNextBlock(t *testing.T) {
	var files []layoutFile
	for i := range 20 {
		files = append(files,
			layoutFile{name: fmt.Sprintf("/f%02d", i), content: strings.Repeat("f", 1000)},
			layoutFile{name: fmt.Sprintf("/l%02d", i), content: strings.Repeat("l", 1000), symlink: true})
	}
	for i := range 10 {
		// Directories with about 1 KiB of dirents.
		for j := range 40 {
			files = append(files, layoutFile{name: fmt.Sprintf("/d%02d/entry-%02d", i, j)})
		}
	}
	efs, img := buildLayoutImage(t, files)
	entries, err := fs.ReadDir(efs, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if got := fi.Sys().(*erofs.Stat).InodeLayout; got != disk.LayoutFlatInline {
			t.Errorf("%s layout = %d, want %d (flat-inline)", e.Name(), got, disk.LayoutFlatInline)
		}
	}
	// With every entry inline there are no data blocks, so metadata starts
	// right after the superblock block.
	if got := binary.LittleEndian.Uint32(img[disk.SuperBlockOffset+40:]); got != 1 {
		t.Errorf("metadata start block = %d, want 1 (no data blocks)", got)
	}
}

// TestLayoutDataBlockWhenCheaper checks that data is given a data block of
// its own when padding to the next metadata block would waste more.
func TestLayoutDataBlockWhenCheaper(t *testing.T) {
	efs, _ := buildLayoutImage(t, []layoutFile{
		{name: "/a", content: strings.Repeat("a", 100)},
		// Does not fit after the root directory and a; padding would waste
		// about 3.7 KiB, a data block only 196 bytes.
		{name: "/b", content: strings.Repeat("b", 3900)},
	})
	if got := layoutOf(t, efs, "a"); got != disk.LayoutFlatInline {
		t.Errorf("a layout = %d, want %d (flat-inline)", got, disk.LayoutFlatInline)
	}
	if got := layoutOf(t, efs, "b"); got != disk.LayoutFlatPlain {
		t.Errorf("b layout = %d, want %d (flat-plain)", got, disk.LayoutFlatPlain)
	}
}

// TestLayoutNoInlineData checks that WithNoInlineData still keeps regular
// file data out of the metadata.
func TestLayoutNoInlineData(t *testing.T) {
	var files []layoutFile
	for i := range 10 {
		files = append(files, layoutFile{name: fmt.Sprintf("/f%02d", i), content: strings.Repeat("f", 1000)})
	}
	efs, _ := buildLayoutImage(t, files, erofs.WithNoInlineData())
	for _, f := range files {
		if got := layoutOf(t, efs, f.name[1:]); got != disk.LayoutFlatPlain {
			t.Errorf("%s layout = %d, want %d (flat-plain)", f.name, got, disk.LayoutFlatPlain)
		}
	}
}

// TestLayoutInlineTail checks that a regular file larger than a block keeps
// its full blocks in data blocks and stores the rest after its inode, and
// that such images copy correctly.
func TestLayoutInlineTail(t *testing.T) {
	pattern := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('a' + i%23)
		}
		return string(b)
	}
	files := []layoutFile{
		{name: "/tail", content: pattern(5000)},
		{name: "/multi", content: pattern(3*4096 + 10)},
		{name: "/exact", content: pattern(2 * 4096)},
		{name: "/large-tail", content: pattern(4096 + 4090)}, // tail too big to inline
	}
	efs, img := buildLayoutImage(t, files)
	want := map[string]uint8{
		"tail":       disk.LayoutFlatInline,
		"multi":      disk.LayoutFlatInline,
		"exact":      disk.LayoutFlatPlain,
		"large-tail": disk.LayoutFlatPlain,
	}
	for name, layout := range want {
		if got := layoutOf(t, efs, name); got != layout {
			t.Errorf("%s layout = %d, want %d", name, got, layout)
		}
	}

	buf := &testBuffer{}
	w := erofs.Create(buf)
	if err := w.CopyFrom(efs); err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), img) {
		t.Error("CopyFrom of the image produced a different image")
	}
	copied, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Open copy: %v", err)
	}
	for _, f := range files {
		erofstest.CheckFile(t, copied, f.name[1:], f.content)
	}

	noInline, _ := buildLayoutImage(t, files, erofs.WithNoInlineData())
	for _, f := range files {
		if got := layoutOf(t, noInline, f.name[1:]); got != disk.LayoutFlatPlain {
			t.Errorf("WithNoInlineData: %s layout = %d, want flat-plain", f.name, got)
		}
	}
}
