package erofs_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/disk"
	"github.com/erofs/go-erofs/internal/erofstest"
)

type mtimeEntry struct {
	name string
	sec  int64
	nsec int64
}

// writeMtimeImage writes "/" and one regular file per entry. The root uses
// rootSec; every file uses its own mtime.
func writeMtimeImage(t *testing.T, rootSec int64, entries []mtimeEntry, opts ...erofs.CreateOpt) []byte {
	t.Helper()
	buf := &testBuffer{}
	w := erofs.Create(buf, opts...)
	for _, e := range entries {
		f, err := w.Create(e.name)
		if err != nil {
			t.Fatalf("Create(%q): %v", e.name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close(%q): %v", e.name, err)
		}
		if err := w.Chtimes(e.name, time.Time{}, time.Unix(e.sec, e.nsec)); err != nil {
			t.Fatalf("Chtimes(%q): %v", e.name, err)
		}
	}
	if err := w.Chtimes("/", time.Time{}, time.Unix(rootSec, 0)); err != nil {
		t.Fatalf("Chtimes(/): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func readSuperBlock(t *testing.T, img []byte) disk.SuperBlock {
	t.Helper()
	var sb disk.SuperBlock
	if _, err := binary.Decode(img[disk.SuperBlockOffset:], binary.LittleEndian, &sb); err != nil {
		t.Fatalf("decode superblock: %v", err)
	}
	return sb
}

// isCompact reports whether the inode for name is a compact inode by
// reading its format bit directly from the image.
func isCompact(t *testing.T, img []byte, fsys fs.FS, name string) bool {
	t.Helper()
	fi, err := fs.Stat(fsys, name)
	if err != nil {
		t.Fatalf("Stat(%q): %v", name, err)
	}
	st := fi.Sys().(*erofs.Stat)
	sb := readSuperBlock(t, img)
	off := int64(sb.MetaBlkAddr)<<sb.BlkSizeBits + st.Ino*disk.SizeInodeCompact
	return binary.LittleEndian.Uint16(img[off:])&1 == 0
}

func openImage(t *testing.T, img []byte) fs.FS {
	t.Helper()
	fsys, err := erofs.EroFS(bytes.NewReader(img))
	if err != nil {
		t.Fatalf("EroFS: %v", err)
	}
	return fsys
}

func checkMtimes(t *testing.T, fsys fs.FS, entries []mtimeEntry) {
	t.Helper()
	for _, e := range entries {
		fi, err := fs.Stat(fsys, e.name)
		if err != nil {
			t.Fatalf("Stat(%q): %v", e.name, err)
		}
		if got, want := fi.ModTime(), time.Unix(e.sec, e.nsec); !got.Equal(want) {
			t.Errorf("Stat(%q).ModTime() = %v, want %v", e.name, got, want)
		}
	}
}

func TestBuildTimeDeterministic(t *testing.T) {
	entries := []mtimeEntry{{"/a", 100, 0}, {"/b", 200, 5}, {"/c", 1700000000, 0}}
	img1 := writeMtimeImage(t, 100, entries)
	time.Sleep(1100 * time.Millisecond)
	img2 := writeMtimeImage(t, 100, entries)
	if !bytes.Equal(img1, img2) {
		t.Error("images written without WithBuildTime differ")
	}
	checkMtimes(t, openImage(t, img1), entries)
}

func TestBuildTimeDerived(t *testing.T) {
	for _, tc := range []struct {
		name    string
		root    int64
		entries []mtimeEntry
		want    uint64
		compact []string
	}{
		{
			name:    "most frequent",
			root:    300,
			entries: []mtimeEntry{{"/a", 100, 0}, {"/b", 100, 0}, {"/c", 100, 0}, {"/d", 200, 0}},
			want:    100,
			compact: []string{"a", "b", "c"},
		},
		{
			name:    "tie picks smallest",
			root:    50,
			entries: []mtimeEntry{{"/a", 200, 0}, {"/b", 200, 0}, {"/c", 100, 0}, {"/d", 100, 0}},
			want:    100,
			compact: []string{"c", "d"},
		},
		{
			name:    "no whole seconds",
			root:    0,
			entries: []mtimeEntry{{"/a", 100, 1}, {"/b", 100, 1}, {"/c", 200, 999999999}},
			want:    0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			img := writeMtimeImage(t, tc.root, tc.entries)
			sb := readSuperBlock(t, img)
			if sb.BuildTime != tc.want || sb.BuildTimeNs != 0 {
				t.Errorf("build time = (%d, %d), want (%d, 0)", sb.BuildTime, sb.BuildTimeNs, tc.want)
			}
			fsys := openImage(t, img)
			compact := map[string]bool{}
			for _, n := range tc.compact {
				compact[n] = true
			}
			for _, e := range tc.entries {
				n := e.name[1:]
				if got := isCompact(t, img, fsys, n); got != compact[n] {
					t.Errorf("%s compact = %v, want %v", n, got, compact[n])
				}
			}
			checkMtimes(t, fsys, tc.entries)
		})
	}
}

// setMtime sets the mtime of name in w.
func setMtime(t *testing.T, w *erofs.Writer, name string, sec, nsec int64) {
	t.Helper()
	if err := w.Chtimes(name, time.Time{}, time.Unix(sec, nsec)); err != nil {
		t.Fatalf("Chtimes(%q): %v", name, err)
	}
}

// createEmpty creates an empty regular file name in w with the given mtime.
func createEmpty(t *testing.T, w *erofs.Writer, name string, sec, nsec int64) {
	t.Helper()
	f, err := w.Create(name)
	if err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%q): %v", name, err)
	}
	setMtime(t, w, name, sec, nsec)
}

func TestBuildTimeNoWholeSecondsRoot(t *testing.T) {
	buf := &testBuffer{}
	w := erofs.Create(buf)
	createEmpty(t, w, "/a", 100, 7)
	setMtime(t, w, "/", 200, 9)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sb := readSuperBlock(t, buf.Bytes()); sb.BuildTime != 0 {
		t.Errorf("build time = %d, want 0", sb.BuildTime)
	}
	checkMtimes(t, openImage(t, buf.Bytes()), []mtimeEntry{{".", 200, 9}, {"a", 100, 7}})
}

func TestBuildTimeHardLinksCountOnce(t *testing.T) {
	buf := &testBuffer{}
	w := erofs.Create(buf)
	createEmpty(t, w, "/x", 200, 0)
	createEmpty(t, w, "/y", 200, 0)
	createEmpty(t, w, "/h", 100, 0)
	for _, n := range []string{"/h2", "/h3"} {
		if err := w.Link("/h", n); err != nil {
			t.Fatalf("Link(%q): %v", n, err)
		}
	}
	setMtime(t, w, "/", 300, 0)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Three names at 100 but one inode; two inodes at 200.
	if sb := readSuperBlock(t, buf.Bytes()); sb.BuildTime != 200 {
		t.Errorf("build time = %d, want 200", sb.BuildTime)
	}
	checkMtimes(t, openImage(t, buf.Bytes()), []mtimeEntry{
		{"x", 200, 0}, {"y", 200, 0}, {"h", 100, 0}, {"h2", 100, 0}, {"h3", 100, 0}, {".", 300, 0},
	})
}

func TestBuildTimeExplicitWins(t *testing.T) {
	entries := []mtimeEntry{{"/a", 100, 0}, {"/b", 100, 0}, {"/c", 5, 0}}
	img := writeMtimeImage(t, 100, entries, erofs.WithBuildTime(5, 0))
	if sb := readSuperBlock(t, img); sb.BuildTime != 5 {
		t.Errorf("build time = %d, want 5", sb.BuildTime)
	}
	fsys := openImage(t, img)
	if !isCompact(t, img, fsys, "c") || isCompact(t, img, fsys, "a") {
		t.Error("only c should be compact")
	}
	checkMtimes(t, fsys, entries)
}

func TestBuildTimeCopyFromImageWins(t *testing.T) {
	entries := []mtimeEntry{{"/a", 100, 0}, {"/b", 100, 0}}
	src := writeMtimeImage(t, 100, entries, erofs.WithBuildTime(42, 0))

	buf := &testBuffer{}
	w := erofs.Create(buf)
	if err := w.CopyFrom(openImage(t, src)); err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sb := readSuperBlock(t, buf.Bytes()); sb.BuildTime != 42 {
		t.Errorf("build time = %d, want 42", sb.BuildTime)
	}
	checkMtimes(t, openImage(t, buf.Bytes()), entries)
}

func TestBuildTimeNanosecondCompact(t *testing.T) {
	entries := []mtimeEntry{{"/a", 100, 0}, {"/b", 100, 500}}
	img := writeMtimeImage(t, 100, entries, erofs.WithBuildTime(100, 500))
	fsys := openImage(t, img)
	if isCompact(t, img, fsys, "a") {
		t.Error("a (100, 0) is compact with build time (100, 500)")
	}
	if !isCompact(t, img, fsys, "b") {
		t.Error("b (100, 500) is not compact with build time (100, 500)")
	}
	checkMtimes(t, fsys, entries)
}

func TestBuildTime48BitEarliest(t *testing.T) {
	entries := []mtimeEntry{
		{"/a", 50, 0},
		{"/b", 1700000000, 0},
		{"/c", 1700000000, 0},
		{"/d", 200, 5}, // nonzero nanoseconds: extended
	}
	img := writeMtimeImage(t, 100, entries, erofs.With48Bit())
	erofstest.FsckErofsBytes(t, img)

	sb := readSuperBlock(t, img)
	if sb.FeatureIncompat&disk.FeatureIncompat48Bit == 0 || sb.FeatureCompat&disk.FeatureCompatMtime == 0 {
		t.Errorf("features = compat %#x incompat %#x, want MTIME and 48BIT", sb.FeatureCompat, sb.FeatureIncompat)
	}
	if sb.BuildTime != 50 || sb.BuildTimeNs != 0 || sb.BuildTimeOffset != 0 {
		t.Errorf("epoch = (%d, %d) + %d, want (50, 0) + 0", sb.BuildTime, sb.BuildTimeNs, sb.BuildTimeOffset)
	}
	fsys := openImage(t, img)
	for name, want := range map[string]bool{".": true, "a": true, "b": true, "c": true, "d": false} {
		if got := isCompact(t, img, fsys, name); got != want {
			t.Errorf("%s compact = %v, want %v", name, got, want)
		}
	}
	checkMtimes(t, fsys, append(entries, mtimeEntry{".", 100, 0}))
}

func TestBuildTime48BitOutOfRange(t *testing.T) {
	entries := []mtimeEntry{{"/a", 0, 0}, {"/b", 1 << 32, 0}}
	img := writeMtimeImage(t, 0, entries, erofs.With48Bit())
	fsys := openImage(t, img)
	if !isCompact(t, img, fsys, "a") || isCompact(t, img, fsys, "b") {
		t.Error("want a compact and b extended (offset 2^32 does not fit)")
	}
	checkMtimes(t, fsys, entries)
}

func TestBuildTime48BitExplicit(t *testing.T) {
	const bt = 5_000_000_000
	entries := []mtimeEntry{
		{"/old", bt - (1<<32 - 1), 0},
		{"/older", bt - 1<<32, 0}, // before the epoch: extended
		{"/now", bt, 0},
		{"/later", bt + 1, 0}, // past the window: extended
	}
	img := writeMtimeImage(t, bt, entries, erofs.With48Bit(), erofs.WithBuildTime(bt, 0))
	erofstest.FsckErofsBytes(t, img)
	sb := readSuperBlock(t, img)
	if sb.BuildTime+uint64(sb.BuildTimeOffset) != bt || sb.BuildTimeOffset != 1<<32-1 {
		t.Errorf("epoch %d + offset %d, want %d + %d", sb.BuildTime, sb.BuildTimeOffset, bt-(1<<32-1), uint32(1<<32-1))
	}
	fsys := openImage(t, img)
	for name, want := range map[string]bool{"old": true, "older": false, "now": true, "later": false} {
		if got := isCompact(t, img, fsys, name); got != want {
			t.Errorf("%s compact = %v, want %v", name, got, want)
		}
	}
	checkMtimes(t, fsys, entries)

	// CopyFrom takes the build time, not the epoch, from a 48-bit image.
	buf := &testBuffer{}
	w := erofs.Create(buf)
	if err := w.CopyFrom(fsys); err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readSuperBlock(t, buf.Bytes()).BuildTime; got != bt {
		t.Errorf("CopyFrom build time = %d, want %d", got, bt)
	}
	checkMtimes(t, openImage(t, buf.Bytes()), entries)
}

// TestRead48BitMkfs checks that images written by mkfs.erofs -E48bit read
// back correctly: compact inodes with per-inode mtimes, and extended inodes,
// which carry the null block address (i_nb 0xffff) when they have no data
// blocks.
func TestRead48BitMkfs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mkfs.erofs directory-source mode not available on Windows")
	}
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not available")
	}
	src := t.TempDir()
	entries := []mtimeEntry{{"a", 1000, 0}, {"b", 1700000000, 0}, {"c", 1800000000, 0}, {"d/e", 1700000000, 0}}
	if err := os.Mkdir(filepath.Join(src, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		p := filepath.Join(src, e.name)
		if err := os.WriteFile(p, []byte(e.name), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, time.Unix(e.sec, 0), time.Unix(e.sec, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(src, "empty"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{filepath.Join(src, "d"), src} {
		if err := os.Chtimes(d, time.Unix(1000, 0), time.Unix(1000, 0)); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name string
		args []string
		// compact reports whether the regular files are compact inodes.
		compact bool
	}{
		{"compact", nil, true},
		// A uid above 65535 makes every inode extended.
		{"extended", []string{"--force-uid=70000"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "img.erofs")
			args := append([]string{"-E48bit", "-T1800000000", "--mkfs-time"}, tc.args...)
			cmd := exec.Command("mkfs.erofs", append(args, out, src)...)
			if b, err := cmd.CombinedOutput(); err != nil {
				t.Skipf("mkfs.erofs -E48bit: %v\n%s", err, b)
			}
			img, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if sb := readSuperBlock(t, img); sb.FeatureIncompat&disk.FeatureIncompat48Bit == 0 {
				t.Skip("mkfs.erofs did not write a 48-bit image")
			}
			fsys := openImage(t, img)
			checkMtimes(t, fsys, entries)
			for _, e := range entries {
				if got := isCompact(t, img, fsys, e.name); got != tc.compact {
					t.Errorf("%s compact = %v, want %v", e.name, got, tc.compact)
				}
				fi, err := fs.Stat(fsys, e.name)
				if err != nil {
					t.Fatal(err)
				}
				if n := fi.Sys().(*erofs.Stat).Nlink; n != 1 {
					t.Errorf("%s nlink = %d, want 1", e.name, n)
				}
				erofstest.CheckFile(t, fsys, e.name, e.name)
			}
			erofstest.CheckFile(t, fsys, "empty", "")
			if got, err := fsys.(interface{ ReadLink(string) (string, error) }).ReadLink("link"); err != nil || got != "a" {
				t.Errorf("ReadLink(link) = %q, %v; want \"a\"", got, err)
			}
			for _, opts := range [][]erofs.CopyOpt{nil, {erofs.MetadataOnly()}} {
				if err := erofs.Create(&testBuffer{}).CopyFrom(fsys, opts...); err != nil {
					t.Errorf("CopyFrom(%d opts): %v", len(opts), err)
				}
			}
		})
	}
}

// TestRead48BitStartBlkHi checks how the reader treats the high bits of an
// inode's block address (i_nb), without depending on mkfs.erofs. The image
// is written by go-erofs and patched.
func TestRead48BitStartBlkHi(t *testing.T) {
	buf := &testBuffer{}
	w := erofs.Create(buf, erofs.With48Bit())
	if err := w.Mkdir("/d", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"/d/small", "/c", "/x"} {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(name)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Mknod("/dev", disk.StatTypeChrdev|0o600, 0x0105); err != nil {
		t.Fatal(err)
	}
	// A uid above 65535 makes an inode extended; /c stays compact.
	for _, name := range []string{"/", "/d", "/d/small", "/x", "/dev"} {
		if err := w.Chown(name, 70000, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	base := buf.Bytes()
	fsys := openImage(t, base)
	for name, want := range map[string]bool{"c": true, ".": false, "d": false, "d/small": false, "x": false, "dev": false} {
		if got := isCompact(t, base, fsys, name); got != want {
			t.Fatalf("%s compact = %v, want %v", name, got, want)
		}
	}
	inodeOff := func(name string) int64 {
		t.Helper()
		fi, err := fs.Stat(fsys, name)
		if err != nil {
			t.Fatal(err)
		}
		sb := readSuperBlock(t, base)
		return int64(sb.MetaBlkAddr)<<sb.BlkSizeBits + fi.Sys().(*erofs.Stat).Ino*32
	}
	type patch struct {
		name   string
		format uint16 // bits to set in i_format
		nb     uint16 // i_nb
		iu     uint32 // i_u, if nonzero
	}
	apply := func(patches ...patch) []byte {
		img := bytes.Clone(base)
		for _, p := range patches {
			off := inodeOff(p.name)
			binary.LittleEndian.PutUint16(img[off:], binary.LittleEndian.Uint16(img[off:])|p.format)
			binary.LittleEndian.PutUint16(img[off+6:], p.nb)
			if p.iu != 0 {
				binary.LittleEndian.PutUint32(img[off+16:], p.iu)
			}
		}
		return img
	}
	check := func(t *testing.T, img []byte, wantErr bool) fs.FS {
		t.Helper()
		efs := openImage(t, img)
		for _, opts := range [][]erofs.CopyOpt{nil, {erofs.MetadataOnly()}} {
			err := erofs.Create(&testBuffer{}).CopyFrom(efs, opts...)
			if wantErr && !errors.Is(err, erofs.ErrNotImplemented) {
				t.Errorf("CopyFrom(%d opts) error = %v, want ErrNotImplemented", len(opts), err)
			} else if !wantErr && err != nil {
				t.Errorf("CopyFrom(%d opts): %v", len(opts), err)
			}
		}
		return efs
	}
	const nlink1 = 1 << disk.InodeNlink1Bit

	t.Run("null address", func(t *testing.T) {
		// Extended inodes without data blocks, as mkfs.erofs writes them.
		img := apply(
			patch{name: ".", nb: 0xFFFF, iu: 0xFFFFFFFF},
			patch{name: "d", nb: 0xFFFF, iu: 0xFFFFFFFF},
			patch{name: "d/small", nb: 0xFFFF, iu: 0xFFFFFFFF},
		)
		erofstest.FsckErofsBytes(t, img)
		efs := check(t, img, false)
		erofstest.CheckFile(t, efs, "d/small", "/d/small")
		erofstest.CheckFile(t, efs, "x", "/x")
	})
	t.Run("device", func(t *testing.T) {
		// i_u holds the device number; the kernel ignores i_nb.
		img := apply(patch{name: "dev", nb: 0xFFFF})
		efs := check(t, img, false)
		if got := erofstest.Stat(t, efs, "dev").Rdev; got != 0x0105 {
			t.Errorf("dev rdev = %#x, want 0x105", got)
		}
	})
	t.Run("nlink-1 bit", func(t *testing.T) {
		img := apply(patch{name: "c", format: nlink1, nb: 0})
		efs := check(t, img, false)
		if got := erofstest.Stat(t, efs, "c").Nlink; got != 1 {
			t.Errorf("c nlink = %d, want 1", got)
		}
		erofstest.CheckFile(t, efs, "c", "/c")
	})
	for _, tc := range []struct {
		name string
		p    patch
	}{
		{"compact wide address", patch{name: "c", format: nlink1, nb: 1}},
		{"extended wide address", patch{name: "x", nb: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			efs := check(t, apply(tc.p), true)
			if _, err := fs.Stat(efs, tc.p.name); !errors.Is(err, erofs.ErrNotImplemented) {
				t.Errorf("Stat(%s) error = %v, want ErrNotImplemented", tc.p.name, err)
			}
		})
	}
}

// TestBuildTime48BitOnlyWhenBetter checks that With48Bit uses the 48-bit
// layout only when it makes more inodes compact.
func TestBuildTime48BitOnlyWhenBetter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		root    int64
		entries []mtimeEntry
		opts    []erofs.CreateOpt
		want48  bool
	}{
		{
			name:    "one mtime",
			root:    100,
			entries: []mtimeEntry{{"/a", 100, 0}, {"/b", 100, 0}},
		},
		{
			name:    "no whole seconds",
			root:    100,
			entries: []mtimeEntry{{"/a", 200, 1}, {"/b", 300, 1}},
		},
		{
			name:    "explicit build time matches all",
			root:    100,
			entries: []mtimeEntry{{"/a", 100, 0}},
			opts:    []erofs.CreateOpt{erofs.WithBuildTime(100, 0)},
		},
		{
			name:    "explicit build time with nanoseconds",
			root:    100,
			entries: []mtimeEntry{{"/a", 50, 0}, {"/b", 60, 0}},
			opts:    []erofs.CreateOpt{erofs.WithBuildTime(100, 5)},
		},
		{
			name:    "explicit build time below the window end",
			root:    100,
			entries: []mtimeEntry{{"/a", 200, 0}, {"/b", 300, 0}},
			opts:    []erofs.CreateOpt{erofs.WithBuildTime(100, 0)},
			want48:  true,
		},
		{
			name:    "distinct mtimes",
			root:    100,
			entries: []mtimeEntry{{"/a", 200, 0}, {"/b", 300, 0}},
			want48:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			img := writeMtimeImage(t, tc.root, tc.entries, append(tc.opts, erofs.With48Bit())...)
			sb := readSuperBlock(t, img)
			if got := sb.FeatureIncompat&disk.FeatureIncompat48Bit != 0; got != tc.want48 {
				t.Errorf("48-bit layout = %v, want %v", got, tc.want48)
			}
			checkMtimes(t, openImage(t, img), tc.entries)
		})
	}
}

// TestBuildTime48BitIgnoresExtendedOnly checks that inodes which need an
// extended inode for other reasons do not make With48Bit choose the 48-bit
// layout.
func TestBuildTime48BitIgnoresExtendedOnly(t *testing.T) {
	buf := &testBuffer{}
	w := erofs.Create(buf, erofs.WithBuildTime(100, 0), erofs.With48Bit())
	createEmpty(t, w, "/a", 50, 0)
	if err := w.Chown("/a", 70000, 0); err != nil {
		t.Fatalf("Chown: %v", err)
	}
	setMtime(t, w, "/", 100, 0)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if readSuperBlock(t, buf.Bytes()).FeatureIncompat&disk.FeatureIncompat48Bit != 0 {
		t.Error("48-bit layout used, but a's UID needs an extended inode either way")
	}
	checkMtimes(t, openImage(t, buf.Bytes()), []mtimeEntry{{"a", 50, 0}, {".", 100, 0}})
}
