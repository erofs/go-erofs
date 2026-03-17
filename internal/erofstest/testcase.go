package erofstest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/erofs/go-erofs"
)

// TestCase is a reusable test case that produces tar content, converts it
// to an EROFS image, and verifies the result.
type TestCase interface {
	// Run creates an EROFS image from tar content using conv, opens it,
	// and verifies the result. If conv is nil, MkfsErofs() is used.
	Run(t testing.TB, conv Converter)
}

type testCase struct {
	tar    func() WriterToTar
	verify func(t testing.TB, fsys fs.FS)
}

// Converter creates an fs.FS from tar content. The converter owns the
// entire pipeline: it may write the tar to disk, convert through an
// intermediate format (e.g. ext4), use a Go-native builder, etc.
type Converter func(t testing.TB, wt WriterToTar) fs.FS

// MkfsErofs returns a Converter that pipes the tar stream to the
// mkfs.erofs binary. Extra CLI flags can be passed via opts.
func MkfsErofs(opts ...string) Converter {
	return func(t testing.TB, wt WriterToTar) fs.FS {
		t.Helper()
		tarStream := TarFromWriterTo(wt)
		defer tarStream.Close()

		path := filepath.Join(t.TempDir(), "test.erofs")
		if err := ConvertTarErofs(context.Background(), tarStream, path, "", opts); err != nil {
			t.Fatal(err)
		}
		return openEroFS(t, path)
	}
}

// MkfsErofsBlobDev returns a Converter that uses the mkfs.erofs binary
// with --blobdev, writing file data to a separate device file. Extra
// CLI flags can be passed via extraOpts.
func MkfsErofsBlobDev(chunkSize int, extraOpts ...string) Converter {
	return func(t testing.TB, wt WriterToTar) fs.FS {
		t.Helper()
		tarStream := TarFromWriterTo(wt)
		defer tarStream.Close()

		path := filepath.Join(t.TempDir(), "test.erofs")
		blobPath := path + ".blob"
		opts := append([]string{
			fmt.Sprintf("--blobdev=%s", blobPath),
			fmt.Sprintf("--chunksize=%d", chunkSize),
		}, extraOpts...)
		if err := ConvertTarErofs(context.Background(), tarStream, path, "", opts); err != nil {
			t.Fatal(err)
		}
		bf, err := os.Open(blobPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { bf.Close() })
		return openEroFS(t, path, erofs.WithExtraDevices(bf))
	}
}

// openEroFS opens an EROFS image file and returns an fs.FS.
func openEroFS(t testing.TB, path string, opts ...erofs.Opt) fs.FS {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	efs, err := erofs.EroFS(f, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return efs
}

func (tc *testCase) Run(t testing.TB, conv Converter) {
	t.Helper()
	if conv == nil {
		conv = MkfsErofs()
	}
	efs := conv(t, tc.tar())
	tc.verify(t, efs)
}

// Basic is the standard test case exercising files, directories, symlinks,
// empty files, large files, case-sensitive names, many-entry directories,
// xattrs (including long prefixes), and device files.
var Basic TestCase = &testCase{
	tar: func() WriterToTar {
		tc := TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

		lotsOfFilesC := make(chan WriterToTar)
		go func() {
			for i := range 5000 {
				lotsOfFilesC <- tc.File(fmt.Sprintf("/usr/lib/testdir/lotsoffiles/%d", i), []byte{}, 0600)
			}
			close(lotsOfFilesC)
		}()

		return TarAll(
			tc.Dir("/usr", 0755),
			tc.Dir("/usr/lib", 0755),
			tc.Dir("/usr/lib/testdir", 0755),
			tc.File("/in-root.txt", []byte("root file content\n"), 0600),
			tc.File("/usr/lib/testdir/emptyfile", []byte{}, 0600),
			tc.File("/usr/lib/testdir/13k-zeros.raw", bytes.Repeat([]byte{0}, 1024*13), 0600),
			tc.File("/usr/lib/testdir/16k-zeros.raw", bytes.Repeat([]byte{0}, 1024*16), 0600),
			tc.File("/usr/lib/testdir/5k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*5), 0600),
			tc.File("/usr/lib/testdir/16k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*16), 0600),
			tc.Dir("/usr/lib/testdir/emptydir", 0600),
			tc.Dir("/usr/lib/testdir/case", 0755),
			tc.File("/usr/lib/testdir/case/file.txt", []byte("lower case dir\n"), 0600),
			tc.Dir("/usr/lib/testdir/CASE", 0755),
			tc.File("/usr/lib/testdir/CASE/file.txt", []byte("upper case dir\n"), 0600),
			tc.File("/usr/lib/testdir/case.txt", []byte("lower case file\n"), 0600),
			tc.File("/usr/lib/testdir/CASE.txt", []byte("upper case file\n"), 0600),
			tc.Symlink("/in-root.txt", "/usr/lib/testdir/link"),
			tc.Symlink("../../../in-root.txt", "/usr/lib/testdir/link-to-root"),
			tc.Symlink("/in-root.txt", "/usr/lib/testdir/abs-link"),
			tc.Symlink("/usr/lib/testdir", "/links/dir-link"),
			tc.Symlink("/links/dir-link", "/links/dir-link2"),
			tc.Symlink("/usr/lib/testdir/abs-link", "/links/double-file-link"),
			tc.Symlink("/links/dir-link", "/links/file-via-dirs"),
			tc.WithXattrs(map[string]string{
				"user.custom":      "value1",
				"user.xdg.comment": "some random comment",
			}).Dir("/usr/lib/withxattr", 0600),
			tc.WithXattrs(map[string]string{
				"user.xdg.comment": "comment for f1",
				"user.common":      "same-value",
			}).File("/usr/lib/withxattr/f1", []byte{}, 0600),
			tc.WithXattrs(map[string]string{
				"user.xdg.comment": "comment for f2",
				"user.common":      "same-value",
			}).File("/usr/lib/withxattr/f2", []byte{}, 0600),
			tc.WithXattrs(map[string]string{
				"user.xdg.comment": "comment for f3",
				"user.common":      "same-value",
			}).File("/usr/lib/withxattr/f3", []byte{}, 0600),
			tc.WithXattrs(map[string]string{
				"user.xdg.comment": "comment for f4",
				"user.common":      "same-value",
			}).File("/usr/lib/withxattr/f4", []byte{}, 0600),
			tc.Dir("/dev", 0755),
			tc.Device("/dev/block0", fs.ModeDevice, 0, 1),
			tc.Device("/dev/block1", fs.ModeDevice, 0, 0),
			tc.Device("/dev/char0", fs.ModeCharDevice, 0, 2),
			tc.Device("/dev/char1", fs.ModeCharDevice, 0, 3),
			tc.Device("/dev/fifo0", fs.ModeNamedPipe, 0, 0),
			tc.Dir("/usr/lib/testdir/lotsoffiles", 0755),
			TarStream(lotsOfFilesC),
		)
	},
	verify: func(t testing.TB, fsys fs.FS) {
		t.Helper()

		CheckFile(t, fsys, "in-root.txt", "root file content\n")
		CheckFile(t, fsys, "usr/lib/testdir/emptyfile", "")
		CheckFileBytes(t, fsys, "usr/lib/testdir/13k-zeros.raw", bytes.Repeat([]byte{0}, 1024*13))
		CheckFileBytes(t, fsys, "usr/lib/testdir/16k-zeros.raw", bytes.Repeat([]byte{0}, 1024*16))
		CheckFileBytes(t, fsys, "usr/lib/testdir/5k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*5))
		CheckFileBytes(t, fsys, "usr/lib/testdir/16k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*16))
		CheckDirEntries(t, fsys, "usr/lib/testdir/emptydir", nil)
		CheckDirSize(t, fsys, "usr/lib/testdir/lotsoffiles", 5000)

		CheckFile(t, fsys, "usr/lib/testdir/case/file.txt", "lower case dir\n")
		CheckFile(t, fsys, "usr/lib/testdir/CASE/file.txt", "upper case dir\n")
		CheckFile(t, fsys, "usr/lib/testdir/case.txt", "lower case file\n")
		CheckFile(t, fsys, "usr/lib/testdir/CASE.txt", "upper case file\n")

		CheckSymlink(t, fsys, "usr/lib/testdir/link", "/in-root.txt")

		CheckNotExists(t, fsys, "not-exists.txt")
		CheckNotExists(t, fsys, "not-exists/somefile")
		CheckNotExists(t, fsys, "usr/lib/testdir/emptydir/somefile")

		CheckXattrs(t, fsys, "usr/lib/withxattr", map[string]string{
			"user.custom":      "value1",
			"user.xdg.comment": "some random comment",
		})
		CheckXattrs(t, fsys, "usr/lib/withxattr/f1", map[string]string{
			"user.xdg.comment": "comment for f1",
			"user.common":      "same-value",
		})
		CheckXattrs(t, fsys, "usr/lib/withxattr/f2", map[string]string{
			"user.xdg.comment": "comment for f2",
			"user.common":      "same-value",
		})
		CheckXattrs(t, fsys, "usr/lib/withxattr/f3", map[string]string{
			"user.xdg.comment": "comment for f3",
			"user.common":      "same-value",
		})
		CheckXattrs(t, fsys, "usr/lib/withxattr/f4", map[string]string{
			"user.xdg.comment": "comment for f4",
			"user.common":      "same-value",
		})

		CheckDevice(t, fsys, "dev/block0", fs.ModeDevice, 1)
		CheckDevice(t, fsys, "dev/block1", fs.ModeDevice, 0)
		CheckDevice(t, fsys, "dev/char0", fs.ModeCharDevice, 2)
		CheckDevice(t, fsys, "dev/char1", fs.ModeCharDevice, 3)
		CheckDevice(t, fsys, "dev/fifo0", fs.ModeNamedPipe, 0)

		// Symlink targets.
		CheckReadLink(t, fsys, "usr/lib/testdir/link-to-root", "../../../in-root.txt")
		CheckReadLink(t, fsys, "usr/lib/testdir/abs-link", "/in-root.txt")
		CheckLstat(t, fsys, "usr/lib/testdir/link-to-root", fs.ModeSymlink)
		CheckLstat(t, fsys, "in-root.txt", 0)
		CheckLstat(t, fsys, "usr/lib/testdir", fs.ModeDir)

		// Open/ReadFile/Stat follow symlinks.
		CheckFile(t, fsys, "usr/lib/testdir/link-to-root", "root file content\n")
		CheckFile(t, fsys, "usr/lib/testdir/abs-link", "root file content\n")
		CheckReadFile(t, fsys, "usr/lib/testdir/link-to-root", "root file content\n")

		// Traversal through symlinked directories.
		CheckFile(t, fsys, "links/dir-link/emptyfile", "")
		CheckFile(t, fsys, "links/dir-link2/emptyfile", "")
		CheckFile(t, fsys, "links/double-file-link", "root file content\n")
		CheckFile(t, fsys, "links/file-via-dirs/abs-link", "root file content\n")

		CheckReadFile(t, fsys, "in-root.txt", "root file content\n")
		CheckReadFileDir(t, fsys, "usr/lib/testdir")
		CheckReadDirSorted(t, fsys, "dev")
	},
}

const (
	longXattrPrefix = "user.long.prefix.vfvzyrvujoemkjztekxczhyyqpzncyav.xiksvigqpjttnvcvxgaxpnrghppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwd."
	longXattrValue  = "value1-ppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwdqfgvgkrgdwcegjpfmelrejllrjkpbwindlfynuzjgvcgygyayjvmtxgsbjkzrydoswbsknrrwjkwzxhasowuzdoxlhbxso"
)

// LongXattrs tests xattrs with very long prefix names and values. This
// requires mkfs.erofs --xattr-prefix support (>= 1.9) for chunk-based
// layouts where inline xattr space is limited. The tar content also
// includes very long PAX headers that mkfs.ext4's libarchive cannot parse,
// so this test case is not suitable for the ext4 converter.
var LongXattrs TestCase = &testCase{
	tar: func() WriterToTar {
		tc := TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		return TarAll(
			tc.Dir("/usr", 0755),
			tc.Dir("/usr/lib", 0755),
			tc.Dir("/usr/lib/generated", 0755),
			tc.Dir("/usr/lib/generated/xattrs", 0755),
			tc.WithXattrs(map[string]string{
				longXattrPrefix + "long-value": longXattrValue,
				longXattrPrefix + "shortvalue": "y",
			}).File("/usr/lib/generated/xattrs/long-prefix-xattrs", []byte{}, 0600),
			tc.WithXattrs(map[string]string{
				"user.short.long-value": longXattrValue,
				"user.short.shortvalue": "y",
			}).File("/usr/lib/generated/xattrs/short-prefix-xattrs", []byte{}, 0600),
		)
	},
	verify: func(t testing.TB, fsys fs.FS) {
		t.Helper()
		CheckXattrs(t, fsys, "usr/lib/generated/xattrs/long-prefix-xattrs", map[string]string{
			longXattrPrefix + "long-value": longXattrValue,
			longXattrPrefix + "shortvalue": "y",
		})
		CheckXattrs(t, fsys, "usr/lib/generated/xattrs/short-prefix-xattrs", map[string]string{
			"user.short.long-value": longXattrValue,
			"user.short.shortvalue": "y",
		})
	},
}

// LargeFiles tests files at 10MB, 50MB, and 150MB. Content is deterministic
// (seeded by file size) so verification can regenerate expected data.
var LargeFiles TestCase = &testCase{
	tar: func() WriterToTar {
		tc := TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		return TarAll(
			tc.File("/10m.bin", generateContent(10*1024*1024), 0644),
			tc.File("/50m.bin", generateContent(50*1024*1024), 0644),
			tc.File("/150m.bin", generateContent(150*1024*1024), 0644),
		)
	},
	verify: func(t testing.TB, fsys fs.FS) {
		t.Helper()
		for _, tc := range []struct {
			name string
			size int
		}{
			{"10m.bin", 10 * 1024 * 1024},
			{"50m.bin", 50 * 1024 * 1024},
			{"150m.bin", 150 * 1024 * 1024},
		} {
			verifyContent(t, fsys, tc.name, tc.size)
		}
	},
}

// generateContent produces deterministic bytes: each byte is i % 251.
func generateContent(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// verifyContent checks a file matches the expected deterministic pattern.
func verifyContent(t testing.TB, fsys fs.FS, name string, size int) {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Errorf("open %s: %v", name, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		t.Errorf("stat %s: %v", name, err)
		return
	}
	if fi.Size() != int64(size) {
		t.Errorf("%s: size %d, want %d", name, fi.Size(), size)
		return
	}

	// Read in chunks and verify against expected pattern.
	buf := make([]byte, 64*1024)
	offset := 0
	for {
		n, err := f.Read(buf)
		for i := range n {
			if buf[i] != byte((offset+i)%251) {
				t.Errorf("%s: mismatch at offset %d: got %d, want %d", name, offset+i, buf[i], (offset+i)%251)
				return
			}
		}
		offset += n
		if err != nil {
			break
		}
	}
	if offset != size {
		t.Errorf("%s: read %d bytes, want %d", name, offset, size)
	}
}

// XattrPrefixFlags returns mkfs.erofs flags to enable long xattr prefix
// compression for the prefixes used in LongXattrs. Returns nil if the
// installed mkfs.erofs doesn't support --xattr-prefix (< 1.9).
func XattrPrefixFlags() []string {
	tooOld, err := CheckMkfsVersion("1.9")
	if err != nil || tooOld {
		return nil
	}
	return []string{
		"--xattr-prefix=user.short",
		"--xattr-prefix=" + longXattrPrefix,
	}
}

// CheckFile verifies that the named file has the expected string content.
func CheckFile(t testing.TB, fsys fs.FS, name, expected string) {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Errorf("open %s: %v", name, err)
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("read %s: %v", name, err)
		return
	}

	if string(data) != expected {
		if len(data) > 64 {
			t.Errorf("%s: content mismatch (len %d vs %d)", name, len(data), len(expected))
		} else {
			t.Errorf("%s: got %q, want %q", name, string(data), expected)
		}
	}
}

// CheckFileBytes verifies that the named file has the expected byte content.
func CheckFileBytes(t testing.TB, fsys fs.FS, name string, expected []byte) {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Errorf("open %s: %v", name, err)
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("read %s: %v", name, err)
		return
	}

	if !bytes.Equal(data, expected) {
		t.Errorf("%s: content mismatch (len %d vs %d)", name, len(data), len(expected))
	}
}

// CheckDirEntries verifies that the named directory contains exactly the
// expected entries (sorted by name).
func CheckDirEntries(t testing.TB, fsys fs.FS, name string, expected []string) {
	t.Helper()
	entries, err := fs.ReadDir(fsys, name)
	if err != nil {
		t.Errorf("readdir %s: %v", name, err)
		return
	}

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}

	if len(names) != len(expected) {
		t.Errorf("readdir %s: got %d entries %v, want %d entries %v", name, len(names), names, len(expected), expected)
		return
	}
	for i, n := range names {
		if n != expected[i] {
			t.Errorf("readdir %s[%d]: got %q, want %q", name, i, n, expected[i])
		}
	}
}

// CheckDirSize verifies that the named directory contains exactly n entries.
func CheckDirSize(t testing.TB, fsys fs.FS, name string, n int) {
	t.Helper()
	entries, err := fs.ReadDir(fsys, name)
	if err != nil {
		t.Errorf("readdir %s: %v", name, err)
		return
	}
	if len(entries) != n {
		t.Errorf("readdir %s: got %d entries, want %d", name, len(entries), n)
	}
}

// CheckSymlink verifies that the named symlink has the expected target
// using ReadLink.
func CheckSymlink(t testing.TB, fsys fs.FS, name, expectedTarget string) {
	t.Helper()
	CheckReadLink(t, fsys, name, expectedTarget)
}

// CheckNotExists verifies that the named path does not exist.
func CheckNotExists(t testing.TB, fsys fs.FS, name string) {
	t.Helper()
	_, err := fsys.Open(name)
	if err == nil {
		t.Errorf("expected error opening %s", name)
	}
}

// readLinkFS is the interface for ReadLink and Lstat, matching os.Root.
type readLinkFS interface {
	ReadLink(name string) (string, error)
	Lstat(name string) (fs.FileInfo, error)
}

// CheckReadLink verifies that ReadLink returns the expected target.
func CheckReadLink(t testing.TB, fsys fs.FS, name, target string) {
	t.Helper()
	rlfs, ok := fsys.(readLinkFS)
	if !ok {
		t.Errorf("FS does not implement ReadLink")
		return
	}
	got, err := rlfs.ReadLink(name)
	if err != nil {
		t.Errorf("ReadLink(%s): %v", name, err)
		return
	}
	if got != target {
		t.Errorf("ReadLink(%s) = %q, want %q", name, got, target)
	}
}

// CheckLstat verifies that Lstat returns the expected file type.
func CheckLstat(t testing.TB, fsys fs.FS, name string, wantType fs.FileMode) {
	t.Helper()
	rlfs, ok := fsys.(readLinkFS)
	if !ok {
		t.Errorf("FS does not implement Lstat")
		return
	}
	fi, err := rlfs.Lstat(name)
	if err != nil {
		t.Errorf("Lstat(%s): %v", name, err)
		return
	}
	gotType := fi.Mode() & fs.ModeType
	if gotType != wantType {
		t.Errorf("Lstat(%s) type = %v, want %v", name, gotType, wantType)
	}
}

// CheckReadFile verifies fs.ReadFile returns the expected content.
func CheckReadFile(t testing.TB, fsys fs.FS, name, expected string) {
	t.Helper()
	got, err := fs.ReadFile(fsys, name)
	if err != nil {
		t.Errorf("ReadFile(%s): %v", name, err)
		return
	}
	if string(got) != expected {
		t.Errorf("ReadFile(%s) = %q, want %q", name, got, expected)
	}
}

// CheckReadFileDir verifies that fs.ReadFile fails on a directory.
func CheckReadFileDir(t testing.TB, fsys fs.FS, name string) {
	t.Helper()
	_, err := fs.ReadFile(fsys, name)
	if err == nil {
		t.Errorf("ReadFile(%s) should fail on directory", name)
	}
}

// CheckReadDirSorted verifies that fs.ReadDir returns entries in sorted order.
func CheckReadDirSorted(t testing.TB, fsys fs.FS, name string) {
	t.Helper()
	entries, err := fs.ReadDir(fsys, name)
	if err != nil {
		t.Errorf("ReadDir(%s): %v", name, err)
		return
	}
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Name() >= entries[i].Name() {
			t.Errorf("ReadDir(%s) not sorted: %q >= %q at index %d", name, entries[i-1].Name(), entries[i].Name(), i)
			return
		}
	}
}
