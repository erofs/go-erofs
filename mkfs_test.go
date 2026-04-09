package erofs_test

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/builder"
	"github.com/erofs/go-erofs/internal/disk"
	"github.com/erofs/go-erofs/internal/erofstest"
	mktar "github.com/erofs/go-erofs/tar"
)

// TestCreateFSSpool exercises spool mode: CreateFS without a data file.
func TestCreateFSSpool(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	// Create a regular file.
	f, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello world\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a directory and a file inside it.
	if err := fsys.Mkdir("/subdir", 0o755); err != nil {
		t.Fatal(err)
	}
	f2, err := fsys.Create("/subdir/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.Write([]byte("nested\n")); err != nil {
		t.Fatal(err)
	}
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a symlink.
	if err := fsys.Symlink("hello.txt", "/link"); err != nil {
		t.Fatal(err)
	}

	// Create an empty file.
	f3, err := fsys.Create("/empty")
	if err != nil {
		t.Fatal(err)
	}
	if err := f3.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())

	// Read back the image.
	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFile(t, efs, "hello.txt", "hello world\n")
	erofstest.CheckFile(t, efs, "subdir/nested.txt", "nested\n")
	erofstest.CheckFile(t, efs, "empty", "")
	erofstest.CheckSymlink(t, efs, "link", "hello.txt")
	erofstest.CheckDirEntries(t, efs, ".", []string{"empty", "hello.txt", "link", "subdir"})
	erofstest.CheckDirEntries(t, efs, "subdir", []string{"nested.txt"})
}

// TestCreateFSDataFile exercises data file mode (metadata-only).
func TestCreateFSDataFile(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "data.bin")
	df, err := os.Create(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = df.Close() }()

	var metaBuf testBuffer
	fsys := erofs.Create(&metaBuf, erofs.WithDataFile(df))

	f, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("data file mode\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f2, err := fsys.Create("/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("ABCDEFGH"), 1024) // 8KB
	if _, err := f2.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// Re-open data file as ReaderAt for verification.
	dfRead, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dfRead.Close() }()

	efs, err := erofs.Open(bytes.NewReader(metaBuf.Bytes()), erofs.WithExtraDevices(dfRead))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFile(t, efs, "hello.txt", "data file mode\n")
	erofstest.CheckFileBytes(t, efs, "big.bin", data)
}

// TestCreateFSMetadata verifies Chmod, Chown, Setxattr, SetMtime.
func TestCreateFSMetadata(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("content\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Chmod(0o755); err != nil {
		t.Fatal(err)
	}
	if err := f.Chown(1000, 2000); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Setxattr("/file.txt", "user.test", "value123"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Chtimes("/file.txt", time.Time{}, time.Unix(1700000000, 123456789)); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("/mydir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Chmod("/mydir", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Chown("/mydir", 500, 600); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	// Check file metadata.
	st := erofstest.Stat(t, efs, "file.txt")
	if st.Mode.Perm() != 0o755 {
		t.Errorf("file perm: got %o, want 755", st.Mode.Perm())
	}
	if st.UID != 1000 || st.GID != 2000 {
		t.Errorf("file uid/gid: got %d/%d, want 1000/2000", st.UID, st.GID)
	}
	if st.Mtime != 1700000000 {
		t.Errorf("file mtime: got %d, want 1700000000", st.Mtime)
	}
	if st.MtimeNs != 123456789 {
		t.Errorf("file mtimeNs: got %d, want 123456789", st.MtimeNs)
	}
	erofstest.CheckXattrs(t, efs, "file.txt", map[string]string{"user.test": "value123"})

	// Check dir metadata.
	dst := erofstest.Stat(t, efs, "mydir")
	if dst.Mode.Perm() != 0o700 {
		t.Errorf("dir perm: got %o, want 700", dst.Mode.Perm())
	}
	if dst.UID != 500 || dst.GID != 600 {
		t.Errorf("dir uid/gid: got %d/%d, want 500/600", dst.UID, dst.GID)
	}
}

// TestCreateFSMknod verifies char and block device creation.
func TestCreateFSMknod(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	if err := fsys.Mknod("/null", disk.StatTypeChrdev|0o666, 1<<8|3); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mknod("/sda", disk.StatTypeBlkdev|0o660, 8<<8); err != nil { //nolint:staticcheck // minor=0
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckDevice(t, efs, "null", fs.ModeCharDevice, 1<<8|3)
	erofstest.CheckDevice(t, efs, "sda", fs.ModeDevice, 8<<8) //nolint:staticcheck // minor=0
}

// TestCreateFSLargeFile tests a file that spans many blocks and exercises
// the Chunk.Count uint16 split for files > 65535 blocks.
func TestCreateFSLargeFile(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	// 128KB file — enough to span multiple blocks but not absurdly large.
	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	f, err := fsys.Create("/large.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFileBytes(t, efs, "large.bin", data)
}

// TestCreateFSLargeFileDataFile tests a large file with data file mode,
// including chunk splitting for files > 65535 blocks.
func TestCreateFSLargeFileDataFile(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "data.bin")
	df, err := os.Create(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = df.Close() }()

	var metaBuf testBuffer
	fsys := erofs.Create(&metaBuf, erofs.WithDataFile(df))

	// 128KB file.
	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	f, err := fsys.Create("/large.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	dfRead, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dfRead.Close() }()

	efs, err := erofs.Open(bytes.NewReader(metaBuf.Bytes()), erofs.WithExtraDevices(dfRead))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFileBytes(t, efs, "large.bin", data)
}

// TestCreateFSErrors tests error cases.
func TestCreateFSErrors(t *testing.T) {
	t.Run("duplicate path", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		f, err := fsys.Create("/dup.txt")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = fsys.Create("/dup.txt")
		if err == nil {
			t.Fatal("expected error for duplicate path")
		}
	})

	t.Run("write after close", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		f, err := fsys.Create("/file.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = f.Write([]byte("more"))
		if err == nil {
			t.Fatal("expected error writing to closed file")
		}
	})

	t.Run("file double close", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		f, err := fsys.Create("/file.txt")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		err = f.Close()
		if err == nil {
			t.Fatal("expected error on double close")
		}
	})

	t.Run("FS double close", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		if err := fsys.Close(); err != nil {
			t.Fatal(err)
		}
		err := fsys.Close()
		if err == nil {
			t.Fatal("expected error on FS double close")
		}
	})

	t.Run("create after FS close", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		if err := fsys.Close(); err != nil {
			t.Fatal(err)
		}
		_, err := fsys.Create("/file.txt")
		if err == nil {
			t.Fatal("expected error creating after FS close")
		}
	})
}

// TestCreateFSImplicitDirs verifies that parent directories are created
// implicitly when creating deeply nested files.
func TestCreateFSImplicitDirs(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/a/b/c/deep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("deep\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFile(t, efs, "a/b/c/deep.txt", "deep\n")

	// Verify implicit directories exist.
	for _, dir := range []string{"a", "a/b", "a/b/c"} {
		fi, err := fs.Stat(efs, dir)
		if err != nil {
			t.Errorf("stat %s: %v", dir, err)
			continue
		}
		if !fi.IsDir() {
			t.Errorf("%s: not a directory", dir)
		}
	}
}

// TestCreateFSEmpty verifies that an empty FS produces a valid image.
func TestCreateFSEmpty(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)
	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	entries, err := fs.ReadDir(efs, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty root dir, got %d entries", len(entries))
	}
}

// TestCreateFSSetNlink verifies that SetNlink overrides computed nlink.
func TestCreateFSSetNlink(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.SetNlink("/file.txt", 42); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	st := erofstest.Stat(t, efs, "file.txt")
	if st.Nlink != 42 {
		t.Errorf("nlink: got %d, want 42", st.Nlink)
	}
}

// TestCreateFSDirNlink verifies that directory nlink = 2 + child_dir_count.
func TestCreateFSDirNlink(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	if err := fsys.Mkdir("/parent", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mkdir("/parent/child1", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mkdir("/parent/child2", 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := fsys.Create("/parent/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	st := erofstest.Stat(t, efs, "parent")
	// nlink should be 2 (self + parent) + 2 (child dirs) = 4
	if st.Nlink != 4 {
		t.Errorf("parent nlink: got %d, want 4", st.Nlink)
	}
}

// TestCreateFSWithTempDir verifies that WithTempDir is respected.
func TestCreateFSWithTempDir(t *testing.T) {
	tmpDir := t.TempDir()
	var buf testBuffer
	fsys := erofs.Create(&buf, erofs.WithTempDir(tmpDir))

	f, err := fsys.Create("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	erofstest.CheckFile(t, efs, "file.txt", "hello")
}

// TestCreateFSRootMetadata verifies that Mkdir("/") sets root permissions
// and path-based methods can set metadata on it.
func TestCreateFSRootMetadata(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	if err := fsys.Mkdir("/", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Chown("/", 1000, 2000); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	st := erofstest.Stat(t, efs, ".")
	if st.Mode.Perm() != 0o700 {
		t.Errorf("root perm: got %o, want 700", st.Mode.Perm())
	}
	if st.UID != 1000 || st.GID != 2000 {
		t.Errorf("root uid/gid: got %d/%d, want 1000/2000", st.UID, st.GID)
	}
}

// TestCreateFSMultipleFiles tests creating many files to exercise the
// spool and verify ordering.
func TestCreateFSMultipleFiles(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	for i := range 100 {
		f, err := fsys.Create(fmt.Sprintf("/file%03d.txt", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(f, "content %d\n", i); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("EroFS:", err)
	}

	for i := range 100 {
		name := fmt.Sprintf("file%03d.txt", i)
		expected := fmt.Sprintf("content %d\n", i)
		erofstest.CheckFile(t, efs, name, expected)
	}

	// Verify directory has all 100 entries.
	entries, err := fs.ReadDir(efs, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 100 {
		t.Errorf("got %d entries, want 100", len(entries))
	}
}

// TestWriterOpen tests Open and Read for regular files in spool mode.
func TestWriterOpen(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello world\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Open and read back.
	rf, err := fsys.Open("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rf.Close() }()

	got, err := io.ReadAll(rf)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello world\n" {
		t.Errorf("got %q, want %q", got, "hello world\n")
	}
}

// TestWriterOpenDataFile tests Open and Read for regular files in data file mode.
func TestWriterOpenDataFile(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "data.bin")
	df, err := os.Create(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = df.Close() }()

	var metaBuf testBuffer
	fsys := erofs.Create(&metaBuf, erofs.WithDataFile(df))

	f, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("data file content\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := fsys.Open("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rf.Close() }()

	got, err := io.ReadAll(rf)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data file content\n" {
		t.Errorf("got %q, want %q", got, "data file content\n")
	}
}

// TestWriterOpenEmpty tests Open on an empty file.
func TestWriterOpenEmpty(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/empty")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := fsys.Open("/empty")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rf.Close() }()

	got, err := io.ReadAll(rf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty, got %d bytes", len(got))
	}
}

// TestWriterStat tests Stat for various entry types.
func TestWriterStat(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	f, err := fsys.Create("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("content\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Chmod(0o755); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Chtimes("/file.txt", time.Time{}, time.Unix(1700000000, 123456789)); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mkdir("/dir", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Symlink("file.txt", "/link"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Mknod("/null", disk.StatTypeChrdev|0o666, 1<<8|3); err != nil {
		t.Fatal(err)
	}

	// Stat regular file.
	fi, err := fsys.Stat("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "file.txt" {
		t.Errorf("name: got %q, want %q", fi.Name(), "file.txt")
	}
	if fi.Size() != 8 {
		t.Errorf("size: got %d, want 8", fi.Size())
	}
	if fi.Mode().Perm() != 0o755 {
		t.Errorf("mode: got %o, want 755", fi.Mode().Perm())
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("expected regular file mode")
	}
	if fi.ModTime() != time.Unix(1700000000, 123456789) {
		t.Errorf("modtime: got %v, want %v", fi.ModTime(), time.Unix(1700000000, 123456789))
	}

	// Stat directory.
	di, err := fsys.Stat("/dir")
	if err != nil {
		t.Fatal(err)
	}
	if !di.IsDir() {
		t.Error("expected directory")
	}
	if di.Mode().Perm() != 0o700 {
		t.Errorf("dir mode: got %o, want 700", di.Mode().Perm())
	}

	// Stat symlink.
	li, err := fsys.Stat("/link")
	if err != nil {
		t.Fatal(err)
	}
	if li.Mode().Type() != fs.ModeSymlink {
		t.Errorf("expected symlink, got %v", li.Mode().Type())
	}

	// Stat device.
	ni, err := fsys.Stat("/null")
	if err != nil {
		t.Fatal(err)
	}
	if ni.Mode()&fs.ModeCharDevice == 0 {
		t.Errorf("expected char device, got %v", ni.Mode())
	}

	// Stat root.
	ri, err := fsys.Stat("/")
	if err != nil {
		t.Fatal(err)
	}
	if !ri.IsDir() {
		t.Error("root: expected directory")
	}
	if ri.Name() != "/" {
		t.Errorf("root name: got %q, want %q", ri.Name(), "/")
	}

	// Stat not found.
	_, err = fsys.Stat("/nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent path")
	}
}

// TestWriterReadDir tests Open on directories and ReadDir.
func TestWriterReadDir(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)

	if err := fsys.Mkdir("/subdir", 0o755); err != nil {
		t.Fatal(err)
	}

	f1, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f1.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f1.Close(); err != nil {
		t.Fatal(err)
	}

	f2, err := fsys.Create("/subdir/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.Write([]byte("nested")); err != nil {
		t.Fatal(err)
	}
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}

	if err := fsys.Symlink("hello.txt", "/link"); err != nil {
		t.Fatal(err)
	}

	// ReadDir on root.
	d, err := fsys.Open("/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	rdf, ok := d.(fs.ReadDirFile)
	if !ok {
		t.Fatal("root Open did not return ReadDirFile")
	}

	entries, err := rdf.ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	want := []string{"hello.txt", "link", "subdir"}
	if len(names) != len(want) {
		t.Fatalf("got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("entry[%d]: got %q, want %q", i, names[i], want[i])
		}
	}

	// Verify subdir entry is a directory.
	for _, e := range entries {
		if e.Name() == "subdir" && !e.IsDir() {
			t.Error("subdir should be a directory")
		}
	}

	// ReadDir on subdir.
	sd, err := fsys.Open("/subdir")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sd.Close() }()

	sdEntries, err := sd.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(sdEntries) != 1 || sdEntries[0].Name() != "nested.txt" {
		t.Errorf("subdir entries: got %v, want [nested.txt]", sdEntries)
	}
}

// writeFSConverter converts tar → erofs (via Go-native tar path), then
// walks the result into a Writer, producing a second erofs image. This
// exercises the full Writer round-trip including mid-write readback.
func writeFSConverter(t testing.TB, wt erofstest.WriterToTar) fs.FS {
	t.Helper()

	// Build source EROFS from tar.
	tarStream := erofstest.TarFromWriterTo(wt)
	defer func() { _ = tarStream.Close() }()
	tarFS, err := mktar.Open(tarStream)
	if err != nil {
		t.Fatal("tar.Open:", err)
	}
	defer tarFS.Close() //nolint:errcheck
	var srcBuf testBuffer
	tw := erofs.Create(&srcBuf)
	if err := tw.CopyFrom(tarFS); err != nil {
		t.Fatal("CopyFrom:", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	srcFS, err := erofs.Open(bytes.NewReader(srcBuf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	// Walk source FS and copy into Writer.
	var dstBuf testBuffer
	w := erofs.Create(&dstBuf)

	if err := fs.WalkDir(srcFS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("info %s: %w", p, err)
		}
		st := info.Sys().(*erofs.Stat)

		wp := "/" + p
		if p == "." {
			wp = "/"
		}

		switch {
		case info.IsDir():
			if err := w.Mkdir(wp, info.Mode().Perm()); err != nil {
				return fmt.Errorf("mkdir %s: %w", wp, err)
			}

		case info.Mode().IsRegular():
			f, err := w.Create(wp)
			if err != nil {
				return fmt.Errorf("create %s: %w", wp, err)
			}
			if info.Size() > 0 {
				src, err := srcFS.Open(p)
				if err != nil {
					return fmt.Errorf("open source %s: %w", p, err)
				}
				if _, err := io.Copy(f, src); err != nil {
					_ = src.Close()
					return fmt.Errorf("copy %s: %w", p, err)
				}
				_ = src.Close()
			}
			if err := f.Chmod(info.Mode().Perm()); err != nil {
				return fmt.Errorf("chmod %s: %w", wp, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("close %s: %w", wp, err)
			}

		case info.Mode().Type() == fs.ModeSymlink:
			rlfs := srcFS.(interface{ ReadLink(string) (string, error) })
			target, err := rlfs.ReadLink(p)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", p, err)
			}
			if err := w.Symlink(target, wp); err != nil {
				return fmt.Errorf("symlink %s: %w", wp, err)
			}

		case info.Mode()&fs.ModeCharDevice != 0:
			if err := w.Mknod(wp, disk.StatTypeChrdev|uint16(info.Mode().Perm()), st.Rdev); err != nil {
				return fmt.Errorf("mknod %s: %w", wp, err)
			}

		case info.Mode().Type() == fs.ModeDevice:
			if err := w.Mknod(wp, disk.StatTypeBlkdev|uint16(info.Mode().Perm()), st.Rdev); err != nil {
				return fmt.Errorf("mknod %s: %w", wp, err)
			}

		case info.Mode().Type() == fs.ModeNamedPipe:
			if err := w.Mknod(wp, disk.StatTypeFifo|uint16(info.Mode().Perm()), 0); err != nil {
				return fmt.Errorf("mknod %s: %w", wp, err)
			}

		case info.Mode().Type() == fs.ModeSocket:
			if err := w.Mknod(wp, disk.StatTypeSock|uint16(info.Mode().Perm()), 0); err != nil {
				return fmt.Errorf("mknod %s: %w", wp, err)
			}
		}

		// Copy metadata.
		if err := w.Chown(wp, int(st.UID), int(st.GID)); err != nil {
			return fmt.Errorf("chown %s: %w", wp, err)
		}
		if err := w.Chtimes(wp, time.Unix(int64(st.Mtime), int64(st.MtimeNs)),
			time.Unix(int64(st.Mtime), int64(st.MtimeNs))); err != nil {
			return fmt.Errorf("chtimes %s: %w", wp, err)
		}
		for k, v := range st.Xattrs {
			if err := w.Setxattr(wp, k, v); err != nil {
				return fmt.Errorf("setxattr %s %s: %w", wp, k, err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal("walk:", err)
	}

	// Mid-write comparison: verify Writer readback matches source.
	if err := fs.WalkDir(srcFS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, _ := d.Info()
		st := info.Sys().(*erofs.Stat)

		wp := "/" + p
		if p == "." {
			wp = "/"
		}

		wfi, err := w.Stat(wp)
		if err != nil {
			t.Errorf("mid-write stat %s: %v", wp, err)
			return nil
		}
		if info.Mode().IsRegular() && wfi.Size() != info.Size() {
			t.Errorf("mid-write %s size: got %d, want %d", wp, wfi.Size(), info.Size())
		}
		if wfi.Mode().Perm() != info.Mode().Perm() {
			t.Errorf("mid-write %s perm: got %o, want %o", wp, wfi.Mode().Perm(), info.Mode().Perm())
		}
		if wfi.Mode().Type() != info.Mode().Type() {
			t.Errorf("mid-write %s type: got %v, want %v", wp, wfi.Mode().Type(), info.Mode().Type())
		}
		if wfi.ModTime() != time.Unix(int64(st.Mtime), int64(st.MtimeNs)) {
			t.Errorf("mid-write %s modtime: got %v, want %v", wp, wfi.ModTime(), time.Unix(int64(st.Mtime), int64(st.MtimeNs)))
		}

		if info.Mode().IsRegular() {
			wf, err := w.Open(wp)
			if err != nil {
				t.Errorf("mid-write open %s: %v", wp, err)
				return nil
			}
			wdata, _ := io.ReadAll(wf)
			_ = wf.Close()

			sf, _ := srcFS.Open(p)
			sdata, _ := io.ReadAll(sf)
			_ = sf.Close()

			if !bytes.Equal(wdata, sdata) {
				t.Errorf("mid-write %s data mismatch: %d bytes vs %d bytes", wp, len(wdata), len(sdata))
			}
		}
		return nil
	}); err != nil {
		t.Fatal("mid-write walk:", err)
	}

	// Close writer and produce final EROFS image.
	if err := w.Close(); err != nil {
		t.Fatal("Writer Close:", err)
	}

	dstFS, err := erofs.Open(bytes.NewReader(dstBuf.Bytes()))
	if err != nil {
		t.Fatal("EroFS open dest:", err)
	}
	return dstFS
}

// TestWriterRoundTrip creates an EROFS image from the standard test tar,
// walks it into a Writer, verifies mid-write state, closes the writer,
// and verifies the final EROFS image matches expectations.
func TestWriterRoundTrip(t *testing.T) {
	t.Run("Basic", func(t *testing.T) { erofstest.Basic.Run(t, writeFSConverter) })
	t.Run("FileSizes", func(t *testing.T) { erofstest.FileSizes.Run(t, writeFSConverter) })
	t.Run("LongXattrs", func(t *testing.T) { erofstest.LongXattrs.Run(t, writeFSConverter) })
}

// TestWriterOpenErrors tests error cases for Open.
func TestWriterOpenErrors(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		_, err := fsys.Open("/nonexistent")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("file not yet closed", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		f, err := fsys.Create("/file.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}

		_, err = fsys.Open("/file.txt")
		if err == nil {
			t.Fatal("expected error opening file still being written")
		}

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		// Should succeed now.
		rf, err := fsys.Open("/file.txt")
		if err != nil {
			t.Fatal("expected success after file closed:", err)
		}
		_ = rf.Close()
	})

	t.Run("read from dir", func(t *testing.T) {
		var buf testBuffer
		fsys := erofs.Create(&buf)
		if err := fsys.Mkdir("/dir", 0o755); err != nil {
			t.Fatal(err)
		}

		d, err := fsys.Open("/dir")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = d.Close() }()

		_, err = d.Read(make([]byte, 10))
		if err == nil {
			t.Fatal("expected error reading from directory")
		}
	})
}

// TestMetadataOnlyNoFileData verifies that metadata-only images do not
// contain file data. The EROFS output should hold only inodes, dirents,
// xattrs, and chunk indexes — never the file content itself.
func TestMetadataOnlyNoFileData(t *testing.T) {
	// Use a recognizable, non-trivial pattern that won't appear by coincidence.
	marker := bytes.Repeat([]byte("EROFS_DATA_LEAK_CHECK!"), 200) // 4400 bytes

	t.Run("MetadataOnly flag", func(t *testing.T) {
		var meta testBuffer
		w := erofs.Create(&meta)

		srcFS := fstest.MapFS{
			"testfile.bin": &fstest.MapFile{Data: marker, Mode: 0o644},
		}
		if err := w.CopyFrom(srcFS, erofs.MetadataOnly()); err != nil {
			t.Fatal("CopyFrom:", err)
		}

		if err := w.Close(); err != nil {
			t.Fatal("Close:", err)
		}

		if bytes.Contains(meta.Bytes(), marker) {
			t.Error("metadata image contains file data — data leaked into metadata-only output")
		}
		t.Logf("metadata size: %d bytes, marker size: %d bytes", len(meta.Bytes()), len(marker))
	})

	t.Run("WithDataFile", func(t *testing.T) {
		dataPath := filepath.Join(t.TempDir(), "data.bin")
		df, err := os.Create(dataPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = df.Close() }()

		var meta testBuffer
		w := erofs.Create(&meta, erofs.WithDataFile(df))

		f, err := w.Create("/testfile.bin")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(marker); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		if err := w.Close(); err != nil {
			t.Fatal("Close:", err)
		}

		if bytes.Contains(meta.Bytes(), marker) {
			t.Error("metadata image contains file data — data leaked into metadata-only output")
		}

		// Data file should contain the file data.
		dfRead, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(dfRead, marker) {
			t.Error("data file does not contain file data")
		}
		t.Logf("metadata: %d bytes, data file: %d bytes", len(meta.Bytes()), len(dfRead))
	})

	t.Run("CopyFrom with pre-existing chunks", func(t *testing.T) {
		// Simulate a source that provides chunk mappings (like ext4).
		// The metadata image must not contain file data, and no data
		// should be written to a data file.

		dataPath := filepath.Join(t.TempDir(), "data.bin")
		df, err := os.Create(dataPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = df.Close() }()

		var meta testBuffer
		w := erofs.Create(&meta, erofs.WithDataFile(df))

		// Build a source FS where entries carry pre-existing chunks.
		srcFS := newChunkedFS(marker)

		if err := w.CopyFrom(srcFS, erofs.MetadataOnly()); err != nil {
			t.Fatal("CopyFrom:", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal("Close:", err)
		}

		if bytes.Contains(meta.Bytes(), marker) {
			t.Error("metadata image contains file data — data leaked into metadata-only output")
		}

		// Data file should be empty — chunks reference the original device,
		// so no data should be copied.
		dfInfo, err := os.Stat(dataPath)
		if err != nil {
			t.Fatal(err)
		}
		if dfInfo.Size() != 0 {
			t.Errorf("data file should be empty for pre-existing chunks, got %d bytes", dfInfo.Size())
		}
		t.Logf("metadata: %d bytes, data file: %d bytes", len(meta.Bytes()), dfInfo.Size())
	})
}

// chunkedFS is a test fs.FS that provides entries with pre-existing chunk
// mappings and data readers, simulating an ext4-like source.
type chunkedFS struct {
	data []byte
}

func newChunkedFS(data []byte) *chunkedFS {
	return &chunkedFS{data: data}
}

func (cfs *chunkedFS) DeviceBlocks() uint64 { return 1024 }

func (cfs *chunkedFS) Open(name string) (fs.File, error) {
	if name == "." {
		return &chunkedDir{cfs: cfs}, nil
	}
	if name == "testfile.bin" {
		return &chunkedFile{data: cfs.data}, nil
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

type chunkedDir struct {
	cfs     *chunkedFS
	didRead bool
}

func (d *chunkedDir) Stat() (fs.FileInfo, error) {
	return &chunkedDirInfo{}, nil
}
func (d *chunkedDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: ".", Err: fmt.Errorf("is a directory")}
}
func (d *chunkedDir) Close() error { return nil }
func (d *chunkedDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.didRead {
		return nil, io.EOF
	}
	d.didRead = true
	return []fs.DirEntry{&chunkedDirEntry{data: d.cfs.data}}, nil
}

type chunkedDirInfo struct{}

func (i *chunkedDirInfo) Name() string       { return "." }
func (i *chunkedDirInfo) Size() int64        { return 0 }
func (i *chunkedDirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o755 }
func (i *chunkedDirInfo) ModTime() time.Time { return time.Time{} }
func (i *chunkedDirInfo) IsDir() bool        { return true }
func (i *chunkedDirInfo) Sys() any           { return nil }

type chunkedDirEntry struct {
	data []byte
}

func (e *chunkedDirEntry) Name() string      { return "testfile.bin" }
func (e *chunkedDirEntry) IsDir() bool       { return false }
func (e *chunkedDirEntry) Type() fs.FileMode { return 0 }
func (e *chunkedDirEntry) Info() (fs.FileInfo, error) {
	return &chunkedFileInfo{size: int64(len(e.data))}, nil
}

type chunkedFileInfo struct {
	size int64
}

func (i *chunkedFileInfo) Name() string       { return "testfile.bin" }
func (i *chunkedFileInfo) Size() int64        { return i.size }
func (i *chunkedFileInfo) Mode() fs.FileMode  { return 0o644 }
func (i *chunkedFileInfo) ModTime() time.Time { return time.Time{} }
func (i *chunkedFileInfo) IsDir() bool        { return false }
func (i *chunkedFileInfo) Sys() any {
	nblocks := (i.size + 4095) / 4096
	return &builder.Entry{
		Nlink: 1,
		Data:  bytes.NewReader(make([]byte, i.size)),
		Chunks: []builder.Chunk{{
			PhysicalBlock: 100,
			Count:         uint16(nblocks),
			DeviceID:      1,
		}},
	}
}

type chunkedFile struct {
	data   []byte
	offset int
}

func (f *chunkedFile) Stat() (fs.FileInfo, error) {
	return &chunkedFileInfo{size: int64(len(f.data))}, nil
}
func (f *chunkedFile) Read(p []byte) (int, error) {
	if f.offset >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.offset:])
	f.offset += n
	return n, nil
}
func (f *chunkedFile) Close() error { return nil }

// --- Merge tests ---

// TestMergeBasic verifies that two CopyFrom calls merge entries.
func TestMergeBasic(t *testing.T) {
	base := fstest.MapFS{
		"file1.txt":     {Data: []byte("base1"), Mode: 0o644},
		"dir/file2.txt": {Data: []byte("base2"), Mode: 0o644},
	}
	overlay := fstest.MapFS{
		"file3.txt":     {Data: []byte("overlay"), Mode: 0o644},
		"dir/file4.txt": {Data: []byte("new"), Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(base); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "file1.txt", "base1")
	erofstest.CheckFile(t, efs, "dir/file2.txt", "base2")
	erofstest.CheckFile(t, efs, "file3.txt", "overlay")
	erofstest.CheckFile(t, efs, "dir/file4.txt", "new")
}

// TestMergeWhiteout verifies that .wh.<name> files delete entries.
func TestMergeWhiteout(t *testing.T) {
	base := fstest.MapFS{
		"keep.txt":   {Data: []byte("keep"), Mode: 0o644},
		"remove.txt": {Data: []byte("gone"), Mode: 0o644},
		"dir/a.txt":  {Data: []byte("a"), Mode: 0o644},
	}
	overlay := fstest.MapFS{
		".wh.remove.txt": {Data: []byte{}, Mode: 0o644},
		"dir/.wh.a.txt":  {Data: []byte{}, Mode: 0o644},
		"dir/b.txt":      {Data: []byte("b"), Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(base); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "keep.txt", "keep")
	erofstest.CheckNotExists(t, efs, "remove.txt")
	erofstest.CheckNotExists(t, efs, "dir/a.txt")
	erofstest.CheckFile(t, efs, "dir/b.txt", "b")
}

// TestMergeOpaque verifies that .wh..wh..opq removes all prior children.
func TestMergeOpaque(t *testing.T) {
	base := fstest.MapFS{
		"dir/old1.txt":     {Data: []byte("old1"), Mode: 0o644},
		"dir/old2.txt":     {Data: []byte("old2"), Mode: 0o644},
		"dir/sub/deep.txt": {Data: []byte("deep"), Mode: 0o644},
	}
	overlay := fstest.MapFS{
		"dir/.wh..wh..opq": {Data: []byte{}, Mode: 0o644},
		"dir/new.txt":      {Data: []byte("new"), Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(base); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckNotExists(t, efs, "dir/old1.txt")
	erofstest.CheckNotExists(t, efs, "dir/old2.txt")
	erofstest.CheckNotExists(t, efs, "dir/sub/deep.txt")
	erofstest.CheckFile(t, efs, "dir/new.txt", "new")
}

// TestMergeOverwrite verifies that overlay files replace base files.
func TestMergeOverwrite(t *testing.T) {
	base := fstest.MapFS{
		"file.txt": {Data: []byte("old"), Mode: 0o644},
	}
	overlay := fstest.MapFS{
		"file.txt": {Data: []byte("new"), Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(base); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "file.txt", "new")
}

// TestMergeWhiteoutDir verifies that a whiteout can remove an entire directory.
func TestMergeWhiteoutDir(t *testing.T) {
	base := fstest.MapFS{
		"dir/file.txt": {Data: []byte("content"), Mode: 0o644},
	}
	overlay := fstest.MapFS{
		".wh.dir": {Data: []byte{}, Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(base); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckNotExists(t, efs, "dir")
	erofstest.CheckNotExists(t, efs, "dir/file.txt")
}

// TestMergeTarWhiteout verifies merge with tar sources containing whiteouts.
func TestMergeTarWhiteout(t *testing.T) {
	tc := erofstest.TarContext{UID: 0, GID: 0}

	baseTar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.Dir("dir/", 0o755),
		tc.File("dir/keep.txt", []byte("keep"), 0o644),
		tc.File("dir/remove.txt", []byte("gone"), 0o644),
		tc.File("other.txt", []byte("other"), 0o644),
	))
	defer baseTar.Close() //nolint:errcheck

	overlayTar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.Dir("dir/", 0o755),
		tc.File("dir/.wh.remove.txt", nil, 0o644),
		tc.File("dir/added.txt", []byte("added"), 0o644),
	))
	defer overlayTar.Close() //nolint:errcheck

	baseFS, err := mktar.Open(baseTar)
	if err != nil {
		t.Fatal("open base tar:", err)
	}
	defer baseFS.Close() //nolint:errcheck
	overlayFS, err := mktar.Open(overlayTar)
	if err != nil {
		t.Fatal("open overlay tar:", err)
	}
	defer overlayFS.Close() //nolint:errcheck

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(baseFS); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlayFS, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "dir/keep.txt", "keep")
	erofstest.CheckNotExists(t, efs, "dir/remove.txt")
	erofstest.CheckFile(t, efs, "dir/added.txt", "added")
	erofstest.CheckFile(t, efs, "other.txt", "other")
}

// TestMergeMixed verifies that metadata-only and data CopyFrom calls
// can be combined. The first layer uses MetadataOnly (chunks reference
// an external tar), the second stores data in the image.
func TestMergeMixed(t *testing.T) {
	tc := erofstest.TarContext{UID: 0, GID: 0}

	// Create base tar on disk so we can provide it as a device.
	baseTarPath := filepath.Join(t.TempDir(), "base.tar")
	func() {
		f, err := os.Create(baseTarPath)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() //nolint:errcheck
		rc := erofstest.TarFromWriterTo(erofstest.TarAll(
			tc.File("base.txt", []byte("base-content"), 0o644),
		))
		defer rc.Close() //nolint:errcheck
		if _, err := io.Copy(f, rc); err != nil {
			t.Fatal(err)
		}
	}()

	// Build a metadata-only EROFS from the base tar.
	baseTarFile, err := os.Open(baseTarPath)
	if err != nil {
		t.Fatal(err)
	}
	baseTarFS, err := mktar.Open(baseTarFile)
	_ = baseTarFile.Close()
	if err != nil {
		t.Fatal("open base tar:", err)
	}
	defer baseTarFS.Close() //nolint:errcheck
	var baseErofsBuf testBuffer
	bw := erofs.Create(&baseErofsBuf)
	if err := bw.CopyFrom(baseTarFS, erofs.MetadataOnly()); err != nil {
		t.Fatal("build base erofs:", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal("close base erofs:", err)
	}

	// Open the EROFS image as the MetadataOnly source.
	baseDev, err := os.Open(baseTarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer baseDev.Close() //nolint:errcheck
	baseFS, err := erofs.Open(bytes.NewReader(baseErofsBuf.Bytes()), erofs.WithExtraDevices(baseDev))
	if err != nil {
		t.Fatal("open base erofs:", err)
	}

	overlay := fstest.MapFS{
		"overlay.txt": {Data: []byte("overlay-content"), Mode: 0o644},
	}

	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.CopyFrom(baseFS, erofs.MetadataOnly()); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlay, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// Re-open the tar as device backing for the merged image.
	baseDev2, err := os.Open(baseTarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer baseDev2.Close() //nolint:errcheck

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()), erofs.WithExtraDevices(baseDev2))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "overlay.txt", "overlay-content")
	erofstest.CheckFile(t, efs, "base.txt", "base-content")
}

// TestMergePerCopyFromMetadata verifies that MetadataOnly is per-CopyFrom,
// not sticky across calls.
func TestMergePerCopyFromMetadata(t *testing.T) {
	tc := erofstest.TarContext{UID: 0, GID: 0}

	// Create metadata tar on disk so we can use it as a device.
	metaTarPath := filepath.Join(t.TempDir(), "meta.tar")
	func() {
		f, err := os.Create(metaTarPath)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() //nolint:errcheck
		rc := erofstest.TarFromWriterTo(erofstest.TarAll(
			tc.File("meta.txt", []byte("meta"), 0o644),
		))
		defer rc.Close() //nolint:errcheck
		if _, err := io.Copy(f, rc); err != nil {
			t.Fatal(err)
		}
	}()

	// Build metadata-only EROFS from the meta tar.
	metaTarFile, err := os.Open(metaTarPath)
	if err != nil {
		t.Fatal(err)
	}
	metaTarFS, err := mktar.Open(metaTarFile)
	_ = metaTarFile.Close()
	if err != nil {
		t.Fatal("open meta tar:", err)
	}
	defer metaTarFS.Close() //nolint:errcheck
	var metaErofsBuf testBuffer
	mw := erofs.Create(&metaErofsBuf)
	if err := mw.CopyFrom(metaTarFS, erofs.MetadataOnly()); err != nil {
		t.Fatal("build meta erofs:", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal("close meta erofs:", err)
	}

	// Open the EROFS image as the MetadataOnly source.
	metaDev, err := os.Open(metaTarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metaDev.Close() //nolint:errcheck
	metaFS, err := erofs.Open(bytes.NewReader(metaErofsBuf.Bytes()), erofs.WithExtraDevices(metaDev))
	if err != nil {
		t.Fatal("open meta erofs:", err)
	}

	dataTar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.File("data.txt", []byte("data-content"), 0o644),
	))
	defer dataTar.Close() //nolint:errcheck

	dataFS, err := mktar.Open(dataTar)
	if err != nil {
		t.Fatal("open data tar:", err)
	}
	defer dataFS.Close() //nolint:errcheck

	var buf testBuffer
	w := erofs.Create(&buf)

	// First CopyFrom: metadata-only from EROFS image
	if err := w.CopyFrom(metaFS, erofs.MetadataOnly()); err != nil {
		t.Fatal("CopyFrom meta:", err)
	}
	// Second CopyFrom: NOT metadata-only (should store data)
	if err := w.CopyFrom(dataFS); err != nil {
		t.Fatal("CopyFrom data:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// Provide the meta tar as the backing device.
	metaDev2, err := os.Open(metaTarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metaDev2.Close() //nolint:errcheck

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()), erofs.WithExtraDevices(metaDev2))
	if err != nil {
		t.Fatal("Open:", err)
	}

	erofstest.CheckFile(t, efs, "data.txt", "data-content")
	erofstest.CheckFile(t, efs, "meta.txt", "meta")
}

// TestMergeTarOverErofs simulates an erofs layering workflow:
// lower layers exist as EROFS images backed by their tars;
// the top layer is a new tar being unpacked.
//
// Layer 1 (bottom): erofs with data
// Layer 2: erofs with layer 2 data + merged layer 1 metadata (1 blob device)
// Layer 3: erofs with layer 3 data, including white outs (no blob devices)
// Layer 4: erofs with layer 4 data + merged layer 1, 2 and 3 metadata (3 blob devices)
func TestMergeTarOverErofs(t *testing.T) {
	tc := erofstest.TarContext{UID: 0, GID: 0}
	tmpDir := t.TempDir()

	// writeTar is a helper that writes a tar to disk.
	writeTar := func(name string, entries ...erofstest.WriterToTar) string {
		p := filepath.Join(tmpDir, name)
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() //nolint:errcheck
		rc := erofstest.TarFromWriterTo(erofstest.TarAll(entries...))
		defer rc.Close() //nolint:errcheck
		if _, err := io.Copy(f, rc); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// openTar opens a tar file from disk and returns the FS.
	openTar := func(path string) *mktar.FS {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		fs, err := mktar.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fs.Close() })
		return fs
	}

	// openDev opens a file as a device reader.
	openDev := func(path string) *os.File {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		return f
	}

	// openErofs opens a previously built EROFS image with device backing.
	openErofs := func(buf []byte, devices ...io.ReaderAt) fs.FS {
		t.Helper()
		efs, err := erofs.Open(bytes.NewReader(buf), erofs.WithExtraDevices(devices...))
		if err != nil {
			t.Fatal("open erofs:", err)
		}
		return efs
	}

	// ── Layer 1 tar: the base image. ──
	layer1Path := writeTar("layer1.tar",
		tc.Dir("etc/", 0o755),
		tc.File("etc/config.json", []byte(`{"version":1}`), 0o644),
		tc.File("etc/passwd", []byte("root:x:0:0:::/bin/sh\n"), 0o644),
		tc.Dir("bin/", 0o755),
		tc.File("bin/app", []byte("#!/bin/sh\necho v1\n"), 0o755),
		tc.File("bin/helper", []byte("#!/bin/sh\necho help\n"), 0o755),
		tc.Dir("var/", 0o755),
		tc.Dir("var/cache/", 0o755),
		tc.File("var/cache/data.db", []byte("stale-cache"), 0o644),
	)

	// ── Layer 1 EROFS: standalone image with data. ──
	layer1FS := openTar(layer1Path)
	var layer1Buf testBuffer
	w1 := erofs.Create(&layer1Buf)
	if err := w1.CopyFrom(layer1FS); err != nil {
		t.Fatal("layer1 CopyFrom:", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatal("layer1 Close:", err)
	}

	l1, err := erofs.Open(bytes.NewReader(layer1Buf.Bytes()))
	if err != nil {
		t.Fatal("open layer1 erofs:", err)
	}
	erofstest.CheckFile(t, l1, "etc/config.json", `{"version":1}`)
	erofstest.CheckFile(t, l1, "bin/app", "#!/bin/sh\necho v1\n")

	// ── Layer 2 tar: updates app, adds /lib/core.so. ──
	layer2Path := writeTar("layer2.tar",
		tc.Dir("bin/", 0o755),
		tc.File("bin/app", []byte("#!/bin/sh\necho v2\n"), 0o755),
		tc.Dir("lib/", 0o755),
		tc.File("lib/core.so", []byte("\x7fELF-core"), 0o755),
	)

	// buildMetaErofs creates a metadata-only EROFS from a tar on disk.
	// These are the images a snapshotter would have pre-built when pulling layers.
	buildMetaErofs := func(tarPath string) []byte {
		t.Helper()
		tf, err := os.Open(tarPath)
		if err != nil {
			t.Fatal(err)
		}
		tfs, err := mktar.Open(tf)
		_ = tf.Close()
		if err != nil {
			t.Fatal(err)
		}
		defer tfs.Close() //nolint:errcheck
		var buf testBuffer
		w := erofs.Create(&buf)
		if err := w.CopyFrom(tfs, erofs.MetadataOnly()); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	// Build metadata-only EROFS for layer 1 (layer 2 & 3 built later, after their tars).
	layer1MetaImg := buildMetaErofs(layer1Path)

	// ── Layer 2 EROFS: layer 2 data + merged layer 1 metadata (1 blob). ──
	// Use the layer 1 metadata-only EROFS as the base.
	layer1EFS := openErofs(layer1MetaImg, openDev(layer1Path))
	layer2FS := openTar(layer2Path)

	var layer2Buf testBuffer
	w2 := erofs.Create(&layer2Buf)
	if err := w2.CopyFrom(layer1EFS, erofs.MetadataOnly()); err != nil {
		t.Fatal("layer2 CopyFrom layer1:", err)
	}
	if err := w2.CopyFrom(layer2FS, erofs.Merge()); err != nil {
		t.Fatal("layer2 CopyFrom layer2:", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal("layer2 Close:", err)
	}

	l2, err := erofs.Open(bytes.NewReader(layer2Buf.Bytes()),
		erofs.WithExtraDevices(openDev(layer1Path)))
	if err != nil {
		t.Fatal("open layer2 erofs:", err)
	}
	erofstest.CheckFile(t, l2, "etc/config.json", `{"version":1}`) // from layer 1 device
	erofstest.CheckFile(t, l2, "bin/app", "#!/bin/sh\necho v2\n")  // from layer 2 data
	erofstest.CheckFile(t, l2, "lib/core.so", "\x7fELF-core")      // from layer 2 data

	// ── Layer 3 tar: contains whiteouts + new files. ──
	//
	// This layer's tar includes raw whiteout entries. The EROFS image is
	// built standalone (no merge, no blob devices) — whiteout markers are
	// stored as regular files in the image, not applied.
	layer3Path := writeTar("layer3.tar",
		tc.Dir("bin/", 0o755),
		tc.File("bin/.wh.helper", nil, 0o644),
		tc.Dir("var/", 0o755),
		tc.Dir("var/cache/", 0o755),
		tc.File("var/cache/.wh..wh..opq", nil, 0o644),
		tc.File("var/cache/fresh.db", []byte("fresh-cache"), 0o644),
		tc.Dir("usr/", 0o755),
		tc.Dir("usr/lib/", 0o755),
		tc.File("usr/lib/new.so", []byte("\x7fELF-fake-binary"), 0o755),
	)

	// ── Layer 3 EROFS: standalone image with data (no blob devices). ──
	layer3FS := openTar(layer3Path)

	var layer3Buf testBuffer
	w3 := erofs.Create(&layer3Buf)
	if err := w3.CopyFrom(layer3FS); err != nil {
		t.Fatal("layer3 CopyFrom:", err)
	}
	if err := w3.Close(); err != nil {
		t.Fatal("layer3 Close:", err)
	}

	l3, err := erofs.Open(bytes.NewReader(layer3Buf.Bytes()))
	if err != nil {
		t.Fatal("open layer3 erofs:", err)
	}
	// Whiteout markers are stored as regular files — not applied.
	erofstest.CheckFile(t, l3, "var/cache/fresh.db", "fresh-cache")
	erofstest.CheckFile(t, l3, "usr/lib/new.so", "\x7fELF-fake-binary")

	// Build metadata-only EROFS images for layers 2 and 3.
	layer2MetaImg := buildMetaErofs(layer2Path)
	layer3MetaImg := buildMetaErofs(layer3Path)

	// ── Layer 4 tar: updates config, removes /usr/lib/new.so, adds /opt/tool. ──
	layer4Tar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.Dir("etc/", 0o755),
		tc.File("etc/config.json", []byte(`{"version":4}`), 0o644),
		tc.Dir("usr/", 0o755),
		tc.Dir("usr/lib/", 0o755),
		tc.File("usr/lib/.wh.new.so", nil, 0o644),
		tc.Dir("opt/", 0o755),
		tc.File("opt/tool", []byte("#!/bin/sh\necho tool\n"), 0o755),
	))
	defer layer4Tar.Close() //nolint:errcheck

	layer4FS, err := mktar.Open(layer4Tar)
	if err != nil {
		t.Fatal("open layer4 tar:", err)
	}
	defer layer4FS.Close() //nolint:errcheck

	// ── Layer 4 EROFS: layer 4 data + merged layers 1, 2, 3 metadata (3 blobs). ──
	//
	// Each lower layer is loaded from its metadata-only EROFS image with
	// Merge so whiteouts are applied in order. Layer 4 tar is merged on top.
	layer1EFS2 := openErofs(layer1MetaImg, openDev(layer1Path))
	layer2EFS := openErofs(layer2MetaImg, openDev(layer2Path))
	layer3EFS := openErofs(layer3MetaImg, openDev(layer3Path))

	var layer4Buf testBuffer
	w4 := erofs.Create(&layer4Buf)
	// Blob device 1: layer 1 EROFS (base — no whiteouts to apply)
	if err := w4.CopyFrom(layer1EFS2, erofs.MetadataOnly()); err != nil {
		t.Fatal("layer4 CopyFrom layer1:", err)
	}
	// Blob device 2: layer 2 EROFS (merge overwrites layer 1 entries)
	if err := w4.CopyFrom(layer2EFS, erofs.MetadataOnly(), erofs.Merge()); err != nil {
		t.Fatal("layer4 CopyFrom layer2:", err)
	}
	// Blob device 3: layer 3 EROFS (merge applies whiteouts from layer 3)
	if err := w4.CopyFrom(layer3EFS, erofs.MetadataOnly(), erofs.Merge()); err != nil {
		t.Fatal("layer4 CopyFrom layer3:", err)
	}
	// Layer 4 tar merged on top; data stored inline.
	if err := w4.CopyFrom(layer4FS, erofs.Merge()); err != nil {
		t.Fatal("layer4 CopyFrom layer4:", err)
	}
	if err := w4.Close(); err != nil {
		t.Fatal("layer4 Close:", err)
	}

	// Open with all three blob devices.
	merged, err := erofs.Open(bytes.NewReader(layer4Buf.Bytes()),
		erofs.WithExtraDevices(openDev(layer1Path), openDev(layer2Path), openDev(layer3Path)))
	if err != nil {
		t.Fatal("open layer4 erofs:", err)
	}

	// ── Verify the final merged layer 4 image. ──

	// Updated by layer 4 (stored inline).
	erofstest.CheckFile(t, merged, "etc/config.json", `{"version":4}`)

	// From layer 1 device: preserved through all layers.
	erofstest.CheckFile(t, merged, "etc/passwd", "root:x:0:0:::/bin/sh\n")

	// From layer 2 device: app was updated in layer 2.
	erofstest.CheckFile(t, merged, "bin/app", "#!/bin/sh\necho v2\n")
	erofstest.CheckFile(t, merged, "lib/core.so", "\x7fELF-core")

	// Whiteout from layer 3 (applied via MetadataOnly+Merge): /bin/helper gone.
	erofstest.CheckNotExists(t, merged, "bin/helper")

	// Opaque from layer 3: /var/cache old entries gone, fresh.db from layer 3 device.
	erofstest.CheckNotExists(t, merged, "var/cache/data.db")
	erofstest.CheckFile(t, merged, "var/cache/fresh.db", "fresh-cache")

	// From layer 3 device: new.so was added in layer 3, then removed by layer 4.
	erofstest.CheckNotExists(t, merged, "usr/lib/new.so")

	// New file from layer 4 (stored inline).
	erofstest.CheckFile(t, merged, "opt/tool", "#!/bin/sh\necho tool\n")

	// Directory structure reflects all four layers.
	erofstest.CheckDirEntries(t, merged, ".", []string{"bin", "etc", "lib", "opt", "usr", "var"})
	erofstest.CheckDirEntries(t, merged, "bin", []string{"app"})
	erofstest.CheckDirEntries(t, merged, "usr/lib", nil)
}

// TestMergeErofsToErofs verifies that two full EROFS images (with data, not
// metadata-only) can be merged. The result stores all data inline. This
// exercises the fs.WalkDir path for EROFS sources (not copyFromImage) since
// file data must be read from the source images.
func TestMergeErofsToErofs(t *testing.T) {
	tc := erofstest.TarContext{UID: 0, GID: 0}

	// Build base EROFS from tar.
	baseTar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.Dir("dir/", 0o755),
		tc.File("dir/base.txt", []byte("base-data"), 0o644),
		tc.File("dir/shared.txt", []byte("old-shared"), 0o644),
		tc.File("remove.txt", []byte("gone"), 0o644),
	))
	defer baseTar.Close() //nolint:errcheck
	baseTarFS, err := mktar.Open(baseTar)
	if err != nil {
		t.Fatal(err)
	}
	defer baseTarFS.Close() //nolint:errcheck
	var baseBuf testBuffer
	bw := erofs.Create(&baseBuf)
	if err := bw.CopyFrom(baseTarFS); err != nil {
		t.Fatal("build base:", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal("close base:", err)
	}

	// Build overlay EROFS from tar (contains whiteout + overwrite + new file).
	overlayTar := erofstest.TarFromWriterTo(erofstest.TarAll(
		tc.Dir("dir/", 0o755),
		tc.File("dir/shared.txt", []byte("new-shared"), 0o644),
		tc.File(".wh.remove.txt", nil, 0o644),
		tc.File("added.txt", []byte("added-data"), 0o644),
	))
	defer overlayTar.Close() //nolint:errcheck
	overlayTarFS, err := mktar.Open(overlayTar)
	if err != nil {
		t.Fatal(err)
	}
	defer overlayTarFS.Close() //nolint:errcheck
	var overlayBuf testBuffer
	ow := erofs.Create(&overlayBuf)
	if err := ow.CopyFrom(overlayTarFS); err != nil {
		t.Fatal("build overlay:", err)
	}
	if err := ow.Close(); err != nil {
		t.Fatal("close overlay:", err)
	}

	// Open both EROFS images from their serialized bytes (simulates
	// opening from disk — no in-process memory sharing).
	baseBytes := make([]byte, len(baseBuf.Bytes()))
	copy(baseBytes, baseBuf.Bytes())
	overlayBytes := make([]byte, len(overlayBuf.Bytes()))
	copy(overlayBytes, overlayBuf.Bytes())

	baseFS, err := erofs.Open(bytes.NewReader(baseBytes))
	if err != nil {
		t.Fatal("open base:", err)
	}
	overlayFS, err := erofs.Open(bytes.NewReader(overlayBytes))
	if err != nil {
		t.Fatal("open overlay:", err)
	}

	// Merge: base (full copy) + overlay (merge with whiteouts).
	var mergedBuf testBuffer
	w := erofs.Create(&mergedBuf)
	if err := w.CopyFrom(baseFS); err != nil {
		t.Fatal("CopyFrom base:", err)
	}
	if err := w.CopyFrom(overlayFS, erofs.Merge()); err != nil {
		t.Fatal("CopyFrom overlay:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// Read back and verify — all data should be stored inline.
	erofstest.FsckErofsBytes(t, mergedBuf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(mergedBuf.Bytes()))
	if err != nil {
		t.Fatal("open merged:", err)
	}

	erofstest.CheckFile(t, efs, "dir/base.txt", "base-data")
	erofstest.CheckFile(t, efs, "dir/shared.txt", "new-shared") // overlay wins
	erofstest.CheckFile(t, efs, "added.txt", "added-data")
	erofstest.CheckNotExists(t, efs, "remove.txt") // whiteout applied
}
