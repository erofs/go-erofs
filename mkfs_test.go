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

// TestCreateFSUIDGID tests a variety of UID/GID values including boundary
// values for compact (uint16) and extended (uint32) inodes.
func TestCreateFSUIDGID(t *testing.T) {
	type uidgid struct {
		uid, gid int
	}
	cases := []uidgid{
		{0, 0},
		{1, 1},
		{1000, 2000},
		{65534, 65534}, // nobody/nogroup on many systems
		{65535, 65535}, // max compact inode UID/GID
		{65536, 65536}, // first value requiring extended inode
		{100000, 100000},
		{0, 65536}, // mixed: compact-range UID, extended-range GID
		{65536, 0}, // mixed: extended-range UID, compact-range GID
		{0x7FFFFFFF, 0x7FFFFFFF},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("uid%d_gid%d", tc.uid, tc.gid), func(t *testing.T) {
			var buf testBuffer
			fsys := erofs.Create(&buf)

			// Test UID/GID on a regular file via File.Chown.
			f, err := fsys.Create("/file.txt")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			if err := f.Chown(tc.uid, tc.gid); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			// Test UID/GID on a directory via Writer.Chown.
			if err := fsys.Mkdir("/dir", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := fsys.Chown("/dir", tc.uid, tc.gid); err != nil {
				t.Fatal(err)
			}

			// Test UID/GID on a symlink.
			if err := fsys.Symlink("file.txt", "/link"); err != nil {
				t.Fatal(err)
			}
			if err := fsys.Chown("/link", tc.uid, tc.gid); err != nil {
				t.Fatal(err)
			}

			if err := fsys.Close(); err != nil {
				t.Fatal("Close:", err)
			}

			erofstest.FsckErofsBytes(t, buf.Bytes())
			efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal("Open:", err)
			}

			wantUID := uint32(tc.uid)
			wantGID := uint32(tc.gid)

			// Verify file UID/GID.
			st := erofstest.Stat(t, efs, "file.txt")
			if st.UID != wantUID || st.GID != wantGID {
				t.Errorf("file uid/gid: got %d/%d, want %d/%d", st.UID, st.GID, wantUID, wantGID)
			}

			// Verify directory UID/GID.
			dst := erofstest.Stat(t, efs, "dir")
			if dst.UID != wantUID || dst.GID != wantGID {
				t.Errorf("dir uid/gid: got %d/%d, want %d/%d", dst.UID, dst.GID, wantUID, wantGID)
			}

			// Verify symlink UID/GID.
			lst := erofstest.Lstat(t, efs, "link")
			if lst.UID != wantUID || lst.GID != wantGID {
				t.Errorf("symlink uid/gid: got %d/%d, want %d/%d", lst.UID, lst.GID, wantUID, wantGID)
			}
		})
	}
}

// TestCopyFromStatSource verifies that CopyFrom preserves directory xattrs,
// directory ownership, and file ownership when the source FS returns *Stat
// from FileInfo.Sys() (the generic/EROFS reader path, not *builder.Entry).
//
// Before the fix, CopyFrom fell through to add(p, info) for directories
// and non-regular entries when be was extracted from *Stat, losing all
// metadata because entryFromSys does not handle *Stat.
func TestCopyFromStatSource(t *testing.T) {
	// Build a source EROFS image with a directory that has xattrs and
	// custom ownership, plus a file with custom ownership.
	var srcBuf erofstest.TestBuffer
	w := erofs.Create(&srcBuf)

	if err := w.Mkdir("/", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("/mydir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Setxattr("/mydir", "trusted.overlay.opaque", "y"); err != nil {
		t.Fatal(err)
	}
	if err := w.Setxattr("/mydir", "user.custom", "val"); err != nil {
		t.Fatal(err)
	}
	if err := w.Chown("/mydir", 1000, 1000); err != nil {
		t.Fatal(err)
	}
	f, err := w.Create("/mydir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("content"))
	_ = f.Close()
	if err := w.Chown("/mydir/file.txt", 2000, 2000); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the source EROFS — Sys() returns *erofs.Stat, not *builder.Entry.
	srcFS, err := erofs.Open(bytes.NewReader(srcBuf.Bytes()))
	if err != nil {
		t.Fatal("open source:", err)
	}

	// CopyFrom into a new Writer (non-MetadataOnly → uses WalkDir path).
	var dstBuf erofstest.TestBuffer
	w2 := erofs.Create(&dstBuf)
	if err := w2.CopyFrom(srcFS); err != nil {
		t.Fatal("CopyFrom:", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// Open the destination and verify metadata survived the round-trip.
	dstFS, err := erofs.Open(bytes.NewReader(dstBuf.Bytes()))
	if err != nil {
		t.Fatal("open dest:", err)
	}

	// Directory xattrs must be preserved.
	erofstest.CheckXattrs(t, dstFS, "mydir", map[string]string{
		"trusted.overlay.opaque": "y",
		"user.custom":            "val",
	})

	// Directory ownership must be preserved.
	dirSt := erofstest.Stat(t, dstFS, "mydir")
	if dirSt.UID != 1000 {
		t.Errorf("mydir UID = %d, want 1000", dirSt.UID)
	}
	if dirSt.GID != 1000 {
		t.Errorf("mydir GID = %d, want 1000", dirSt.GID)
	}

	// File content and ownership must be preserved.
	erofstest.CheckFile(t, dstFS, "mydir/file.txt", "content")
	fileSt := erofstest.Stat(t, dstFS, "mydir/file.txt")
	if fileSt.UID != 2000 {
		t.Errorf("file.txt UID = %d, want 2000", fileSt.UID)
	}
	if fileSt.GID != 2000 {
		t.Errorf("file.txt GID = %d, want 2000", fileSt.GID)
	}
}
