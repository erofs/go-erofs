package tar_test

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
	mktar "github.com/erofs/go-erofs/tar"
)

// tarConverter creates an EROFS image from a tar stream using tar.Open + CopyFrom.
func tarConverter(t testing.TB, wt erofstest.WriterToTar) fs.FS {
	t.Helper()
	tarStream := erofstest.TarFromWriterTo(wt)
	defer func() { _ = tarStream.Close() }()

	tarFS, err := mktar.Open(tarStream)
	if err != nil {
		t.Fatal("tar.Open:", err)
	}
	defer tarFS.Close() //nolint:errcheck

	var buf erofstest.TestBuffer
	fsys := erofs.Create(&buf)
	if err := fsys.CopyFrom(tarFS); err != nil {
		t.Fatal("CopyFrom:", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}
	return efs
}

// tarFileConverter is like tarConverter but writes the tar to an *os.File
// first, exercising the io.ReaderAt zero-copy path.
func tarFileConverter(t testing.TB, wt erofstest.WriterToTar) fs.FS {
	t.Helper()
	tarPath := filepath.Join(t.TempDir(), "test.tar")
	tarFile, err := os.Create(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	tarStream := erofstest.TarFromWriterTo(wt)
	if _, err := io.Copy(tarFile, tarStream); err != nil {
		t.Fatal("write tar:", err)
	}
	_ = tarStream.Close()
	if _, err := tarFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	tarFS, err := mktar.Open(tarFile)
	if err != nil {
		t.Fatal("tar.Open:", err)
	}
	defer tarFS.Close() //nolint:errcheck

	var buf erofstest.TestBuffer
	fsys := erofs.Create(&buf)
	if err := fsys.CopyFrom(tarFS); err != nil {
		t.Fatal("CopyFrom:", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	_ = tarFile.Close()

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}
	return efs
}

// TestTarRoundTrip builds a tar stream using the standard test cases,
// creates an EROFS image via the tar iter + Add pattern, and reads it back.
func TestTarRoundTrip(t *testing.T) {
	t.Run("Basic", func(t *testing.T) { erofstest.Basic.Run(t, tarConverter) })
	t.Run("FileSizes", func(t *testing.T) { erofstest.FileSizes.Run(t, tarConverter) })
	t.Run("SparseFiles", func(t *testing.T) { erofstest.SparseFiles.Run(t, tarConverter) })
	t.Run("LongXattrs", func(t *testing.T) { erofstest.LongXattrs.Run(t, tarConverter) })
}

// TestTarFileRoundTrip is the same as TestTarRoundTrip but passes an
// *os.File (which implements io.ReaderAt) to exercise the zero-copy path.
func TestTarFileRoundTrip(t *testing.T) {
	t.Run("Basic", func(t *testing.T) { erofstest.Basic.Run(t, tarFileConverter) })
	t.Run("FileSizes", func(t *testing.T) { erofstest.FileSizes.Run(t, tarFileConverter) })
	t.Run("SparseFiles", func(t *testing.T) { erofstest.SparseFiles.Run(t, tarFileConverter) })
	t.Run("LongXattrs", func(t *testing.T) { erofstest.LongXattrs.Run(t, tarFileConverter) })
}

// TestTarWhiteoutConversion verifies that AUFS-style whiteout files in OCI
// layer tars are converted to overlayfs equivalents.
func TestTarWhiteoutConversion(t *testing.T) {
	tc := erofstest.TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	writerTo := erofstest.TarAll(
		tc.Dir("/", 0755),
		tc.File("/keep.txt", []byte("kept\n"), 0644),
		tc.Dir("/mydir", 0755),
		tc.File("/mydir/a.txt", []byte("aaa\n"), 0644),
		tc.File("/.wh.removed.txt", []byte{}, 0644),
		tc.File("/mydir/.wh..wh..opq", []byte{}, 0644),
	)

	tarStream := erofstest.TarFromWriterTo(writerTo)
	defer func() { _ = tarStream.Close() }()

	tarFS, err := mktar.Open(tarStream, mktar.ConvertWhiteouts())
	if err != nil {
		t.Fatal("tar.Open:", err)
	}
	defer tarFS.Close() //nolint:errcheck

	var erofsBuf erofstest.TestBuffer
	wfsys := erofs.Create(&erofsBuf)
	if err := wfsys.CopyFrom(tarFS); err != nil {
		t.Fatal("CopyFrom:", err)
	}
	if err := wfsys.Close(); err != nil {
		t.Fatal("Close failed:", err)
	}

	efs, err := erofs.Open(bytes.NewReader(erofsBuf.Bytes()))
	if err != nil {
		t.Fatal("Open failed:", err)
	}

	erofstest.CheckFile(t, efs, "keep.txt", "kept\n")
	erofstest.CheckFile(t, efs, "mydir/a.txt", "aaa\n")
	erofstest.CheckDevice(t, efs, "removed.txt", fs.ModeCharDevice, 0)
	erofstest.CheckXattrs(t, efs, "mydir", map[string]string{
		"trusted.overlay.opaque": "y",
	})
	erofstest.CheckNotExists(t, efs, ".wh.removed.txt")
	erofstest.CheckNotExists(t, efs, "mydir/.wh..wh..opq")
}
