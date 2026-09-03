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
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
)

// TestCompressedLZ4 runs the standard erofstest TestCases through an
// LZ4-compressed image (full lcluster layout, forced via -Elegacy-compress).
// All sub-tests skip if mkfs.erofs lacks LZ4.
func TestCompressedLZ4(t *testing.T) {
	erofstest.RequireMkfsLZ4(t)

	for _, tc := range []struct {
		name  string
		test  erofstest.TestCase
		flags []string
	}{
		{"Basic", erofstest.Basic, nil},
		{"FileSizes", erofstest.FileSizes, nil},
		{"LongXattrs", erofstest.LongXattrs, erofstest.XattrPrefixFlags()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.test.Run(t, erofstest.MkfsErofsLZ4(tc.flags...))
		})
	}

	t.Run("LargeFile", func(t *testing.T) {
		erofstest.LargeFile.Run(t, erofstest.MkfsErofsLZ4())
	})
}

// TestCompressedContent exercises edge cases specific to compression:
// content compressibility, file-size boundaries, and multi-pcluster reads.
func TestCompressedContent(t *testing.T) {
	erofstest.RequireMkfsLZ4(t)

	tc := erofstest.TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	t.Run("HighlyCompressible", func(t *testing.T) {
		// 4 MiB of zeros plus 4 MiB of a repeating pattern — both very
		// compressible.  Exercises HEAD lclusters with logically-long pclusters.
		zeros := bytes.Repeat([]byte{0}, 4*1024*1024)
		repeating := bytes.Repeat([]byte{'A', 'B', 'C', 'D'}, 1024*1024)
		fsys := erofstest.MkfsErofsLZ4()(t, erofstest.TarAll(
			tc.File("/zeros.bin", zeros, 0o644),
			tc.File("/abcd.bin", repeating, 0o644),
		))
		readAndCompare(t, fsys, "zeros.bin", zeros)
		readAndCompare(t, fsys, "abcd.bin", repeating)
	})

	t.Run("Incompressible", func(t *testing.T) {
		// Deterministic-random bytes; mkfs.erofs should fall back to PLAIN
		// clusters for these.
		incompressible := make([]byte, 256*1024)
		fillPRNG(incompressible, 0xDEADBEEF)
		fsys := erofstest.MkfsErofsLZ4()(t, erofstest.TarAll(
			tc.File("/random.bin", incompressible, 0o644),
		))
		readAndCompare(t, fsys, "random.bin", incompressible)
	})

	t.Run("BlockBoundary", func(t *testing.T) {
		// Files at sizes near lcluster (== block size = 4 KiB) boundaries.
		// Each file's content is verified byte-for-byte.
		sizes := []int{1, 4095, 4096, 4097, 8191, 8192, 8193}
		var entries []erofstest.WriterToTar
		want := make(map[string][]byte, len(sizes))
		for _, sz := range sizes {
			buf := make([]byte, sz)
			for i := range buf {
				buf[i] = byte(i % 251)
			}
			name := fmt.Sprintf("/sz-%d.bin", sz)
			entries = append(entries, tc.File(name, buf, 0o644))
			want[name[1:]] = buf
		}
		fsys := erofstest.MkfsErofsLZ4()(t, erofstest.TarAll(entries...))
		for name, expected := range want {
			readAndCompare(t, fsys, name, expected)
		}
	})

	t.Run("MultiPclusterSpanningRead", func(t *testing.T) {
		// Read a large compressible file in small chunks to force many
		// per-block loadCompressedBlock calls across multiple pclusters.
		size := 2 * 1024 * 1024
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte((i / 64) % 251)
		}
		fsys := erofstest.MkfsErofsLZ4()(t, erofstest.TarAll(
			tc.File("/big.bin", buf, 0o644),
		))
		f, err := fsys.Open("big.bin")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		got := make([]byte, 0, size)
		chunk := make([]byte, 173) // intentionally not a power of two
		for {
			n, err := f.Read(chunk)
			got = append(got, chunk[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got, buf) {
			t.Fatalf("multi-pcluster small-read mismatch: len got=%d want=%d", len(got), len(buf))
		}
	})
}

// TestCompressedCrossValidate builds the same tar content into both an
// uncompressed and an LZ4 image and walks both filesystems, byte-comparing
// every regular file.
func TestCompressedCrossValidate(t *testing.T) {
	erofstest.RequireMkfsLZ4(t)

	tc := erofstest.TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	entries := []erofstest.WriterToTar{
		tc.Dir("/d", 0o755),
		tc.File("/d/empty.bin", []byte{}, 0o644),
		tc.File("/d/small.bin", []byte("hello world\n"), 0o644),
		tc.File("/d/just-block.bin", bytes.Repeat([]byte{0xAB}, 4096), 0o644),
		tc.File("/d/just-over.bin", bytes.Repeat([]byte{0xCD}, 4097), 0o644),
		tc.File("/d/sequence.bin", generateSeq(64*1024), 0o644),
		tc.File("/d/zeros.bin", bytes.Repeat([]byte{0}, 64*1024), 0o644),
		tc.File("/d/repeating.bin", bytes.Repeat([]byte("the quick brown fox jumps\n"), 1024), 0o644),
	}

	plainFsys := erofstest.MkfsErofs()(t, erofstest.TarAll(entries...))
	lz4Fsys := erofstest.MkfsErofsLZ4()(t, erofstest.TarAll(entries...))

	err := fs.WalkDir(plainFsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		plainBytes, err := fs.ReadFile(plainFsys, p)
		if err != nil {
			t.Errorf("plain %s: read: %v", p, err)
			return nil
		}
		lz4Bytes, err := fs.ReadFile(lz4Fsys, p)
		if err != nil {
			t.Errorf("lz4 %s: read: %v", p, err)
			return nil
		}
		if !bytes.Equal(plainBytes, lz4Bytes) {
			t.Errorf("%s: lz4 vs plain mismatch (len %d vs %d)", p, len(lz4Bytes), len(plainBytes))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCompressedDeferred verifies that compact-layout compressed images
// (which this library does not decode) report ErrNotImplemented from
// Open or the first read, rather than silently producing wrong data.
func TestCompressedDeferred(t *testing.T) {
	erofstest.RequireMkfsLZ4(t)

	t.Run("CompactLayout", func(t *testing.T) {
		// Default mkfs.erofs `-z lz4` produces the compact lcluster
		// layout, which this library does not decode.
		tc := erofstest.TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		wt := erofstest.TarAll(
			tc.File("/big.bin", bytes.Repeat([]byte{0xAA}, 64*1024), 0o644),
		)
		tarStream := erofstest.TarFromWriterTo(wt)
		defer func() { _ = tarStream.Close() }()
		path := filepath.Join(t.TempDir(), "compact.erofs")
		if err := erofstest.ConvertTarErofs(context.Background(), tarStream, path, "", []string{"-z", "lz4"}); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()

		fsys, err := erofs.Open(f)
		if err != nil {
			// Some mkfs.erofs versions may produce full layout even without
			// -Elegacy-compress for short files; if Open already errored,
			// that's also acceptable as long as it's ErrNotImplemented.
			if !errors.Is(err, erofs.ErrNotImplemented) {
				t.Fatalf("Open: want ErrNotImplemented, got %v", err)
			}
			return
		}
		// Reading the compressed file should fail with ErrNotImplemented
		// at loadCompressedBlock time.
		_, err = fs.ReadFile(fsys, "big.bin")
		if err == nil {
			t.Fatal("expected an error reading compact-layout file, got nil")
		}
		if !errors.Is(err, erofs.ErrNotImplemented) {
			t.Errorf("compact-layout read: want ErrNotImplemented, got %v", err)
		}
	})
}

// readAndCompare opens name, reads it fully, and compares against want.
func readAndCompare(t testing.TB, fsys fs.FS, name string, want []byte) {
	t.Helper()
	got, err := fs.ReadFile(fsys, name)
	if err != nil {
		t.Errorf("read %s: %v", name, err)
		return
	}
	if !bytes.Equal(got, want) {
		// Find the first mismatch for a useful message.
		for i := 0; i < len(got) && i < len(want); i++ {
			if got[i] != want[i] {
				t.Errorf("%s: first mismatch at offset %d (got 0x%02x, want 0x%02x); lengths got=%d want=%d",
					name, i, got[i], want[i], len(got), len(want))
				return
			}
		}
		t.Errorf("%s: length mismatch got=%d want=%d", name, len(got), len(want))
	}
}

func generateSeq(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// fillPRNG fills b with deterministic bytes via xorshift32 seeded by seed.
func fillPRNG(b []byte, seed uint32) {
	x := seed
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
}
