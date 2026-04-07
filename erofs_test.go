package erofs_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
)

func TestErofs(t *testing.T) {
	if _, err := erofstest.CheckMkfsVersion("1.0"); err != nil {
		t.Skipf("skipping: %v", err)
	}

	minChunk := os.Getpagesize()

	for _, tc := range []struct {
		name  string
		test  erofstest.TestCase
		flags []string // extra mkfs.erofs flags
	}{
		{"Basic", erofstest.Basic, nil},
		{"FileSizes", erofstest.FileSizes, nil},
		{"LongXattrs", erofstest.LongXattrs, erofstest.XattrPrefixFlags()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, cc := range []struct {
				name string
				conv erofstest.Converter
			}{
				{"default", erofstest.MkfsErofs(tc.flags...)},
				{fmt.Sprintf("chunk-%d", minChunk), erofstest.MkfsErofs(append(tc.flags, fmt.Sprintf("--chunksize=%d", minChunk))...)},
				{fmt.Sprintf("chunk-%d", minChunk*2), erofstest.MkfsErofs(append(tc.flags, fmt.Sprintf("--chunksize=%d", minChunk*2))...)},
				{"chunk-index", erofstest.MkfsErofsBlobDev(minChunk, tc.flags...)},
			} {
				t.Run(cc.name, func(t *testing.T) {
					tc.test.Run(t, cc.conv)
				})
			}
		})
	}

	// Large file: 256MB+ to exercise chunk index overflow (run once with 4K chunks).
	t.Run("LargeFile", func(t *testing.T) {
		erofstest.LargeFile.Run(t, erofstest.MkfsErofs(fmt.Sprintf("--chunksize=%d", minChunk)))
	})

	// Sparse files require --chunksize and produce images under 1MB
	// despite 30MB of logical content.
	t.Run("SparseFiles", func(t *testing.T) {
		chunkFlag := fmt.Sprintf("--chunksize=%d", minChunk)
		erofstest.SparseFiles.Run(t, erofstest.MkfsErofsMaxSize(1024*1024, chunkFlag))
	})

	// Compression format is unimplemented — verify EroFS returns ErrNotImplemented.
	t.Run("lz4-unimplemented", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("mkfs.erofs compression is not included on Windows")
		}
		tc := erofstest.TarContext{}
		wt := erofstest.TarAll(
			tc.File("/file.txt", []byte("content\n"), 0644),
		)
		tarStream := erofstest.TarFromWriterTo(wt)
		defer func() {
			if err := tarStream.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		path := filepath.Join(t.TempDir(), "compressed.erofs")
		if err := erofstest.ConvertTarErofs(context.Background(), tarStream, path, "", []string{"-zlz4"}); err != nil {
			t.Fatal(err)
		}

		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		_, err = erofs.Open(f)
		if !errors.Is(err, erofs.ErrNotImplemented) {
			t.Fatalf("expected ErrNotImplemented, got %v", err)
		}
	})
}

func BenchmarkLookup(b *testing.B) {
	if _, err := erofstest.CheckMkfsVersion("1.0"); err != nil {
		b.Skipf("skipping: %v", err)
	}

	tc := erofstest.TarContext{}.WithModTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	// Build a tar with directories of varying sizes.
	lotsOfFiles := make(chan erofstest.WriterToTar)
	go func() {
		for i := range 5000 {
			lotsOfFiles <- tc.File(fmt.Sprintf("/bigdir/%d", i), []byte{}, 0600)
		}
		close(lotsOfFiles)
	}()

	wt := erofstest.TarAll(
		tc.Dir("/a", 0755),
		tc.Dir("/a/b", 0755),
		tc.Dir("/a/b/c", 0755),
		tc.File("/a/b/c/file.txt", []byte("content\n"), 0644),
		tc.Dir("/smalldir", 0755),
		tc.File("/smalldir/file.txt", []byte("content\n"), 0644),
		tc.Dir("/bigdir", 0755),
		erofstest.TarStream(lotsOfFiles),
	)

	fsys := erofstest.MkfsErofs()(b, wt)

	for _, bc := range []struct {
		name string
		path string
	}{
		{"shallow", "smalldir/file.txt"},
		{"deep", "a/b/c/file.txt"},
		{"bigdir-first", "bigdir/0"},
		{"bigdir-last", "bigdir/4999"},
		{"bigdir-notfound", "bigdir/nonexistent"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for range b.N {
				f, err := fsys.Open(bc.path)
				if err != nil {
					if !errors.Is(err, fs.ErrNotExist) {
						b.Fatal(err)
					}
					continue
				}
				_ = f.Close()
			}
		})
	}
}
