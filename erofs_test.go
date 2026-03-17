package erofs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
		{"LargeFiles", erofstest.LargeFiles, nil},
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

	// Compression format is unimplemented — verify EroFS returns ErrNotImplemented.
	t.Run("lz4-unimplemented", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("mkfs.erofs compression is not included on Windows")
		}
		// Create a compressed erofs image using mkfs.erofs -zlz4.
		tc := erofstest.TarContext{}
		wt := erofstest.TarAll(
			tc.File("/file.txt", []byte("content\n"), 0644),
		)
		tarStream := erofstest.TarFromWriterTo(wt)
		defer tarStream.Close()

		path := filepath.Join(t.TempDir(), "compressed.erofs")
		if err := erofstest.ConvertTarErofs(context.Background(), tarStream, path, "", []string{"-zlz4"}); err != nil {
			t.Fatal(err)
		}

		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		_, err = erofs.EroFS(f)
		if !errors.Is(err, erofs.ErrNotImplemented) {
			t.Fatalf("expected ErrNotImplemented, got %v", err)
		}
	})
}
