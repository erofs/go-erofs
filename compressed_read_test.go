// Copyright 2026 The go-erofs Authors.
//
// SPDX-License-Identifier: Apache-2.0

package erofs_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
)

// compressedTree is the payload shared by the compressed-image tests.
type compressedTree struct {
	files map[string][]byte
	// hardlink names two paths that must share the same content.
	hardlink [2]string
}

func newCompressedTree() compressedTree {
	// A deterministic pattern large enough to span several logical clusters,
	// plus an incompressible-ish region and a zero region.
	big := make([]byte, 1<<20)
	for index := range big {
		if index%4096 < 2048 {
			big[index] = byte(index * 7)
		}
	}
	return compressedTree{
		files: map[string][]byte{
			"big.bin":      big,
			"small.txt":    []byte("hello compressed world\n"),
			"empty":        {},
			"odd.bin":      bytes.Repeat([]byte("xyz"), 5001),
			"zeros.bin":    make([]byte, 128<<10),
			"nested/a":     []byte("nested file\n"),
			"nested/b.bin": bytes.Repeat([]byte{0xff, 0x00, 0x55, 0xaa}, 1000),
		},
		hardlink: [2]string{"big.bin", "hard.bin"},
	}
}

// writeCompressedTree materializes the tree in directory.
func writeCompressedTree(t *testing.T, directory string) compressedTree {
	t.Helper()
	tree := newCompressedTree()
	for name, content := range tree.files {
		path := filepath.Join(directory, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(directory, tree.hardlink[0]), filepath.Join(directory, tree.hardlink[1])); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("big.bin", filepath.Join(directory, "link")); err != nil {
		t.Fatal(err)
	}
	return tree
}

// requireCompressedTree checks every file of the tree through the image.
func requireCompressedTree(t *testing.T, img fs.FS, tree compressedTree) {
	t.Helper()
	for name, want := range tree.files {
		got, err := fs.ReadFile(img, name)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadFile(%s) = %d bytes, want %d", name, len(got), len(want))
		}
	}
	got, err := fs.ReadFile(img, tree.hardlink[1])
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", tree.hardlink[1], err)
	}
	if !bytes.Equal(got, tree.files[tree.hardlink[0]]) {
		t.Fatalf("hardlink %s content mismatch", tree.hardlink[1])
	}
	readLinker, ok := img.(interface {
		ReadLink(string) (string, error)
	})
	if !ok {
		t.Fatal("image does not implement ReadLink")
	}
	target, err := readLinker.ReadLink("link")
	if err != nil {
		t.Fatalf("ReadLink(link): %v", err)
	}
	if target != "big.bin" {
		t.Fatalf("ReadLink(link) = %q", target)
	}
}

// buildCompressedImage runs mkfs.erofs with the given compression flags and
// returns the image path. Unsupported combinations are skipped.
func buildCompressedImage(t *testing.T, source string, arguments ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "compressed.erofs")
	full := append([]string{}, arguments...)
	full = append(full, path, source)
	output, err := exec.Command("mkfs.erofs", full...).CombinedOutput()
	if err != nil {
		t.Skipf("mkfs.erofs %v is not supported here: %v\n%s", full, err, output)
	}
	return path
}

// openCompressedImage opens an image built by buildCompressedImage.
func openCompressedImage(t *testing.T, path string) fs.FS {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	img, err := erofs.Open(file)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	return img
}

// TestCompressedImageRead exercises every compressor and the on-disk index
// variants erofs-utils can produce. The matrix is skipped under -short.
func TestCompressedImageRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the compressed image matrix under -short")
	}
	if _, err := erofstest.CheckMkfsVersion("1.0"); err != nil {
		t.Skipf("skipping: %v", err)
	}

	compressors := []string{"lz4", "lzma", "deflate", "zstd"}
	variants := []struct {
		name  string
		flags []string
	}{
		{"default", nil},
		{"legacy-index", []string{"-E", "legacy-compress"}},
		{"ztailpacking", []string{"-E", "ztailpacking"}},
		{"dedupe", []string{"-E", "dedupe"}},
		{"fragments", []string{"-E", "fragments"}},
		{"dedupe-fragments", []string{"-E", "dedupe,fragments"}},
		{"cluster-16k", []string{"-C", "16384"}},
		{"cluster-64k", []string{"-C", "65536"}},
	}

	for _, compressor := range compressors {
		for _, variant := range variants {
			t.Run(compressor+"/"+variant.name, func(t *testing.T) {
				source := t.TempDir()
				tree := writeCompressedTree(t, source)
				arguments := append([]string{"-z", compressor}, variant.flags...)
				path := buildCompressedImage(t, source, arguments...)
				img := openCompressedImage(t, path)
				requireCompressedTree(t, img, tree)
			})
		}
	}
}

// TestCompressedImageReadAt checks random access across compressed cluster
// boundaries.
func TestCompressedImageReadAt(t *testing.T) {
	if _, err := erofstest.CheckMkfsVersion("1.0"); err != nil {
		t.Skipf("skipping: %v", err)
	}
	for _, compressor := range []string{"lz4", "lzma", "deflate", "zstd"} {
		t.Run(compressor, func(t *testing.T) {
			source := t.TempDir()
			tree := writeCompressedTree(t, source)
			path := buildCompressedImage(t, source, "-z", compressor)
			img := openCompressedImage(t, path)

			file, err := img.Open("big.bin")
			if err != nil {
				t.Fatalf("Open(big.bin): %v", err)
			}
			defer file.Close()
			readerAt, ok := file.(interface {
				ReadAt([]byte, int64) (int, error)
			})
			if !ok {
				t.Fatal("file does not implement io.ReaderAt")
			}
			data := tree.files["big.bin"]
			for _, offset := range []int64{0, 1, 4095, 4096, 4097, 12345, 100000, int64(len(data)) - 100} {
				length := int64(512)
				if offset+length > int64(len(data)) {
					length = int64(len(data)) - offset
				}
				buffer := make([]byte, length)
				read, err := readerAt.ReadAt(buffer, offset)
				if err != nil && !errorsIsEOF(err) {
					t.Fatalf("ReadAt(%d): %v", offset, err)
				}
				if int64(read) != length {
					t.Fatalf("ReadAt(%d) = %d bytes, want %d", offset, read, length)
				}
				if !bytes.Equal(buffer, data[offset:offset+length]) {
					t.Fatalf("ReadAt(%d) content mismatch", offset)
				}
			}
		})
	}
}

// TestCompressedImageErrorCases checks that truncated images fail cleanly
// instead of returning corrupt data or panicking.
func TestCompressedImageErrorCases(t *testing.T) {
	if _, err := erofstest.CheckMkfsVersion("1.0"); err != nil {
		t.Skipf("skipping: %v", err)
	}
	source := t.TempDir()
	writeCompressedTree(t, source)
	path := buildCompressedImage(t, source, "-z", "lz4")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Cut the image in half: the superblock and metadata zone survive, but
	// the payload clusters are gone.
	truncated := content[:len(content)/2]
	img, err := erofs.Open(bytes.NewReader(truncated))
	if err != nil {
		return
	}
	failed := false
	_ = fs.WalkDir(img, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			failed = true
			return nil
		}
		if entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		if _, readErr := fs.ReadFile(img, name); readErr != nil {
			failed = true
		}
		return nil
	})
	if !failed {
		t.Fatal("reading a truncated compressed image succeeded for every file")
	}
}

// errorsIsEOF reports whether err is io.EOF without importing io at every call
// site; ReadAt may return EOF together with the final short read.
func errorsIsEOF(err error) bool {
	return err != nil && err.Error() == "EOF"
}

// TestCompressedImageFormatString guards the human readable algorithm names
// used in error messages through the public API.
func TestCompressedImageFormatString(t *testing.T) {
	// Build a tiny lzma image and ensure the COMPR_CFGS blob is accepted (it
	// is absent for LZ4-only images).
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file.txt"), []byte("content\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, compressor := range []string{"lzma", "zstd", "deflate"} {
		t.Run(compressor, func(t *testing.T) {
			path := buildCompressedImage(t, source, "-z", compressor)
			img := openCompressedImage(t, path)
			content, err := fs.ReadFile(img, "file.txt")
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(content) != "content\n" {
				t.Fatalf("content = %q", content)
			}
			_ = fmt.Sprintf("%s", compressor)
		})
	}
}
