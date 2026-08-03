package erofs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

// newTestImage builds a bare image whose backing data is exactly the given
// bytes, for exercising the block loader directly.
func newTestImage(data []byte, blkBits uint8) *image {
	img := &image{meta: bytes.NewReader(data)}
	img.sb.BlkSizeBits = blkBits
	img.blkPool.New = func() any {
		return &block{buf: make([]byte, 1<<blkBits)}
	}

	return img
}

// TestLoadAtShortReadAtEOF covers a whole-block read whose block runs past the
// end of the image. Callers ask for a full block even when the structure they
// want is smaller, so treating the short read as fatal made anything in the
// final block unreadable — which is exactly where mkfs.erofs puts the shared
// xattr area of a small image.
func TestLoadAtShortReadAtEOF(t *testing.T) {
	const blkBits = 12
	const blkSize = 1 << blkBits

	// An image far shorter than one block.
	data := bytes.Repeat([]byte("z"), 100)
	img := newTestImage(data, blkBits)

	blk, err := img.loadAt(0, blkSize)
	if err != nil {
		t.Fatalf("loadAt(0, %d) on a %d byte image: %v", blkSize, len(data), err)
	}
	got := blk.bytes()
	if len(got) != len(data) {
		t.Errorf("got %d bytes, want %d", len(got), len(data))
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
	img.putBlock(blk)

	// A partial read starting mid-image is still served.
	blk, err = img.loadAt(60, blkSize)
	if err != nil {
		t.Fatalf("loadAt(60, %d): %v", blkSize, err)
	}
	if n := len(blk.bytes()); n != 40 {
		t.Errorf("got %d bytes from offset 60, want 40", n)
	}
	img.putBlock(blk)
}

// TestLoadAtBeyondEOF covers reads with nothing to return, which must still
// fail rather than hand back an empty block.
func TestLoadAtBeyondEOF(t *testing.T) {
	img := newTestImage(bytes.Repeat([]byte("z"), 100), 12)

	if _, err := img.loadAt(100, 4096); err == nil {
		t.Error("loadAt at EOF succeeded; want an error")
	} else if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v; want io.EOF", err)
	}

	if _, err := img.loadAt(5000, 4096); err == nil {
		t.Error("loadAt past EOF succeeded; want an error")
	}

	if _, err := img.loadAt(0, 0); err == nil {
		t.Error("loadAt with size 0 succeeded; want an error")
	}
}

// TestReadReferenceImage reads an image built by the reference mkfs.erofs.
// Small images put the shared xattr area in the final block, so this is the
// end-to-end case that used to fail outright with an EOF error.
func TestReadReferenceImage(t *testing.T) {
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not available")
	}

	for _, blockSize := range []string{"4096", "16384"} {
		t.Run(blockSize, func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "src")
			if err := os.MkdirAll(filepath.Join(src, "dir"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(src, "dir", "b.txt"), []byte("world\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("a.txt", filepath.Join(src, "link")); err != nil {
				t.Fatal(err)
			}

			imgPath := filepath.Join(dir, "img.erofs")
			cmd := exec.Command("mkfs.erofs", "--quiet", "-b", blockSize, imgPath, src)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Skipf("mkfs.erofs -b %s failed: %v\n%s", blockSize, err, out)
			}

			f, err := os.Open(imgPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()

			img, err := Open(f)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			// Walking touches every inode, and Info() on each pulls in the
			// xattr area, shared entries included.
			var seen []string
			err = fs.WalkDir(img, ".", func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if _, err := d.Info(); err != nil {
					return err
				}
				seen = append(seen, p)

				return nil
			})
			if err != nil {
				t.Fatalf("WalkDir over a reference image: %v", err)
			}

			for _, want := range []string{".", "a.txt", "dir", "dir/b.txt", "link"} {
				if !slices.Contains(seen, want) {
					t.Errorf("%q missing from walk; saw %v", want, seen)
				}
			}

			// Content must survive too.
			got, err := fs.ReadFile(img, "dir/b.txt")
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(got) != "world\n" {
				t.Errorf("dir/b.txt = %q, want %q", got, "world\n")
			}

			target, err := img.(interface {
				ReadLink(string) (string, error)
			}).ReadLink("link")
			if err != nil {
				t.Fatalf("ReadLink: %v", err)
			}
			if target != "a.txt" {
				t.Errorf("link -> %q, want %q", target, "a.txt")
			}
		})
	}
}
