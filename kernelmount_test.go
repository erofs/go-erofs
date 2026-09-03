package erofs_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	erofs "github.com/erofs/go-erofs"
)

// TestKernelMountCompressed builds a compressed (big-pcluster) image with
// the writer, asks the running kernel to mount it via loopback, and verifies
// every file reads back byte-identical. It exercises the on-disk encoding
// against a real kernel driver — something the rest of the Go test suite
// can't do, since it only round-trips through this library's own reader.
//
// Off by default because it requires:
//   - Linux (the only OS with an EROFS driver),
//   - root or passwordless sudo for mount/umount,
//   - the erofs module to be available on the running kernel.
//
// Opt in with:
//
//	EROFS_KERNEL_TESTS=1 go test -run TestKernelMountCompressed ./...
//
// Would have caught the FEATURE_INCOMPAT_BIG_PCLUSTER omission that
// previously shipped only to be fixed in a follow-up commit.
func TestKernelMountCompressed(t *testing.T) {
	if os.Getenv("EROFS_KERNEL_TESTS") == "" {
		t.Skip("set EROFS_KERNEL_TESTS=1 to enable; requires Linux + sudo")
	}
	if runtime.GOOS != "linux" {
		t.Skip("kernel mount tests only run on Linux")
	}

	// Build an image with content spanning the writer's interesting paths:
	// inline-eligible, big-pcluster compressible, partial-tail compressible,
	// incompressible (forces PLAIN fallback).
	files := map[string][]byte{
		"/tiny.txt":     []byte("hi\n"),
		"/compressible": bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog\n"), 1500),
		"/partial-tail": append(bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 4096), []byte("trailer\n")...),
		"/incompressible": func() []byte {
			b := make([]byte, 32*1024)
			for i := range b {
				b[i] = byte(i*1103515245 + 12345)
			}
			return b
		}(),
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.erofs")
	out, err := os.Create(imgPath)
	if err != nil {
		t.Fatal(err)
	}
	w := erofs.Create(out, erofs.WithCompression(erofs.CompressionLZ4))
	for path, data := range files {
		f, err := w.Create(path)
		if err != nil {
			t.Fatalf("Create %s: %v", path, err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatalf("Write %s: %v", path, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close %s: %v", path, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal("Writer.Close:", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	// Mount via the kernel driver. sudo -n fails fast when passwordless
	// sudo isn't configured; skip the test rather than hang prompting.
	mp := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	mountCmd := exec.Command("sudo", "-n", "mount", "-t", "erofs", "-o", "loop", imgPath, mp)
	if mountOut, err := mountCmd.CombinedOutput(); err != nil {
		t.Skipf("kernel mount failed (sudo / loop / erofs module not available?): %v\n%s", err, mountOut)
	}
	t.Cleanup(func() {
		if out, err := exec.Command("sudo", "-n", "umount", mp).CombinedOutput(); err != nil {
			t.Logf("umount failed: %v\n%s", err, out)
		}
	})

	// Verify every file reads back byte-identical.
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(mp, path))
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: content mismatch (got md5=%x, want md5=%x; %d vs %d bytes)",
				path, md5.Sum(got), md5.Sum(want), len(got), len(want))
		}
	}

	t.Logf("verified %d files via kernel mount of %s",
		len(files), fmt.Sprintf("%s (%d bytes)", imgPath, fileSize(t, imgPath)))
}

func fileSize(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
