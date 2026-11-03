package erofs_test

import (
	"bytes"
	"io/fs"
	"testing"

	erofs "github.com/erofs/go-erofs"
)

// TestCreateHole verifies that CreateHole writes a regular-file inode carrying
// the requested size but no data: the image holds no bytes for it, a read
// returns that many zeros, and — the property the method exists for — the
// inode's i_size is the size asked for, not zero.
//
// The zero-length-metadata-inode failure this guards against is subtle: an
// overlayfs metacopy copy-up sizes the data it copies by the lower inode's
// i_size, so a metadata file written with no size copies up zero bytes and
// silently drops its redirected content. A hole of the true size is what makes
// a composefs-style metadata image round-trip, so the metadata, mode and
// overlay xattrs are all exercised here alongside the size.
func TestCreateHole(t *testing.T) {
	const (
		holeSize = 149                 // arbitrary; must survive to i_size
		redirect = "/aa/bb/ccddeeff01" // a composefs overlay redirect target
	)

	var buf testBuffer
	fsys := erofs.Create(&buf)
	if err := fsys.CreateHole("/meta.txt", holeSize); err != nil {
		t.Fatalf("CreateHole: %v", err)
	}
	// The composefs use: the hole carries the overlay redirect and metacopy
	// marker and a real mode, all set after creation exactly as for a file
	// created any other way.
	if err := fsys.Chmod("/meta.txt", 0o640); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	if err := fsys.Setxattr("/meta.txt", "trusted.overlay.redirect", redirect); err != nil {
		t.Fatalf("Setxattr redirect: %v", err)
	}
	if err := fsys.Setxattr("/meta.txt", "trusted.overlay.metacopy", ""); err != nil {
		t.Fatalf("Setxattr metacopy: %v", err)
	}
	// A zero-size hole is a valid empty file.
	if err := fsys.CreateHole("/empty", 0); err != nil {
		t.Fatalf("CreateHole(0): %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	fi, err := fs.Stat(efs, "meta.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// The whole point: the size survives to i_size.
	if fi.Size() != holeSize {
		t.Errorf("hole Size = %d, want %d", fi.Size(), holeSize)
	}
	if got := fi.Mode().Perm(); got != 0o640 {
		t.Errorf("hole mode = %o, want %o", got, 0o640)
	}
	if fi.IsDir() {
		t.Error("hole IsDir = true, want regular file")
	}

	// The overlay xattrs round-trip.
	xg, ok := fi.(interface {
		GetXattr(string) (string, bool)
	})
	if !ok {
		t.Fatal("FileInfo does not expose GetXattr")
	}
	if v, ok := xg.GetXattr("trusted.overlay.redirect"); !ok || v != redirect {
		t.Errorf("redirect xattr = %q (ok=%v), want %q", v, ok, redirect)
	}
	if v, ok := xg.GetXattr("trusted.overlay.metacopy"); !ok || v != "" {
		t.Errorf("metacopy xattr = %q (ok=%v), want empty", v, ok)
	}

	// The image holds no data for the hole, so a read returns holeSize zeros.
	got, err := fs.ReadFile(efs, "meta.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(got) != holeSize {
		t.Errorf("ReadFile returned %d bytes, want %d", len(got), holeSize)
	}
	if !bytes.Equal(got, make([]byte, holeSize)) {
		t.Errorf("hole content is not all zeros: %x", got)
	}

	// The zero-size hole is a valid empty regular file.
	efi, err := fs.Stat(efs, "empty")
	if err != nil {
		t.Fatalf("Stat empty: %v", err)
	}
	if efi.Size() != 0 {
		t.Errorf("empty hole Size = %d, want 0", efi.Size())
	}
}

// TestCreateHoleErrors covers CreateHole's argument refusals.
func TestCreateHoleErrors(t *testing.T) {
	var buf testBuffer
	fsys := erofs.Create(&buf)
	if err := fsys.CreateHole("/", 10); err == nil {
		t.Error("CreateHole at root: got nil error, want a refusal")
	}
	if err := fsys.CreateHole("/neg", -1); err == nil {
		t.Error("CreateHole with negative size: got nil error, want a refusal")
	}
}
