package erofs_test

import (
	"bytes"
	"errors"
	"io/fs"
	"path"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/erofs/go-erofs/internal/erofstest"
)

// removeAll is a caller-side recursive remove implemented on top of
// Writer.Remove + fs.ReadDir. It is duplicated in the test rather than
// added to the public API to demonstrate that recursive removal does not
// require a dedicated writer method.
func removeAll(w *erofs.Writer, p string) error {
	entries, err := fs.ReadDir(w, cleanForReadDir(p))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		// Not a directory: fall through to Remove below.
		var pe *fs.PathError
		if !errors.As(err, &pe) {
			return err
		}
	}
	for _, e := range entries {
		if err := removeAll(w, path.Join(p, e.Name())); err != nil {
			return err
		}
	}
	if err := w.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// cleanForReadDir converts an absolute writer path to the form
// fs.ReadDir expects (relative, no leading slash; "." for root).
func cleanForReadDir(p string) string {
	if p == "/" || p == "" {
		return "."
	}
	if len(p) > 0 && p[0] == '/' {
		return p[1:]
	}
	return p
}

// TestWriterRemoveFile verifies Remove deletes a regular file and the
// resulting image contains no trace of it.
func TestWriterRemoveFile(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/keep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("keep\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f2, err := w.Create("/drop.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.Write([]byte("drop\n")); err != nil {
		t.Fatal(err)
	}
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Remove("/drop.txt"); err != nil {
		t.Fatal("Remove:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())

	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal("Open:", err)
	}
	erofstest.CheckFile(t, efs, "keep.txt", "keep\n")
	erofstest.CheckDirEntries(t, efs, ".", []string{"keep.txt"})
}

// TestWriterRemoveEmptyDir verifies that an empty directory can be removed.
func TestWriterRemoveEmptyDir(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.Mkdir("/a", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("/b", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove("/b"); err != nil {
		t.Fatal("Remove empty dir:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckDirEntries(t, efs, ".", []string{"a"})
}

// TestWriterRemoveNonEmptyDirFails verifies that Remove returns an error
// for a directory that still has children.
func TestWriterRemoveNonEmptyDirFails(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.Mkdir("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := w.Create("/dir/child")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	err = w.Remove("/dir")
	if err == nil {
		t.Fatal("expected error removing non-empty directory")
	}
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("error is not *fs.PathError: %T %v", err, err)
	}
}

// TestWriterRemoveMissingReturnsErrNotExist verifies Remove signals
// fs.ErrNotExist for a path that was never added.
func TestWriterRemoveMissingReturnsErrNotExist(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	err := w.Remove("/missing")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Remove missing: got %v, want fs.ErrNotExist", err)
	}
}

// TestWriterRemoveRootFails verifies Remove cannot delete "/".
func TestWriterRemoveRootFails(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)
	if err := w.Remove("/"); err == nil {
		t.Fatal("expected error removing root")
	}
}

// TestWriterRemoveSymlink verifies a symlink can be removed.
func TestWriterRemoveSymlink(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/target")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Symlink("target", "/link"); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove("/link"); err != nil {
		t.Fatal("Remove symlink:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckDirEntries(t, efs, ".", []string{"target"})
}

// TestWriterRemoveHardlinkAlias verifies that removing an alias leaves the
// canonical path intact with the correct nlink.
func TestWriterRemoveHardlinkAlias(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/orig")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hardlink payload\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/orig", "/alias1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/orig", "/alias2"); err != nil {
		t.Fatal(err)
	}
	// Remove one alias.
	if err := w.Remove("/alias1"); err != nil {
		t.Fatal("Remove alias:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckFile(t, efs, "orig", "hardlink payload\n")
	erofstest.CheckFile(t, efs, "alias2", "hardlink payload\n")
	erofstest.CheckDirEntries(t, efs, ".", []string{"alias2", "orig"})

	stOrig := erofstest.Stat(t, efs, "orig")
	stAlias := erofstest.Stat(t, efs, "alias2")
	if stOrig.Ino != stAlias.Ino {
		t.Errorf("Ino mismatch after alias remove: orig=%d alias2=%d", stOrig.Ino, stAlias.Ino)
	}
	if stOrig.Nlink != 2 {
		t.Errorf("orig nlink after alias remove: got %d, want 2", stOrig.Nlink)
	}
}

// TestWriterRemoveHardlinkCanonicalPromotes verifies that removing the
// canonical path of a hardlink group promotes the first surviving alias
// to canonical (POSIX unlink semantics).
func TestWriterRemoveHardlinkCanonicalPromotes(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/orig")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("promote me\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/orig", "/alias1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/orig", "/alias2"); err != nil {
		t.Fatal(err)
	}

	// Remove the canonical entry; data should survive via the aliases.
	if err := w.Remove("/orig"); err != nil {
		t.Fatal("Remove canonical:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckFile(t, efs, "alias1", "promote me\n")
	erofstest.CheckFile(t, efs, "alias2", "promote me\n")
	erofstest.CheckDirEntries(t, efs, ".", []string{"alias1", "alias2"})

	st1 := erofstest.Stat(t, efs, "alias1")
	st2 := erofstest.Stat(t, efs, "alias2")
	if st1.Ino != st2.Ino {
		t.Errorf("Ino mismatch after canonical remove: alias1=%d alias2=%d", st1.Ino, st2.Ino)
	}
	if st1.Nlink != 2 {
		t.Errorf("alias1 nlink: got %d, want 2", st1.Nlink)
	}
}

// TestWriterRemoveHardlinkAllAliases verifies removing every alias of a
// pair drops both paths and the data along with them.
func TestWriterRemoveHardlinkAllAliases(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/orig")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("doomed\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/orig", "/alias"); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove("/orig"); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove("/alias"); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckDirEntries(t, efs, ".", []string{})
}

// TestCallerRecursiveRemove verifies that callers can implement recursive
// removal on top of Writer.Remove + fs.ReadDir without a dedicated writer
// method.
func TestCallerRecursiveRemove(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.Mkdir("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("/dir/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/dir/a", "/dir/b", "/dir/sub/c"} {
		f, err := w.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(p + "\n")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Mkdir("/other", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := w.Create("/other/keep")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := removeAll(w, "/dir"); err != nil {
		t.Fatal("removeAll:", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckDirEntries(t, efs, ".", []string{"other"})
	erofstest.CheckDirEntries(t, efs, "other", []string{"keep"})
}

// TestCallerRecursiveRemoveMissing verifies the caller-side recursive
// pattern is a no-op on a path that does not exist.
func TestCallerRecursiveRemoveMissing(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)
	if err := removeAll(w, "/does/not/exist"); err != nil {
		t.Fatalf("removeAll missing: got %v, want nil", err)
	}
}

// TestCallerRecursiveRemoveHardlinkInside verifies that the caller-side
// recursive pattern over a subtree containing a hardlink alias correctly
// updates the canonical entry's link count when the alias is removed.
func TestCallerRecursiveRemoveHardlinkInside(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	if err := w.Mkdir("/keep", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := w.Create("/keep/orig")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("payload\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("/scratch", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/keep/orig", "/scratch/alias"); err != nil {
		t.Fatal(err)
	}

	if err := removeAll(w, "/scratch"); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	erofstest.CheckFile(t, efs, "keep/orig", "payload\n")
	st := erofstest.Stat(t, efs, "keep/orig")
	if st.Nlink != 1 {
		t.Errorf("keep/orig nlink after scratch removal: got %d, want 1", st.Nlink)
	}
}

// TestFileChtimes verifies that *File.Chtimes sets the per-entry
// timestamps that are then visible after the image is finalised.
func TestFileChtimes(t *testing.T) {
	var buf testBuffer
	w := erofs.Create(&buf)

	f, err := w.Create("/timed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("when\n")); err != nil {
		t.Fatal(err)
	}
	wantMtime := time.Unix(1_600_000_000, 123_456_789)
	wantAtime := time.Unix(1_700_000_000, 987_654_321)
	if err := f.Chtimes(wantAtime, wantMtime); err != nil {
		t.Fatal("Chtimes:", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	erofstest.FsckErofsBytes(t, buf.Bytes())
	efs, err := erofs.Open(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	st := erofstest.Stat(t, efs, "timed.txt")
	if uint64(wantMtime.Unix()) != st.Mtime {
		t.Errorf("Mtime: got %d, want %d", st.Mtime, wantMtime.Unix())
	}
	if uint32(wantMtime.Nanosecond()) != st.MtimeNs {
		t.Errorf("MtimeNs: got %d, want %d", st.MtimeNs, wantMtime.Nanosecond())
	}
}
