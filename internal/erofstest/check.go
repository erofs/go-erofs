package erofstest

import (
	"io/fs"
	"testing"

	"github.com/erofs/go-erofs"
)

// CheckXattrs verifies that the named path has exactly the expected xattrs.
func CheckXattrs(t testing.TB, fsys fs.FS, name string, expected map[string]string) {
	t.Helper()

	fi, err := fs.Stat(fsys, name)
	if err != nil {
		t.Errorf("stat %s: %v", name, err)
		return
	}

	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		t.Errorf("%s: expected *erofs.Stat from Sys(), got %T", name, fi.Sys())
		return
	}

	if len(st.Xattrs) != len(expected) {
		t.Errorf("%s: xattr count %d, want %d", name, len(st.Xattrs), len(expected))
		return
	}

	for k, v := range expected {
		if actual, ok := st.Xattrs[k]; !ok {
			t.Errorf("%s: missing xattr %q: %v", name, k, st.Xattrs)
		} else if actual != v {
			t.Errorf("%s: xattr %q: got %q, want %q", name, k, actual, v)
		}
	}
}

// CheckDevice verifies that the named path is a device/fifo with the expected
// type and rdev.
func CheckDevice(t testing.TB, fsys fs.FS, name string, ftype fs.FileMode, rdev uint32) {
	t.Helper()

	f, err := fsys.Open(name)
	if err != nil {
		t.Errorf("open %s: %v", name, err)
		return
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		t.Errorf("stat %s: %v", name, err)
		return
	}

	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		t.Errorf("%s: expected *erofs.Stat from Sys(), got %T", name, fi.Sys())
		return
	}

	if st.Mode&fs.ModeType != ftype {
		t.Errorf("%s: type %v, want %v", name, st.Mode&fs.ModeType, ftype)
	}
	if st.Rdev != rdev {
		t.Errorf("%s: rdev %d, want %d", name, st.Rdev, rdev)
	}
}

// Stat returns the *erofs.Stat for the named path, or calls t.Fatal if it
// cannot be obtained.
func Stat(t testing.TB, fsys fs.FS, name string) *erofs.Stat {
	t.Helper()

	fi, err := fs.Stat(fsys, name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}

	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		t.Fatalf("%s: expected *erofs.Stat from Sys(), got %T", name, fi.Sys())
	}
	return st
}

// lstatFS is the interface for Lstat only, without requiring ReadLink.
type lstatFS interface {
	Lstat(name string) (fs.FileInfo, error)
}

// Lstat returns the *erofs.Stat for the named path without following symlinks,
// or calls t.Fatal if it cannot be obtained.
func Lstat(t testing.TB, fsys fs.FS, name string) *erofs.Stat {
	t.Helper()

	lfs, ok := fsys.(lstatFS)
	if !ok {
		t.Fatalf("FS does not implement Lstat")
	}

	fi, err := lfs.Lstat(name)
	if err != nil {
		t.Fatalf("lstat %s: %v", name, err)
	}

	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		t.Fatalf("%s: expected *erofs.Stat from Sys(), got %T", name, fi.Sys())
	}
	return st
}
