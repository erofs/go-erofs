package erofs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
)

func TestBasic(t *testing.T) {

	for _, name := range []string{
		"default",
		"chunk-4096",
		"chunk-8192",
		// TODO: Add compressed layout
	} {
		t.Run(name, func(t *testing.T) {
			efs, err := EroFS(loadTestFile(t, "basic-"+name))
			if err != nil {
				t.Fatal(err)
			}

			checkFileString(t, efs, "/in-root.txt", "root file content\n")
			checkFileString(t, efs, "/usr/lib/testdir/emptyfile", "")
			checkFileBytes(t, efs, "/usr/lib/testdir/13k-zeros.raw", bytes.Repeat([]byte{0}, 1024*13))
			checkFileBytes(t, efs, "/usr/lib/testdir/16k-zeros.raw", bytes.Repeat([]byte{0}, 1024*16))
			checkFileBytes(t, efs, "/usr/lib/testdir/5k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*5))
			checkFileBytes(t, efs, "/usr/lib/testdir/16k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*16))
			checkDirectorySize(t, efs, "/usr/lib/testdir/emptydir", 0)
			checkDirectorySize(t, efs, "/usr/lib/testdir/lotsoffiles", 5000)
			checkNotExists(t, efs, "/not-exists.txt")
			checkNotExists(t, efs, "/not-exists/somefile")
			checkNotExists(t, efs, "/usr/lib/testdir/emptydir/somefile")
			checkFileString(t, efs, "/usr/lib/testdir/case/file.txt", "lower case dir\n")
			checkFileString(t, efs, "/usr/lib/testdir/CASE/file.txt", "upper case dir\n")
			checkFileString(t, efs, "/usr/lib/testdir/case.txt", "lower case file\n")
			checkFileString(t, efs, "/usr/lib/testdir/CASE.txt", "upper case file\n")
			checkXattrs(t, efs, "/usr/lib/withxattr", map[string]string{
				"user.custom":      "value1",
				"user.xdg.comment": "some random comment",
			})
			checkXattrs(t, efs, "/usr/lib/withxattr/f1", map[string]string{
				"user.xdg.comment": "comment for f1",
				"user.common":      "same-value",
			})
			checkXattrs(t, efs, "/usr/lib/withxattr/f2", map[string]string{
				"user.xdg.comment": "comment for f2",
				"user.common":      "same-value",
			})
			checkXattrs(t, efs, "/usr/lib/withxattr/f3", map[string]string{
				"user.xdg.comment": "comment for f3",
				"user.common":      "same-value",
			})
			checkXattrs(t, efs, "/usr/lib/withxattr/f4", map[string]string{
				"user.xdg.comment": "comment for f4",
				"user.common":      "same-value",
			})
			// Value is defined in /usr/lib/generated/generate.sh of testdata
			longPrefix := "user.long.prefix.vfvzyrvujoemkjztekxczhyyqpzncyav.xiksvigqpjttnvcvxgaxpnrghppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwd."
			longValue := "value1-ppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwdqfgvgkrgdwcegjpfmelrejllrjkpbwindlfynuzjgvcgygyayjvmtxgsbjkzrydoswbsknrrwjkwzxhasowuzdoxlhbxso"
			checkXattrs(t, efs, "/usr/lib/generated/xattrs/long-prefix-xattrs", map[string]string{
				longPrefix + "long-value": longValue,
				longPrefix + "shortvalue": "y",
			})
			checkXattrs(t, efs, "/usr/lib/generated/xattrs/short-prefix-xattrs", map[string]string{
				"user.short.long-value": longValue,
				"user.short.shortvalue": "y",
			})
			checkDevice(t, efs, "/dev/block0", fs.ModeDevice, 0x00000101)
			checkDevice(t, efs, "/dev/block1", fs.ModeDevice, 0)
			checkDevice(t, efs, "/dev/char0", fs.ModeCharDevice, 0x00000202)
			checkDevice(t, efs, "/dev/char1", fs.ModeCharDevice, 0x00000303)
			checkDevice(t, efs, "/dev/fifo0", fs.ModeNamedPipe, 0)
		})
	}
}

func loadTestFile(t testing.TB, name string) io.ReaderAt {
	t.Helper()
	f, err := os.Open("testdata/" + name + ".erofs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.Close()
	})
	return f
}

func checkFileString(t testing.TB, fsys fs.FS, name, content string) {
	t.Helper()

	f, err := fsys.Open(name)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
		return
	}

	actual := string(b)
	if actual != content {
		t.Errorf("Unexpected content in %s\n\tActual:   %q\n\tExpected: %q", name, actual, content)
	}
}

func checkFileBytes(t testing.TB, fsys fs.FS, name string, content []byte) {
	t.Helper()

	f, err := fsys.Open(name)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
		return
	}

	if !bytes.Equal(b, content) {
		if len(b) != len(content) {
			t.Logf("Unexpected content in %s\n\tActual Len: %d\n\tExpected Len: %d", name, len(b), len(content))
		} else if len(b) < 8192 {
			t.Logf("Unexpected content in %s\n\tActual:   %x\n\tExpected: %x", name, b, content)
		} else {
			t.Logf("Unexpected content in %s\n\tActual:   %x...%x\n\tExpected: %x...%x", name, b[:4096], b[len(b)-4096:], content[:4096], content[len(content)-4096:])
		}
		t.Fail()
	}
}

func checkDevice(t testing.TB, fsys fs.FS, name string, ftype fs.FileMode, rdev uint32) {
	t.Helper()

	f, err := fsys.Open(name)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		t.Error(err)
		return
	}
	st := fi.Sys().(*Stat)
	if st.Mode&fs.ModeType != ftype {
		t.Errorf("Unexpected file type in %s\n\tMode: %x\n\tExpected Type: %x", name, st.Mode, ftype)
		return
	}

	if st.Rdev != rdev {
		t.Errorf("Unexpected rdev in %s\n\tActual Rdev: %x\n\tExpected Rdev: %x", name, st.Rdev, rdev)
		return
	}

}

func checkDirectorySize(t testing.TB, fsys fs.FS, name string, n int) {
	t.Helper()

	entries, err := fs.ReadDir(fsys, name)
	if err != nil {
		t.Error(err)
	}
	if len(entries) != n {
		t.Errorf("Unexpected directory entries in %s: Got %d, expected %d", name, len(entries), n)
	}
}

func checkNotExists(t testing.TB, fsys fs.FS, name string) {
	t.Helper()

	_, err := fsys.Open(name)
	if err == nil {
		t.Errorf("expected error opening %s", name)
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected not exist error opening %s, got %v", name, err)
	}
}

func checkXattrs(t testing.TB, fsys fs.FS, name string, expected map[string]string) {
	t.Helper()

	fi, err := fs.Stat(fsys, name)
	if err != nil {
		t.Error(err)
		return
	}

	st, ok := fi.Sys().(*Stat)
	if !ok {
		t.Errorf("expected *Stat, got %T", fi.Sys())
		return
	}

	if len(st.Xattrs) != len(expected) {
		t.Errorf("Unexpected xattr count for %s: got %d, expected %d", name, len(st.Xattrs), len(expected))
		return
	}

	for k, v := range expected {
		if actual, ok := st.Xattrs[k]; !ok || actual != v {
			if !ok {
				t.Errorf("Missing xattr %q for %s: %v", k, name, st.Xattrs)
			} else {
				t.Errorf("Unexpected xattr %q for %s: got %q, expected %q", k, name, actual, v)
			}
		}
	}
}
