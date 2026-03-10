package erofs

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

	"github.com/erofs/go-erofs/internal/erofstest"
	"github.com/erofs/go-erofs/internal/tartest"
)

const (
	longPrefix = "user.long.prefix.vfvzyrvujoemkjztekxczhyyqpzncyav.xiksvigqpjttnvcvxgaxpnrghppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwd."
	longValue  = "value1-ppufylkopprkdsfncibznsvmbicfknlkbnuntpuqmwffxkrnuhtpucxwllkxrfzmbvmdcluahylidncngjrxnlipwikplkxgfpiiiqtzsnigpcojpkxtzbzqcosttdxhtspbxltuezcakskakmskmaznvpwcqjakbyapaglwdqfgvgkrgdwcegjpfmelrejllrjkpbwindlfynuzjgvcgygyayjvmtxgsbjkzrydoswbsknrrwjkwzxhasowuzdoxlhbxso"
)

func TestBasic(t *testing.T) {
	hasXattrPrefix, err := erofstest.CheckMkfsVersion("1.9")
	if err != nil {
		t.Skipf("skipping: %v", err)
	}

	minChunk := os.Getpagesize()
	for _, tc := range []struct {
		name string
		opts []createOpt
	}{
		{"default", nil},
		{fmt.Sprintf("chunk-%d", minChunk), []createOpt{withChunkSize(minChunk)}},
		{fmt.Sprintf("chunk-%d", minChunk*2), []createOpt{withChunkSize(minChunk * 2)}},
		{"chunk-index", []createOpt{withBlobDev}},
		// TODO: Add compressed layout
	} {
		t.Run(tc.name, func(t *testing.T) {
			copts := tc.opts
			if hasXattrPrefix {
				copts = append(copts, withXattrPrefix)
			}
			er, eopts := createTestFile(t, tc.name, copts...)
			efs, err := EroFS(er, eopts...)
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
			checkXattrs(t, efs, "/usr/lib/generated/xattrs/long-prefix-xattrs", map[string]string{
				longPrefix + "long-value": longValue,
				longPrefix + "shortvalue": "y",
			})
			checkXattrs(t, efs, "/usr/lib/generated/xattrs/short-prefix-xattrs", map[string]string{
				"user.short.long-value": longValue,
				"user.short.shortvalue": "y",
			})
			checkDevice(t, efs, "/dev/block0", fs.ModeDevice, 1)
			checkDevice(t, efs, "/dev/block1", fs.ModeDevice, 0)
			checkDevice(t, efs, "/dev/char0", fs.ModeCharDevice, 2)
			checkDevice(t, efs, "/dev/char1", fs.ModeCharDevice, 3)
			checkDevice(t, efs, "/dev/fifo0", fs.ModeNamedPipe, 0)
		})
	}
}

type createOptions struct {
	chunkSize   int
	blobDev     bool
	xattrPrefix bool
}

type createOpt func(*createOptions)

func withChunkSize(chunkSize int) createOpt {
	return func(o *createOptions) {
		o.chunkSize = chunkSize
	}
}

func withBlobDev(o *createOptions) {
	o.blobDev = true
}

func withXattrPrefix(o *createOptions) {
	o.xattrPrefix = true
}

func createTestFile(t testing.TB, name string, opts ...createOpt) (io.ReaderAt, []Opt) {
	t.Helper()

	var (
		options  createOptions
		mkfsArgs []string
	)
	for _, opt := range opts {
		opt(&options)
	}
	if options.chunkSize != 0 {
		mkfsArgs = append(mkfsArgs, fmt.Sprintf("--chunksize=%d", options.chunkSize))
	}

	if options.xattrPrefix {
		mkfsArgs = append(mkfsArgs, "--xattr-prefix=user.short")
		mkfsArgs = append(mkfsArgs, fmt.Sprintf("--xattr-prefix=%s", longPrefix))
	}

	tc := tartest.TarContext{}.WithModTime(time.Now().UTC())

	lotsOfFilesC := make(chan tartest.WriterToTar)
	go func() {
		for i := range 5000 {
			lotsOfFilesC <- tc.File(fmt.Sprintf("/usr/lib/testdir/lotsoffiles/%d", i), []byte{}, 0600)
		}
		close(lotsOfFilesC)
	}()

	writerTo := tartest.TarAll(
		tc.File("/in-root.txt", []byte("root file content\n"), 0600),
		tc.File("/usr/lib/testdir/emptyfile", []byte{}, 0600),
		tc.File("/usr/lib/testdir/13k-zeros.raw", bytes.Repeat([]byte{0}, 1024*13), 0600),
		tc.File("/usr/lib/testdir/16k-zeros.raw", bytes.Repeat([]byte{0}, 1024*16), 0600),
		tc.File("/usr/lib/testdir/5k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*5), 0600),
		tc.File("/usr/lib/testdir/16k-sequence.raw", bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 128*16), 0600),
		tc.Dir("/usr/lib/testdir/emptydir", 0600),
		tc.File("/usr/lib/testdir/case/file.txt", []byte("lower case dir\n"), 0600),
		tc.File("/usr/lib/testdir/CASE/file.txt", []byte("upper case dir\n"), 0600),
		tc.File("/usr/lib/testdir/case.txt", []byte("lower case file\n"), 0600),
		tc.File("/usr/lib/testdir/CASE.txt", []byte("upper case file\n"), 0600),
		tc.WithXattrs(map[string]string{
			"user.custom":      "value1",
			"user.xdg.comment": "some random comment",
		}).Dir("/usr/lib/withxattr", 0600),
		tc.WithXattrs(map[string]string{
			"user.xdg.comment": "comment for f1",
			"user.common":      "same-value",
		}).File("/usr/lib/withxattr/f1", []byte{}, 0600),
		tc.WithXattrs(map[string]string{
			"user.xdg.comment": "comment for f2",
			"user.common":      "same-value",
		}).File("/usr/lib/withxattr/f2", []byte{}, 0600),
		tc.WithXattrs(map[string]string{
			"user.xdg.comment": "comment for f3",
			"user.common":      "same-value",
		}).File("/usr/lib/withxattr/f3", []byte{}, 0600),
		tc.WithXattrs(map[string]string{
			"user.xdg.comment": "comment for f4",
			"user.common":      "same-value",
		}).File("/usr/lib/withxattr/f4", []byte{}, 0600),
		tc.WithXattrs(map[string]string{
			longPrefix + "long-value": longValue,
			longPrefix + "shortvalue": "y",
		}).File("/usr/lib/generated/xattrs/long-prefix-xattrs", []byte{}, 0600),
		tc.WithXattrs(map[string]string{
			"user.short.long-value": longValue,
			"user.short.shortvalue": "y",
		}).File("/usr/lib/generated/xattrs/short-prefix-xattrs", []byte{}, 0600),
		tc.Device("/dev/block0", fs.ModeDevice, 0, 1),
		tc.Device("/dev/block1", fs.ModeDevice, 0, 0),
		tc.Device("/dev/char0", fs.ModeCharDevice, 0, 2),
		tc.Device("/dev/char1", fs.ModeCharDevice, 0, 3),
		tc.Device("/dev/fifo0", fs.ModeNamedPipe, 0, 0),
	)

	writerTo = tartest.TarAll(
		writerTo,
		tartest.TarStream(lotsOfFilesC),
	)

	td := t.TempDir()
	path := filepath.Join(td, name+".erofs")
	blobDataPath := ""
	if options.blobDev {
		blobDataPath = filepath.Join(td, name+"-data.erofs")
		mkfsArgs = append(mkfsArgs, fmt.Sprintf("--blobdev=%s", blobDataPath))
		if options.chunkSize == 0 {
			mkfsArgs = append(mkfsArgs, fmt.Sprintf("--chunksize=%d", os.Getpagesize()))
		}
	}

	err := tartest.ConvertTarErofs(context.Background(), tartest.TarFromWriterTo(writerTo), path, "", mkfsArgs)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var eOpts []Opt
	if blobDataPath != "" {
		bf, err := os.Open(blobDataPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { bf.Close() })

		eOpts = append(eOpts, WithExtraDevices(bf))
	}
	t.Cleanup(func() { f.Close() })
	return f, eOpts
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
