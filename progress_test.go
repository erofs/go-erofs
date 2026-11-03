package erofs_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	erofs "github.com/erofs/go-erofs"
)

// progressWriter records the Writer's progress on every image write.
type progressWriter struct {
	testBuffer
	w       *erofs.Writer
	samples []erofs.Progress
}

func (p *progressWriter) Write(b []byte) (int, error) {
	p.samples = append(p.samples, p.w.Progress())
	return p.testBuffer.Write(b)
}

func newProgressWriter() (*progressWriter, *erofs.Writer) {
	out := &progressWriter{}
	out.w = erofs.Create(out)
	return out, out.w
}

func checkProgress(t *testing.T, w *erofs.Writer, want erofs.Progress) {
	t.Helper()
	if got := w.Progress(); got != want {
		t.Errorf("Progress() = %+v, want %+v", got, want)
	}
}

// checkProgressComplete checks that after Close, Progress reports every
// entry in the image as written and all counted file data as written.
func checkProgressComplete(t *testing.T, w *erofs.Writer, img fs.FS) {
	t.Helper()
	var inImage int64
	err := fs.WalkDir(img, ".", func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != "." {
			inImage++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk image: %v", err)
	}
	got := w.Progress()
	if got.Entries != inImage || got.EntriesWritten != inImage {
		t.Errorf("after Close: Entries = %d, EntriesWritten = %d, want %d entries in the image", got.Entries, got.EntriesWritten, inImage)
	}
	if got.DataBytesWritten != got.DataBytes {
		t.Errorf("after Close: DataBytesWritten = %d, want DataBytes %d", got.DataBytesWritten, got.DataBytes)
	}
}

func checkBufferComplete(t *testing.T, w *erofs.Writer, b *testBuffer) {
	t.Helper()
	img, err := erofs.Open(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	checkProgressComplete(t, w, img)
}

func createFile(t *testing.T, w *erofs.Writer, name string, size int) {
	t.Helper()
	f, err := w.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// checkMonotonic checks that the samples taken during Close never go
// backwards and never report Close as finished, and returns the number of
// distinct EntriesWritten and DataBytesWritten values.
func checkMonotonic(t *testing.T, samples []erofs.Progress) (entryValues, byteValues int) {
	t.Helper()
	for i, s := range samples {
		if s.EntriesWritten >= s.Entries && s.Entries > 0 {
			t.Fatalf("sample %d: EntriesWritten = %d of %d before Close finished", i, s.EntriesWritten, s.Entries)
		}
		if i == 0 {
			entryValues, byteValues = 1, 1
			continue
		}
		prev := samples[i-1]
		if s.EntriesWritten < prev.EntriesWritten || s.DataBytesWritten < prev.DataBytesWritten {
			t.Fatalf("sample %d went backwards: %+v after %+v", i, s, prev)
		}
		if s.EntriesWritten != prev.EntriesWritten {
			entryValues++
		}
		if s.DataBytesWritten != prev.DataBytesWritten {
			byteValues++
		}
	}
	return entryValues, byteValues
}

func TestProgress(t *testing.T) {
	out, w := newProgressWriter()
	checkProgress(t, w, erofs.Progress{})

	if err := w.Mkdir("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		createFile(t, w, fmt.Sprintf("/dir/f%d", i), 5000)
	}
	// A missing parent directory is created implicitly and counted.
	if err := w.Symlink("/dir/f0", "/implicit/link"); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("/dir/f1", "/hard"); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 14, DataBytes: 50000})

	// Removing a file drops its data. Removing one name of a hard-linked
	// file keeps the data, which the other name still refers to.
	if err := w.Remove("/dir/f9"); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 13, DataBytes: 45000})
	if err := w.Remove("/hard"); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 12, DataBytes: 45000})
	if err := w.RemoveAll("/implicit"); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 10, DataBytes: 45000})

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkBufferComplete(t, w, &out.testBuffer)
	if got := w.Progress().DataBytes; got != 45000 {
		t.Errorf("DataBytes after Close = %d, want 45000", got)
	}
	entryValues, byteValues := checkMonotonic(t, out.samples)
	if entryValues < 5 || byteValues < 5 {
		t.Errorf("saw %d EntriesWritten and %d DataBytesWritten values during Close, want at least 5 of each", entryValues, byteValues)
	}
}

// TestProgressLargeFile checks that DataBytesWritten moves while a single large
// file is copied.
func TestProgressLargeFile(t *testing.T) {
	out, w := newProgressWriter()
	createFile(t, w, "/large", 20<<20)
	if got := w.Progress().DataBytes; got != 20<<20 {
		t.Errorf("DataBytes = %d, want %d", got, 20<<20)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkBufferComplete(t, w, &out.testBuffer)
	// 20 MiB in 8 MiB pieces: the samples before the second and third
	// pieces see the earlier pieces counted.
	if _, byteValues := checkMonotonic(t, out.samples); byteValues < 3 {
		t.Errorf("saw %d DataBytesWritten values, want at least 3", byteValues)
	}
}

// TestProgressConcurrent polls Progress while entries are added and the
// image is written; run it with -race.
func TestProgressConcurrent(t *testing.T) {
	var out testBuffer
	w := erofs.Create(&out)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				w.Progress()
			}
		}
	}()
	for i := 0; i < 100; i++ {
		createFile(t, w, fmt.Sprintf("/f%d", i), 100)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-done
	checkBufferComplete(t, w, &out)
}

// TestProgressCopyFromReplace checks the counts when CopyFrom replaces
// entries, including a directory replaced by a file, which drops the
// directory's contents. Data taken by reference counts toward DataBytes.
func TestProgressCopyFromReplace(t *testing.T) {
	var out testBuffer
	w := erofs.Create(&out)
	if err := w.CopyFrom(fstest.MapFS{
		"d":     {Mode: fs.ModeDir | 0o755},
		"d/x":   {Data: []byte("x")},
		"d/s/y": {Data: []byte("y")},
		"f":     {Data: []byte("f")},
	}); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 5, DataBytes: 3})
	if err := w.CopyFrom(fstest.MapFS{
		"d": {Data: []byte("now a file")},
		"f": {Mode: fs.ModeDir | 0o755},
	}); err != nil {
		t.Fatal(err)
	}
	// "x", "y" and "f" are gone; only "now a file" remains.
	checkProgress(t, w, erofs.Progress{Entries: 2, DataBytes: 10})
	if _, err := w.Stat("/d/x"); err == nil {
		t.Errorf("Stat(/d/x) after /d became a file: got nil error, want an error")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkBufferComplete(t, w, &out)
}

// TestProgressDataFile checks DataBytes when file data goes to a data file.
func TestProgressDataFile(t *testing.T) {
	df, err := os.Create(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = df.Close() }()
	var out testBuffer
	w := erofs.Create(&out, erofs.WithDataFile(df), erofs.WithBlockSize(512))
	createFile(t, w, "/a", 1000)
	if err := w.CopyFrom(fstest.MapFS{"b": {Data: make([]byte, 300)}}); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 2, DataBytes: 1300})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	img, err := erofs.Open(bytes.NewReader(out.Bytes()), erofs.WithExtraDevices(df))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	checkProgressComplete(t, w, img)
}

// TestProgressMetadataPass checks that entries without data blocks, such as
// small files stored inline, are counted while metadata is written, and
// that EntriesWritten stays below Entries until Close finishes.
func TestProgressMetadataPass(t *testing.T) {
	out, w := newProgressWriter()
	// Put the inline files after the last directory in write order, so
	// none of them is counted by a later data block.
	if err := w.Mkdir("/a", 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		createFile(t, w, fmt.Sprintf("/z%03d", i), 5)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkBufferComplete(t, w, &out.testBuffer)
	checkMonotonic(t, out.samples)

	// The last write before the superblock is the metadata. By then the
	// metadata pass has counted most of the inline files.
	if len(out.samples) < 2 {
		t.Fatalf("got %d writes, want at least 2", len(out.samples))
	}
	if got := out.samples[len(out.samples)-1].EntriesWritten; got < 150 {
		t.Errorf("EntriesWritten at superblock write = %d, want most of the 201 entries counted", got)
	}
}

func TestProgressEmpty(t *testing.T) {
	out, w := newProgressWriter()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range out.samples {
		if s.EntriesWritten != 0 {
			t.Fatalf("EntriesWritten = %d during Close of an empty image, want 0", s.EntriesWritten)
		}
	}
	checkBufferComplete(t, w, &out.testBuffer)
}

// TestProgressDataFileRemove checks that in data-file mode, data of a
// removed file is not counted even though it stays in the data file.
func TestProgressDataFileRemove(t *testing.T) {
	df, err := os.Create(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = df.Close() }()
	var out testBuffer
	w := erofs.Create(&out, erofs.WithDataFile(df), erofs.WithBlockSize(512))
	createFile(t, w, "/a", 1000)
	createFile(t, w, "/b", 3000)
	if err := w.Remove("/a"); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 1, DataBytes: 3000})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 1, EntriesWritten: 1, DataBytes: 3000, DataBytesWritten: 3000})
}

// TestProgressRemoveOpenFile checks that data written to a file after it
// was removed is not counted.
func TestProgressRemoveOpenFile(t *testing.T) {
	var out testBuffer
	w := erofs.Create(&out)
	f, err := w.Create("/a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove("/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	createFile(t, w, "/a", 50)
	checkProgress(t, w, erofs.Progress{Entries: 1, DataBytes: 50})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkBufferComplete(t, w, &out)
}

// TestProgressMetadataOnly checks that files from a MetadataOnly CopyFrom,
// whose data never passes through the Writer, are not counted.
func TestProgressMetadataOnly(t *testing.T) {
	var src testBuffer
	sw := erofs.Create(&src)
	createFile(t, sw, "/big", 10000)
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	img, err := erofs.Open(bytes.NewReader(src.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var out testBuffer
	w := erofs.Create(&out)
	if err := w.CopyFrom(img, erofs.MetadataOnly()); err != nil {
		t.Fatal(err)
	}
	createFile(t, w, "/own", 700)
	checkProgress(t, w, erofs.Progress{Entries: 2, DataBytes: 700})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	checkProgress(t, w, erofs.Progress{Entries: 2, EntriesWritten: 2, DataBytes: 700, DataBytesWritten: 700})
}
