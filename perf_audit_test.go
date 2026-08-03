package erofs

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"testing"
)

// countingReaderAt counts ReadAt calls to quantify syscall amplification.
type countingReaderAt struct {
	ra    io.ReaderAt
	calls int
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.calls++

	return c.ra.ReadAt(p, off)
}

func buildPerfImage(tb testing.TB, size int) []byte {
	tb.Helper()

	out := &seekBuf{}
	w := Create(out, WithBuildTime(1000, 0))
	f, err := w.Create("/big")
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte("x"), size)); err != nil {
		tb.Fatal(err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}

	return out.buf
}

func TestPerfReadCallCount(t *testing.T) {
	const size = 8 << 20
	buf := buildPerfImage(t, size)

	c := &countingReaderAt{ra: bytes.NewReader(buf)}
	img, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	fh, err := img.Open("big")
	if err != nil {
		t.Fatal(err)
	}
	c.calls = 0
	n, err := io.Copy(&countingWriter{}, fh)
	if err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	t.Logf("io.Copy of %d bytes: %d ReadAt calls", n, c.calls)

	// A single Read with a large buffer must not fan out into per-block reads.
	fh2, err := img.Open("big")
	if err != nil {
		t.Fatal(err)
	}
	c.calls = 0
	dst := make([]byte, size)
	got, rerr := fh2.Read(dst)
	if rerr != nil && rerr != io.EOF {
		t.Fatal(rerr)
	}
	_ = fh2.Close()
	t.Logf("single Read of %d bytes: %d ReadAt calls", got, c.calls)
	if c.calls > 4 {
		t.Errorf("one Read of the whole file took %d ReadAt calls; want a small constant", c.calls)
	}
}

// countingWriter accepts everything without implementing io.ReaderFrom, so
// io.Copy uses the source's WriteTo rather than its own buffering.
type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))

	return len(p), nil
}

func BenchmarkPerfSequentialRead(b *testing.B) {
	const size = 8 << 20
	tmp, err := os.CreateTemp(b.TempDir(), "img")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := tmp.Write(buildPerfImage(b, size)); err != nil {
		b.Fatal(err)
	}
	img, err := Open(tmp)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		fh, err := img.Open("big")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, fh); err != nil {
			b.Fatal(err)
		}
		_ = fh.Close()
	}
}

func buildWideImage(tb testing.TB, nfiles int) fs.FS {
	tb.Helper()

	out := &seekBuf{}
	w := Create(out, WithBuildTime(1000, 0))
	if err := w.Mkdir("/d", 0o755); err != nil {
		tb.Fatal(err)
	}
	for i := range nfiles {
		name := "/d/f" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26))
		f, err := w.Create(name)
		if err != nil {
			continue
		}
		if _, err := f.Write([]byte("x")); err != nil {
			tb.Fatal(err)
		}
		if err := f.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	img, err := Open(bytes.NewReader(out.buf))
	if err != nil {
		tb.Fatal(err)
	}

	return img
}

// BenchmarkPerfLookup exercises path resolution: one inode decode plus a
// binary search over dirents per component.
func BenchmarkPerfLookup(b *testing.B) {
	img := buildWideImage(b, 500)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := fs.Stat(img, "d/faaa"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfReadDir exercises dirent decoding across a wide directory.
func BenchmarkPerfReadDir(b *testing.B) {
	img := buildWideImage(b, 500)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := fs.ReadDir(img, "d"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfStatInode isolates inode decoding.
func BenchmarkPerfStatInode(b *testing.B) {
	img := buildWideImage(b, 8).(*image)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f := &file{img: img, nid: uint64(img.sb.RootNid)}
		if _, err := f.readInfo(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfWriteTree covers the writer's layout and serialization work
// across many directories.
func BenchmarkPerfWriteTree(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		out := &seekBuf{}
		w := Create(out, WithBuildTime(1000, 0))
		for d := range 20 {
			dir := "/d" + string(rune('a'+d))
			if err := w.Mkdir(dir, 0o755); err != nil {
				b.Fatal(err)
			}
			for j := range 100 {
				f, err := w.Create(dir + "/f" + string(rune('a'+j%26)) + string(rune('a'+j/26)))
				if err != nil {
					continue
				}
				if _, err := f.Write([]byte("x")); err != nil {
					b.Fatal(err)
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfWriteWideDirs isolates the layout and serialization work that
// scales with directory width. Entries carry no data, so the spool I/O that
// dominates BenchmarkPerfWriteTree stays out of the way.
func BenchmarkPerfWriteWideDirs(b *testing.B) {
	names := make([]string, 0, 4000)
	for d := range 20 {
		for j := range 200 {
			names = append(names, "/d"+string(rune('a'+d))+
				"/f"+string(rune('a'+j%26))+string(rune('a'+(j/26)%26))+string(rune('a'+(j/676)%26)))
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		w := Create(&seekBuf{}, WithBuildTime(1000, 0))
		for _, n := range names {
			if err := w.Symlink("t", n); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
