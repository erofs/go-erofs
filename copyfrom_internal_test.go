package erofs

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
)

// testImage is an in-memory io.WriteSeeker for internal tests.
type testImage struct {
	buf []byte
	pos int
}

func (b *testImage) Write(p []byte) (int, error) {
	if end := b.pos + len(p); end > len(b.buf) {
		b.buf = append(b.buf, make([]byte, end-len(b.buf))...)
	}
	b.pos += copy(b.buf[b.pos:], p)
	return len(p), nil
}

func (b *testImage) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case 1:
		off += int64(b.pos)
	case 2:
		off += int64(len(b.buf))
	}
	b.pos = int(off)
	return off, nil
}

// TestCopyFromImageReturnsBlocks checks that copying an image with inline
// files returns the blocks it reads to the image's block pool, rather than
// allocating a new block per file.
func TestCopyFromImageReturnsBlocks(t *testing.T) {
	const files = 200
	src := &testImage{}
	w := Create(src)
	for i := range files {
		f, err := w.Create(fmt.Sprintf("/f%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(strings.Repeat("x", 100))); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	fsys, err := Open(bytes.NewReader(src.buf))
	if err != nil {
		t.Fatal(err)
	}
	img := fsys.(*image)
	var allocated atomic.Int64
	newBlock := img.blkPool.New
	img.blkPool.New = func() any {
		allocated.Add(1)
		return newBlock()
	}
	// Keep the pool from being cleared by a GC during the copy.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	cw := Create(&testImage{})
	if err := cw.CopyFrom(img); err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	if n := allocated.Load(); n > 10 {
		t.Errorf("CopyFrom allocated %d blocks for %d inline files, want at most 10", n, files)
	}
}
