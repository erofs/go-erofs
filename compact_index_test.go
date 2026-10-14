package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/erofs/go-erofs/internal/builder"
	"github.com/erofs/go-erofs/internal/disk"
)

func TestCompactChunkIndex(t *testing.T) {
	for _, tt := range []struct {
		name   string
		block  uint64
		device uint16
		want   uint32
		fail   bool
	}{
		{"first", 0, 0, 0, false},
		{"last", 0xfffffffe, 0, 0xfffffffe, false},
		{"hole", builder.NullPhysicalBlock, 0, 0xffffffff, false},
		{"reserved", 0xffffffff, 0, 0, true},
		{"wide", 1 << 32, 0, 0, true},
		{"external", 1, 1, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			w := &erofsWriter{compactIndexes: true}
			n, err := w.writeChunkIndex(&out, tt.block, tt.device)
			if tt.fail {
				if err == nil || n != 0 || out.Len() != 0 {
					t.Fatalf("invalid address emitted: n=%d err=%v", n, err)
				}
				return
			}
			if err != nil || n != 4 || out.Len() != 4 {
				t.Fatalf("n=%d err=%v", n, err)
			}
			if got := binary.LittleEndian.Uint32(out.Bytes()); got != tt.want {
				t.Fatalf("index=%#x want=%#x", got, tt.want)
			}
		})
	}
}

func TestChunkIndexAlignment(t *testing.T) {
	for _, compact := range []bool{false, true} {
		w := &erofsWriter{blockSize: 4096, compactIndexes: compact}
		for _, xattrs := range []int{0, 12, 16, 20, 24} {
			e := &erofsEntry{mode: disk.StatTypeReg, size: 4097, layout: disk.LayoutChunkBased, xattrSize: xattrs}
			e.trailingSize = w.calcTrailingSize(e)
			pad := w.chunkIndexPad(e)
			if e.trailingSize != 2*w.chunkIndexSize() || (inodeCoreSize(e)+xattrs+pad)%w.chunkIndexSize() != 0 || pad >= w.chunkIndexSize() {
				t.Fatalf("compact=%v xattrs=%d pad=%d size=%d", compact, xattrs, pad, e.trailingSize)
			}
		}
	}
}

type shortIndexWriter struct{}

func (shortIndexWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestChunkIndexShortWrite(t *testing.T) {
	for _, compact := range []bool{false, true} {
		w := &erofsWriter{compactIndexes: compact}
		if _, err := w.writeChunkIndex(shortIndexWriter{}, 1, 0); !errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
	}
}

func TestWideChunkIndex(t *testing.T) {
	w := &erofsWriter{}
	var out bytes.Buffer
	if _, err := w.writeChunkIndex(&out, 1<<32|7, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), []byte{1, 0, 2, 0, 7, 0, 0, 0}) {
		t.Fatalf("index=%x", out.Bytes())
	}
	for _, block := range []uint64{(1 << 48) - 1, 1 << 48} {
		if _, err := w.writeChunkIndex(io.Discard, block, 0); err == nil {
			t.Fatalf("accepted %#x", block)
		}
	}
}
