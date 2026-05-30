package zerofs

import (
	"errors"
	"testing"

	"github.com/erofs/go-erofs/internal/disk"
)

func TestParseHeaderRejectsUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*[disk.SizeZMapHeader]byte)
	}{
		{"InlinePcluster", func(b *[disk.SizeZMapHeader]byte) {
			// advise bit 3 (ZAdviseInlinePcluster) at byte offset 4 low byte
			b[4] = disk.ZAdviseInlinePcluster
		}},
		{"FragmentPcluster", func(b *[disk.SizeZMapHeader]byte) {
			b[4] = disk.ZAdviseFragmentPcluster
		}},
		{"InterlacedPcluster", func(b *[disk.SizeZMapHeader]byte) {
			b[4] = disk.ZAdviseInterlacedPcluster
		}},
		{"BigPcluster2", func(b *[disk.SizeZMapHeader]byte) {
			b[4] = disk.ZAdviseBigPcluster2
		}},
		{"IdataSize", func(b *[disk.SizeZMapHeader]byte) {
			b[2] = 0x10 // IdataSize low byte
		}},
		{"AlgoLZMA", func(b *[disk.SizeZMapHeader]byte) {
			b[7] = disk.ComprAlgLZMA // algorithm type byte = 0x2 (LZMA in head1)
		}},
		{"AlgoHead2", func(b *[disk.SizeZMapHeader]byte) {
			b[7] = 0x10 // head2 algorithm = 1, head1 = 0
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b [disk.SizeZMapHeader]byte
			tc.mod(&b)
			_, err := ParseHeader(b[:])
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, ErrNotImplemented) {
				t.Fatalf("expected ErrNotImplemented, got %v", err)
			}
		})
	}
}

func TestParseHeaderAcceptsZero(t *testing.T) {
	var b [disk.SizeZMapHeader]byte
	h, err := ParseHeader(b[:])
	if err != nil {
		t.Fatalf("zero header should be accepted, got %v", err)
	}
	if h.Advise != 0 || h.AlgorithmType != 0 {
		t.Fatalf("decoded zero header has non-zero fields: %+v", h)
	}
}

func TestParseHeaderAcceptsBigPcluster1(t *testing.T) {
	// BigPcluster1 is accepted (handled by FindPclusterForOffset).
	var b [disk.SizeZMapHeader]byte
	b[4] = disk.ZAdviseBigPcluster1
	h, err := ParseHeader(b[:])
	if err != nil {
		t.Fatalf("BigPcluster1 should be accepted, got %v", err)
	}
	if h.Advise&disk.ZAdviseBigPcluster1 == 0 {
		t.Fatalf("BigPcluster1 bit lost in decode")
	}
}

func TestParseHeaderShortBuffer(t *testing.T) {
	_, err := ParseHeader(make([]byte, 4))
	if err == nil {
		t.Fatal("expected error on short buffer, got nil")
	}
}
