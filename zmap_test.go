// Copyright 2026 The go-erofs Authors.
//
// SPDX-License-Identifier: Apache-2.0

package erofs

import (
	"encoding/binary"
	"errors"
	"testing"
)

// encodeCompactEntry writes one compacted-index entry into pack at idx,
// mirroring the kernel's bit layout: type in the two bits above lobits and the
// low address bits in the lobits below.
func encodeCompactEntry(pack []byte, p compactPack, idx int, typ uint8, low uint32) {
	bitPos := p.encodeBits * idx
	bytePos := bitPos / 8
	shift := bitPos % 8
	value := uint32(typ)<<p.lobits | (low & (1<<p.lobits - 1))
	window := binary.LittleEndian.Uint32(pack[bytePos : bytePos+4])
	window |= value << shift
	binary.LittleEndian.PutUint32(pack[bytePos:bytePos+4], window)
}

// TestDecodeCompactEntry checks the compacted-2B (16 entries, 14 bits each)
// and compacted-4B (2 entries, 16 bits each) bit decoding.
func TestDecodeCompactEntry(t *testing.T) {
	cases := []struct {
		name string
		pack compactPack
	}{
		{"compacted-2b", compactPack{packBytes: 32, vcnt: 16, encodeBits: 14, lobits: 12}},
		{"compacted-4b", compactPack{packBytes: 8, vcnt: 2, encodeBits: 16, lobits: 12}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			pack := make([]byte, testCase.pack.packBytes)
			for index := 0; index < testCase.pack.vcnt; index++ {
				typ := uint8(index % 4)
				low := uint32(0x100 + index)
				encodeCompactEntry(pack, testCase.pack, index, typ, low)
				gotType, gotLow, err := decodeCompactEntry(pack, testCase.pack, index)
				if err != nil {
					t.Fatalf("decodeCompactEntry(%d): %v", index, err)
				}
				if gotType != typ || gotLow != low {
					t.Fatalf("entry %d = type %d low %#x, want type %d low %#x", index, gotType, gotLow, typ, low)
				}
			}
		})
	}
}

// TestDecodeCompactEntryErrors checks the range validation.
func TestDecodeCompactEntryErrors(t *testing.T) {
	pack := make([]byte, 8)
	geometry := compactPack{packBytes: 8, vcnt: 2, encodeBits: 16, lobits: 12}
	if _, _, err := decodeCompactEntry(pack, geometry, 2); !errors.Is(err, ErrInvalid) {
		t.Errorf("out-of-range index error = %v, want ErrInvalid", err)
	}
	if _, _, err := decodeCompactEntry(pack, geometry, -1); !errors.Is(err, ErrInvalid) {
		t.Errorf("negative index error = %v, want ErrInvalid", err)
	}
	short := make([]byte, 2)
	if _, _, err := decodeCompactEntry(short, geometry, 1); !errors.Is(err, ErrInvalid) {
		t.Errorf("short pack error = %v, want ErrInvalid", err)
	}
}

// TestCompactPackGeometry checks the pack sizes derived from the lcluster
// exponent.
func TestCompactPackGeometry(t *testing.T) {
	z := &zmap{zLclusterBits: 12}
	pack, err := z.compactPack(2)
	if err != nil {
		t.Fatal(err)
	}
	if pack.packBytes != 8 || pack.vcnt != 2 || pack.encodeBits != 16 || pack.lobits != 12 {
		t.Errorf("compacted-4B geometry = %+v", pack)
	}
	pack, err = z.compactPack(1)
	if err != nil {
		t.Fatal(err)
	}
	if pack.packBytes != 32 || pack.vcnt != 16 || pack.encodeBits != 14 || pack.lobits != 12 {
		t.Errorf("compacted-2B geometry = %+v", pack)
	}
	if _, err := z.compactPack(0); !errors.Is(err, ErrInvalid) {
		t.Errorf("invalid amortized shift error = %v, want ErrInvalid", err)
	}
	tooLarge := &zmap{zLclusterBits: 15}
	if _, err := tooLarge.compactPack(2); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("oversized lcluster bits error = %v, want ErrNotImplemented", err)
	}
}

// TestCompactLocation checks the byte offsets and intra-pack indices of the
// three compacted regions: 4B initial, 2B middle, 4B trailer.
func TestCompactLocation(t *testing.T) {
	z := &zmap{
		zLclusterBits:    12,
		ebase:            1000,
		compact4bInitial: 4,
		compact2b:        8,
	}
	cases := []struct {
		lcn       uint64
		packStart int64
		packBytes int64
		index     int
	}{
		{0, 1000, 8, 0},
		{1, 1000, 8, 1},
		{3, 1008, 8, 1},
		{4, 992, 32, 12},
		{5, 992, 32, 13},
		{11, 1024, 32, 3},
		{12, 1032, 8, 0},
		{13, 1032, 8, 1},
	}
	for _, testCase := range cases {
		start, pack, index, err := z.compactLocation(testCase.lcn)
		if err != nil {
			t.Fatalf("compactLocation(%d): %v", testCase.lcn, err)
		}
		if start != testCase.packStart || pack.packBytes != testCase.packBytes || index != testCase.index {
			t.Errorf("compactLocation(%d) = (%d, %d-byte pack, %d), want (%d, %d-byte pack, %d)",
				testCase.lcn, start, pack.packBytes, index, testCase.packStart, testCase.packBytes, testCase.index)
		}
	}
}

// TestZmapHeaderIDataSize checks the union accessor used for fragment tails.
func TestZmapHeaderIDataSize(t *testing.T) {
	header := zmapHeader{fragmentOffOrIData: 0x00420000}
	if got := header.idataSize(); got != 0x42 {
		t.Errorf("idataSize() = %#x, want 0x42", got)
	}
}

// TestZclusterEntryIsHeadOrPlain checks the tail classification.
func TestZclusterEntryIsHeadOrPlain(t *testing.T) {
	if !(zclusterEntry{typ: zErofsLclusterTypePlain}.isHeadOrPlain()) {
		t.Error("plain lcluster not classified as head/plain")
	}
	if !(zclusterEntry{typ: zErofsLclusterTypeHead1}.isHeadOrPlain()) {
		t.Error("HEAD1 lcluster not classified as head/plain")
	}
	if (zclusterEntry{typ: zErofsLclusterTypeNonhead}).isHeadOrPlain() {
		t.Error("nonhead lcluster classified as head/plain")
	}
}
