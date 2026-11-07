// Copyright 2026 The go-erofs Authors.
//
// SPDX-License-Identifier: Apache-2.0

package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/erofs/go-erofs/internal/disk"
)

// comprCfgsBlob builds a COMPR_CFGS metadata blob: a sequence of
// length-prefixed records, each padded to a 4-byte boundary.
func comprCfgsBlob(records ...[]byte) []byte {
	var blob bytes.Buffer
	for _, record := range records {
		var length [2]byte
		binary.LittleEndian.PutUint16(length[:], uint16(len(record)))
		blob.Write(length[:])
		blob.Write(record)
		for blob.Len()%4 != 0 {
			blob.WriteByte(0)
		}
	}
	return blob.Bytes()
}

// imageWithComprCfgs lays out a superblock plus COMPR_CFGS blob the way
// readCompressionConfig expects to find them.
func imageWithComprCfgs(superblock disk.SuperBlock, blob []byte) []byte {
	image := make([]byte, disk.SuperBlockOffset+disk.SizeSuperBlock+len(blob))
	copy(image[disk.SuperBlockOffset+disk.SizeSuperBlock:], blob)
	return image
}

// TestReadComprCfgsRecord checks the length-prefixed record reader and its
// 4-byte alignment.
func TestReadComprCfgsRecord(t *testing.T) {
	first := []byte{1, 0, 0, 0}
	second := []byte{9, 8}
	blob := comprCfgsBlob(first, second)
	reader := bytes.NewReader(blob)

	payload, next, err := readComprCfgsRecord(reader, 0)
	if err != nil {
		t.Fatalf("first record: %v", err)
	}
	if !bytes.Equal(payload, first) || next != 8 {
		t.Errorf("first record = %v next %d, want %v next 8", payload, next, first)
	}
	payload, next, err = readComprCfgsRecord(reader, next)
	if err != nil {
		t.Fatalf("second record: %v", err)
	}
	if !bytes.Equal(payload, second) || next != 12 {
		t.Errorf("second record = %v next %d, want %v next 12", payload, next, second)
	}

	// A record length above the sanity cap is rejected.
	oversized := make([]byte, 2)
	binary.LittleEndian.PutUint16(oversized, maxComprCfgsRecord+1)
	if _, _, err := readComprCfgsRecord(bytes.NewReader(oversized), 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("oversized record error = %v, want ErrInvalid", err)
	}
	if _, _, err := readComprCfgsRecord(bytes.NewReader(nil), 0); err == nil {
		t.Error("truncated record read did not fail")
	}
}

// lzmaRecord builds a z_erofs_lzma_cfgs payload.
func lzmaRecord(dictSize uint32, format uint16) []byte {
	record := make([]byte, 14)
	binary.LittleEndian.PutUint32(record[0:4], dictSize)
	binary.LittleEndian.PutUint16(record[4:6], format)
	return record
}

// TestReadCompressionConfig checks the per-algorithm config parsing.
func TestReadCompressionConfig(t *testing.T) {
	const expectedDict = 1 << 20

	t.Run("no-feature", func(t *testing.T) {
		superblock := disk.SuperBlock{}
		cfg, err := readCompressionConfig(bytes.NewReader(nil), &superblock)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.lzma.dictSize != expectedDict {
			t.Errorf("dictSize = %d, want default %d", cfg.lzma.dictSize, expectedDict)
		}
	})

	t.Run("lzma-only", func(t *testing.T) {
		superblock := disk.SuperBlock{
			FeatureIncompat: disk.FeatureIncompatComprCfgs,
			ComprAlgs:       1 << disk.CompressionLZMA,
		}
		image := imageWithComprCfgs(superblock, comprCfgsBlob(lzmaRecord(1<<23, 0)))
		cfg, err := readCompressionConfig(bytes.NewReader(image), &superblock)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.lzma.dictSize != 1<<23 {
			t.Errorf("dictSize = %d, want %d", cfg.lzma.dictSize, 1<<23)
		}
	})

	t.Run("lz4-and-lzma", func(t *testing.T) {
		superblock := disk.SuperBlock{
			FeatureIncompat: disk.FeatureIncompatComprCfgs,
			ComprAlgs:       (1 << disk.CompressionLZ4) | (1 << disk.CompressionLZMA),
		}
		lz4 := make([]byte, 4) // max_distance, max_pclusterblks
		image := imageWithComprCfgs(superblock, comprCfgsBlob(lz4, lzmaRecord(1<<22, 0)))
		cfg, err := readCompressionConfig(bytes.NewReader(image), &superblock)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.lzma.dictSize != 1<<22 {
			t.Errorf("dictSize = %d, want %d", cfg.lzma.dictSize, 1<<22)
		}
	})

	t.Run("unsupported-bitmap", func(t *testing.T) {
		superblock := disk.SuperBlock{
			FeatureIncompat: disk.FeatureIncompatComprCfgs,
			ComprAlgs:       0x10,
		}
		if _, err := readCompressionConfig(bytes.NewReader(nil), &superblock); !errors.Is(err, ErrNotImplemented) {
			t.Errorf("unsupported bitmap error = %v, want ErrNotImplemented", err)
		}
	})

	t.Run("unsupported-lzma-format", func(t *testing.T) {
		superblock := disk.SuperBlock{
			FeatureIncompat: disk.FeatureIncompatComprCfgs,
			ComprAlgs:       1 << disk.CompressionLZMA,
		}
		image := imageWithComprCfgs(superblock, comprCfgsBlob(lzmaRecord(1<<20, 2)))
		if _, err := readCompressionConfig(bytes.NewReader(image), &superblock); !errors.Is(err, ErrNotImplemented) {
			t.Errorf("lzma format error = %v, want ErrNotImplemented", err)
		}
	})

	t.Run("invalid-deflate-window", func(t *testing.T) {
		superblock := disk.SuperBlock{
			FeatureIncompat: disk.FeatureIncompatComprCfgs,
			ComprAlgs:       1 << disk.CompressionDeflate,
		}
		deflate := make([]byte, 6)
		deflate[0] = 16 // window bits above 15
		image := imageWithComprCfgs(superblock, comprCfgsBlob(deflate))
		if _, err := readCompressionConfig(bytes.NewReader(image), &superblock); !errors.Is(err, ErrInvalid) {
			t.Errorf("deflate window error = %v, want ErrInvalid", err)
		}
	})
}
