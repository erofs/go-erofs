// Copyright 2026 The go-erofs Authors.
//
// COMPR_CFGS parsing follows the EROFS on-disk format and the MIT-licensed
// am-fs-erofs implementation (https://github.com/antimatter-studios/rust-fs-erofs),
// Copyright (c) 2026 Antimatter Studios.

package erofs

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/erofs/go-erofs/internal/disk"
)

// maxComprCfgsRecord is the largest per-codec configuration record defined by
// the on-disk format (z_erofs_lzma_cfgs is 14 bytes). The length is a u16, so
// cap absurd values to keep a corrupt image from driving huge allocations.
const maxComprCfgsRecord = 4096

// compressionConfig holds reader-relevant parameters from the COMPR_CFGS
// blob that immediately follows the superblock.
type compressionConfig struct {
	lzma lzmaConfig
}

// readCompressionConfig parses the post-superblock COMPR_CFGS blob.
//
// The blob is a sequence of records in algorithm-ID order:
//
//	__le16 size; u8 payload[size]; padding to 4-byte alignment
//
// One record is present for each algorithm bit set in the superblock's
// available_compr_algs field. Only LZMA carries reader-relevant parameters
// today (its dictionary size); the other records are validated but ignored.
func readCompressionConfig(r io.ReaderAt, sb *disk.SuperBlock) (compressionConfig, error) {
	cfg := compressionConfig{lzma: defaultLZMAConfig()}
	if sb.FeatureIncompat&disk.FeatureIncompatComprCfgs == 0 {
		return cfg, nil
	}

	algos := sb.ComprAlgs
	if algos&^uint16(0xf) != 0 {
		return cfg, fmt.Errorf("unsupported compression algorithm bitmap 0x%x: %w", algos, ErrNotImplemented)
	}

	offset := int64(disk.SuperBlockOffset + disk.SizeSuperBlock + int(sb.ExtSlots)*16)
	for id := uint8(0); id <= disk.CompressionZstd; id++ {
		if algos&(1<<id) == 0 {
			continue
		}
		payload, next, err := readComprCfgsRecord(r, offset)
		if err != nil {
			return cfg, err
		}
		offset = next

		switch id {
		case disk.CompressionLZ4:
			// __le16 max_distance; __le16 max_pclusterblks. The block
			// decoder does not need either value.
			if len(payload) < 4 {
				return cfg, fmt.Errorf("lz4 compression config is %d bytes: %w", len(payload), ErrInvalid)
			}
		case disk.CompressionLZMA:
			if len(payload) < 4 {
				return cfg, fmt.Errorf("lzma compression config is %d bytes: %w", len(payload), ErrInvalid)
			}
			dictSize := binary.LittleEndian.Uint32(payload[0:4])
			if dictSize != 0 {
				cfg.lzma.dictSize = dictSize
			}
			// format is reserved and must be zero for the only LZMA1
			// dialect currently defined.
			if len(payload) >= 6 {
				if format := binary.LittleEndian.Uint16(payload[4:6]); format != 0 {
					return cfg, fmt.Errorf("unsupported lzma format %d: %w", format, ErrNotImplemented)
				}
			}
		case disk.CompressionDeflate:
			if len(payload) < 6 {
				return cfg, fmt.Errorf("deflate compression config is %d bytes: %w", len(payload), ErrInvalid)
			}
			if wb := payload[0]; wb > 15 {
				return cfg, fmt.Errorf("unsupported deflate window bits %d: %w", wb, ErrInvalid)
			}
		case disk.CompressionZstd:
			if len(payload) < 6 {
				return cfg, fmt.Errorf("zstd compression config is %d bytes: %w", len(payload), ErrInvalid)
			}
			// format is currently always 0 (standard Zstandard frame).
			if format := payload[0]; format != 0 {
				return cfg, fmt.Errorf("unsupported zstd format %d: %w", format, ErrNotImplemented)
			}
		}
	}
	return cfg, nil
}

// readComprCfgsRecord reads one length-prefixed metadata record. The next
// record starts at a 4-byte boundary, matching the kernel's
// erofs_read_metadata() behavior.
func readComprCfgsRecord(r io.ReaderAt, offset int64) ([]byte, int64, error) {
	var lenBuf [2]byte
	if _, err := r.ReadAt(lenBuf[:], offset); err != nil {
		return nil, 0, fmt.Errorf("reading compression config length at %d: %w", offset, err)
	}
	size := int(binary.LittleEndian.Uint16(lenBuf[:]))
	if size > maxComprCfgsRecord {
		return nil, 0, fmt.Errorf("compression config record size %d is too large: %w", size, ErrInvalid)
	}
	payload := make([]byte, size)
	if size > 0 {
		if _, err := r.ReadAt(payload, offset+2); err != nil {
			return nil, 0, fmt.Errorf("reading compression config payload at %d: %w", offset+2, err)
		}
	}
	next := offset + 2 + int64(size)
	if rem := next & 3; rem != 0 {
		next += 4 - rem
	}
	return payload, next, nil
}
