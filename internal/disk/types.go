package disk

const (
	MagicNumber      = 0xe0f5e1e2
	SuperBlockOffset = 1024

	FeatureIncompatLZ4_0Padding = 0x1
	// FeatureIncompatComprCfgs covers both the per-algorithm compression
	// configuration area (placed right after the superblock) and the
	// "big pcluster" capability — the kernel uses 0x02 for both.
	FeatureIncompatComprCfgs            = 0x2
	FeatureIncompatBigPcluster          = 0x2
	FeatureIncompatChunkedFile          = 0x4
	FeatureIncompatDeviceTable          = 0x8
	FeatureIncompatFragments            = 0x20
	FeatureIncompatXattrPrefixes        = 0x40
	FeatureIncompatAll           uint32 = FeatureIncompatLZ4_0Padding |
		FeatureIncompatComprCfgs | FeatureIncompatChunkedFile |
		FeatureIncompatDeviceTable | FeatureIncompatFragments |
		FeatureIncompatXattrPrefixes

	SizeSuperBlock      = 128
	SizeInodeCompact    = 32
	SizeInodeExtended   = 64
	SizeDirent          = 12
	SizeXattrBodyHeader = 12
	SizeXattrEntry      = 4
	SizeDeviceSlot      = 128
	SizeChunkIndex      = 8

	LayoutFlatPlain         = 0
	LayoutCompressedFull    = 1
	LayoutFlatInline        = 2
	LayoutCompressedCompact = 3
	LayoutChunkBased        = 4

	LayoutChunkFormatBits    = 0x001F
	LayoutChunkFormatIndexes = 0x0020
	LayoutChunkFormat48Bit   = 0x0040

	// Compression algorithm identifiers stored in ZErofsMapHeader.AlgorithmType
	// and indexed as bit positions in SuperBlock.ComprAlgs.
	ZErofsCompressionLZ4     = 0
	ZErofsCompressionLZMA    = 1
	ZErofsCompressionDeflate = 2
	ZErofsCompressionZstd    = 3
	ZErofsCompressionMax     = 4

	// Bit flags in ZErofsMapHeader.HAdvise.
	ZErofsAdviseCompacted2B        = 0x0001
	ZErofsAdviseBigPcluster1       = 0x0002
	ZErofsAdviseBigPcluster2       = 0x0004
	ZErofsAdviseInlinePcluster     = 0x0008
	ZErofsAdviseInterlacedPcluster = 0x0010
	ZErofsAdviseFragmentPcluster   = 0x0020

	// Logical cluster types in the low 2 bits of ZErofsLclusterIndex.DiAdvise.
	ZErofsLclusterTypePlain   = 0
	ZErofsLclusterTypeHead1   = 1
	ZErofsLclusterTypeNonhead = 2
	ZErofsLclusterTypeHead2   = 3
	ZErofsLclusterTypeMask    = 0x3

	// Additional flags packed into ZErofsLclusterIndex.DiAdvise above the
	// 2-bit type. PartialRef marks the trailing lcluster of a partial-ref
	// pcluster; D0CblkCnt marks a NONHEAD whose di_u carries the block count
	// of the preceding pcluster instead of a delta.
	ZErofsLiPartialRef = 1 << 15
	ZErofsLiD0CblkCnt  = 1 << 11

	SizeZErofsMapHeader     = 8
	SizeZErofsLclusterIndex = 8
	SizeZErofsLZ4Cfgs       = 14 // struct z_erofs_lz4_cfgs (excludes 2-byte length prefix)

	// MaxPclusterSize is the upper bound on a single pcluster's on-disk
	// size in bytes, matching the kernel's Z_EROFS_PCLUSTER_MAX_SIZE
	// (fs/erofs/erofs_fs.h). Used to bound buffer allocations on the read
	// path so a corrupted (or malicious) lcluster index can't drive a
	// huge allocation. 1 MiB is the kernel's absolute cap regardless of
	// block size.
	MaxPclusterSize = 1024 * 1024
)

// SuperBlock represents the EROFS on-disk superblock.
// See: https://docs.kernel.org/filesystems/erofs.html#on-disk-layout
type SuperBlock struct {
	MagicNumber      uint32
	Checksum         uint32
	FeatureCompat    uint32
	BlkSizeBits      uint8
	ExtSlots         uint8
	RootNid          uint16
	Inos             uint64
	BuildTime        uint64
	BuildTimeNs      uint32
	Blocks           uint32
	MetaBlkAddr      uint32
	XattrBlkAddr     uint32
	UUID             [16]uint8
	VolumeName       [16]uint8
	FeatureIncompat  uint32
	ComprAlgs        uint16
	ExtraDevices     uint16
	DevtSlotOff      uint16
	DirBlkBits       uint8
	XattrPrefixCount uint8
	XattrPrefixStart uint32
	PackedNid        uint64 // Nid of the special "packed" inode for shared data/prefixes
	XattrFilterRes   uint8
	Reserved         [23]uint8
}

// InodeCompact represents the 32-byte on-disk compact inode.
type InodeCompact struct {
	Format     uint16 // i_format
	XattrCount uint16 // i_xattr_icount
	Mode       uint16 // i_mode
	Nlink      uint16 // i_nlink
	Size       uint32 // i_size
	Reserved   uint32 // i_reserved
	InodeData  uint32 // i_u (i_raw_blkaddr, i_rdev, etc.)
	Inode      uint32 // i_ino
	UID        uint16 // i_uid
	GID        uint16 // i_gid
	Reserved2  uint32 // i_reserved2
}

// InodeExtended represents the 64-byte on-disk extended inode.
type InodeExtended struct {
	Format     uint16 // i_format
	XattrCount uint16 // i_xattr_icount
	Mode       uint16 // i_mode
	Reserved   uint16 // i_reserved
	Size       uint64 // i_size
	InodeData  uint32 // i_u (i_raw_blkaddr, i_rdev, etc.)
	Inode      uint32 // i_ino
	UID        uint32 // i_uid
	GID        uint32 // i_gid
	Mtime      uint64 // i_mtime
	MtimeNs    uint32 // i_mtime_nsec
	Nlink      uint32 // i_nlink
	Reserved2  [16]uint8
}

type Dirent struct {
	Nid      uint64
	NameOff  uint16
	FileType uint8
	Reserved uint8
}

// XattrHeader is the header after an inode containing xattr information
//
// Original definition:
// inline xattrs (n == i_xattr_icount):
// erofs_xattr_ibody_header(1) + (n - 1) * 4 bytes
//
//	12 bytes           /                   \
//	                  /                     \
//	                 /-----------------------\
//	                 |  erofs_xattr_entries+ |
//	                 +-----------------------+
//
// inline xattrs must starts in erofs_xattr_ibody_header,
// for read-only fs, no need to introduce h_refcount
// Actual name is prefix | long prefix (prefix + infix) + name
type XattrHeader struct {
	NameFilter  uint32 // bit value 1 indicate not-present
	SharedCount uint8
	Reserved    [7]uint8
}

type XattrEntry struct {
	NameLen   uint8  // length of name
	NameIndex uint8  // index of name in XattrHeader, 0x80 set indicates long prefix at index&0x7F + XattrPrefixStart
	ValueLen  uint16 // length of value
	// Name+Value
}

type XattrLongPrefixitem struct {
	PrefixAddr uint32 // address of the long prefix
	PrefixLen  uint8  // length of the long prefix
}

type XattrLongPrefix struct {
	BaseIndex uint8 // short xattr name prefix index
	// Infix part after short prefix
}

type InodeChunkIndex struct {
	StartBlkHi uint16 // part of 48-bit support (not yet implemented)
	DeviceID   uint16
	StartBlkLo uint32
}

// ZErofsMapHeader is the 8-byte z_erofs_map_header that immediately follows
// the inode core + xattr area for compressed inodes and precedes the
// lcluster index table.
//
// On disk the first 4 bytes are a union of h_fragmentoff (for fragment
// inodes), h_reserved1+h_idata_size (for inline-pcluster inodes), and
// h_extents_lo. The remaining 4 bytes hold h_advise plus either
// h_algorithmtype+h_clusterbits or h_extents_hi.
type ZErofsMapHeader struct {
	FragmentOff   uint32 // overloaded with h_idata_size in high 16 bits
	HAdvise       uint16
	AlgorithmType uint8 // overloaded as low byte of h_extents_hi
	ClusterBits   uint8 // overloaded as high byte of h_extents_hi
}

// ZErofsLclusterIndex is one full-format (8-byte) logical cluster index entry.
// The low ZErofsLclusterTypeMask bits of DiAdvise select the cluster type.
// For HEAD1/HEAD2/PLAIN clusters DiU is a block address; for NONHEAD it is
// two little-endian uint16 deltas (delta[0]=distance back to head pcluster,
// delta[1]=distance forward to the next head).
type ZErofsLclusterIndex struct {
	DiAdvise     uint16
	DiClusterOfs uint16
	DiU          uint32
}

// DeviceSlot represents the on-disk device table entry (erofs_deviceslot).
// See: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/erofs/erofs_fs.h
type DeviceSlot struct {
	Tag           [64]uint8 // digest(sha256), etc.
	Blocks        uint32    // total fs blocks of this device
	MappedBlkAddr uint32    // map starting at mapped_blkaddr
	Reserved      [56]uint8
}
