package disk

const (
	MagicNumber      = 0xe0f5e1e2
	SuperBlockOffset = 1024

	FeatureIncompatLZ4_0Padding = 0x1
	// FeatureIncompatComprCfgs (also Z_EROFS_FEATURE_INCOMPAT_BIG_PCLUSTER, same
	// bit) marks the presence of compression configuration descriptors after the
	// superblock.
	FeatureIncompatComprCfgs            = 0x2
	FeatureIncompatChunkedFile          = 0x4
	FeatureIncompatDeviceTable          = 0x8
	FeatureIncompatFragments            = 0x20
	FeatureIncompatXattrPrefixes        = 0x40
	FeatureIncompatAll           uint32 = FeatureIncompatLZ4_0Padding |
		FeatureIncompatComprCfgs |
		FeatureIncompatChunkedFile | FeatureIncompatDeviceTable |
		FeatureIncompatFragments | FeatureIncompatXattrPrefixes

	SizeSuperBlock      = 128
	SizeInodeCompact    = 32
	SizeInodeExtended   = 64
	SizeDirent          = 12
	SizeXattrBodyHeader = 12
	SizeXattrEntry      = 4
	SizeDeviceSlot      = 128
	SizeChunkIndex      = 8
	SizeZMapHeader      = 8
	SizeLclusterIndex   = 8
	SizeLZ4Cfgs         = 14

	LayoutFlatPlain         = 0
	LayoutCompressedFull    = 1
	LayoutFlatInline        = 2
	LayoutCompressedCompact = 3
	LayoutChunkBased        = 4

	LayoutChunkFormatBits    = 0x001F
	LayoutChunkFormatIndexes = 0x0020
	LayoutChunkFormat48Bit   = 0x0040

	// ComprAlg* are bits in SuperBlock.ComprAlgs identifying which compression
	// algorithms appear in the image.
	ComprAlgLZ4     = 0x1
	ComprAlgLZMA    = 0x2
	ComprAlgDeflate = 0x4
	ComprAlgZstd    = 0x8

	// ZLclusterType* are the values of the lower 2 bits of
	// z_erofs_lcluster_index.di_advise.
	ZLclusterTypePlain   = 0
	ZLclusterTypeHead1   = 1
	ZLclusterTypeNonhead = 2
	ZLclusterTypeHead2   = 3
	ZLclusterTypeMask    = 0x3

	// ZLclusterPartialRef indicates the lcluster's data may be shared with
	// another inode (dedupe). Unused for read; kept here for completeness.
	ZLclusterPartialRef = 1 << 2

	// ZLclusterD0CBlkCnt: when set in a full-layout NONHEAD's delta[0]
	// (low 16 bits of di_u), the remaining 11 bits encode the physical block
	// count of the surrounding pcluster (big pcluster).
	ZLclusterD0CBlkCnt = 1 << 11

	// ZAdvise* are bits in z_erofs_map_header.h_advise.
	ZAdviseCompacted2B        = 1 << 0
	ZAdviseBigPcluster1       = 1 << 1
	ZAdviseBigPcluster2       = 1 << 2
	ZAdviseInlinePcluster     = 1 << 3
	ZAdviseInterlacedPcluster = 1 << 4
	ZAdviseFragmentPcluster   = 1 << 5
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

// ZMapHeader is the 8-byte z_erofs_map_header that sits after xattrs in a
// compressed inode and describes the cluster index layout that follows.
type ZMapHeader struct {
	Reserved1     uint16
	IdataSize     uint16
	Advise        uint16
	ClusterBits   uint8 // log2(lcluster size) - SuperBlock.BlkSizeBits (low 3 bits)
	AlgorithmType uint8 // low 4 bits: head1 algo; high 4 bits: head2 algo
}

// LclusterIndex is an 8-byte z_erofs_lcluster_index entry in the
// LayoutCompressedFull index map.
//
// For PLAIN/HEAD types, U is the physical block address of the (start of the)
// pcluster.  For NONHEAD types, the low 16 bits of U are delta[0] (distance
// back to the head lcluster) and the high 16 bits are delta[1] (distance
// forward to the next head).
type LclusterIndex struct {
	Advise     uint16
	ClusterOfs uint16
	U          uint32
}

// LZ4Cfgs is the LZ4 entry of the COMPR_CFGS records that follow the
// superblock when FeatureIncompatComprCfgs is set.
type LZ4Cfgs struct {
	MaxDistance     uint16
	MaxPclusterBlks uint16
	Reserved        [10]uint8
}

// DeviceSlot represents the on-disk device table entry (erofs_deviceslot).
// See: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/erofs/erofs_fs.h
type DeviceSlot struct {
	Tag           [64]uint8 // digest(sha256), etc.
	Blocks        uint32    // total fs blocks of this device
	MappedBlkAddr uint32    // map starting at mapped_blkaddr
	Reserved      [56]uint8
}
