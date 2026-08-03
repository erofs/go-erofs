package disk

import "encoding/binary"

const (
	MagicNumber      = 0xe0f5e1e2
	SuperBlockOffset = 1024

	FeatureIncompatLZ4_0Padding         = 0x1
	FeatureIncompatChunkedFile          = 0x4
	FeatureIncompatDeviceTable          = 0x8
	FeatureIncompatFragments            = 0x20
	FeatureIncompatXattrPrefixes        = 0x40
	FeatureIncompatAll           uint32 = FeatureIncompatLZ4_0Padding |
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

	LayoutFlatPlain         = 0
	LayoutCompressedFull    = 1
	LayoutFlatInline        = 2
	LayoutCompressedCompact = 3
	LayoutChunkBased        = 4

	LayoutChunkFormatBits    = 0x001F
	LayoutChunkFormatIndexes = 0x0020
	LayoutChunkFormat48Bit   = 0x0040
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

// DeviceSlot represents the on-disk device table entry (erofs_deviceslot).
// See: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/erofs/erofs_fs.h
type DeviceSlot struct {
	Tag           [64]uint8 // digest(sha256), etc.
	Blocks        uint32    // total fs blocks of this device
	MappedBlkAddr uint32    // map starting at mapped_blkaddr
	Reserved      [56]uint8
}

// The Unmarshal methods below decode the fixed on-disk layouts by hand.
// binary.Decode reaches the same result via reflection, which costs roughly
// two orders of magnitude more per call — enough to dominate path lookup and
// directory reads, where one of these runs per inode and per dirent.
// Offsets mirror the struct definitions above.

// Unmarshal decodes a 32-byte compact inode. b must be at least
// SizeInodeCompact bytes.
func (i *InodeCompact) Unmarshal(b []byte) {
	_ = b[SizeInodeCompact-1] // bounds check once
	i.Format = binary.LittleEndian.Uint16(b[0:2])
	i.XattrCount = binary.LittleEndian.Uint16(b[2:4])
	i.Mode = binary.LittleEndian.Uint16(b[4:6])
	i.Nlink = binary.LittleEndian.Uint16(b[6:8])
	i.Size = binary.LittleEndian.Uint32(b[8:12])
	i.Reserved = binary.LittleEndian.Uint32(b[12:16])
	i.InodeData = binary.LittleEndian.Uint32(b[16:20])
	i.Inode = binary.LittleEndian.Uint32(b[20:24])
	i.UID = binary.LittleEndian.Uint16(b[24:26])
	i.GID = binary.LittleEndian.Uint16(b[26:28])
	i.Reserved2 = binary.LittleEndian.Uint32(b[28:32])
}

// Unmarshal decodes a 64-byte extended inode. b must be at least
// SizeInodeExtended bytes.
func (i *InodeExtended) Unmarshal(b []byte) {
	_ = b[SizeInodeExtended-1] // bounds check once
	i.Format = binary.LittleEndian.Uint16(b[0:2])
	i.XattrCount = binary.LittleEndian.Uint16(b[2:4])
	i.Mode = binary.LittleEndian.Uint16(b[4:6])
	i.Reserved = binary.LittleEndian.Uint16(b[6:8])
	i.Size = binary.LittleEndian.Uint64(b[8:16])
	i.InodeData = binary.LittleEndian.Uint32(b[16:20])
	i.Inode = binary.LittleEndian.Uint32(b[20:24])
	i.UID = binary.LittleEndian.Uint32(b[24:28])
	i.GID = binary.LittleEndian.Uint32(b[28:32])
	i.Mtime = binary.LittleEndian.Uint64(b[32:40])
	i.MtimeNs = binary.LittleEndian.Uint32(b[40:44])
	i.Nlink = binary.LittleEndian.Uint32(b[44:48])
	copy(i.Reserved2[:], b[48:64])
}

// Unmarshal decodes a 12-byte dirent. b must be at least SizeDirent bytes.
func (d *Dirent) Unmarshal(b []byte) {
	_ = b[SizeDirent-1] // bounds check once
	d.Nid = binary.LittleEndian.Uint64(b[0:8])
	d.NameOff = binary.LittleEndian.Uint16(b[8:10])
	d.FileType = b[10]
	d.Reserved = b[11]
}

// Unmarshal decodes the 12-byte inline xattr header. b must be at least
// SizeXattrBodyHeader bytes.
func (h *XattrHeader) Unmarshal(b []byte) {
	_ = b[SizeXattrBodyHeader-1] // bounds check once
	h.NameFilter = binary.LittleEndian.Uint32(b[0:4])
	h.SharedCount = b[4]
	copy(h.Reserved[:], b[5:12])
}

// Unmarshal decodes a 4-byte xattr entry header. b must be at least
// SizeXattrEntry bytes.
func (e *XattrEntry) Unmarshal(b []byte) {
	_ = b[SizeXattrEntry-1] // bounds check once
	e.NameLen = b[0]
	e.NameIndex = b[1]
	e.ValueLen = binary.LittleEndian.Uint16(b[2:4])
}
