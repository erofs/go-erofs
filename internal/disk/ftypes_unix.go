package disk

import (
	"golang.org/x/sys/unix"
)

func RdevFromMode(mode uint16, inodeData uint32) uint64 {
	switch mode & StatTypeMask {
	case StatTypeChrdev, StatTypeBlkdev, StatTypeFifo, StatTypeSock:
		// inodeData field is device number for some file types
		major := (inodeData & 0xfff00) >> 8
		minor := (inodeData & 0xff) | ((inodeData >> 12) & 0xfff00)
		return unix.Mkdev(major, minor)
	default:
		return 0 // Not a device type
	}
}
