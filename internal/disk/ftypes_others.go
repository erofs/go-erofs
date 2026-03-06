//go:build !unix
package disk

func RdevFromMode(mode uint16, inodeData uint32) uint64 {
	return 0
}
