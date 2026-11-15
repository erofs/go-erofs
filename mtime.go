package erofs

import "math"

// mtimePolicy derives the superblock timestamp for compact inodes from the
// mtimes of the inodes written. observe is called once per inode; result
// returns the seconds and nanoseconds to store in the superblock.
type mtimePolicy interface {
	observe(sec uint64, nsec uint32)
	result() (sec uint64, nsec uint32)
}

// mostCommonMtime selects the most frequent whole-second mtime, preferring
// the smallest value on a tie, or 0 if no mtime is a whole second. It suits
// layouts where a compact inode's mtime equals the superblock timestamp.
type mostCommonMtime struct {
	counts map[uint64]int
}

func newMostCommonMtime() *mostCommonMtime {
	return &mostCommonMtime{counts: make(map[uint64]int)}
}

func (p *mostCommonMtime) observe(sec uint64, nsec uint32) {
	if nsec == 0 {
		p.counts[sec]++
	}
}

func (p *mostCommonMtime) result() (uint64, uint32) {
	var best uint64
	var bestN int
	for t, n := range p.counts {
		if n > bestN || (n == bestN && t < best) {
			best, bestN = t, n
		}
	}
	return best, 0
}

// earliest returns the earliest whole-second mtime observed, or 0 if none.
func (p *mostCommonMtime) earliest() uint64 {
	var min uint64
	first := true
	for t := range p.counts {
		if first || t < min {
			min, first = t, false
		}
	}
	return min
}

// count returns the number of inodes observed with whole-second mtime t.
func (p *mostCommonMtime) count(t uint64) int {
	return p.counts[t]
}

// countRange returns the number of inodes observed with a whole-second
// mtime from lo to hi, inclusive.
func (p *mostCommonMtime) countRange(lo, hi uint64) int {
	n := 0
	for t, c := range p.counts {
		if t >= lo && t <= hi {
			n += c
		}
	}
	return n
}

// compactFields reports whether e's ownership, link count and size fit in
// a compact inode. The mtime is checked separately by compactMtime.
func compactFields(e *erofsEntry) bool {
	return e.uid <= 0xFFFF && e.gid <= 0xFFFF &&
		e.nlink <= 0xFFFF && e.size <= 0xFFFFFFFF
}

// compactMtime reports whether e's mtime can be stored in a compact inode
// and returns the value for its i_mtime field. Without the 48-bit layout
// the field must be 0, so the mtime must equal the superblock timestamp.
func (w *erofsWriter) compactMtime(e *erofsEntry) (uint32, bool) {
	if e.mtimeNs != w.buildTimeNs || e.mtime < w.buildTime {
		return 0, false
	}
	off := e.mtime - w.buildTime
	if off == 0 || (w.format48Bit && off <= math.MaxUint32) {
		return uint32(off), true
	}
	return 0, false
}
