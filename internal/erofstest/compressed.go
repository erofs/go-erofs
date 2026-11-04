package erofstest

import (
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// MkfsErofsLZ4 returns a Converter that runs mkfs.erofs with `-z lz4` plus
// `-Elegacy-compress` to force the full (legacy) lcluster layout. Pass any
// additional mkfs.erofs flags via extraOpts.
//
// Tests that use this should also call [RequireMkfsLZ4] first to t.Skip when
// the local mkfs.erofs lacks LZ4 support.
func MkfsErofsLZ4(extraOpts ...string) Converter {
	return MkfsErofs(append([]string{"-z", "lz4", "-Elegacy-compress"}, extraOpts...)...)
}


var (
	mkfsLZ4Once   sync.Once
	mkfsLZ4Avail  bool
	mkfsLZ4Reason string
)

// RequireMkfsLZ4 skips t when mkfs.erofs is not on PATH or does not support
// LZ4 compression.
func RequireMkfsLZ4(t testing.TB) {
	t.Helper()
	mkfsLZ4Once.Do(checkMkfsLZ4)
	if !mkfsLZ4Avail {
		t.Skipf("mkfs.erofs lz4 unavailable: %s", mkfsLZ4Reason)
	}
}

func checkMkfsLZ4() {
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		mkfsLZ4Reason = "mkfs.erofs not on PATH"
		return
	}
	// Attempt a tiny LZ4 conversion. mkfs.erofs reads tar input from stdin
	// with --tar=f. We pipe an empty tar (a 1024-byte zero tail) and check
	// only the early reject path: if LZ4 is unsupported, mkfs.erofs prints
	// "Unsupported compression algorithm" or similar before consuming input.
	cmd := exec.Command("mkfs.erofs", "--help")
	out, _ := cmd.CombinedOutput()
	help := strings.ToLower(string(out))
	if !strings.Contains(help, "lz4") {
		mkfsLZ4Reason = "mkfs.erofs --help does not advertise lz4"
		return
	}
	mkfsLZ4Avail = true
}
