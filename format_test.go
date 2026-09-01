package erofs

import "testing"

func TestPOSIXACLXattrPrefix(t *testing.T) {
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default"} {
		index, suffix := xattrSplit(name)
		if suffix != "" {
			t.Fatalf("xattrSplit(%q) suffix=%q, want empty", name, suffix)
		}
		if got := xattrIndex(index).String(); got != name {
			t.Fatalf("xattr index %d prefix=%q, want %q", index, got, name)
		}
	}
}
