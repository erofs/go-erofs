package erofs_test

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	erofs "github.com/erofs/go-erofs"
)

type layerMember struct {
	name, data, link string
	kind             byte
	xattrs           map[string]string
}

func layerTar(t *testing.T, members ...layerMember) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, m := range members {
		kind := m.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		h := &tar.Header{Name: m.name, Typeflag: kind, Mode: 0o755, Size: int64(len(m.data)), Linkname: m.link, Xattrs: m.xattrs}
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(m.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func goLayer(t *testing.T, archive []byte, noinline bool) []byte {
	t.Helper()
	var b testBuffer
	opts := []erofs.CreateOpt{erofs.WithBuildTime(0, 0), erofs.WithDeviceAlignment(512)}
	if noinline {
		opts = append(opts, erofs.WithNoInlineData())
	}
	w := erofs.Create(&b, opts...)
	defer w.Abort()
	if err := w.CopyTar(bytes.NewReader(archive), erofs.WithAUFSWhiteouts()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func mergeLayers(t *testing.T, layers [][]byte, metadata bool) []byte {
	t.Helper()
	var b testBuffer
	w := erofs.Create(&b, erofs.WithNoInlineData(), erofs.WithDeviceAlignment(512), erofs.WithMappedDevices(), erofs.WithBuildTime(0, 0))
	defer w.Abort()
	for _, layer := range layers {
		img, err := erofs.Open(bytes.NewReader(layer))
		if err != nil {
			t.Fatal(err)
		}
		opts := []erofs.CopyOpt{erofs.Merge()}
		if metadata {
			opts = append(opts, erofs.MetadataOnly())
		}
		if err = w.CopyFrom(img, opts...); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func checkMergedLayer(t *testing.T, data []byte) {
	t.Helper()
	img, err := erofs.Open(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]string{"keep": "keep-data", "dir/.dot": "dot-data", "dir/new": "new-data"} {
		got, err := fs.ReadFile(img, p)
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q, %v; want %q", p, got, err, want)
		}
	}
	for _, p := range []string{"victim", "dir/old"} {
		if _, err := fs.Stat(img, p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s survived: %v", p, err)
		}
	}
}
func TestLayerMergeMappedImage(t *testing.T) {
	baseTar := layerTar(t, layerMember{name: "keep", data: "keep-data"}, layerMember{name: "victim", data: "old"}, layerMember{name: "dir/old", data: "old"})
	upperTar := layerTar(t, layerMember{name: ".wh.victim"}, layerMember{name: "dir/.dot", data: "dot-data"}, layerMember{name: "dir/.wh..wh..opq"}, layerMember{name: "dir/new", data: "new-data"})
	for _, inline := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "inline"}[inline], func(t *testing.T) {
			layers := [][]byte{goLayer(t, baseTar, !inline), goLayer(t, upperTar, !inline)}
			checkMergedLayer(t, mergeLayers(t, layers, false))
			meta := mergeLayers(t, layers, true)
			if len(meta)%(2<<20) != 0 {
				t.Fatalf("unaligned metadata size %d", len(meta))
			}
			if binary.LittleEndian.Uint32(meta[1060:1064])*4096 != uint32(len(meta)) {
				t.Fatal("superblock size excludes padding")
			}
			flat := append(append(append([]byte{}, meta...), layers[0]...), layers[1]...)
			checkMergedLayer(t, flat)
		})
	}
}
func TestNativeLayerMergeMappedImage(t *testing.T) {
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs unavailable")
	}
	a := layerTar(t, layerMember{name: "keep", data: "keep-data"}, layerMember{name: "victim", data: "old"}, layerMember{name: "dir/old", data: "old"})
	b := layerTar(t, layerMember{name: ".wh.victim"}, layerMember{name: "dir/.dot", data: "dot-data"}, layerMember{name: "dir/.wh..wh..opq"}, layerMember{name: "dir/new", data: "new-data"})
	var layers [][]byte
	for i, archive := range [][]byte{a, b} {
		p := filepath.Join(t.TempDir(), "layer.erofs")
		cmd := exec.Command("mkfs.erofs", "--tar=f", "--aufs", "--quiet", "-Enoinline_data", "--fsalignblks=512", p)
		cmd.Stdin = bytes.NewReader(archive)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("native layer %d: %s: %v", i, out, err)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		layers = append(layers, data)
	}
	meta := mergeLayers(t, layers, true)
	flat := append(append(append([]byte{}, meta...), layers[0]...), layers[1]...)
	checkMergedLayer(t, flat)
}
func TestCopyTarRejectsHardlinkParent(t *testing.T) {
	archive := layerTar(t, layerMember{name: "link", link: "elsewhere", kind: tar.TypeSymlink}, layerMember{name: "hard", link: "link/file", kind: tar.TypeLink})
	var b testBuffer
	w := erofs.Create(&b)
	defer w.Abort()
	if err := w.CopyTar(bytes.NewReader(archive)); !errors.Is(err, erofs.ErrNotDirectory) {
		t.Fatalf("got %v", err)
	}
}
