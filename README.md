# go-erofs

A Go library for reading and creating [EROFS](https://erofs.docs.kernel.org/) filesystem images using the standard [fs.FS](https://pkg.go.dev/io/fs#FS) interface.

## Features

- **Read** EROFS images through Go's `fs.FS` interface
- **Create** EROFS images from directories or any `fs.FS`
- **Merge** multiple filesystem sources with overlay whiteout support
- **Metadata-only** mode for container layer indexing (chunk-based references to original data)
- Pure Go, no CGO — uses only the standard library

### Status

- [x] Read erofs files created with default `mkfs.erofs` options
- [x] Read chunk-based erofs files with indexes
- [x] Xattr support including long xattr prefixes
- [x] Extra devices for chunked data
- [x] Create erofs files from any `fs.FS`
- [x] Directory to erofs packing
- [x] AUFS whiteout to overlayfs conversion
- [x] Merge multiple filesystem layers with whiteout processing
- [ ] Read erofs files with compression

## Reading an EROFS image

```go
f, err := os.Open("image.erofs")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

img, err := erofs.Open(f)
if err != nil {
    log.Fatal(err)
}

fs.WalkDir(img, ".", func(path string, d fs.DirEntry, err error) error {
    fmt.Println(path)
    return nil
})
```

## Merging multiple layers

Combine multiple filesystem sources into one image. The `Merge` option enables overlay semantics — AUFS-style whiteout files (`.wh.<name>`) delete entries from prior layers:

```go
outFile, _ := os.Create("merged.erofs")
w := erofs.Create(outFile)

w.CopyFrom(baseLayer)
w.CopyFrom(overlayLayer, erofs.Merge())
w.Close()
```

Merge can also be combined with `MetadataOnly` to build a merged index without copying data:

```go
w := erofs.Create(outFile)
w.CopyFrom(layer1, erofs.MetadataOnly())
w.CopyFrom(layer2, erofs.MetadataOnly(), erofs.Merge())
w.Close()
```

## Building an image programmatically

```go
outFile, _ := os.Create("image.erofs")
w := erofs.Create(outFile)

f, _ := w.Create("/hello.txt")
f.Write([]byte("hello world\n"))
f.Close()

w.Mkdir("/dir", 0o755)
w.Symlink("hello.txt", "/link")

w.Close()
outFile.Close()
```

`Create` needs a seekable destination: it lays the image out data-first and seeks
back to fill in the superblock.

## Streaming an image, with the layout known up front

`NewWriter` builds the same tree but lays it out metadata-first, so the image can
be written to a plain `io.Writer` — a pipe, a compressor, a hash. `Prepare`
returns the layout before a single byte is emitted, which lets a caller
pre-compute digests, plan a chunk index, or route each out-of-line payload
somewhere of its own.

```go
w := erofs.NewWriter(erofs.WithBuildTime(0, 0))

f, _ := w.Create("/bin/tool")
io.Copy(f, payload)
f.Close()
w.SetToken("/bin/tool", payloadDigest) // recovered from the layout below

layout, _ := w.Prepare()
for _, e := range layout.Extents {
    if e.Kind == erofs.ExtentFileData {
        // [e.Offset, e.Offset+e.Size) is this file's bytes verbatim,
        // followed by e.Pad zero bytes.
        fmt.Println(e.Path, e.Offset, e.Size, e.Token)
    }
}

w.WriteTo(dst) // emits exactly layout.ImageSize bytes, in ascending order
```

`Link` adds a second dirent for an existing inode, and `ShareData` gives two
inodes with independent metadata one shared payload — so duplicate content can be
stored once. `WithInlineThreshold` caps tail-packing, which decides what gets an
extent of its own; `WithSyntheticDirMetadata` and `SyntheticDirs` deal with the
directories a filesystem image needs but a layer changeset may not mention.
