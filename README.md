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

## Planning metadata around an existing data area

`LayoutBuilder` is a lower-level alternative for callers that manage data blocks
themselves. It produces a single-device, uncompressed image with compact 32-bit
chunk indexes. It does not open source files, deduplicate content, spool payload,
or change the meaning of the existing `DataRange` API.

1. Create a builder with root `Node` metadata and explicit `MemoryLimit` and
   `MetadataLimit` in `LayoutOptions`. The default block size is 4096 bytes.
2. Add nodes in increasing name order within each parent. Parents must exist
   before children. `Link` adds a second name for an existing non-directory
   inode; link counts reflect the names supplied to the builder.
3. Call `AddBlocks` for each regular file. References are zero-based block
   numbers in the caller's data area, or `HoleBlock`. The number of references
   must equal the file size rounded up to a block. A repeated reference shares
   storage without merging inode identity. The caller pads a partial data block.
4. `Prepare(ctx, dataBlocks)` fixes the layout and transfers ownership to the
   returned `Layout`. `Size`, `DataOffset`, and `DataSize` describe its regions.
5. `WriteMetadata(ctx, writerAt)` writes the superblock, directories, symlinks,
   inodes and chunk indexes. It leaves the data area untouched. Supply the data
   blocks separately before treating the image as complete. An unwritten data
   area is **not** a logical file hole.
6. Close the layout to release its indexes. Close the builder on a construction
   error. Neither object closes caller-owned files. Metadata writes may be
   repeated before close; errors can leave partial output, which is not a
   successfully completed image. Methods are not safe for concurrent use.

The memory budget admits node/string storage, maps, slice capacities (including
old/new capacity during growth), traversal arrays, directory/xattr serialization
workspace, and output buffers **before** allocation. `MemoryUsage` reports the
current reservation and its peak. This is a bound on controlled allocations, not
on process RSS: allocator rounding, garbage collection and caller allocations
still affect process memory. Close drops references without forcing global GC.
The metadata budget bounds image bytes outside the data area. Address overflow,
missing data, unsupported metadata and budget exhaustion return errors; the
builder does not silently change block size or discard entries.

`Node` accepts optional xattrs. Applications that omit them retain their own
no-xattr policy. Build time and node timestamps are explicit, and the UUID is
zero; input order and metadata determine the output, not the wall clock.

Regression tests compare reader-visible contents and metadata, hard-link
identity, sparse blocks, repeated output and budget/error behavior. Native
`fsck.erofs` provides an independent check when available. Blocks larger than the
host page size are covered by the Go checks because native erofs-utils rejects
them on that host; 4096-byte images receive both checks on supported CI hosts.
