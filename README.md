# go-erofs

A Go library for reading and creating [EROFS](https://erofs.docs.kernel.org/) filesystem images using the standard [fs.FS](https://pkg.go.dev/io/fs#FS) interface.

## About this fork

A fork of [github.com/erofs/go-erofs](https://github.com/erofs/go-erofs),
hardened against untrusted images, corrected on several on-disk paths, and
made substantially faster to read. Every change below is covered by a
regression test that fails against the code it replaced.

**Untrusted input.** `CopyFrom` sized allocations directly from superblock and
inode fields, so a few edited bytes in an 8 KiB image could request a
terabyte-scale buffer — which Go reports as an unrecoverable fatal OOM, not a
returned error. The directory walk also had no cycle detection and never
terminated on an image whose directories reach themselves. Both paths are now
bounded, and a fuzz target covers `copyFromImage`, the second on-disk parser,
which no existing target reached.

**Correctness.** Chunk extents are mapped at block granularity and holes are
preserved across a re-index; previously a region declared as a hole could read
back as real bytes from the backing device, at an offset the image never
referenced. Short reads are no longer silently zero-filled, entries can no
longer be attached beneath a regular file and then vanish from the image, two
files can no longer be opened for writing at once and share one data region,
and xattr names and values are checked against their on-disk field widths
instead of being truncated into them.

**Compatibility.** Images produced by the reference `mkfs.erofs` are readable.
Any inode carrying a shared xattr — which is how SELinux labels are stored —
used to fail outright, because the shared xattr area sits in the image's final
block and a whole-block read there runs past the end of the file. The reader
also now honours the [`fs.FS`](https://pkg.go.dev/io/fs#FS) path contract:
rooted paths, `.` and empty elements, and `..` traversal are rejected, so
`fstest.TestFS` passes. Names that are not valid UTF-8 remain readable, since
EROFS stores names as byte strings.

**Performance.** Reading a whole file takes 2 `ReadAt` calls instead of one per
filesystem block, and `io.Copy` goes through `io.WriterTo`. Inodes, dirents and
xattr headers are decoded directly rather than through reflection, and a pooled
block that was being stranded on every inode read is returned. Measured against
the fork point:

| operation | before | after |
| --- | --- | --- |
| Stat an inode | 580 ns, 4243 B | **71 ns, 96 B** |
| Resolve a path | 1638 ns | **425 ns** |
| Read a 500-entry directory | 28.8 µs | **15.9 µs** |
| Read an 8 MiB file | 827 µs, 2049 reads | **437 µs, 2 reads** |

Requires Go 1.25.

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

Close each file before creating the next one. File data is appended to a single
stream, so two open writers would interleave their bytes; `Create` returns an
error rather than let that corrupt the image silently.
