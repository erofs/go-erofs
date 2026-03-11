#ifndef _POSIX_COMPAT_H
#define _POSIX_COMPAT_H
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
/*
 * MinGW-w64 honours _FILE_OFFSET_BITS=64 for off_t (making it 64-bit),
 * but does NOT remap lseek/ftruncate to their 64-bit variants the way
 * glibc does on Linux.  Provide the remapping here so every call site
 * automatically gets 64-bit offsets.
 *
 * These must come AFTER the system headers (included above) so the
 * original prototypes are already declared.
 */
#define lseek  lseek64
#define ftruncate ftruncate64

/*
 * MinGW may define S_IFBLK with a non-Linux value (0x3000 vs 0x6000).
 * erofs uses the Linux on-disk format, so force Linux values here.
 * S_IFLNK and S_IFSOCK are not defined by MinGW at all.
 */
#ifdef S_IFBLK
#undef S_IFBLK
#endif
#define S_IFBLK 0060000
#ifndef S_IFLNK
#define S_IFLNK 0120000
#endif
#ifndef S_IFSOCK
#define S_IFSOCK 0140000
#endif

#ifndef S_ISLNK
#define S_ISLNK(m) (((m) & 0xF000) == 0xA000)
#endif
#ifndef S_ISSOCK
#define S_ISSOCK(m) (((m) & 0xF000) == 0xC000)
#endif
static inline char* strndup(const char* s, size_t n) {
  size_t len = strnlen(s, n);
  char* result = malloc(len + 1);
  if (result) { memcpy(result, s, len); result[len] = 0; }
  return result;
}
static inline int lstat(const char* path, struct stat* buf) { return stat(path, buf); }
static inline char* realpath(const char* path, char* resolved) {
  char* buf = resolved;
  if (!buf) buf = malloc(260);
  if (!buf) return NULL;
  char* result = _fullpath(buf, path, 260);
  if (!result && !resolved) free(buf);
  return result;
}
static inline ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  off_t old = lseek64(fd, 0, SEEK_CUR);
  if (old < 0) return -1;
  if (lseek64(fd, offset, SEEK_SET) < 0) return -1;
  ssize_t r = read(fd, buf, count);
  lseek64(fd, old, SEEK_SET);
  return r;
}
static inline ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  off_t old = lseek64(fd, 0, SEEK_CUR);
  if (old < 0) return -1;
  if (lseek64(fd, offset, SEEK_SET) < 0) return -1;
  ssize_t r = write(fd, buf, count);
  lseek64(fd, old, SEEK_SET);
  return r;
}
static inline int fsync(int fd) { return _commit(fd); }
static inline ssize_t readlink(const char* path, char* buf, size_t bufsiz) {
  fprintf(stderr, "WARNING: readlink() called but not supported on Windows (path=%s)\n", path);
  errno = EINVAL;
  return -1;
}
/*
 * Linux new_encode_dev/new_decode_dev compatible device number encoding.
 * erofs-utils uses this scheme in erofs_new_encode_dev() / erofs_new_decode_dev().
 *   Bits  0-7:  minor bits 0-7
 *   Bits  8-19: major bits 0-11
 *   Bits 20-31: minor bits 8-19
 *
 * These must be macros (not inline functions) because erofs-utils declares
 * local variables named "major"/"minor" that shadow function names.
 */
#define major(dev) (((unsigned int)(dev) >> 8) & 0xfff)
#define minor(dev) (((unsigned int)(dev) & 0xff) | (((unsigned int)(dev) >> 12) & ~0xffu))
#define makedev(maj, min) (((unsigned int)(min) & 0xff) | ((unsigned int)(maj) << 8) | (((unsigned int)(min) & ~0xffu) << 12))
static inline int fchmod(int fd, mode_t mode) { return 0; }
static inline int getpagesize(void) { return 4096; }
static inline unsigned int getuid(void) { return 0; }
static inline unsigned int getgid(void) { return 0; }
#endif
