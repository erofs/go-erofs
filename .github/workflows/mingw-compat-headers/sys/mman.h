#ifndef _SYS_MMAN_H
#define _SYS_MMAN_H
/* Stub for Windows - memory mapping not supported */
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#define PROT_READ 1
#define PROT_WRITE 2
#define MAP_SHARED 1
#define MAP_FAILED ((void*)-1)
static inline void* mmap(void*addr, size_t len, int prot, int flags, int fd, off_t off) {
  fprintf(stderr, "WARNING: mmap() called but not supported on Windows (fd=%d, len=%zu)\n", fd, (size_t)len);
  errno = ENOSYS;
  return MAP_FAILED;
}
static inline int munmap(void*addr, size_t len) {
  fprintf(stderr, "WARNING: munmap() called but not supported on Windows\n");
  return -1;
}
#endif
