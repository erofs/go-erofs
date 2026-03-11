#ifndef _SYS_XATTR_H
#define _SYS_XATTR_H
/* Stub xattr functions - not supported on Windows */
#define llistxattr(path, list, size) (0)
#define lgetxattr(path, name, value, size) (-1)
#define lsetxattr(path, name, value, size, flags) (-1)
#endif
