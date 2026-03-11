#ifndef _REGEX_H
#define _REGEX_H
/* Minimal regex stub for Windows - exclude patterns disabled */
#include <stddef.h>
typedef struct { int dummy; } regex_t;
typedef size_t regoff_t;
typedef struct { regoff_t rm_so; regoff_t rm_eo; } regmatch_t;
#define REG_EXTENDED 1
#define REG_NOSUB 2
#define REG_NEWLINE 4
static inline int regcomp(regex_t *preg, const char *regex, int cflags) { return 0; }
static inline int regexec(const regex_t *preg, const char *string, size_t nmatch, regmatch_t pmatch[], int eflags) { return 1; }
static inline void regfree(regex_t *preg) { }
static inline size_t regerror(int errcode, const regex_t *preg, char *errbuf, size_t errbuf_size) { return 0; }
#endif
