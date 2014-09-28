#ifndef __MEMCACHE_UTIL_H_
#define __MEMCACHE_UTIL_H_

#include <stdio.h>

bool safe_strtoull(const char* str, uint64_t* out);
bool safe_strtoll(const char* str, int64_t* out);
bool safe_strtoul(const char* str, uint32_t* out);
bool safe_strtol(const char* str, int32_t *out);

#ifdef __GCC
#define __gcc_attribute__ __attribute__
#else
#define __gcc_attribute__(x)
#endif

void vperror(const char* fmt, ...)
	__gcc_attribute__ ((format (printf, 1, 2)));

#endif





