#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "memcached.h"

#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoull(const char* str, uint64_t* out)
{
	assert(out != NULL);
	errno = 0;
	*out = 0;

	char* endptr;
	unsigned long long ull = strtoull(str, &endptr, 10);
	if(errno == ERANGE || str == endptr)
		return false;

	if(xisspace(*endptr) || (*endptr == '\0' && endptr != str)){ //一定是有数字的，否则返回失败
		if((long long)ull < 0){
			if(strchr(str, '-') != NULL)
				return false;
		}
		*out = ull;
		return true;
	}

	return false;
}

bool safe_strtoll(const char* str, int64_t *out)
{
	assert(out != NULL);
	errno = 0;
	*out = 0;

	char* endptr;

	long long ll = strtoll(str, &endptr, 10);
	if(errno == ERANGE || str == endptr) //转换出错，或者字符串前面不可以转换为数字
		return false;

	if(xisspace(*endptr) || (*endptr == '\0' && endptr != str)){
		*out = ll;
		return true;
	}

	return false;
}

bool safe_strtoul(const char* str, uint32_t* out)
{
	assert(out != NULL);
	errno = 0;
	*out = 0;

	char* endptr;
	unsigned int l = strtoul(str, &endptr, 10);
	if(errno == ERANGE || str == endptr)
		return false;

	if(xisspace(*endptr) || (*endptr == '\0' && endptr != str)){
		if((int)l < 0){
			if(strchr(str, '-') != NULL)
				return false;
		}

		return true;
	}
}

bool safe_strtol(const char *str, int32_t *out) 
{
	assert(out != NULL);
	errno = 0;
	*out = 0;
	char *endptr;
	long l = strtol(str, &endptr, 10);
	if ((errno == ERANGE) || (str == endptr)) {
		return false;
	}

	if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
		*out = l;
		return true;
	}
	return false;
}

void vperror(const char* fmt, ...)
{
	int old_errno = errno;
	char buf[1024];
	va_list ap;

	va_start(ap, fmt);
	if(vsnprintf(buf, sizeof(buf), fmt, ap) == -1)
		 buf[sizeof(buf) - 1] = '\0';
	
	va_end(ap);

	errno = old_errno;

	perror(buf);
}

#ifndef HAVE_HTONLL
static uint64_t mc_swap64(uint64_t in){
#ifdef ENDIAN_LITTLE
	int64_t rv = 0;
	int i = 0;
	for(i = 0; i < 8; i ++){
		rv = (rv << 8) | (in & 0xff);
		in >>= 8;
	}
	return rv;
#else
	return in;
#endif
}

uint64_t ntohll(uint64_t val)
{
	return mc_swap64(val);
}

uint64_t htonll(uint64_t val)
{
	return mc_swap64(val);
}

#endif







