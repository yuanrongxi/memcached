#ifndef __CACHE_H_
#define __CACHE_H_

#include <pthread.h>

//cache是对象池的设计实现

#ifdef HAVE_UMEM_H
#include <umem.h>

#define cache_t umem_cache_t;
#define cache_alloc(a) umem_cache_alloc(a, UMEM_DEFAULT)
#define cache_free(a, b) umem_cache_free(a, b)
#define cache_create(a,b,c,d,e) umem_cache_create((char*)a, b, c, d, e, NULL, NULL, NULL, 0)
#define cache_destroy(a) umem_cache_destroy(a);
#else

#ifndef NDEBUG
extern int cache_error;
#endif

typedef int cache_constructor_t(void* obj, void* notused1, int notused2);
typedef void cache_destructor_t(void* obj, void* notused);

typedef struct  
{
	pthread_mutex_t mutex;
	char* name;
	void** ptr;
	size_t bufsize;
	int freetotal;
	int freecurr;

	cache_constructor_t* constructor;
	cache_destructor_t* destructor;
}cache_t;

cache_t* cache_create(const char* name, size_t bufsize, size_t align, cache_constructor_t* constructor, cache_destructor_t* destructor);

void cache_destroy(cache_t* handle);

void* cache_alloc(cache_t* handle);

void cache_free(cache_t* handle, void* ptr);

#endif

#endif



