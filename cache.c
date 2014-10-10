#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#ifndef NDEBUG
#include <signal.h>
#endif

#include "cache.h"

#ifndef NDEBUG
const uint64_t redzone_patten = 0xdeadbeefcafebabe;
int cache_error = 0;
#endif

const int intitial_pool_size = 64;

cache_t* cache_create(const char* name, size_t bufsize, size_t align, 
				cache_constructor_t* constructor, cache_destructor_t* destructor)
{
	cache_t* ret = calloc(1, sizeof(cache_t));
	char* nm = strdup(name);
	void** ptr = calloc(intitial_pool_size, sizeof(void**)); //64个void*数组, calloc会自动初始化0
	if(ret == NULL || ptr == NULL || nm == NULL 
		|| pthread_mutex_init(&ret->mutex, NULL) == -1){
			free(ret);
			free(nm);
			free(ptr);
			return NULL;
	}

	ret->name = nm;
	ret->ptr = ptr;
	ret->freetotal = intitial_pool_size;
	ret->constructor = constructor;
	ret->destructor = destructor;

#ifndef NDEBUG
	ret->bufsize = bufsize + 2 * sizeof(redzone_patten);
#else
	ret->bufsize = bufsize;
#endif

	return ret;
}

static inline void* get_object(void* ptr)
{
#ifndef NDEBUG
	uint64_t *pre = (uint64_t *)ptr;
	return pre + 1;
#else
	return ptr;
#endif
}

void cache_destroy(cache_t* handle)
{
	while(handle->freecurr > 0){
		void* ptr = handle->ptr[--cache->freecurr];
		if(handle->destructor) //对object的析构
			handle->destructor(get_object(ptr), NULL);

		//释放指针
		free(ptr);
	}

	free(handle->name);
	free(handle->ptr);
	//对锁的释放
	pthread_mutex_destroy(&handle->mutex);
	free(handle);
}

void* cache_alloc(cache_t* handle)
{
	void* ret;
	void* object;

	pthread_mutex_lock(&handle->mutex);
	if(handle->freecurr > 0){
		ret = handle->ptr[-- handle->freecurr];
		object = get_object(ret);
	}
	else{
		object = ret = malloc(handle->bufsize);
		if(ret != NULL){
			object = get_object(ret);
			if(handle->constructor != NULL && handle->constructor(object, NULL, 0) != 0){ //进行构造调用
				free(ret);
				object = NULL;
			}
		}
	}

	pthread_mutex_unlock(&handle->mutex);

#ifndef NDEBUG
	if(object != NULL){ //在对象object的前后各加入一个check sum
		uint64_t* pre = (uint64_t *)ret;
		*pre = redzone_patten;
		ret = (void *)(pre + 1);
		memcpy((char*)ret + handle->bufsize - (2 * sizeof(redzone_patten)), &redzone_patten, sizeof(redzone_patten));
	}
#endif

	return object;
}

void cache_free(cache_t* cache, void* ptr)
{
	pthread_mutex_lock(&cache->mutex);

#ifndef NDEBUG
	//检查最后一个uint64_t的check sum
	if(memcmp(((char*)ptr) + cache->bufsize - 2 * sizeof(redzone_patten), &redzone_patten, sizeof(redzone_patten)) != 0){
		raise(SIGABRT); //发送一个错误的调试信号
		cache_error = 1;
		pthread_mutex_unlock(&cache->mutex);
		return;
	};
	//检查第一个check sum
	uint64_t *pre = (uint64_t*)ptr;
	--pre;
	if(*pre != redzone_patten){
		raise(SIGABRT);
		cache_error = 1;
		pthread_mutex_unlock(&cache->mutex);
		return;
	}
	ptr = pre;
#endif

	if(cache->freecurr < cache->freetotal){
		cache->ptr[cache->freecurr ++] = ptr;
	}
	else{ //扩大两倍，用于存储更多的object,这个地方是不是做一个最大值会比较好，否则内存会一直在,或者做个time gc的释放
		size_t newtotal = cache->freetotal * 2;
		void** new_free = realloc(cache->ptr, sizeof(char *) * newtotal);
		if(new_free){
			cache->freetotal = newtotal;
			cache->ptr = new_free;
			cache->ptr[cache->freecurr ++] = ptr;
		}
		else{
			if(cache->destructor){
				cache->destructor(ptr, NULL);
			}
			free(ptr);
		}
	}

	pthread_mutex_unlock(&cache->mutex);
}



