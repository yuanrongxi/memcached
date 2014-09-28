#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>


typedef struct
{
	unsigned int size;     /*item 的大小*/
	unsigned int perslab;  /*每个slabs包含的item个数*/

	void* slots;			/*item list头指针*/
	unsigned int sl_curr;

	unsigned int slabs; /*这个class 可以分配的最大slabs的个数*/

	void** slab_list;	 /*slabs的指针数组*/
	unsigned int list_size; /*slab list size*/

	unsigned int killing;
	size_t requested;   /*被使用过的字节数*/
}slabclass_t;

static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
static size_t mem_limit = 0;
static size_t mem_malloced = 0;
static int power_largest;

static void* mem_base = NULL;
static void* mem_current = NULL;
static size_t mem_avail = 0;

static pthread_mutex_t slabs_lock = PTHREAD_MUTEX_INITIALIZER;
static ptrhead_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;

static int do_slabs_newslab(const unsigned int id);
static void* memory_allocate(size_t size);
static void do_slabs_free(void* ptr, const size_t size, unsigned int id);
static void slabs_preallocate(const unsigned int maxslabs);

//定位到对应的SIZE的slabs下标
unsigned int slabs_clsid(const size_t size)
{
	int res = POWER_SMALLEST;
	if(size == 0)
		return 0;

	while(size > slabclass[res].size){
		if(res ++ == power_largest)
			return 0;
	}

	return res;
}

void slabs_init(const size_t limit, const double factor, const bool prealloc)
{
	int i = POWER_SMALLEST - 1;
	unsigned int size = sizeof(item) + settings.chunk_size;

	mem_limit = limit;
	if(prealloc){ //预分配
		mem_base = malloc(mem_limit);
		if(mem_base != NULL){
			mem_current = mem_base;
			mem_avail = mem_limit;
		}
		else
			fprintf(stderr, "Warning: Failed to allocate requested memory in one large chunk.\nWill allocate in smaller chunks\n");
	}

	memset(slabclass, 0, sizeof(slabclass));

	while(++i < POWER_LARGEST && size <= settings.item_size_max / factor){
		if(size % CHUNK_ALIGN_BYTES)
			size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

		slabclass[i].size = size;
		slabclass[i].perslab = settings.item_size_max / slabclass[i].size;
		size *= factor;

		if(settings.verbose > 1)
			fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n", i, slabclass[i].size, slabclass[i].perslab);
	}

	power_largest = i;
	slabclass[power_largest].size = settings.item_size_max;
	slabclass[power_largest].perslab = 1;
	if(settings.verbose > 1)
		fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n", i, slabclass[i].size, slabclass[i].perslab);

	{
		char* t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
		if(t_initial_malloc)
			mem_malloced = (size_t)atol(t_initial_malloc);
	}

	//内存预分配
	if(prealloc)
		slabs_preallocate(power_largest);
}

static void slabs_preallocate(const unsigned int maxslabs)
{
	int i;
	unsigned int prealloc = 0;
	for(i = POWER_SMALLEST; i < POWER_LARGEST; i ++){
		if(++prealloc > maxslabs)
			return;

		if(do_slabs_newslab(i) == 0){
			fprintf(stderr, "Error while preallocating slab memory!\n"
				"If using -L or other prealloc options, max memory must be "
				"at least %d megabytes.\n", power_largest);
			exit(1);
		}
	}
}

static int grow_slab_list(const unsigned int id)
{
	slabclass_t* p = &slabclass[id];
	if(p->slabs == p->list_size){ //已经到最大了
		size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16; //将slabs数组扩大为原来的2倍
		void* new_list = realloc(p->slab_list, new_size * sizeof(void *));
		if(new_list == 0)
			return 0;
		p->list_size = new_size;
		p->slab_list = new_list;
	}

	return 1;
}

static void split_slab_page_into_freelist(char* ptr, const unsigned int id)
{
	slabclass_t* p = &slabclass[id];
	int x;
	for(x = 0; x < p->perslab; x++){
		do_slabs_free(ptr, 0, id);
		ptr += p->size;
	}
}

static void *memory_allocate(size_t size)
{
	void *ret;
	if(mem_base == NULL){
		ret = malloc(size);
	}
	else { //预分配状态
		ret = mem_current;
		if(size > mem_avail)
			return NULL;

		if(size % CHUNK_ALIGN_BYTES)
			size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES); //8字节对齐

		mem_current = (char*)mem_current + size;
		if(size < mem_avail)
			mem_avail -= size;
		else
			mem_avail = 0;
	}
	
	return ret;
}

static int do_slabs_newslab(const unsigned int id)
{
	slabclass_t* p = &slabclass[id];
	int len = settings.slab_ressign ? settings.item_size_max : p->size * p->perslab;
	
	char* ptr;
	if((mem_limit && mem_malloced + len > mem_limit && p->slabs > 0) || (grow_slab_list(id) == 0) ||((ptr = memory_allocate((size_t)len)) == 0)){
		MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
		return 0;
	}

	memset(ptr, 0, (size_t)len);
	//将slab的内存块ptr分作若干个item
	split_slab_page_into_freelist(ptr, id);

	p->slab_list[p->slabs ++] = ptr;
	mem_malloced += len;

	MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

	return 1;
}

static void* do_slabs_alloc(const size_t size, unsigned int id)
{
	slabclass_t *p;
	void* ret = NULL;
	item* it = NULL;

	if(id < POWER_SMALLEST || id > power_largest){
		MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
		return 0;
	}

	p = &slabclass[id];
	assert(p->sl_curr == 0 || ((item *)p->slots)->slabs_clsid == 0);

	if(!(p->sl_curr != 0 || do_slabs_newslab(id) == 0)){
		ret = NULL;
	}
	else if(p->sl_curr != 0){
		it = (item *)p->slots;
		p->slots = it->next;
		if(it->next)
			it->next->prev = NULL;
		p->sl_curr --;
		ret = (void *)it;
	}

	if(ret){
		p->requested += size;
		MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
	}
	else{
		MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
	}

	return ret;
}

static void do_slabs_free(void* ptr, const size_t size, unsigned int id)
{
	slabclass_t* p;
	item* it;

	assert(((item *)ptr)->slabs_clsid == 0);
	assert(id >= POWER_SMALLEST && id <= power_largest);
	if(id < POWER_SMALLEST || id > power_largest)
		return ;

	MEMCACHED_SLABS_FREE(size, id, ptr);
	p = &slabclass[id];

	it = (item *)ptr;
	it->it_flags |= ITEM_SLABBED;
	it->prev = 0;
	it->next = p->slots;

	if(it->next)
		it->next->prev = it;
	p->slots = it;

	p->sl_curr ++;
	p->requested -= size;
	return;
}

//字符串比较
static int nz_strcmp(int nzlength, const char* nz, const char* z)
{
	int zlength = strlen(z);
	return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char* stat_type, int nkey, ADD_STAT add_stats, void* c)
{
	bool ret = true;
	if(add_stats != NULL){
		if (!stat_type) {
			STATS_LOCK();
			APPEND_STAT("bytes", "%llu", (unsigned long long)stats.curr_bytes);
			APPEND_STAT("curr_items", "%u", stats.curr_items);
			APPEND_STAT("total_items", "%u", stats.total_items);
			STATS_UNLOCK();
			item_stats_totals(add_stats, c);
		} 
		else if (nz_strcmp(nkey, stat_type, "items") == 0) {
			item_stats(add_stats, c);
		} 
		else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
			slabs_stats(add_stats, c);
		} 
		else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
			item_stats_sizes(add_stats, c);
		} 
		else {
			ret = false;
		}
	}
	else
		ret = false;

	return ret;
}

static void do_slabs_stats(ADD_STAT add_stats, void *c)
{
	int i, total;
	struct thread_stats thread_stats;
	threadlocal_stats_aggregate(&thread_stats);

	total = 0;
	for(i = POWER_SMALLEST; i <= power_largest; i ++){
		slabclass_t *p = &slabclass[i];
		if(p->slabs != 0){
			uint32_t perslab, slabs;
			slabs = p->slabs;
			perslab = p->perslab;

			char key_str[STAT_KEY_LEN];
			char val_str[STAT_VAL_LEN];
			int klen = 0, vlen = 0;

			APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
			APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
			APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
			APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
			APPEND_NUM_STAT(i, "used_chunks", "%u", slabs*perslab - p->sl_curr);
			APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
			/* Stat is dead, but displaying zero instead of removing it. */
			APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
			APPEND_NUM_STAT(i, "mem_requested", "%llu", (unsigned long long)p->requested);
			APPEND_NUM_STAT(i, "get_hits", "%llu", (unsigned long long)thread_stats.slab_stats[i].get_hits);
			APPEND_NUM_STAT(i, "cmd_set", "%llu", (unsigned long long)thread_stats.slab_stats[i].set_cmds);
			APPEND_NUM_STAT(i, "delete_hits", "%llu", (unsigned long long)thread_stats.slab_stats[i].delete_hits);
			APPEND_NUM_STAT(i, "incr_hits", "%llu", (unsigned long long)thread_stats.slab_stats[i].incr_hits);
			APPEND_NUM_STAT(i, "decr_hits", "%llu", (unsigned long long)thread_stats.slab_stats[i].decr_hits);
			APPEND_NUM_STAT(i, "cas_hits", "%llu", (unsigned long long)thread_stats.slab_stats[i].cas_hits);
			APPEND_NUM_STAT(i, "cas_badval", "%llu", (unsigned long long)thread_stats.slab_stats[i].cas_badval);
			APPEND_NUM_STAT(i, "touch_hits", "%llu",(unsigned long long)thread_stats.slab_stats[i].touch_hits);

			total++;
		}
	}

	APPEND_STAT("active_slabs", "%d", total);
	APPEND_STAT("total_malloced", "%llu", (unsigned long long)mem_malloced);

	add_stats(NULL, 0, NULL, 0, c);
}

void* slabs_alloc(size_t size, unsigned int id)
{
	void* ret;

	pthread_mutex_lock(&slabs_lock);
	ret = do_slabs_alloc(size, id);
	pthread_mutex_unlock(&slabs_lock);
}

void slabs_free(void* ptr, size_t size, unsigned int id)
{
	pthread_mutex_lock(&slabs_lock);
	do_slabs_free(ptr, size, id);
	pthread_mutex_unlock(&slabs_lock);
}

void slabs_stats(ADD_STAT add_stats, void* c)
{
	pthread_mutex_lock(&slabs_lock);
	do_slabs_stats(add_stats, c);
	pthread_mutex_unlock(&slabs_lock);
}

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal)
{
	pthread_mutex_lock(&slabs_lock);
	
	slabclass_t *p;
	if(id < POWER_SMALLEST || id > power_largest){
		fprintf(stderr, "Internal error! Invalid slab class\n");
		abort();
	}

	p = &slabclass[id];
	p->requested = p->requested - old + ntotal;

	pthread_mutex_unlock(&slabs_lock);
}

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t slab_rebalance_cond = PTHREAD_COND_INITIALIZER;
static volatile int do_run_slab_thread = 1;
static volatile int do_run_slab_rebalance_thread = 1;

#define DEFAULT_SLAB_BULK_CHECK 1
int slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

static int slab_rebalance_start()
{
	slabclass_t* s_cls;
	int no_go = 0;

	pthread_mutex_lock(&cache_lock);
	pthread_mutex_lock(&slabs_lock);

	//对slab_rebal的检查
	if(slab_rebal.s_clsid < POWER_SMALLEST ||
	   slab_rebal.s_clsid > power_largest ||
	   slab_rebal.d_clsid < POWER_SMALLEST ||
	   slab_rebal.d_clsid > power_largest  ||
	   slab_rebal.s_clsid == slab_rebal.d_clsid)
	   no_go = -2;

	s_cls = &slabclass[slab_rebal.s_clsid];

	//d_scsid slab无法扩大（realloc失败）
	if(!grow_slab_list(slab_rebal.d_clsid)){
		no_go = -1;
	}

	if(s_cls->slabs < 2)
		no_go = -3;

	//rebalance 失败
	if(no_go != 0){
		pthread_mutex_unlock(&slabs_lock);
		pthread_mutex_unlock(&cache_lock);
		return no_go;
	}

	//设置KILL标识
	s_cls->killing = 1;

	slab_rebal.slab_start = s_cls->slab_list[s_cls->killing - 1];
	slab_rebal.slab_end = (char *)slab_rebal.slab_start + (s_cls->size * s_cls->perslab); //释放掉一个slab 页面
	slab_rebal.slab_pos = slab_rebal.slab_start;
	slab_rebal.done = 0;

	slab_rebalance_cond = 2;

	if(settings.verbose > 1)
		fprintf(stderr, "Started a slab rebalance\n");

	pthread_mutex_unlock(&slabs_lock);
	pthread_mutex_unlock(&cache_lock);

	STATS_LOCK();
	stats.slab_reassign_running = true;
	STATS_UNLOCK();

	return 0;
}

enum move_status
{
	MOVE_PASS = 0,
	MOVE_DONE,
	MOVE_BUSY,
	MOVE_LOCKED
};

static int slab_rebalance_move()
{
	slabclass_t *s_cls;
	int x;
	int was_busy = 0;
	int refcount = 0;

	enum move_status status = MOVE_PASS;

	pthread_mutex_lock(&cache_lock);
	pthread_mutex_lock(&slabs_lock);

	pthread_mutex_unlock(&slabs_lock);
	pthread_mutex_unlock(&cache_lock);

	return was_busy;
}


