#include "memcached.h"
#include "slabs.h"
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

	//将对应的slab class中的slab内存空间转移到需要的内存slab class上,这种情况下会造成以缓冲的kv丢失
	s_cls = &slabclass[slab_rebal.s_clsid];
	for(x = 0; x < slab_bulk_check; x++){
		item* it = (item *)slab_rebal.slab_pos;
		status = MOVE_PASS;
		if(it->slabs_clsid != 225){
			void* hold_lock = NULL;

			uint32_t hv = hash(ITEM_key(it), it->nkey);
			if(hold_lock == item_trylock(hv) == NULL) //item无法lock,表示可能有item忙
				status = MOVE_LOCKED;
			else{
				refcount = refcount_incr(&it->refcount);
				if(refcount == 1){
					if(it->it_flags & ITEM_SLABBED){
						if(s_cls->slots == it)
							s_cls->slots = it->next;
						//先移除free list 
						if(it->next)
							it->next->prev = it->prev;
						if(it->prev)
							it->prev->next = it->next;

						s_cls->sl_curr --;
						status = MOVE_DONE;
					}
					else
						status = MOVE_BUSY;
				}
				else if(refcount == 2){
					if((it->it_flags & ITEM_LINKED) != 0){ //如果item不忙，可以直接移除KV
						do_item_unlink_nolock(it, hv);
						status = MOVE_DONE;
					}
					else
						status = MOVE_BUSY;
				}
				else{
					if(settings.verbose > 2)
						fprintf(stderr, "Slab reassign hit a busy item: refcount: %d (%d -> %d)\n", it->refcount, slab_rebal.s_clsid, slab_rebal.d_clsid);

					status = MOVE_BUSY;
				}
				item_trylock_unlock(hold_lock);
			}
		}

		switch(status){
		case MOVE_DONE:
			it->refcount = 0;
			it->it_flags = 0;
			it->slabs_clsid = 255;
			break;

		case MOVE_BUSY:
			refcount_decr(&it->refcount);
		case MOVE_LOCKED:
			slab_rebal.busy_items ++;
			was_busy ++;
			break;

		case MOVE_PASS:
			break;
		}

		if(slab_rebal.slab_pos >= slab_rebal.slab_end){
			if(slab_rebal.busy_items){ //移除失败,重新重头开始移除
				slab_rebal.slab_pos = slab_rebal.slab_start;
				slab_rebal.busy_items = 0;
			}
			else
				slab_rebal.done ++;
		}
	}

	pthread_mutex_unlock(&slabs_lock);
	pthread_mutex_unlock(&cache_lock);

	return was_busy;
}

static void slab_rebalance_finish()
{
	slabclass_t* s_cls;
	slabclass_t *d_cls;

	pthread_mutex_lock(&cache_lock);
	pthread_mutex_lock(&slabs_lock);

	s_cls = &slabclass[slab_rebal.s_clsid];
	d_cls = &slabclass[slab_rebal.d_clsid];

	//移除slab 页
	s_cls->slab_list[s_cls->killing - 1] = s_cls->slab_list[s_cls->slabs - 1];
	s_cls->slabs --;
	s_cls->killing = 0;
	
	//将空余出来的内存放到
	memset(slab_rebal.slab_start, 0, (size_t)settings.item_size_max);
	d_cls->slab_list[d_cls->slabs ++] = slab_rebal.slab_start;

	//对slab进行item切分
	split_slab_page_into_freelist(slab_rebal.slab_start, slab_rebal.d_clsid);

	//复位slab内存转移状态信息
	slab_rebal.done       = 0;
	slab_rebal.s_clsid    = 0;
	slab_rebal.d_clsid    = 0;
	slab_rebal.slab_start = NULL;
	slab_rebal.slab_end   = NULL;
	slab_rebal.slab_pos   = NULL;

	pthread_mutex_unlock(&cache_lock);
	pthread_mutex_unlock(&slabs_lock);

	STATS_LOCK();
	stats.slab_reassign_running = false;
	stats.slabs_moved++;
	STATS_UNLOCK();

	if (settings.verbose > 1)
		fprintf(stderr, "finished a slab move\n");
}

//选择被转移的slab class和slab页
static int slab_automove_decision(int* src, int *dst)
{
	static uint64_t evicted_old[POWER_LARGEST];
	static unsigned int slab_zeroes[POWER_LARGEST];
	static unsigned int slab_winner = 0;
	static unsigned int slab_wins   = 0;

	uint64_t evicted_new[POWER_LARGEST];
	uint64_t evicted_diff = 0;
	uint64_t evicted_max  = 0;
	unsigned int highest_slab = 0;
	unsigned int total_pages[POWER_LARGEST];

	int i;
	int source = 0;
	int dest = 0;

	static rel_time_t next_run;

	if(current_time >= next_run)
		next_run = current_time + 10;
	else 
		return 0;

	item_stats_evictions(evicted_new);
	pthread_mutex_lock(&cache_lock);
	//获取每个slab class的页数
	for(i = POWER_SMALLEST; i < power_largest; i ++)
		total_pages[i] = slabclass[i].slabs;

	pthread_mutex_unlock(&cache_lock);
	for(i = POWER_SMALLEST; i < power_largest; i ++){
		//计算与上一次扫描的item差异
		evicted_diff = evicted_new[i] - evicted_old[i];
		if(evicted_diff == 0 && total_pages[i] > 2){ //item无变化，且至少有两页
			slab_zeroes[i]++;
			if(source == 0 && slab_zeroes[i] >= 3)
				source = i;
		}
		else{ //有变化，说明slab class是活跃的
			slab_zeroes[i] = 0;
			if (evicted_diff > evicted_max){
				evicted_max = evicted_diff;
				highest_slab = i;
			}
		}

		//进行活跃保存
		evicted_old[i] = evicted_new[i];
	}

	if(slab_winner != 0 && slab_winner == highest_slab){ //检测变化一直为最大的slab class,就会设置为dest
		slab_wins ++;
		if(slab_wins >= 3)
			dest = slab_winner;
	}
	else{
		slab_wins = 1;
		slab_winner = highest_slab;
	}

	//选定转移的源slab和目的slab
	if(source && dest){
		*src = source;
		*dst = dest;
		return 1;
	}

	return 0;
}


static void* slab_maintenance_thread(void* arg)
{
	int src, dest;

	while(do_run_slab_thread){
		if(settings.slab_automove){
			if(slab_automove_decision(&src, &dest) == 1){ //判断是否需要rebalance
				slabs_reassign(src, dest); //进行rebalance信号通告
			}
			sleep(1);
		}
		else
			Sleep(5);
	}

	return NULL;
}

static void* slab_rebalance_thread(void* arg)
{
	int was_busy = 0;
	mutex_lock(&slabs_rebalance_lock);

	while(do_run_slab_rebalance_thread){
		if(slab_rebalance_signal == 1){
			if(slab_rebalance_start() < 0) //开始rebalance,如果启动失败，放弃本次rebalance
				slab_rebalance_signal = 0;

			was_busy = 0;
		}
		else if(slab_rebalance_signal && slab_rebal.slab_start != NULL)
			was_busy = slab_rebalance_move();  //进行内存转移

		if(slab_rebal.done) //rebalance
			slab_rebalance_finish();
		else if(was_busy)
			usleep(50);

		//没有rebalance在处理，进行下一次rebanlance的信号等待
		if(slab_rebalance_signal == 0)
			 pthread_cond_wait(&slab_rebalance_cond, &slabs_rebalance_lock);
	}

	return NULL;
}

//挑选可以进行rebanlance的源，一般从大序号查到小序号，因为item size大的概率比item小的概率小
static int slabs_ressign_pick_any(int dst)
{
	static int cur = POWER_SMALLEST - 1;
	int tries = power_largest - POWER_SMALLEST + 1;
	for(; tries > 0; tries --){
		cur ++;
		if(cur > power_largest)
			cur = POWER_SMALLEST;

		if(cur == dst)
			continue;

		if(slabclass[cur].slabs > 1)
			return cur;
	}

	return -1;
}

static enum reassign_result_type do_slabs_reassign(int src, int dst)
{
	//已经在rebalance
	if (slab_rebalance_signal != 0)
		return REASSIGN_RUNNING;

	if(src == dst)
		return REASSIGN_SRC_DST_SAME;

	//转移源没有指定，从slab classes中重新选择一个
	if(src == -1)
		src = slabs_ressign_pick_any(dst);

	if(src < POWER_SMALLEST || src > power_largest ||
		dst < POWER_SMALLEST || dst > power_largest)
		return REASSIGN_BADCLASS;

	if(slabclass[src].slabs < 2)
		return REASSIGN_NOSPARE;

	//设置转移的参数
	slab_rebal.s_clsid = src;
	slab_rebal.d_clsid = dst;

	//设置rebalance启动的标识
	slab_rebalance_signal = 1;
	pthread_cond_signal(&slab_rebalance_cond);

	return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(int src, int dst)
{
	enum reassign_result_type ret;
	if(pthread_mutex_trylock(&slabs_rebalance_lock) != 0)
		return REASSIGN_RUNNING;

	ret = do_slabs_reassign(src, dst);
	pthread_mutex_unlock(&slabs_rebalance_lock);
	return ret;
}

void slabs_rebalancer_pause()
{
    pthread_mutex_lock(&slabs_rebalance_lock);
}

void slabs_rebalancer_resume() 
{
	pthread_mutex_unlock(&slabs_rebalance_lock);
}

static pthread_t maintenance_tid;
static pthread_t rebalance_tid;

int start_slab_maintenance_thread()
{
	int ret;
	slab_rebalance_signal = 0;
	slab_rebal.slab_start = NULL;

	//获取一次内存转移的item个数
	char* env = getenv("MEMCACHED_SLAB_BULK_CHECK");
	if(env != NULL){
		slab_bulk_check = atoi(env);
		if (slab_bulk_check == 0)
			slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
	}

	if(pthread_cond_init(&slab_rebalance_cond, NULL) != 0){
		fprintf(stderr, "Can't intiialize rebalance condition\n");
		return -1;
	}

	pthread_mutex_init(&slabs_rebalance_lock, NULL);

	//启动内存转移判断线程
	if((ret = pthread_create(&maintenance_tid, NULL, slab_maintenance_thread, NULL)) != 0){
		fprintf(stderr, "Can't create slab maint thread: %s\n", strerror(ret));
		return -1;
	}
	//启动rebalance线程
	if ((ret = pthread_create(&rebalance_tid, NULL, slab_rebalance_thread, NULL)) != 0) {
		fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
		return -1;
	}
}

void stop_slab_maintenance_thread(void) 
{
	mutex_lock(&cache_lock);

	do_run_slab_thread = 0;
	do_run_slab_rebalance_thread = 0;

	pthread_cond_signal(&maintenance_cond);
	pthread_mutex_unlock(&cache_lock);

	pthread_join(maintenance_tid, NULL);
	pthread_join(rebalance_tid, NULL);
}




