#include "memcached.h"
#include "items.h"
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
#include <time.h>
#include <assert.h>
#include <unistd.h>

static void item_link_q(item* it);
static void item_unlink_q(item* it);

#define LARGEST_ID POWER_LARGEST

typedef struct
{
	uint64_t	evicted;
	uint64_t	evicted_nozero;
	rel_time_t	evicted_time;
	uint64_t	reclaimed;
	uint64_t	outofmemory;
	uint64_t	tailrepairs;
	uint64_t	expired_unfetched;
	uint64_t	evicted_unfetched;
	uint64_t	crawler_reclaimed;
}itemstats_t;

static item* heads[LARGEST_ID];
static item* tails[LARGEST_ID];
static crawler crawlers[LARGEST_ID];
static itemstats_t itemstats[LARGEST_ID];
static unsigned int sizes[LARGEST_ID];

static int crawler_count = 0;
static volatile int do_run_lru_crawler_thread = 0;
static int lru_crawler_initialized = 0;
static pthread_mutex_t lru_crawler_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t lru_crawler_cond = PTHREAD_COND_INITIALIZER;

void item_stats_reset()
{
	mutex_lock(&cache_lock);
	memset(itemstats, 0, sizeof(itemstats));
	mutex_unlock(&cache_lock);
}

uint64_t get_cas_id()
{
	static uint64_t cas_id = 0;
	++ cas_id;
}

#if 0
# define DEBUG_REFCNT(it,op) \
	fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
	it, op, it->refcount, \
	(it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
	(it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

//构建头信息，主要是suffix信息
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix)
{
	*nsuffix = (uint8_t)snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
	return sizeof(item) + nkey + *nsuffix + nbytes;
}

item* do_item_alloc(char* key, const size_t nkey, const int flags, const rel_time_t exptime, int nbytes, const uint32_t cur_hv)
{
	uint8_t nsuffix;
	item* it = NULL;
	char suffix[40];

	size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);
	if(settings.use_cas) //头要加入一个cas
		ntotal += sizeof(uint64_t);

	//获得长队对应的slab class序号
	unsigned int id = slabs_clsid(ntotal);
	if(id == 0) //无效的slab class,有可能ntotal大于所有slab item size
		return 0;

	mutex_lock(&cache_lock);

	int tries = 5;
	int tried_alloc = 0;
	item *search;
	voi* hold_lock = NULL;
	rel_time_t oldest_live = settings.oldest_live;

	search = tails[id];
	for(; tries > 0 && search != NULL; tries --, search = search->prev){
		if(search->nbtyes == 0 && search->nkey == 0 && search->it_flags == 1){
			tries ++;
			continue;
		}

		uint32_t hv = hash(ITEM_key(search), search->nkey);
		if(hv == cur_hv || (hold_lock == item_trylock(hv)) == NULL)
			continue;

		//只是为了修复引用计数泄露的问题
		if(recount_incr(&search->refcount) != 2){
			refcount_decr(&search->refcount);

			if(settings.tail_repair_time && search->time + settings.tail_repair_time < current_time){
				itemstats[id].tailrepairs ++;
				search->refcount = 1;
				do_item_unlink_nolock(search, hv);
			}

			if(hold_lock)
				item_trylock_unlock(hold_lock);

			continue;
		}

		//这个search 是直接item 过期，对item进行删除关联，并用做新的kv的item
		if((search->exptime != 0 && search->exptime < current_time) || (search->time <= oldest_live && oldest_live <= current_time)){
			itemstats[id].reclaimed ++;
			if((search->it_flags & ITEM_FETCHED) == 0)
				itemstats[id].expired_unfetched ++;

			it = search;
			slabs_adjust_mem_requested(it->slabs_clsid, ITEM_ntotal(it), ntotal);
			do_item_unlink_nolock(it, hv);
			it->slabs_clsid = 0;
		}
		else if((it = slabs_alloc(ntotal, id)) == NULL){ //没有合适的替换item,从slabs从新分配，假如失败
			tried_alloc = 1;
			if(settings.evict_to_free == 0) //内存不足
				itemstats[id].outofmemory ++;
			else { 
				itemstats[id].evicted ++;
				itemstats[id].evicted_time = current_time - search->time;
				if(search->exptime != 0)
					itemstats[id].evicted_nozero ++;

				if(search->it_flags & ITEM_FETCHED == 0)
					itemstats[id].evicted_unfetched ++;

				it = search;
				slabs_adjust_mem_requested(it->slabs_clsid, ITEM_ntotal(it), ntotal);
				do_item_unlink_nolock(it, hv);
				it->slabs_clsid = 0;

				//ntotal对应的slab class内存不够，发起对内存的rebalance操作
				if (settings.slab_automove == 2)
					slabs_reassign(-1, id);
			}
		}

		refcount_decr(&search->refcount);

		if(hold_lock)
			item_trylock_unlock(hold_lock);

		break;
	}

	//没有合适的item但又没有slabs alloc,调动一次slab_alloc
	if(!tried_alloc && (tries == 0 || search == NULL))
		it = slabs_alloc(ntotal, id);

	if(it == NULL){ //对统计记录out of memory
		itemstats[id].outofmemory++;
		mutex_unlock(&cache_lock);
		return NULL;
	}

	assert(it->slabs_clsid == 0);
	assert(it != heads[id]);

	it->refcount = 1;
	mutex_unlock(&cache_lock);
	it->next = it->prev = it->h_next = NULL;
	it->slabs_clsid = id;

	DEBUG_REFCNT(it, '*');
	it->it_flags = settings.use_cas ? ITEM_CAS : 0;
	it->nkey = nkey;
	it->nbtyes = nbytes;
	memcpy(ITEM_key(it), key, nkey);
	it->exptime = exptime;	//指定生命周期
	//设置suffix
	memcpy(ITEM_key(it), suffix, (size_t)nsuffix);
	it->nsuffix = nsuffix;

	return it;
}

void item_free(item* it)
{
	size_t ntotal = ITEM_ntotal(it);
	unsigned int clsid;
	
	assert((it->it_flags & ITEM_LINKED) == 0);
	assert(it != heads[it->slabs_clsid]);
	assert(it != tails[it->slabs_clsid]);
	assert(it->refcount == 0);

	clsid = it->slabs_clsid;
	it->slabs_clsid = 0;
	
	DEBUG_REFCNT(it, 'F');

	//将item 的内存归还给slab page
	slabs_free(it, ntotal, clsid);
}

bool item_size_ok(const size_t nkey, const int flags, const int nbytes)
{
	char prefix[40];
	uint8_t nsuffix;

	size_t ntotal = item_make_header(nkey + 1, flags, nbytes, prefix, &nsuffix);
	if(settings.use_cas) //检查CAS
		ntotal += sizeof(uint64_t);

	return slabs_clsid(ntotal) != 0;
}

static void item_link_q(item* it)
{
	item** head, **tail;

	assert(it->slabs_clsid < LARGEST_ID);
	assert((it->it_flags & ITEM_SLABBED) == 0);

	head = &heads[it->slabs_clsid];
	tail = &tails[it->slabs_clsid];

	assert(it != *head);
	assert((*head && *tail) || (*head == 0 && *tail == 0));

	it->prev = 0;
	it->next = *head;
	if(it->next)
		it->next->prev = it;
	*head = it;
	if(*tail == 0)
		*tail = it;

	sizes[it->slabs_clsid] ++;
}

static void item_unlink_q(item* it)
{
	item **head,**tail;
	assert(it->slabs_clsid < LARGEST_ID);
	head = &heads[it->slabs_clsid];
	tail = &tails[it->slabs_clsid];

	if(*head == it){
		assert(it->prev == 0);
		*head = it->next;
	}

	if(*tail == it){
		assert(it->next == 0);
		*tail = it->prev;
	}

	assert(it->next != it);
	assert(it->prev != it);

	if(it->next)
		it->next->prev = it->prev;

	if(it->prev)
		it->prev->next = it->next;

	sizes[it->slabs_clsid]--;
}

int do_item_link(item* it, const uint32_t hv)
{
	MEMCACHED_ITEM_LINK(ITEM_key(it), it->nkey, it->nbytes);
	assert((it->it_flags & (ITEM_LINKED|ITEM_SLABBED)) == 0);

	mutex_lock(&cache_lock);
	it->it_flags |= ITEM_LINKED;
	it->time = current_time;

	STATS_LOCK();
	stats.curr_bytes += ITEM_ntotal(it);
	stats.curr_items ++;
	stats.total_items ++;
	STATS_UNLOCK();

	ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
	assoc_insert(it, hv); //插入到hash cache中
	item_link_q(it);	  //建立item slab关联
	refcount_incr(&it->refcount);
	mutex_unlock(&cache_lock);

	return 1;
}

void do_item_unlink_nolock(item* it, const uint32_t hv)
{
	MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
	if((it->it_flags & ITEM_LINKED) != 0){

	}
}





