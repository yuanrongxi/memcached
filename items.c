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

//����ͷ��Ϣ����Ҫ��suffix��Ϣ
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
	if(settings.use_cas) //ͷҪ����һ��cas
		ntotal += sizeof(uint64_t);

	//��ó��Ӷ�Ӧ��slab class���
	unsigned int id = slabs_clsid(ntotal);
	if(id == 0) //��Ч��slab class,�п���ntotal��������slab item size
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

		//ֻ��Ϊ���޸����ü���й¶������
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

		//���search ��ֱ��item ���ڣ���item����ɾ���������������µ�kv��item
		if((search->exptime != 0 && search->exptime < current_time) || (search->time <= oldest_live && oldest_live <= current_time)){
			itemstats[id].reclaimed ++;
			if((search->it_flags & ITEM_FETCHED) == 0)
				itemstats[id].expired_unfetched ++;

			it = search;
			slabs_adjust_mem_requested(it->slabs_clsid, ITEM_ntotal(it), ntotal);
			do_item_unlink_nolock(it, hv);
			it->slabs_clsid = 0;
		}
		else if((it = slabs_alloc(ntotal, id)) == NULL){ //û�к��ʵ��滻item,��slabs���·��䣬����ʧ��
			tried_alloc = 1;
			if(settings.evict_to_free == 0) //�ڴ治��
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

				//ntotal��Ӧ��slab class�ڴ治����������ڴ��rebalance����
				if (settings.slab_automove == 2)
					slabs_reassign(-1, id);
			}
		}

		refcount_decr(&search->refcount);

		if(hold_lock)
			item_trylock_unlock(hold_lock);

		break;
	}

	//û�к��ʵ�item����û��slabs alloc,����һ��slab_alloc
	if(!tried_alloc && (tries == 0 || search == NULL))
		it = slabs_alloc(ntotal, id);

	if(it == NULL){ //��ͳ�Ƽ�¼out of memory
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
	it->exptime = exptime;	//ָ����������
	//����suffix
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

	//��item ���ڴ�黹��slab page
	slabs_free(it, ntotal, clsid);
}

bool item_size_ok(const size_t nkey, const int flags, const int nbytes)
{
	char prefix[40];
	uint8_t nsuffix;

	size_t ntotal = item_make_header(nkey + 1, flags, nbytes, prefix, &nsuffix);
	if(settings.use_cas) //���CAS
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
	assoc_insert(it, hv); //���뵽hash cache��
	item_link_q(it);	  //����item slab����
	refcount_incr(&it->refcount);
	mutex_unlock(&cache_lock);

	return 1;
}

void do_item_unlink(item* it, const uint32_t hv)
{
	MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
	mutex_lock(&cache_lock);

	if((it->it_flags & ITEM_LINKED) != 0){
		it->it_flags &= ~ITEM_LINKED;
		STATS_LOCK();
		stats.curr_bytes -= ITEM_ntotal(it);
		stats.curr_items -= 1;
		STATS_UNLOCK();

		//��hash table��ɾ��
		assoc_delete(ITEM_key(it), it->nkey, hv);
		//���������ϵ
		item_unlink_q(it);
		do_item_remove(it);
	}

	 mutex_unlock(&cache_lock);
}

void do_item_unlink_nolock(item* it, const uint32_t hv)
{
	MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
	if((it->it_flags & ITEM_LINKED) != 0){
		it->it_flags &= ~ITEM_LINKED;
		
		STATS_LOCK();
		stats.curr_bytes -= ITEM_ntotal(it);
		stats.curr_items -= 1;
		STATS_UNLOCK();

		assoc_delete(ITEM_key(it), it->nkey, hv);
		item_unlink_q(it);
		do_item_remove(it);
	}
}

void do_item_remove(item* it)
{
	MEMCACHED_ITEM_REMOVE(ITEM_key(it), it->nkey, it->nbytes);
	assert(it->it_flags & ITEM_SLABBED == 0);
	assert(it->refcount > 0);

	//�������ü���
	if(refcount_decr(&it->refcount) == 0)
		item_free(it);
}

void do_item_update(item* it)
{
	MEMCACHED_ITEM_UPDATE(ITEM_key(it), it->nkey, it->nbytes);
	if(it->time < current_time - ITEM_UPDATE_INTERVAL){ //�ϴ�ʱ�̺����ʱ������1���ӣ��Ͳ���д��
		assert(it->it_flags & ITEM_SLABBED == 0);

		mutex_lock(&cache_lock);
		if((it->it_flags & ITEM_LINKED) != 0){ //���¸����������ڣ���ֹ����
			item_unlink_q(it);
			it->time = current_time;
			item_link_q(it);
		}
		mutex_unlock(&cache_lock);
	}
}

int do_item_replace(item *it, item *new_it, const uint32_t hv)
{
	MEMCACHED_ITEM_REPLACE(ITEM_key(it), it->nkey, it->nbytes, ITEM_key(new_it), new_it->nkey, new_it->nbytes);
	assert(it->it_flags & ITEM_SLABBED == 0);
	//ɾ���ϵ�item
	do_item_unlink(it, hv);
	//����µ�item
	return do_item_link(new_it);
}

char *do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes) {
	unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
	char *buffer;
	unsigned int bufcurr;
	item *it;
	unsigned int len;
	unsigned int shown = 0;
	char key_temp[KEY_MAX_LENGTH + 1];
	char temp[512];

	it = heads[slabs_clsid];

	buffer = malloc((size_t)memlimit);
	if (buffer == 0) return NULL;
	bufcurr = 0;

	while (it != NULL && (limit == 0 || shown < limit)) {
		assert(it->nkey <= KEY_MAX_LENGTH);
		if (it->nbytes == 0 && it->nkey == 0) {
			it = it->next;
			continue;
		}
		/* Copy the key since it may not be null-terminated in the struct */
		strncpy(key_temp, ITEM_key(it), it->nkey);
		key_temp[it->nkey] = 0x00; /* terminate */
		len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n", key_temp, it->nbytes - 2, (unsigned long)it->exptime + process_started);
		if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
			break;

		memcpy(buffer + bufcurr, temp, len);
		bufcurr += len;
		shown++;
		it = it->next;
	}

	memcpy(buffer + bufcurr, "END\r\n", 6);
	bufcurr += 5;

	*bytes = bufcurr;
	return buffer;
}

//ͳ�Ƹ���slab class��item�Ļ��յĴ���(�ڱ�slab class�ռ䲻���ʱ�򣬻�Ա�slab class��һЩ���ڵ�slab������)
void item_stats_evictions(uint64_t *evicted)
{
	int i;
	mutex_lock(&cache_lock);

	for(i = 0; i < LARGEST_ID; i ++)
		evicted[i] = itemstats[i].evicted;

	mutex_unlock(&cache_lock);
}

void do_item_stats_totals(ADD_STAT add_stats, void *c) 
{
	itemstats_t totals;
	memset(&totals, 0, sizeof(itemstats_t));

	int i;
	for (i = 0; i < LARGEST_ID; i++) {
		totals.expired_unfetched += itemstats[i].expired_unfetched;
		totals.evicted_unfetched += itemstats[i].evicted_unfetched;
		totals.evicted += itemstats[i].evicted;
		totals.reclaimed += itemstats[i].reclaimed;
		totals.crawler_reclaimed += itemstats[i].crawler_reclaimed;
	}

	APPEND_STAT("expired_unfetched", "%llu",
		(unsigned long long)totals.expired_unfetched);
	APPEND_STAT("evicted_unfetched", "%llu",
		(unsigned long long)totals.evicted_unfetched);
	APPEND_STAT("evictions", "%llu",
		(unsigned long long)totals.evicted);
	APPEND_STAT("reclaimed", "%llu",
		(unsigned long long)totals.reclaimed);
	APPEND_STAT("crawler_reclaimed", "%llu",
		(unsigned long long)totals.crawler_reclaimed);
}

void do_item_stats(ADD_STAT add_stats, void *c) 
{
	int i;
	for (i = 0; i < LARGEST_ID; i++) {
		if (tails[i] != NULL) {
			const char *fmt = "items:%d:%s";
			char key_str[STAT_KEY_LEN];
			char val_str[STAT_VAL_LEN];
			int klen = 0, vlen = 0;
			if (tails[i] == NULL) {
				/* We removed all of the items in this slab class */
				continue;
			}
			APPEND_NUM_FMT_STAT(fmt, i, "number", "%u", sizes[i]);
			APPEND_NUM_FMT_STAT(fmt, i, "age", "%u", current_time - tails[i]->time);
			APPEND_NUM_FMT_STAT(fmt, i, "evicted",
				"%llu", (unsigned long long)itemstats[i].evicted);
			APPEND_NUM_FMT_STAT(fmt, i, "evicted_nonzero",
				"%llu", (unsigned long long)itemstats[i].evicted_nonzero);
			APPEND_NUM_FMT_STAT(fmt, i, "evicted_time",
				"%u", itemstats[i].evicted_time);
			APPEND_NUM_FMT_STAT(fmt, i, "outofmemory",
				"%llu", (unsigned long long)itemstats[i].outofmemory);
			APPEND_NUM_FMT_STAT(fmt, i, "tailrepairs",
				"%llu", (unsigned long long)itemstats[i].tailrepairs);
			APPEND_NUM_FMT_STAT(fmt, i, "reclaimed",
				"%llu", (unsigned long long)itemstats[i].reclaimed);
			APPEND_NUM_FMT_STAT(fmt, i, "expired_unfetched",
				"%llu", (unsigned long long)itemstats[i].expired_unfetched);
			APPEND_NUM_FMT_STAT(fmt, i, "evicted_unfetched",
				"%llu", (unsigned long long)itemstats[i].evicted_unfetched);
			APPEND_NUM_FMT_STAT(fmt, i, "crawler_reclaimed",
				"%llu", (unsigned long long)itemstats[i].crawler_reclaimed);
		}
	}

	/* getting here means both ascii and binary terminators fit */
	add_stats(NULL, 0, NULL, 0, c);
}

void do_item_stats_sizes(ADD_STAT add_stats, void* c)
{
	const int num_buckets = 32768;
	unsigned int *histogram = calloc(num_buckets, sizeof(int));

	if(histogram != NULL){
		int i;
		for(i = 0; i < LARGEST_ID; i++){
			item* iter = heads[i];
			while(iter){
				int ntotal = ITEM_ntotal(iter);
				int bucket = ntotal / 32;
				if((ntotal % 32) != 0)
					bucket ++;
				if(bucket < num_buckets)
					histogram[bucket]++;

				iter = iter->next;
			}
		}

		for(i = 0; i < num_buckets; i ++){
			if (histogram[i] != 0) {
				char key[8];
				snprintf(key, sizeof(key), "%d", i * 32);
				APPEND_STAT(key, "%u", histogram[i]);
			}
		}

		free(histogram);
	}
	add_stats(NULL, 0, NULL, 0, c);
}

item* do_item_get(const char* key, const size_t nkey, const uint32_t hv)
{
	item* it = assoc_find(key, nkey, hv);
	if(it != NULL){
		refcount_incr(&it->refcount);

		//��rebalance���ڴ�ռ䷶Χ��,ֱ��ɾ����item������Ϊ�˸����rebalance
		if(slab_rebalance_signal && ((void *)it >= slab_rebal.slab_start && (void *)it < slab_rebal.slab_end)){
			do_item_unlink_nolock(it, hv);
			do_item_remove(it);
			it = NULL;
		}
	}

	int was_found = 0;

	if (settings.verbose > 2) {
		int ii;
		if (it == NULL) {
			fprintf(stderr, "> NOT FOUND ");
		} else {
			fprintf(stderr, "> FOUND KEY ");
			was_found++;
		}
		for (ii = 0; ii < nkey; ++ii) {
			fprintf(stderr, "%c", key[ii]);
		}
	}

	if(it != NULL){
		if(settings.oldest_live != 0 && settings.oldest_live <= current_time && it->time <= settings.oldest_live){ //����cacheϵͳ����������
			do_item_unlink(it, hv);
			do_item_remove(it);
			it = NULL;
			if (was_found)
				fprintf(stderr, " -nuked by flush");
		}
		else if(it->exptime != 0 && it->exptime <= current_time){ //kv������������ڽ���
			do_item_unlink(it, hv);
			do_item_remove(it);
			it = NULL;
			if (was_found)
				fprintf(stderr, " -nuked by expire");
		}
		else{
			it->it_flags |= ITEM_FETCHED;
			DEBUG_REFCNT(it, '+');

		}
	}

	if(settings.verbose > 2)
		fprintf(stderr, "\n");

	return it;
}

//������������ʱ��
item* do_item_touch(const char* key, size_t nkey, uint32_t exptime, const uint32_t hv)
{
	item* it = do_item_get(key, nkey, hv);
	if(it != NULL)
		it->exptime = exptime;

	return it;
}

void do_item_flush_expired()
{
	int i;
	item* iter, *next;
	if(settings.oldest_live == 0)
		return ;

	for(i = 0; i < LARGEST_ID; i++){
		for(iter = heads[i]; iter != NULL; iter = next){
			if(iter->time != 0 && iter->time >= settings.oldest_live){
				next = iter->next;
				if(iter->it_flags & ITEM_SLABBED == 0)
					do_item_unlink_nolock(iter, hash(ITEM_key(iter), iter->nkey));
			}
			else
				break;
		}
	}
}

static void crawler_link_q(item* it)
{
	item** head, **tail;
	assert(it->slabs_clsid < LARGEST_ID);
	assert(it->it_flags == 1);
	assert(it->nbytes == 0);

	head = &heads[it->slabs_clsid];
	tail = &tails[it->slabs_clsid];

	assert(*tail != 0);
	assert(it != *tail);
	assert((*head && *tail) || (*head == 0 && *tail == 0));

	it->prev = *tail;
	it->next = 0;
	if(it->prev){
		assert(it->prev->next == 0);
		it->prev->next = it;
	}

	*tail = it;
	if(*head == 0)
		*head = it;
}

static void crawler_unlink_q(item* it)
{
	item** head, **tail;
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
}

static item* crawler_crawl_q(item* it)
{
	item** head, **tail;

	assert(it->it_flags == 1);
	assert(it->nbytes == 0);
	assert(it->slabs_clsid < LARGEST_ID);

	head = &heads[it->slabs_clsid];
	tail = &tails[it->slabs_clsid];

	//�Ѿ�����ǰ�棬��ɾ��item������ϵ
	if(it->prev == 0){
		assert(*head == it);
		if(it->next){
			*head = it->next;
			assert(it->next->prev == it);
			it->next->prev = 0;
		}

		return NULL;
	}

	assert(it->prev != NULL);
	if(it->prev){
		if(*head == it->prev)
			*head = it;

		if(*tail == it)
			*tail = it->prev;

		if(it->next){
			it->prev->next = it->next;
			it->next->prev = it->prev;
		}
		else
			it->prev->next = 0;

		//���Լ���ǰ��һ��λ��
		it->next = it->prev;
		it->prev = it->next->prev;
		it->next->prev = it;

		if(it->prev)
			it->prev->next = it;
	}

	return it->next;
}

static void item_crawler_evaluate(item* search, uint32_t hv, int i)
{
	rel_time_t oldest_live = settings.oldest_live;
	//�������ڽ���
	if((search->exptime != 0 && search->exptime < current_time) || (search->time <= oldest_live && oldest_live <= current_time)){
		itemstats[i].crawler_reclaimed ++;
		
		if(settings.verbose > 1){
			int ii;
			char* key = ITEM_key(search);
			fprintf(stderr, "LRU crawler found an expired item (flags: %d, slab: %d): ", search->it_flags, search->slabs_clsid);
			for(ii = 0; i < search->nkey; ++ii)
				fprintf(stderr, "%c", key[ii]);

			fprintf(stderr, "\n");
		}

		if(search->it_flags & ITEM_FETCHED == 0)
			itemstats[i].expired_unfetched ++;

		//ɾ��kv������ϵ
		do_item_unlink_nolock(search, hv);
		do_item_remove(search);

		assert(search->slabs_clsid == 0);
	}
	else
		refcount_decr(&search->refcount);
}

static void *item_crawler_thread(void* arg)
{
	int i;

	pthread_mutex_lock(&lru_crawler_lock);
	if(settings.verbose > 2)
		fprintf(stderr, "Starting LRU crawler background thread\n");

	while(do_run_lru_crawler_thread){
		pthread_cond_wait(&lru_crawler_cond, &lru_crawler_lock);

		while(crawler_count){
			item* search = NULL;
			void* hold_lock = NULL;

			for(i = 0; i < LARGEST_ID; i++){
				if(crawlers[i].it_flags != 1)
					continue;

				pthread_mutex_lock(&cache_lock);
				search = crawler_crawl_q((item *)&crawlers[i]);
				if(search == NULL || (crawlers[i].remaining && --crawlers[i].remaining < 1)){
					if (settings.verbose > 2)
						fprintf(stderr, "Nothing left to crawl for %d\n", i);

					crawlers[i].it_flags = 0;
					crawler_count --;

					crawler_unlink_q((item *)&crawlers[i]);
					pthread_mutex_unlock(&cache_lock);

					continue;
				}

				//item busy
				uint32_t hv = hash(ITEM_key(search), search->nkey);
				if((hold_lock = item_trylock(hv)) == NULL){
					pthread_mutex_unlock(&cache_lock);
					continue;
				}

				if (refcount_incr(&search->refcount) != 2) {
					refcount_decr(&search->refcount);
					if (hold_lock)
						item_trylock_unlock(hold_lock);
					pthread_mutex_unlock(&cache_lock);
					continue;
				}

				//����������ڻ��ߵݼ����ü���
				item_crawler_evaluate(search, hv, i);

				if (hold_lock)
					item_trylock_unlock(hold_lock);
				pthread_mutex_unlock(&cache_lock);

				if (settings.lru_crawler_sleep)
					usleep(settings.lru_crawler_sleep);
			}
		}

		if(settings.verbose > 2)
			fprintf(stderr, "LRU crawler thread sleeping\n");

		STATS_LOCK();
		stats.lru_crawler_running = false;
		STATS_UNLOCK();
	}
	
	pthread_mutex_unlock(&lru_crawler_lock);
	if(settings.verbose > 2)
		fprintf(stderr, "LRU crawler thread stopping\n");

	return NULL;
}

int stop_item_crawler_thread()
{
	int ret;
	pthread_mutex_lock(&lru_crawler_lock);
	do_run_lru_crawler_thread = 0;
	pthread_cond_signal(&lru_crawler_cond);
	pthread_mutex_unlock(&lru_crawler_lock);

	if ((ret = pthread_join(item_crawler_tid, NULL)) != 0) {
		fprintf(stderr, "Failed to stop LRU crawler thread: %s\n", strerror(ret));
		return -1;
	}
	settings.lru_crawler = false;
}

int start_item_crawler_thread(void)
{
	int ret;

	if(settings.lru_crawler)
		return -1;

	pthread_mutex_lock(&lru_crawler_lock);
	do_run_lru_crawler_thread = 1;
	settings.lru_crawler = true;

	if((ret = pthread_create()) != 0){
		fprintf(stderr, "Can't create LRU crawler thread: %s\n",
			strerror(ret));
		pthread_mutex_unlock(&lru_crawler_lock);
		return -1;
	}

	pthread_mutex_unlock(&lru_crawler_lock);

	return 0;
}

enum crawler_result_type lru_crawler_crawl(char* slabs)
{
	char *b = NULL;
	uint32_t sid = 0;
	uint8_t tocrawl[POWER_LARGEST];
	if (pthread_mutex_trylock(&lru_crawler_lock) != 0) {
		return CRAWLER_RUNNING;
	}
	pthread_mutex_lock(&cache_lock);

	if (strcmp(slabs, "all") == 0) {
		for (sid = 0; sid < LARGEST_ID; sid++) {
			tocrawl[sid] = 1;
		}
	} else {
		for (char *p = strtok_r(slabs, ",", &b);
			p != NULL;
			p = strtok_r(NULL, ",", &b)) {

				if (!safe_strtoul(p, &sid) || sid < POWER_SMALLEST
					|| sid > POWER_LARGEST) {
						pthread_mutex_unlock(&cache_lock);
						pthread_mutex_unlock(&lru_crawler_lock);
						return CRAWLER_BADCLASS;
				}
				tocrawl[sid] = 1;
		}
	}

	for (sid = 0; sid < LARGEST_ID; sid++) {
		if (tocrawl[sid] != 0 && tails[sid] != NULL) {
			if (settings.verbose > 2)
				fprintf(stderr, "Kicking LRU crawler off for slab %d\n", sid);
			crawlers[sid].nbytes = 0;
			crawlers[sid].nkey = 0;
			crawlers[sid].it_flags = 1; /* For a crawler, this means enabled. */
			crawlers[sid].next = 0;
			crawlers[sid].prev = 0;
			crawlers[sid].time = 0;
			crawlers[sid].remaining = settings.lru_crawler_tocrawl;
			crawlers[sid].slabs_clsid = sid;
			crawler_link_q((item *)&crawlers[sid]);
			crawler_count++;
		}
	}
	pthread_mutex_unlock(&cache_lock);
	pthread_cond_signal(&lru_crawler_cond);
	STATS_LOCK();
	stats.lru_crawler_running = true;
	STATS_UNLOCK();
	pthread_mutex_unlock(&lru_crawler_lock);
	return CRAWLER_OK;
}

int init_lru_crawler(void) 
{
	if (lru_crawler_initialized == 0) {
		if (pthread_cond_init(&lru_crawler_cond, NULL) != 0) {
			fprintf(stderr, "Can't initialize lru crawler condition\n");
			return -1;
		}
		pthread_mutex_init(&lru_crawler_lock, NULL);
		lru_crawler_initialized = 1;
	}
	return 0;
}











