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

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;

typedef unsigned long int ub4;
typedef unsigned char ub1;

unsigned int hashpower = HASHPOWER_DEFAULT; //16为默认的hash长度？

#define hashsize(n) ((ub4)1 << n)
#define hashmask(n) (hashsize(n) - 1)

static item** primary_hashtable = 0;

static item** old_hashtable = 0;

static unsigned int hash_items = 0;
static bool expanding = false;
static bool started_expanding = false;

static unsigned int expand_bucket = 0;

void assoc_init(const int hashtable_init)
{
	if(hashtable_init)
		hashpower = hashtable_init;

	//创建一个长度为1 << hashpower的数组,如果是hashpower = 16，那么就是开辟64K长度的数组
	primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
	if(primary_hashtable == NULL){
		fprintf(stderr, "Failed to init hashtable.\n");
		exit(EXIT_FAILURE);
	}

	//加入统计信息
	STATS_LOCK();
	stats.hash_power_level = hashpower;
	stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
	STAT_UNLOCK();
}

//通过KEY和 hv找到对应的item
item* assoc_find(const char* key, const size_t nkey, const uint32_t hv)
{
	item* it;
	unsigned int oldbucket;

	//hash table正在expanding且KEY是还未转移到新hash table上的位置
	if(expanding && (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
		it = old_hashtable[oldbucket];
	else
		it = primary_hashtable[hv & hashmask[hashpower]];

	item *ret = NULL;
	int depth = 0;
	while(it){ //在hv对应list上找,进行item key的比较
		if(nkey == it->nkey && memcmp(key, ITEM_key(it), nkey) == 0){
			ret = it;
			break;
		}

		it = it->h_next;
		++depth;
	}

	MEMCACHED_ASSOC_FIND(key, nkey, depth);
}

//查找key的前一个item
static item** _hashitem_before(const char* key, const size_t nkey, const uint32_t hv)
{
	item** pos;
	unsigned int oldbucket;
	
	//hash table正在expanding且KEY是还未转移到新hash table上的位置
	if(expanding && (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
		pos = &old_hashtable[oldbucket];
	else
		pos = &primary_hashtable[hv & hashmask(hashpower)];

	//查找hv对应的list
	while(*pos && (nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))
		pos = pos->h_next;

	return pos;
}

//进行rehash
static void assoc_expand()
{
	old_hashtable = primary_hashtable;

	primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));
	if(primary_hashtable){
		if(settings.verbose) //日志记录
			fprintf(stderr, "Hash table expansion starting\n");
		//hash size扩大1倍
		hashpower ++;
		//设置为正在rehash的标识
		expanding = true;
		expand_bucket = 0;

		//记录统计信息
		STATS_LOCK();
		stats.hash_power_level = hashpower;
		stats.hash_bytes += hashsize(hashpower) * sizeof(void *);
		stats.hash_is_expanding = 1;
		STATS_UNLOCK();
	}
	else
		primary_hashtable = old_hashtable;
}

static void assoc_start_expand()
{
	if(started_expanding)
		return;

	started_expanding = true;
	//发送信号到maintenance_cond
	pthread_cond_signal(&maintenance_cond);
}

int assoc_insert(item* it, const uint32_t hv)
{
	unsigned int oldbucket = (hv & hashmask(hashpower - 1));

	if(expanding && oldbucket > expand_bucket){ //在旧的HASH TABLE上进行添加
		it->h_next = old_hashtable[oldbucket];
		old_hashtable[oldbucket] = it;
	}
	else{
		oldbucket = hv & hashmask(hashpower);
		it->h_next = primary_hashtable[oldbucket];
		primary_hashtable[oldbucket] = it;
	}

	hash_items ++;
	//对rehash进行判断
	if(!expanding && hash_items > (hashsize(hashpower) * 3) / 2)) //平均每个bucket至少存储了1.5个item,需要扩大hash table
		assoc_start_expand();

	MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);

	return 1;
}

void assoc_delete(const char* key, const size_t nkey, const uint32_t hv)
{
	item **before = _hashitem_before(key, nkey, hv);
	if(*before != NULL){
		item* nxt;
		hash_items --;

		MEMCACHED_ASSOC_DELETE(key, nkey, hash_items);
		nxt = (*before)->h_next;
		*before = nxt;
		return ;
	}

	assert(*before != NULL);
}

static volatile int do_run_maintenance_thread = 1;
#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void* arg)
{
	while(do_run_maintenance_thread){
		int ii = 0;

		item_lock_global();
		mutex_lock(&cache_lock);

		for(ii = 0; ii < hash_bulk_move && expanding; ++ ii){
			item* it, *next;
			int bucket;
			
			for(it = old_hashtable[expand_bucket]; NULL != it; it = next){
				next = it->h_next;
				//重新计算key对应bucket的在新的HASH TABLEDE位置
				bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
				it->h_next = primary_hashtable[bucket];
				primary_hashtable[bucket] = it;
			}
			//清空旧的hash table对应的桶的kv
			old_hashtable[expand_bucket] = NULL;

			expand_bucket ++;
			if(expand_bucket == hashsize(hashpower - 1)){ //KV转移结束
				expanding = false;
				free(old_hashtable);

				//更新统计信息
				STATS_LOCK();
				stats.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
				stats.hash_is_expanding = 0;
				STATS_UNLOCK();
				if(settings.verbose > 1)
					fprintf(stderr, "Hash table expansion done");
			}
		}

		mutex_unlock(&cache_lock);
		item_unlock_global();

		if(!expanding){
			switch_item_lock_type(ITEM_LOCK_GRANULAR);
			slabs_rebalancer_resume();

			mutex_lock(&cache_lock);
			started_expanding = false;
			//进行对maintenance_cond的信号等待,如果有信号来，说明有新的expanding
			pthread_cond_wait(&maintenance_cond, &cache_lock);

			mutex_unlock(&cache_lock);
			slabs_rebalancer_pause();
			switch_item_lock_type(ITEM_LOCK_GLOBAL);

			mutex_lock(&cache_lock);
			//进行hash table 扩大
			assoc_expand();
			mutex_unlock(&cache_lock);
		}
	}

	return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread()
{
	int ret;
	char* env = getenv("MEMCACHED_HASH_BULK_MOVE");
	if(env != NULL){
		hash_bulk_move = atoi(env);
		if(hash_bulk_move == 0)
			hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
	}

	//启动expanding thread
	ret = pthread_create(&maintenance_tid, NULL, assoc_maintenance_thread, NULL);
	if(ret != 0){
		fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
		return -1;
	}
	return 0;
}

void stop_assoc_maintenance_thread()
{
	mutex_lock(&cache_lock);
	do_run_maintenance_thread = 0;
	//发送一个信号让线程体退出
	pthread_cond_signal(&maintenance_cond);
	mutex_unlock(&cache_lock);
	//等待进程结束
	pthread_join(maintenance_tid, NULL);
}



