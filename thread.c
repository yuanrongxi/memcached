#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>

#ifdef __sun
#include <atomic.h>
#endif

#define	ITEMS_PER_ALLOC 64


typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item
{
	int						ifd;
	enum conn_states		init_state;
	int						event_flags;
	int						read_buffer_size;
	enum network_transport	transport;
	CQ_ITEM*				next;
};

typedef struct conn_queue CQ;
struct conn_queue
{
	CQ_ITEM* head;
	CQ_ITEM* tail;
	pthread_mutex_t lock;
};

pthread_mutex_t cache_lock;

pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

#if !defined(HAVE_GCC_ATOMICS) && !defined(__sun)
pthread_mutex_t atomics_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

static pthread_mutex_t stats_lock;

static CQ_ITEM* cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

static pthread_mutex_t *item_locks;
static uint32_t imtem_lock_count;

static unsigned int item_lock_hashpower;

#define hashsize(n) ((unsigned long int) 1<< n)
#define hashmask(n) (hashsize(n) - 1)

static pthread_mutex_t item_global_lock;
static pthread_key_t item_lock_type_key;

static LIBEVENT_DISPATCHER_THREAD dispatcher_thread;

static LIBEVENT_THREAD* thread;

static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;

static void thread_libevent_process(int fd, short which, void* arg);

unsigned short refcount_incr(unsigned short* refcount)
{
#ifdef HAVE_GCC_ATOMICS
	return __sync_add_and_fetch(refcount, 1);
#elif (__sun)
	return atomic_inc_ushort_nv(refcount);
#else
	unsigned short res;
	mutex_lock(&atomics_mutex);
	(*refcount)++;
	res = *refcount;
	mutex_unlock(&atomics_mutex);
	return res;
#endif
}

unsigned short refcount_decr(unsigned short* refcount)
{
#ifdef HAVE_GCC_ATOMICS
	return __sync_sub_and_fetch(refcount, 1);
#elif (__sun)
	return atomic_dec_ushort_nv(refcount);
#else
	unsigned short res;
	mutex_lock(&atomics_mutex);
	(*refcount)--;
	res = *refcount;
	mutex_unlock(&atomics_mutex);
	return res;
#endif
}

void item_lock_global()
{
	mutex_lock(&item_global_lock);
}

void item_unlock_global()
{
	mutex_unlock(&item_global_lock);
}

void item_lock(uint32_t hv)
{
	uint8_t* lock_type = pthread_getspecific(item_lock_type_key); 
	if (likely(*lock_type == ITEM_LOCK_GRANULAR))
		mutex_lock(&item_locks[hv & hashmask(item_lock_hashpower)]);
	else
		mutex_lock(&item_global_lock);
}

void item_try_lock(uint32_t hv)
{
	pthread_mutex_t *lock = &item_locks[hv & hashmask(item_lock_hashpower)];
	if (pthread_mutex_trylock(lock) == 0)
		return lock;

	return NULL;
}

void item_trylock_unlock(void *lock) 
{
	mutex_unlock((pthread_mutex_t *) lock);
}

void item_unlock(uint32_t hv)
{
	uint8_t *lock_type = pthread_getspecific(item_lock_type_key);
	if (likely(*lock_type == ITEM_LOCK_GRANULAR))
		mutex_unlock(&item_locks[hv & hashmask(item_lock_hashpower)]);
	else
		mutex_unlock(&item_global_lock);
}

static void wait_for_thread_registration(int nthreads) 
{
	while (init_count < nthreads)
		pthread_cond_wait(&init_cond, &init_lock);
}

static void register_thread_initialized(void) 
{
	pthread_mutex_lock(&init_lock);
	init_count++;
	pthread_cond_signal(&init_cond);
	pthread_mutex_unlock(&init_lock);
}

void switch_item_lock_type(enum item_lock_types type)
{
	char buf[1];
	int i;

	switch(type){
	case ITEM_LOCK_GRANULAR:
		buf[0] = 'l';
		break;

	case ITEM_LOCK_GLOBAL:
		buf[1] = 'g';
		break;

	default:
		fprintf(stderr, "Unknown lock type: %d\n", type);
		assert(1 == 0);
		break;
	}

	pthread_mutex_lock(&init_lock);
	init_count = 0;
	for(i = 0; i < settings.num_threads; i ++){
		if (write(threads[i].notify_send_fd, buf, 1) != 1)
			perror("Failed writing to notify pipe");
	}

	wait_for_thread_registration(settings.num_threads);
	pthread_mutex_unlock(&init_lock);
}

static void cq_init(CQ* cq)
{
	pthread_mutex_init(&cq->lock, NULL);
	cq->tail = NULL;
	cq->head = NULL;
}

static CQ_ITEM* cq_pop(CQ* cq)
{
	CQ_ITEM *item;

	pthread_mutex_lock(&cq->lock);
	item = cq->head;
	if (NULL != item){
		cq->head = item->next;
		if(NULL == cq->head)
			cq->tail = NULL;
	}
	pthread_mutex_unlock(&cq->lock);

	return item;
}

static void cq_push(CQ* cq, CQ_ITEM* item)
{
	item->next = NULL;

	pthread_mutex_lock(&cq->lock);
	if(cq->tail == NULL)
		cq->head = item;
	else
		cq->tail->next = item;
	cq->tail = item;

	pthread_mutex_unlock(&cq->lock);
}

static CQ_ITEM* cq_new()
{
	CQ_ITEM* item = NULL;
	pthread_mutex_lock(&cqi_freelist_lock);
	if(cqi_freelist){
		item = cqi_freelist;
		cqi_freelist = item->next;
	}
	pthread_mutex_unlock(&cqi_freelist_lock);

	if(NULL == item){
		int i;
		item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
		if (NULL == item) {
			STATS_LOCK();
			stats.malloc_fails++;
			STATS_UNLOCK();
			return NULL;
		}

		for (i = 2; i < ITEMS_PER_ALLOC; i++)
			item[i - 1].next = &item[i];

		pthread_mutex_lock(&cqi_freelist_lock);
		item[ITEMS_PER_ALLOC - 1].next = cqi_freelist; //NULL
		cqi_freelist = &item[1];
		pthread_mutex_unlock(&cqi_freelist_lock);
	}

	return item;
}

static void cqi_free(CQ_ITEM *item)
{
	pthread_mutex_lock(&cqi_freelist_lock);
	item->next = cqi_freelist;
	cqi_freelist = item;
	pthread_mutex_unlock(&cqi_freelist_lock);
}

static void create_worker(void* (*func)(void*), void* arg)
{
	pthread_t       thread;
	pthread_attr_t  attr;
	int             ret;


	pthread_attr_init(&attr);

	ret = pthread_create(&thread, &attr, func, arg);
	if(ret != 0){
		fprintf(stderr, "Can't create thread: %s\n",
			strerror(ret));
		exit(1);
	}
}

void accept_new_conns(const bool do_accept)
{
	pthread_mutex_lock(&conn_lock);
	do_accept_new_conns(do_accept);
	pthread_mutex_unlock(&conn_lock);
}

/****************************** LIBEVENT THREADS *****************************/
static void setup_thread(LIBEVENT_THREAD* me)
{
	me->base = event_init();
	if(!me->base){
		fprintf(stderr, "Can't allocate event base\n");
		exit(1);
	}

	event_set(&me->notify_event, me->notify_receive_fd, EV_READ | EV_PERSIST, thread_libevent_process, me);
	event_base_set(me->base, &me->notify_event);

	if (event_add(&me->notify_event, 0) == -1) {
		fprintf(stderr, "Can't monitor libevent notify pipe\n");
		exit(1);
	}

	me->new_conn_queue = malloc(sizeof(struct conn_queue));
	if (me->new_conn_queue == NULL) {
		perror("Failed to allocate memory for connection queue");
		exit(EXIT_FAILURE);
	}

	cq_init(me->new_conn_queue);
	if(pthread_mutex_init(&me->stats.mutex, NULL) != 0){
		perror("Failed to initialize mutex");
		exit(EXIT_FAILURE);
	}
	//建立一个suffix POOL
	me->suffix_cache = cache_create("suffix", SUFFIX_SIZE, sizeof(char*), NULL, NULL);

	if (me->suffix_cache == NULL) {
		fprintf(stderr, "Failed to create suffix cache\n");
		exit(EXIT_FAILURE);
	}
}

static void* worker_libevent(void* arg)
{
	LIBEVENT_THREAD *me = arg;
	me->item_lock_type = ITEM_LOCK_GRANULAR;
	pthread_setspecific(item_lock_type_key, &me->item_lock_type);

	register_thread_initialized();
	 event_base_loop(me->base, 0);

	 return NULL;
}





