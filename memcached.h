#ifndef MEMCACHED_H_
#define MEMCACHED_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <pthread.h>
#include <unistd.h>

#include "protocol_binary.h"
#include "cache.h"

#include "sasl_defs.h"

#include "stats.h"
#include "slabs.h"
#include "assoc.h"
#include "items.h"
#include "trace.h"
#include "hash.h"
#include "util.h"

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

/** Size of an incr buf. */
#define INCR_MAX_STORAGE_LEN 24

#define DATA_BUFFER_SIZE 2048
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

#define SUFFIX_SIZE 24
#define ITEM_LIST_INITIAL 200
#define SUFFIX_LIST_INITIAL 20
#define IOV_LIST_INITIAL 400
#define MSG_LIST_INITIAL 10

#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

#define MIN_BIN_PKT_LENGTH 16
#define BIN_PKT_HDR_WORDS (MIN_BIN_PKT_LENGTH/sizeof(uint32_t))

//slab sizing 定义
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)

#define TAIL_REPAIR_TIME_DEFAULT 0

#define HASHPOWER_DEFAULT 16

#define ITEM_UPDATE_INTERVAL 60

#define ITEM_get_cas(i) (((i)->it_flags & ITEM_CAS) ? (i)->data->cas : 0)

#define ITEM_set_cas(i, v) { \
	if((i)->it_flags & ITEM_CAS){ \
		(i)->data->cas = v; \
	}\
}

//获的item的key其实地址
#define ITEM_key(item) (((char *)&((item)->data)) + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

//获得item suffix的其实地址
#define ITEM_suffix(item) ((char*) &((item)->data) + (item)->nkey + 1 + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

//获得ITEM的data其实地址
#define ITEM_data(item) ((char*) &((item)->data) + (item)->nkey + 1 + (item)->nsuffix + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

//获得ITEM的总长度
#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nkey + 1 + (item)->nsuffix + (item)->nbytes + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

#define APPEND_STAT(name, fmt, val)  append_stat(name, add_stats, c, fmt, val);

#define APPEND_NUM_FMT_STAT(name_fmt, num, name, fmt, val)	\
	klen = snprintf(key_str, STAT_KEY_LEN, name_fmt, num, name); \
	vlen = snprintf(val_str, STAT_VAL_LEN, fmt, val);      \
	add_stats = (key_str, val_str, vlen, c);

#define APPEND_NUM_STAT(num, name, fmt, val) \
	APPEND_NUM_FMT_STAT("%d:%s", num, name, fmt, val)

typedef void (*ADD_STAT)(const char *key, const uint16_t klen,
	const char *val, const uint32_t vlen,
	const void *cookie);

enum conn_states 
{
	conn_listening,  /**< the socket which listens for connections */
	conn_new_cmd,    /**< Prepare connection for next command */
	conn_waiting,    /**< waiting for a readable socket */
	conn_read,       /**< reading in a command line */
	conn_parse_cmd,  /**< try to parse a command from the input buffer */
	conn_write,      /**< writing out a simple response */
	conn_nread,      /**< reading in a fixed number of bytes */
	conn_swallow,    /**< swallowing unnecessary bytes w/o storing */
	conn_closing,    /**< closing this connection */
	conn_mwrite,     /**< writing out many items sequentially */
	conn_closed,     /**< connection is closed */
	conn_max_state   /**< Max state value (used for assertion) */
};

enum bin_substates {
	bin_no_state,
	bin_reading_set_header,
	bin_reading_cas_header,
	bin_read_set_value,
	bin_reading_get_key,
	bin_reading_stat,
	bin_reading_del_header,
	bin_reading_incr_header,
	bin_read_flush_exptime,
	bin_reading_sasl_auth,
	bin_reading_sasl_auth_data,
	bin_reading_touch_key,
};

enum protocol {
	ascii_prot = 3, /* arbitrary value. */
	binary_prot,
	negotiating_prot /* Discovering the protocol */
};

enum network_transport {
	local_transport, /* Unix sockets*/
	tcp_transport,
	udp_transport
};

enum item_lock_types {
	ITEM_LOCK_GRANULAR = 0,
	ITEM_LOCK_GLOBAL
};

#define IS_UDP(x) (x == udp_transport)

#define NREAD_ADD 1
#define NREAD_SET 2
#define NREAD_REPLACE 3
#define NREAD_APPEND 4
#define NREAD_PREPEND 5
#define NREAD_CAS 6

enum store_item_type {
	NOT_STORED=0, STORED, EXISTS, NOT_FOUND
};

enum delta_result_type {
	OK, NON_NUMERIC, EOM, DELTA_ITEM_NOT_FOUND, DELTA_ITEM_CAS_MISMATCH
};

typedef unsigned int rel_time_t;

#define ITEM_LINKED 1
#define ITEM_CAS 2

/* temp */
#define ITEM_SLABBED 4

#define ITEM_FETCHED 8

typedef struct _stritem
{
	struct _stritem*	next;
	struct _stritem*	prev;
	struct _stritem*	h_next;
	rel_time_t			time;
	rel_time_t			exptime; 
	int					nbytes;
	unsigned short		refcount;
	uint8_t				nsuffix;
	uint8_t				it_flags;			//data的标识
	uint8_t				slabs_clsid;
	uint8_t				nkey;

	union{
		uint64_t		cas;
		char			end;
	}data[];
}item;

typedef struct 
{
	struct _stritem *next;
	struct _stritem *prev;
	struct _stritem *h_next;    /* hash chain next */
	rel_time_t      time;       /* least recent access */
	rel_time_t      exptime;    /* expire time */
	int             nbytes;     /* size of data */
	unsigned short  refcount;
	uint8_t         nsuffix;    /* length of flags-and-length string */
	uint8_t         it_flags;   /* ITEM_* above */
	uint8_t         slabs_clsid;/* which slab class we're in */
	uint8_t         nkey;       /* key length, w/terminating null and padding */
	uint32_t        remaining;  /* Max keys to crawl per slab per invocation */
} crawler;

typedef struct
{
	pthread_t			thread_id;
	struct event_base*	base;
	struct event		notify_event;
	int					notify_receive_fd;
	int					notify_send_fd;
	struct thread_stats stats;
	struct conn_queue*  new_conn_queue;
	cache_t*			suffix_cache;
	uint8_t				item_lock_type;
}LIBEVENT_THREAD;

typedef struct 
{
	pthread_t thread_id;        /* unique ID of this thread */
	struct event_base *base;    /* libevent handle this thread uses */
} LIBEVENT_DISPATCHER_THREAD;

/* current time of day (updated periodically) */
extern volatile rel_time_t current_time;

/* TODO: Move to slabs.h? */
extern volatile int slab_rebalance_signal;

struct slab_rebalance 
{
	void *slab_start;
	void *slab_end;
	void *slab_pos;
	int s_clsid;
	int d_clsid;
	int busy_items;
	uint8_t done;
};

extern struct slab_rebalance slab_rebal;

struct stats {
	pthread_mutex_t mutex;
	unsigned int  curr_items;
	unsigned int  total_items;
	uint64_t      curr_bytes;
	unsigned int  curr_conns;
	unsigned int  total_conns;
	uint64_t      rejected_conns;
	uint64_t      malloc_fails;
	unsigned int  reserved_fds;
	unsigned int  conn_structs;
	uint64_t      get_cmds;
	uint64_t      set_cmds;
	uint64_t      touch_cmds;
	uint64_t      get_hits;
	uint64_t      get_misses;
	uint64_t      touch_hits;
	uint64_t      touch_misses;
	uint64_t      evictions;
	uint64_t      reclaimed;
	time_t        started;          /* when the process was started */
	bool          accepting_conns;  /* whether we are currently accepting */
	uint64_t      listen_disabled_num;
	unsigned int  hash_power_level; /* Better hope it's not over 9000 */
	uint64_t      hash_bytes;       /* size used for hash tables */
	bool          hash_is_expanding; /* If the hash table is being expanded */
	uint64_t      expired_unfetched; /* items reclaimed but never touched */
	uint64_t      evicted_unfetched; /* items evicted but never touched */
	bool          slab_reassign_running; /* slab reassign in progress */
	uint64_t      slabs_moved;       /* times slabs were moved around */
	bool          lru_crawler_running; /* crawl in progress */
};

#define MAX_VERBOSITY_LEVEL 2

struct settings {
    size_t maxbytes;
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    rel_time_t oldest_live;		/* ignore existing items older than this */
    int evict_to_free;
    char *socketpath;			/* path to unix socket if using local socket */
    int access;					/* access mask (a la chmod) for unix domain socket */
    double factor;				/* chunk size growth factor */
    int chunk_size;
    int num_threads;			/* number of worker (without dispatcher) libevent threads to run */
    int num_threads_per_udp;	/* number of worker threads serving each udp socket */
    char prefix_delimiter;		/* character that marks a key prefix (for stats) */
    int detail_enabled;			/* nonzero if we're collecting detailed stats */
    int reqs_per_event;			/* Maximum number of io to process on each io-event. */
    bool use_cas;
    enum protocol binding_protocol;
    int backlog;
    int item_size_max;			/* Maximum item size, and upper end for slabs */
    bool sasl;					/* SASL on/off */
    bool maxconns_fast;			/* Whether or not to early close connections */
    bool lru_crawler;			/* Whether or not to enable the autocrawler thread */
    bool slab_reassign;			/* Whether or not slab reassignment is allowed */
    int slab_automove;			/* Whether or not to automatically move slabs */
    int hashpower_init;			/* Starting hash power level */
    bool shutdown_command;		/* allow shutdown command */
    int tail_repair_time;		/* LRU tail refcount leak repair time */
    bool flush_enabled;			/* flush_all enabled */
    char *hash_algorithm;		/* Hash algorithm in use */
    int lru_crawler_sleep;		/* Microsecond sleep between items */
    uint32_t lru_crawler_tocrawl; /* Number of items to crawl per run */
};

typedef struct conn conn;
struct conn
{
	int		sfd;
	sasl_conn_t* sasl_conn;
	bool	authenticated;
	enum conn_states state;
	enum bin_substates substate;
	rel_time_t last_cmd_time;
	struct event event;
	short  ev_flags;
	short  which; 

	char   *rbuf;
	char   *rcurr;
	int    rsize;
	int    rbytes;

	char   *wbuf;
	char   *wcurr;
	int    wsize;
	int    wbytes;

	enum conn_states  write_and_go;
	void   *write_and_free;

	char   *ritem;
	int    rlbytes;
	void   *item;
	int    sbytes;

	struct iovec *iov;
	int    iovsize;
	int    iovused;

	struct msghdr *msglist;
	int    msgsize;
	int    msgused;
	int    msgcurr;
	int    msgbytes;

	item   **ilist;
	int    isize;
	item   **icurr;
	int    ileft;

	char   **suffixlist;
	int    suffixsize;
	char   **suffixcurr;
	int    suffixleft;

	enum protocol protocol;
	enum network_transport transport;

	int    request_id;
	struct sockaddr_in6 request_addr;
	socklen_t request_addr_size;
	unsigned char *hdrbuf;
	int    hdrsize; 

	bool   noreply;

	struct {
		char *buffer;
		size_t size;
		size_t offset;
	} stats;

	protocol_binary_request_header binary_header;
	uint64_t cas;
	short cmd;
	int opaque;
	int keylen;
	conn   *next; 

	LIBEVENT_THREAD *thread;
};

extern conn **conns;

extern struct stats stats;
extern time_t process_started;
extern struct settings settings;

/********************************************thread.c function*****************************************/
void thread_init(int nthreads, struct event_base *main_base);
int dispatch_event_add(int thread, conn* c);
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags, int read_buffer_size, enum network_transport transport);

enum delta_result_type add_delta(conn *c, const char *key, const size_t nkey, const int incr, const int64_t delta, char *buf, uint64_t *cas);
void accept_new_conns(const bool do_accept);
conn* conn_from_freelist();
bool conn_add_to_freelist(conn *c);

int is_listen_thread();

item* item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes);
char* item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void  item_flush_expired();
item *item_get(const char *key, const size_t nkey);
item *item_touch(const char *key, const size_t nkey, uint32_t exptime);
int   item_link(item *it);
void  item_remove(item *it);
int   item_replace(item *it, item *new_it, const uint32_t hv);
void  item_stats(ADD_STAT add_stats, void *c);
void  item_stats_totals(ADD_STAT add_stats, void *c);
void  item_stats_sizes(ADD_STAT add_stats, void *c);
void  item_unlink(item *it);
void  item_update(item *it);

void item_lock_global(void);
void item_unlock_global(void);
void item_lock(uint32_t hv);
void *item_trylock(uint32_t hv);
void item_trylock_unlock(void *arg);
void item_unlock(uint32_t hv);
void switch_item_lock_type(enum item_lock_types type);
unsigned short refcount_incr(unsigned short *refcount);
unsigned short refcount_decr(unsigned short *refcount);
void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_reset(void);
void threadlocal_stats_aggregate(struct thread_stats *stats);
void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out);

/********************************************************************************************************/
/* Stat processing functions */
void append_stat(const char *name, ADD_STAT add_stats, conn *c,
	const char *fmt, ...);

enum store_item_type store_item(item *item, int comm, conn *c);

#if HAVE_DROP_PRIVILEGES
extern void drop_privileges(void);
#else
#define drop_privileges()
#endif


#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#endif
