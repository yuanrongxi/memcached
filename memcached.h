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

//slab sizing 定义
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)

#define TAIL_REPAIR_TIME_DEFAULT 0

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

enum item_lock_types {
	ITEM_LOCK_GRANULAR = 0,
	ITEM_LOCK_GLOBAL
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

#endif
