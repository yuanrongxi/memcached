#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>

#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif

#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif

#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>

#ifndef IOV_MAX
#if defined(__FreeBSD__) || defined(__APPLE__)
# define IOV_MAX 1024
#endif
#endif

static void drive_machine(conn* c);
static int new_socket(struct addrinfo *ai);
static int try_read_command(conn* c);

enum try_read_result
{
	READ_DATA_RECEIVED,
	READ_NO_DATA_RECEIVED,
	READ_ERROR,
	READ_MEMORY_ERROR
};

static enum try_read_result try_read_network(conn* c);
static enum try_read_result try_read_udp(conn* c);

static void conn_set_state(conn* c, enum conn_states state);

static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c);
static void process_stat_settings(ADD_STAT add_stats, void *c);
static void conn_to_str(const conn *c, char *buf);

//初始化配置函数
static void settings_init(void);

//网络函数
static void event_handler(const int fd, const short which, void* arg);
static void conn_close(conn* c);
static void conn_init();
static bool update_event(conn* c, const int new_flags);
static void complete_nread(conn* c);
static void process_command(conn* c, char* command);
static void write_and_free(conn* c, char* buf, int bytes);
static int ensure_iov_space(conn* c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msgaddr(conn* c);
static void write_bin_error(conn *c, protocol_binary_response_status err, const char *errstr, int swallow);
static void conn_free(conn* c);

struct stats stats;
struct settings settings;
time_t process_started;
conn** conns;

struct slab_rebalance slab_rebal;
volatile int slab_rebalance_signal;

static conn* listen_conn = NULL;
static int max_fds;
//libevent base
static struct event_base* main_base;

enum transmit_result
{
	TRANSMIT_COMPLETE,
	TRANSMIT_INCOMPLETE,
	TRANSMIT_SOFT_ERROR,
	TRANSMIT_HARD_ERROR,
};

#define REALTIME_MAXDELTA 60*60*24*30 //30天

static enum transmit_result transmit(conn* c);

static volatile bool allow_new_conns = true;
static struct event maxconnsevent;

static void maxconns_handler(const int fd, const short which, void* arg)
{
	struct timeval t = {.tv_sec = 0, .tv_usec = 10000}; //10毫秒
	if(fd == -42 || allow_new_conns == false){ //不允许接受新的连接，设置一个10毫秒的定时器，10毫秒后触发接受新的连接
		evtimer_set(&maxconnsevent, maxconns_handler, 0);
		event_base_set(main_base, &maxconnsevent);
		evtimer_add(&maxconnsevent, &t);
	}
	else{ //可以接受新的连接，进行连接接受
		evtimer_del(&maxconnsevent);
		accept_new_conns(true);
	}
}

static rel_time_t realtime(const time_t exptime)
{
	if(exptime == 0)
		return 0;

	if(exptime > REALTIME_MAXDELTA){ //传过来的可能是个绝对时刻
		if(exptime <= process_started)
			return 1;
		return (rel_time_t)(exptime - process_started);
	}
	else
		return (rel_time_t)(exptime + current_time);
}

static void stats_init(void)
{
	stats.curr_items = stats.total_items = stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
	stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = stats.evictions = stats.reclaimed = 0;
	stats.touch_cmds = stats.touch_misses = stats.touch_hits = stats.rejected_conns = 0;
	stats.malloc_fails = 0;
	stats.curr_bytes = stats.listen_disabled_num = 0;
	stats.hash_power_level = stats.hash_bytes = stats.hash_is_expanding = 0;
	stats.expired_unfetched = stats.evicted_unfetched = 0;
	stats.slabs_moved = 0;
	stats.accepting_conns = true; /* assuming we start in this state. */
	stats.slab_reassign_running = false;
	stats.lru_crawler_running = false;

	//计算开始时刻
	process_started = time(0) - ITEM_UPDATE_INTERVAL - 2;
	//初始化统计
	stats_prefix_init(); 
}

static void stats_reset()
{
	STATS_LOCK();
	stats.total_items = stats.total_conns = 0;
	stats.rejected_conns = 0;
	stats.malloc_fails = 0;
	stats.evictions = 0;
	stats.reclaimed = 0;
	stats.listen_disabled_num = 0;
	stats_prefix_clear();
	STATS_UNLOCK();

	threadlocal_stats_reset();
	item_stats_reset();
}

//默认配置
static void settings_init()
{
	settings.use_cas = true;
	settings.access = 0700;
	//默认端口
	settings.port = 11211;
	settings.udpport = 11211;
	settings.inter = NULL;
	settings.maxbytes = 64 * 1024 * 1024; //64M
	settings.maxconns = 1024; //最大连接数
	settings.verbose = 0;
	settings.oldest_live = 0;
	settings.evict_to_free = 1;
	settings.socketpath = NULL;
	settings.factor = 1.25;
	settings.chunk_size = 48;
	settings.num_threads = 4;
	settings.num_threads_per_udp = 0;
	settings.prefix_delimiter = ':';
	settings.detail_enabled = 0;
	settings.reqs_per_event = 20;
	settings.backlog = 1024;
	settings.binding_protocol = negotiating_prot;
	settings.item_size_max = 1024 * 1024; //1M,item最大尺寸
	settings.maxconns_fast = false;
	settings.lru_crawler = false;
	settings.lru_crawler_sleep = 100;
	settings.lru_crawler_tocrawl = 0;
	settings.hashpower_init = 0;
	settings.slab_automove = 0;
	settings.shutdown_command = false;
	settings.tail_repair_time = TAIL_REPAIR_TIME_DEFAULT;
	settings.flush_enabled = true;
}

static int add_msghdr(conn* c)
{
	struct msghdr* msg;
	assert(c != NULL);

	if(c->msgsize == c->msgused){
		msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
		if(msg == NULL){
			STATS_LOCK();
			stats.malloc_fails++;
			STATS_UNLOCK();
			return -1;
		}

		c->msglist = msg;
		c->msgsize *= 2;
	}

	//初始化新开辟的内存
	msg = c->msglist + c->msgused;
	memset(msg, 0, sizeof(struct msghdr));

	msg->msg_iov = &c->iov[c->iovused];

	if(IS_UDP(c->transport) && c->request_addr_size > 0){
		msg->msg_name = &c->request_addr;
		msg->msg_namelen = c->request_addr_size;
	}
	
	c->msgbytes = 0;
	c->msgused ++;

	if(IS_UDP(c->transport))
		return add_iov(c, NULL, UDP_HEADER_SIZE);

	return 0;
}

extern pthread_mutex_t conn_lock;

static void conn_init()
{
	int next_fd = dup(1);
	INT headroom = 10;
	struct rlimit rl;

	//计算最大的句柄数
	max_fds = settings.maxconns + headroom + next_fd;
	if(getrlimit(RLIMIT_NOFILE, &rl) == 0) //获得系统的句柄数限制
		max_fds = rl.rlim_max;
	else
		fprintf(stderr, "Failed to query maximum file descriptor; falling back to maxconns\n");

	close(next_fd);

	if((conns = calloc(max_fds, size(conn*))) == NULL){
		fprintf(stderr, "Failed to allocate connection structures\n");
		exit(1);
	}
}

static const char* prot_text(enum protocol prot)
{
	char* rv = "unknown";
	switch(prot){
	case ascii_prot:
		rv = "ascii";
		break;

	case binary_prot:
		rv = "binary";
		break;

	case negotiating_prot:
		rv = "auto-negotiate";
		break;
	}

	return rv;
}

conn* conn_new(const int sfd, enum conn_stats init_state, const int event_flags, const int read_buffer_size,
				enum network_transport transport, struct event_base* base)
{
	conn* c;
	assert(sfd >= 0 && sfd < max_fds);
	c = conns[sfd];

	if(c == NULL){
		if(!(c = (conn *)calloc(1, sizeof(conn)))){
			STATS_LOCK();
			stats.malloc_fails++;
			STATS_UNLOCK();
			fprintf(stderr, "Failed to allocate connection object\n");
			return NULL;
		}

		MEMCACHED_CONN_CREATE(c);

		c->rbuf = c->wbuf = 0;
		c->ilist = 0;
		c->suffixlist = 0;
		c->iov = 0;
		c->msglist = 0;
		c->hdrbuf = 0;

		c->rsize = read_buffer_size;
		c->wsize = DATA_BUFFER_SIZE;
		c->isize = ITEM_LIST_INITIAL;
		c->suffixsize = SUFFIX_LIST_INITIAL;
		c->msgsize = IOV_LIST_INITIAL;
		c->hdrsize = 0;

		c->rbuf = (char*)malloc((size_t)c->rsize);
		c->wbuf = (char *)malloc((size_t)c->wsize);
		c->ilist = (item **)malloc(sizeof(item *) * c->isize);
		c->iov = (struct iovec*)malloc(sizeof(struct iovec) * c->iovsize);
		c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

		if(c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 || c->msglist == 0 || c->suffixlist == 0){
			conn_free(c);
			STATS_LOCK();
			stats.malloc_fails++;
			STATS_UNLOCK();
			fprintf(stderr, "Failed to allocate buffers for connection\n");
			return NULL;
		}

		STATS_LOCK();
		stats.conn_structs ++;
		STATS_UNLOCK();

		c->sfd = sfd;
		conns[sfd] = c;
	}

	c->transport = transport;
	c->protocol = settings.binding_protocol;
	if(!settings.socketpath)
		c->request_addr_size = sizeof(c->request_addr);
	else
		c->request_addr_size = 0;

	//如果是TCP，获取对端的IP
	if(transport == tcp_transport && init_state == conn_new_cmd){
		if (getpeername(sfd, (struct sockaddr *) &c->request_addr, &c->request_addr_size)) {
				perror("getpeername");
				memset(&c->request_addr, 0, sizeof(c->request_addr));
		}
	}

	if (settings.verbose > 1) {
		if (init_state == conn_listening) {
			fprintf(stderr, "<%d server listening (%s)\n", sfd,
				prot_text(c->protocol));
		} else if (IS_UDP(transport)) {
			fprintf(stderr, "<%d server listening (udp)\n", sfd);
		} else if (c->protocol == negotiating_prot) {
			fprintf(stderr, "<%d new auto-negotiating client connection\n",
				sfd);
		} else if (c->protocol == ascii_prot) {
			fprintf(stderr, "<%d new ascii client connection.\n", sfd);
		} else if (c->protocol == binary_prot) {
			fprintf(stderr, "<%d new binary client connection.\n", sfd);
		} else {
			fprintf(stderr, "<%d new unknown (%d) client connection\n",
				sfd, c->protocol);
			assert(false);
		}
	}

	c->state = init_state;
	c->rlbytes = 0;
	c->cmd = -1;
	c->rbytes = c->wbytes = 0;
	c->wcurr = c->wbuf;
	c->rcurr = c->rbuf;
	c->ritem = 0;
	c->icurr = c->ilist;
	c->suffixcurr = c->suffixlist;
	c->ileft = 0;
	c->suffixleft = 0;
	c->iovused = 0;
	c->msgcurr = 0;
	c->msgused = 0;
	
	c->authenticated = false;
	c->write_and_go = init_state;
	c->write_and_free = 0;
	c->item = 0;

	c->noreply = false;

	//设置socket事件监听
	event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
	event_base_set(base, &c->event);
	c->ev_flags = event_flags;
	if(event_add(&c->event, 0) == -1){
		perror("event_add");
		return NULL;
	}

	//更新统计信息
	STATS_LOCK();
	stats.curr_conns++;
	stats.total_conns++;
	STATS_UNLOCK();

	MEMCACHED_CONN_ALLOCATE(c->sfd);

	return c;
};

static void conn_release_items(conn* c)
{
	assert(c != NULL);

	if(c->item){
		item_remove(c->item);
		c->item = 0;
	}

	while(c->ileft > 0){
		item* it = *(c->icurr);
		assert((it->it_flags & ITEM_SLABBED) == 0);
		item_remove(it);
		c->icurr ++;
		c->ileft --;
	}

	//回收
	if(c->suffixleft != 0){
		for(; c->suffixleft > 0; c->suffixleft --, c->suffixcurr ++)
			cache_free(c->thread->suffix_cache, *(c->suffixcurr));
	}

	c->icurr = c->ilist;
	c->suffixcurr = c->suffixlist;
}

static void conn_cleanup(conn* c)
{
	assert(c != NULL);

	conn_release_items(c);

	if(c->write_and_free){
		free(c->write_and_free);
		c->write_and_free = 0;
	}

	if (c->sasl_conn) {
		assert(settings.sasl);
		sasl_dispose(&c->sasl_conn);
		c->sasl_conn = NULL;
	}

	if (IS_UDP(c->transport))
		conn_set_state(c, conn_read);
}

void conn_free(conn* c)
{
	if(c){
		assert(c != NULL);
		assert(c->sfd >= 0 && c->sfd < max_fds);

		MEMCACHED_CONN_DESTROY(c);
		conns[c->sfd] = NULL;

		if(c->hdrbuf)
			free(c->hdrbuf);

		if (c->msglist)
			free(c->msglist);

		if (c->rbuf)
			free(c->rbuf);

		if (c->wbuf)
			free(c->wbuf);

		if (c->ilist)
			free(c->ilist);

		if (c->suffixlist)
			free(c->suffixlist);

		if (c->iov)
			free(c->iov);

		free(c);
	}
}

static void conn_close(conn* c)
{
	assert(c != NULL);

	//清除监听事件
	event_del(&c->event);

	if (settings.verbose > 1)
		fprintf(stderr, "<%d connection closed.\n", c->sfd);

	//清除连接的状态
	conn_cleanup(c);

	MEMCACHED_CONN_RELEASE(c->sfd);
	//关闭连接
	conn_set_state(c, conn_closed);
	close(c->sfd);

	//设置可以接受新的连接标识
	pthread_mutex_lock(&conn_lock);
	allow_new_conns = true;
	pthread_mutex_unlock(&conn_lock);

	//更新统计
	STATS_LOCK();
	stats.curr_conns--;
	STATS_UNLOCK();

	return;
}

static void conn_shrink(conn* c)
{
	assert(c != NULL);

	if(IS_UDP(c->transport))
		return ;

	if(c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE){
		char *newbuf;
		if(c->rcurr != c->rbuf)
			memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

		//缩小缓冲区
		newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);
		if(newbuf){
			c->rbuf = newbuf;
			c->rsize = DATA_BUFFER_SIZE;
		}
		c->rcurr = c->rbuf;
	}

	//对缩小item list
	if (c->isize > ITEM_LIST_HIGHWAT) {
		item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
		if (newbuf) {
			c->ilist = newbuf;
			c->isize = ITEM_LIST_INITIAL;
		}
	}

	if (c->msgsize > MSG_LIST_HIGHWAT) {
		struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
		if (newbuf) {
			c->msglist = newbuf;
			c->msgsize = MSG_LIST_INITIAL;
		}
	}

	if (c->iovsize > IOV_LIST_HIGHWAT) {
		struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
		if (newbuf) {
			c->iov = newbuf;
			c->iovsize = IOV_LIST_INITIAL;
		}
	}
}

static const char* state_text(enum conn_states state)
{
	const char* const statenames[] = { 
		"conn_listening",
		"conn_new_cmd",
		"conn_waiting",
		"conn_read",
		"conn_parse_cmd",
		"conn_write",
		"conn_nread",
		"conn_swallow",
		"conn_closing",
		"conn_mwrite",
		"conn_closed" };

	return statenames[state];
};












