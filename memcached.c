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

static void conn_set_state(conn* c, enum conn_states state)
{
	assert(c != NULL);
	assert(state >= conn_listening && state < conn_max_state);

	if(state != c->state){
		if(settings.verbose > 2)
			fprintf(stderr, "%d: going from %s to %s\n", c->sfd, state_text(c->state),state_text(state));
	}

	if(state == conn_write || state == conn_mwrite)
		MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);

	c->state = state;
}

static int ensure_iov_space(conn* c)
{
	assert(c != NULL);

	if(c->iovused >= c->iovsize){
		int i, iovnum;
		struct iovec *new_iov = (struct iovec *)realloc(c->iov, (c->iovsize * 2) * sizeof(struct iovec));
		if(!new_iov){
			STATS_LOCK();
			stats.malloc_fails ++;
			STATS_UNLOCK();
			return -1;
		}

		c->iov = new_iov;
		c->iovsize *= 2;

		for(i = 0, iovnum = 0; i < c->msgused; i ++){
			c->msglist[i].msg_iov = &c->iov[iovnum];
			iovnum += c->msglist[i].msg_iovlen;
		}
	}

	return 0;
}

static int add_iov(conn* c, const void* buf, int len)
{
	struct msghdr* m;
	int leftover;
	bool limit_to_mtu;

	assert(c != NULL);
	do{
		m = &c->msglist[c->msgused - 1];
		limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

		if(m->msg_iovlen == IOV_MAX || (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)){
			add_msgaddr(c); //c->msgused在此增加？
			m = &c->msglist[c->msgused - 1];
		}

		if(ensure_iov_space(c) != 0)
			return -1;

		if(limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE){ //udp分步发送
			leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
			len -= leftover;
		}
		else
			leftover = 0;

		m = &c->msglist[c->msgused - 1];
		m->msg_iov[m->msg_iovlen].iov_base = buf;
		m->msg_iov[m->msg_iovlen].iov_len = len;

		c->msgbytes += len;
		c->iovused ++;
		m->msg_iovlen ++;

		buf = (char *)buf + len;
		len = leftover;

	}while(leftover);
}

static int bulid_udp_headers(conn* c)
{
	int i;
	unsigned char* hdr;
	
	assert(c != NULL);

	//检查头缓冲区
	if(c->msgused > c->hdrsize){
		void* new_hdrbuf;
		if(c->hdrbuf)
			new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
		else
			new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);

		if(!new_hdrbuf){
			STATS_LOCK();
			stats.malloc_fails++;
			STATS_UNLOCK();
			return -1;
		}
		c->hdrbuf = new_hdrbuf;
		c->hdrsize = c->msgused * 2;
	}

	//构建头信息
	hdr = c->hdrbuf;
	for(i = 0; i < c->msgused; i ++){
		c->msglist[i].msg_iov[0].iov_base = (void*)hdr;
		c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
		//request id
		*hdr++ = c->request_id / 256;
		*hdr++ = c->request_id % 256;
		//序号i
		*hdr++ = i / 256;
		*hdr++ = i % 256;
		//msg个数
		*hdr++ = c->msgused / 256;
		*hdr++ = c->msgused % 256;
		*hdr++ = 0;
		*hdr++ = 0;
	}

	return 0;
}

static void out_string(conn* c, const char* str)
{
	size_t len;
	assert(c != NULL);

	if(c->noreply){
		if(settings.verbose > 1)
			fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
		c->noreply = false;
		conn_set_state(c, conn_new_cmd);
		return ;
	}

	if (settings.verbose > 1)
		fprintf(stderr, ">%d %s\n", c->sfd, str);

	c->msgcurr = 0;
	c->msgused = 0;
	c->iovused = 0;
	add_msghdr(c);

	len = strlen(str);
	if((len + 2) > c->wsize){
		str = "SERVER_ERROR output line too long";
		len = strlen(str);
	}

	memcpy(c->wbuf, str, len);
	memcpy(c->wbuf + len, "\r\n", 2);
	c->wbytes = len + 2;
	c->wcurr = c->wbuf;

	conn_set_state(c, conn_write);
	c->write_and_go = conn_new_cmd;
}

static void out_of_memory(conn* c, char* ascii_error)
{
	const static char error_prefix[] = "SERVER_ERROR ";
	const static int error_prefix_len = sizeof(error_prefix) - 1;

	if(c->protocol == binary_prot){
		if(!strncmp(ascii_error, error_prefix, error_prefix_len))
			ascii_error += error_prefix_len;
		
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, ascii_error, 0);
	}
	else
		out_string(c, ascii_error);
}

static void complete_nread_ascii(conn* c)
{
	item* it = c->item;
	int comm = c->cmd;
	enum store_item_type ret;

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	if(strncmp(ITEM_data(it) + it->nbytes - 2,"\r\n", 2) != 0)
		out_string(c, "CLIENT_ERROR bad data chunk");
	else
		ret = store_item(it, comm, c);

#ifdef ENABLE_DTRACE
	uint64_t cas = ITEM_get_cas(it);
	switch (c->cmd) {
	case NREAD_ADD:
		MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
			(ret == 1) ? it->nbytes : -1, cas);
		break;
	case NREAD_REPLACE:
		MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
			(ret == 1) ? it->nbytes : -1, cas);
		break;
	case NREAD_APPEND:
		MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
			(ret == 1) ? it->nbytes : -1, cas);
		break;
	case NREAD_PREPEND:
		MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
			(ret == 1) ? it->nbytes : -1, cas);
		break;
	case NREAD_SET:
		MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
			(ret == 1) ? it->nbytes : -1, cas);
		break;
	case NREAD_CAS:
		MEMCACHED_COMMAND_CAS(c->sfd, ITEM_key(it), it->nkey, it->nbytes,
			cas);
		break;
	}
#endif

	switch(ret){
	case STORED:
		out_string(c, "STORED");
		break;
	case EXISTS:
		out_string(c, "EXISTS");
		break;
	case NOT_FOUND:
		out_string(c, "NOT_FOUND");
		break;
	case NOT_STORED:
		out_string(c, "NOT_STORED");
		break;
	default:
		out_string(c, "SERVER_ERROR Unhandled storage type.");
	}

	//这里不是正真意义上的删除，只是减少引用计数，item是通过引用计数来删除的
	item_remove(c->item);
	c->item = NULL;
}

static void* binary_get_request(conn* c)
{
	char* ret = c->rcurr;
	ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen + c->binary_header.request.extlen);
	assert(ret >= c->rbuf);

	return ret;
}

static char* binary_get_key(conn* c)
{
	return c->rcurr - (c->binary_header.request.keylen);
}

//返回一个二进制reponse头
static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len)
{
	protocol_binary_response_header* header;

	assert(c);

	c->msgcurr = 0;
	c->msgused = 0;
	c->iovused = 0;

	if(add_msghdr(c) != 0){
		out_of_memory(c, "SERVER_ERROR out of memory adding binary header");
		return ;
	}

	header = (protocol_binary_response_header *)c->wbuf;

	header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
	header->response.opcode = c->binary_header.request.opcode;
	header->response.keylen = (uint16_t)htons(key_len);

	header->response.extlen = (uint8_t)hdr_len;
	header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
	header->response.status = (uint16_t)htons(err);

	header->response.bodylen = htonl(body_len);
	header->response.opaque = c->opaque;
	header->response.cas = htonll(c->cas);

	if(settings.verbose > 1){
		int ii;
		fprintf(stderr, ">%d Writing bin response:", c->sfd);
		for (ii = 0; ii < sizeof(header->bytes); ++ii) {
			if (ii % 4 == 0) {
				fprintf(stderr, "\n>%d  ", c->sfd);
			}
			fprintf(stderr, " 0x%02x", header->bytes[ii]);
		}
		fprintf(stderr, "\n");
	}

	//加入到iov发送队列中
	add_iov(c, c->wbuf, sizeof(header->response));
}

//二进制协议的错误返回
static void write_bin_error(conn *c, protocol_binary_response_status err, const char *errstr, int swallow) 
{
	size_t len;

	if (!errstr) {
		switch (err) {
		case PROTOCOL_BINARY_RESPONSE_ENOMEM:
			errstr = "Out of memory";
			break;
		case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
			errstr = "Unknown command";
			break;
		case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
			errstr = "Not found";
			break;
		case PROTOCOL_BINARY_RESPONSE_EINVAL:
			errstr = "Invalid arguments";
			break;
		case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
			errstr = "Data exists for key.";
			break;
		case PROTOCOL_BINARY_RESPONSE_E2BIG:
			errstr = "Too large.";
			break;
		case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
			errstr = "Non-numeric server-side value for incr or decr";
			break;
		case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
			errstr = "Not stored.";
			break;
		case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
			errstr = "Auth failure.";
			break;
		default:
			assert(false);
			errstr = "UNHANDLED ERROR";
			fprintf(stderr, ">%d UNHANDLED ERROR: %d\n", c->sfd, err);
		}
	}

	if (settings.verbose > 1) {
		fprintf(stderr, ">%d Writing an error: %s\n", c->sfd, errstr);
	}

	len = strlen(errstr);
	add_bin_header(c, err, 0, 0, len);
	if (len > 0) {
		add_iov(c, errstr, len);
	}

	conn_set_state(c, conn_mwrite);

	if(swallow > 0) {
		c->sbytes = swallow;
		c->write_and_go = conn_swallow;
	} else {
		c->write_and_go = conn_new_cmd;
	}
}

static void write_bin_response(conn* c, void *d, int hlen, int keylen, int dlen)
{
	if(!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET || c->cmd == PROTOCOL_BINARY_CMD_GETK){
		add_bin_header(c, 0, hlen, keylen, dlen);
		if(dlen > 0)
			add_iov(c, d, dlen);
		
		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
	}
	else
		conn_set_state(c, conn_new_cmd);
}

static void complete_incr_bin(conn* c)
{
	item* it;
	char* key;
	size_t nkey;

	char tmpbuf[INCR_MAX_STORAGE_LEN];
	uint64_t cas = 0;

	protocol_binary_response_incr* rsp = (protocol_binary_response_incr *)c->wbuf;
	protocol_binary_request_incr* req = binary_get_request(c);

	assert(c != NULL);
	assert(c->wsize >= sizeof(*rsp));

	req->message.body.delta = ntohll(req->message.body.delta);
	req->message.body.initial = ntohll(req->message.body.initial);
	req->message.body.expiration = ntohl(req->message.body.expiration);
	key = binary_get_key(c);
	nkey = c->binary_header.request.keylen;
	
	if(settings.verbose > 1){
		int i;
		fprintf(stderr, "incr ");

		for (i = 0; i < nkey; i++) 
			fprintf(stderr, "%c", key[i]);
		
		fprintf(stderr, " %lld, %llu, %d\n", (long long)req->message.body.delta, (long long)req->message.body.initial, req->message.body.expiration);
	}

	if(c->binary_header.request.cas != 0)
		cas = c->binary_header.request.cas;

	switch(add_delta(c, key, nkey, c->cmd == PROTOCOL_BINARY_CMD_INCREMENT,  req->message.body.delta, tmpbuf, &cas)){
	case OK:
		rsp->message.body.value = htonll(strtoull(tmpbuf, NULL, 10));
		if(cas)
			c->cas = cas;

		write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body.value));
		break;

	case NON_NUMERIC:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, NULL, 0);
		break;

	case EOM:
		out_of_memory(c, "SERVER_ERROR Out of memory incrementing value");
		break;

	case DELTA_ITEM_NOT_FOUND:
		if(req->message.body.expiration != 0xffffffff){
			rsp->message.body.value = htonll(req->message.body.initial);

			snprintf(tmpbuf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)req->message.body.initial);
			int res = strlen(tmpbuf);
			it = item_alloc(key, nkey, 0, realtime(req->message.body.expiration), res + 2);
			if(it != NULL){
				memcpy(ITEM_data(it), tmpbuf, res);
				memcpy(ITEM_data(it) + res, "\r\n", 2);

				if(store_item(it, NREAD_ADD, c)){
					c->cas = ITEM_get_cas(it);
					write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body.value));
				}
				else
					write_bin_error(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED,NULL, 0);

				item_remove(it);
			}
			else
				out_of_memory(c, "SERVER_ERROR Out of memory allocating new item");
		}
		else{
			pthread_mutex_lock(&c->thread->stats.mutex);
			if(c->cmd == PROTOCOL_BINARY_CMD_INCREMENT)
				c->thread->stats.incr_misses ++;
			else
				c->thread->stats.decr_misses++;
			 pthread_mutex_unlock(&c->thread->stats.mutex);

			 write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
		}
		break;

	case DELTA_ITEM_CAS_MISMATCH:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);
		break;
	}
}

static void complete_update_bin(conn* c)
{
	protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
	enum store_item_type ret = NOT_STORED;
	assert(c != NULL);

	item* it = c->item;

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	//二进制协议不能确定末尾是"\r\n",所以强制加上
	*(ITEM_data(it) + it->nbytes - 2) = '\r';
	*(ITEM_data(it) + it->nbytes - 1) = '\n';

	ret = store_item(it, c->cmd, c);

#ifdef ENABLE_DTRACE
	uint64_t cas = ITEM_get_cas(it);
	switch (c->cmd) {
	case NREAD_ADD:
		MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
			(ret == STORED) ? it->nbytes : -1, cas);
		break;
	case NREAD_REPLACE:
		MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
			(ret == STORED) ? it->nbytes : -1, cas);
		break;
	case NREAD_APPEND:
		MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
			(ret == STORED) ? it->nbytes : -1, cas);
		break;
	case NREAD_PREPEND:
		MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
			(ret == STORED) ? it->nbytes : -1, cas);
		break;
	case NREAD_SET:
		MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
			(ret == STORED) ? it->nbytes : -1, cas);
		break;
	}
#endif
	
	switch(ret){
	case STORED:
		write_bin_response(c, NULL, 0, 0, 0);
		break;

	case EXISTS:
		 write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);
		break;

	case NOT_STORED:
		if (c->cmd == NREAD_ADD){
			eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
		} 
		else if(c->cmd == NREAD_REPLACE){
			eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
		} 
		else{
			eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
		}
		write_bin_error(c, eno, NULL, 0);
	}

	item_remove(c->item);
	c->item = 0;
}

static void process_bin_get_or_touch(conn *c) 
{
	item *it;

	protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
	char* key = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;
	int should_touch = (c->cmd == PROTOCOL_BINARY_CMD_TOUCH || c->cmd == PROTOCOL_BINARY_CMD_GAT || c->cmd == PROTOCOL_BINARY_CMD_GATK);
	int should_return_key = (c->cmd == PROTOCOL_BINARY_CMD_GETK || c->cmd == PROTOCOL_BINARY_CMD_GATK);
	int should_return_value = (c->cmd != PROTOCOL_BINARY_CMD_TOUCH);

	if (settings.verbose > 1) {
		fprintf(stderr, "<%d %s ", c->sfd, should_touch ? "TOUCH" : "GET");
		fwrite(key, 1, nkey, stderr);
		fputc('\n', stderr);
	}

	if (should_touch) {
		protocol_binary_request_touch *t = binary_get_request(c);
		time_t exptime = ntohl(t->message.body.expiration);

		it = item_touch(key, nkey, realtime(exptime));
	} 
	else
		it = item_get(key, nkey);

	if (it) {
		uint16_t keylen = 0;
		uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

		item_update(it);

		pthread_mutex_lock(&c->thread->stats.mutex);
		if (should_touch) {
			c->thread->stats.touch_cmds++;
			c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
		} 
		else {
			c->thread->stats.get_cmds++;
			c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
		}
		pthread_mutex_unlock(&c->thread->stats.mutex);

		if (should_touch) {
			MEMCACHED_COMMAND_TOUCH(c->sfd, ITEM_key(it), it->nkey,
				it->nbytes, ITEM_get_cas(it));
		} 
		else {
			MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
				it->nbytes, ITEM_get_cas(it));
		}

		if (c->cmd == PROTOCOL_BINARY_CMD_TOUCH) {
			bodylen -= it->nbytes - 2;
		} 
		else if (should_return_key) {
			bodylen += nkey;
			keylen = nkey;
		}

		add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
		rsp->message.header.response.cas = htonll(ITEM_get_cas(it));

		// add the flags
		rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
		add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

		if (should_return_key) {
			add_iov(c, ITEM_key(it), nkey);
		}

		if (should_return_value) {
			/* Add the data minus the CRLF */
			add_iov(c, ITEM_data(it), it->nbytes - 2);
		}

		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
		/* Remember this command so we can garbage collect it later */
		c->item = it;
	} else {
		pthread_mutex_lock(&c->thread->stats.mutex);
		if (should_touch) {
			c->thread->stats.touch_cmds++;
			c->thread->stats.touch_misses++;
		} else {
			c->thread->stats.get_cmds++;
			c->thread->stats.get_misses++;
		}
		pthread_mutex_unlock(&c->thread->stats.mutex);

		if (should_touch) {
			MEMCACHED_COMMAND_TOUCH(c->sfd, key, nkey, -1, 0);
		} else {
			MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
		}

		if (c->noreply) {
			conn_set_state(c, conn_new_cmd);
		} 
		else {
			if (should_return_key) {
				char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
				add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, nkey, nkey);
				memcpy(ofs, key, nkey);
				add_iov(c, ofs, nkey);
				conn_set_state(c, conn_mwrite);
				c->write_and_go = conn_new_cmd;
			} 
			else {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
			}
		}
	}

	if (settings.detail_enabled)
		stats_prefix_record_get(key, nkey, NULL != it);
}

static void append_bin_stats(const char *key, const uint16_t klen,const char *val, const uint32_t vlen, conn *c) {
	char *buf = c->stats.buffer + c->stats.offset;
	uint32_t bodylen = klen + vlen;
	protocol_binary_response_header header = {
		.response.magic = (uint8_t)PROTOCOL_BINARY_RES,
		.response.opcode = PROTOCOL_BINARY_CMD_STAT,
		.response.keylen = (uint16_t)htons(klen),
		.response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
		.response.bodylen = htonl(bodylen),
		.response.opaque = c->opaque
	};

	memcpy(buf, header.bytes, sizeof(header.response));
	buf += sizeof(header.response);

	if (klen > 0){
		memcpy(buf, key, klen);
		buf += klen;

		if (vlen > 0)
			memcpy(buf, val, vlen);
	}

	c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen, const char *val, const uint32_t vlen, conn *c) {
	char *pos = c->stats.buffer + c->stats.offset;
	uint32_t nbytes = 0;
	int remaining = c->stats.size - c->stats.offset;
	int room = remaining - 1;

	if (klen == 0 && vlen == 0)
		nbytes = snprintf(pos, room, "END\r\n");
	else if (vlen == 0) 
		nbytes = snprintf(pos, room, "STAT %s\r\n", key);
	else
		nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);

	c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn* c, size_t needed)
{
	size_t nsize = c->stats.size;
	size_t available = nsize - c->stats.offset;
	bool rv = true;

	if(c->stats.buffer == NULL){
		nsize = 1024;
		available = c->stats.size = c->stats.offset = 0;
	}

	//确定是否需要放大nsize,是不是要设置一个最大值？
	while(needed > available){
		assert(nsize > 0);
		nsize = nsize << 1; //放大2倍
		available = nsize - c->stats.offset;
	}

	if(nsize != c->stats.size){
		char* ptr = realloc(c->stats.buffer, nsize);
		if(ptr){
			c->stats.buffer = ptr;
			c->stats.size = nsize;
		}
		else{
			STATS_LOCK();
			stats.malloc_fails ++;
			STATS_UNLOCK();
			rv = false;
		}
	}

	return rv;
}

static void append_stats(const char *key, const uint16_t klen, const char *val, const uint32_t vlen, const void *cookie)
{
	if(klen == 0 && vlen > 0)
		return;

	conn* c = (conn *)cookie;
	if(c->protocol == binary_prot){
		size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
		if(!grow_stats_buf(c, needed))
			return ;
		append_bin_stats(key, klen, val, vlen, c);
	}
	else{
		size_t needed = vlen + klen + 10;
		if(!grow_stats_buf(c, needed))
			return ;
		append_ascii_stats(key, klen, val, vlen, c);
	}

	assert(c->stats.offset <= c->stats.size);
}

static void process_bin_stat(conn* c)
{
	//获得KEY
	char *subcommand = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;

	if(nkey == 0){
		server_stats(&append_stats, c);
		(void)get_stats(NULL, 0, &append_stats, c);
	}
	else if(strncmp(subcommand, "reset", 5) == 0)
		stats_reset();
	else if (strncmp(subcommand, "settings", 8) == 0)
		process_stat_settings(&append_stats, c);
	else if(strncmp(subcommand, "detail", 6) == 0){
		char *subcmd_pos = subcommand + 6;
		if(strncmp(subcmd_pos, " dump", 5) == 0){
			int len;
			char *dump_buf = stats_prefix_dump(&len);
			if (dump_buf == NULL || len <= 0){
				out_of_memory(c, "SERVER_ERROR Out of memory generating stats");
				return ;
			} 
			else{
				append_stats("detailed", strlen("detailed"), dump_buf, len, c);
				free(dump_buf);
			}
		}
		else if(strncmp(subcmd_pos, " on", 3) == 0)
			settings.detail_enabled = 1;
		else if(strncmp(subcmd_pos, " off", 4) == 0)
			settings.detail_enabled = 0;
		else{
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
			return;
		}
	}
	else{
		if(get_stats(subcommand, nkey, &append_stats, c)){
			if(c->stats.buffer == NULL)
				out_of_memory(c, "SERVER_ERROR Out of memory generating stats");
			else{
				write_and_free(c, c->stats.buffer, c->stats.offset);
				c->stats.buffer = NULL;
			}
		}
		else
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);

		return ;
	}

	append_stats(NULL, 0, NULL, 0, c);
	if(c->stats.buffer == NULL){
		out_of_memory(c, "SERVER_ERROR Out of memory preparing to send stats");
	}
	else{
		write_and_free(c, c->stats.buffer, c->stats.offset);
		c->stats.buffer = NULL;
	}
}

static void bin_read_key(conn* c, enum bin_substates next_substate, int extra)
{
	assert(c);
	c->substate = next_substate;
	c->rlbytes = c->keylen + extra;

	ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
	if(c->rlbytes > c->rsize - offset){
		size_t nsize = c->rsize;
		size_t size = c->rlbytes + sizeof(protocol_binary_request_header);
		while(size > nsize)
			nsize *= 2;

		if(nsize != c->rsize){
			if (settings.verbose > 1) {
				fprintf(stderr, "%d: Need to grow buffer from %lu to %lu\n", c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
			}

			char* newm = realloc(c->rbuf, nsize);
			if(newm == NULL){
				STATS_LOCK();
				stats.malloc_fails++;
				STATS_UNLOCK();
				if (settings.verbose) {
					fprintf(stderr, "%d: Failed to grow buffer.. closing connection\n",
						c->sfd);
				}
				conn_set_state(c, conn_closing);
				return;
			}
			
			c->rbuf = newm;
			c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
			c->rsize = nsize;
		}

		if(c->rbuf != c->rcurr){
			 memmove(c->rbuf, c->rcurr, c->rbytes);
			 c->rcurr = c->rbuf;
			 if (settings.verbose > 1)
				 fprintf(stderr, "%d: Repack input buffer\n", c->sfd);
		}
	}

	c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
	conn_set_state(c, conn_read);
}

static void handle_binary_protocol_error(conn* c)
{
	write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, NULL, 0);
	if(settings.verbose)
		fprintf(stderr, "Protocol error (opcode %02x), close connection %d\n", c->binary_header.request.opcode, c->sfd);

	c->write_and_go = conn_closing;
}

static void init_sasl_conn(conn* c)
{
	assert(c);
	/* should something else be returned? */
	if (!settings.sasl)
		return;

	c->authenticated = false;

	if (!c->sasl_conn){
		int result=sasl_server_new("memcached", NULL, my_sasl_hostname[0] ? my_sasl_hostname : NULL, NULL, NULL, NULL, 0, &c->sasl_conn);
		if (result != SASL_OK){
			if (settings.verbose)
				fprintf(stderr, "Failed to initialize SASL conn.\n");

			c->sasl_conn = NULL;
		}
	}
}

static void bin_list_sasl_mechs(conn *c) 
{
	// Guard against a disabled SASL.
	if (!settings.sasl) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
			c->binary_header.request.bodylen
			- c->binary_header.request.keylen);
		return;
	}

	init_sasl_conn(c);
	const char *result_string = NULL;
	unsigned int string_length = 0;
	int result=sasl_listmech(c->sasl_conn, NULL,
		"",   /* What to prepend the string with */
		" ",  /* What to separate mechanisms with */
		"",   /* What to append to the string */
		&result_string, &string_length,
		NULL);
	if (result != SASL_OK) {
		/* Perhaps there's a better error for this... */
		if (settings.verbose) {
			fprintf(stderr, "Failed to list SASL mechanisms.\n");
		}
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
		return;
	}
	write_bin_response(c, (char*)result_string, 0, 0, string_length);
}

static void process_bin_sasl_auth(conn *c) 
{
	if (!settings.sasl) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
			c->binary_header.request.bodylen
			- c->binary_header.request.keylen);
		return;
	}

	assert(c->binary_header.request.extlen == 0);

	int nkey = c->binary_header.request.keylen;
	int vlen = c->binary_header.request.bodylen - nkey;

	if (nkey > MAX_SASL_MECH_LEN) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, NULL, vlen);
		c->write_and_go = conn_swallow;
		return;
	}

	char *key = binary_get_key(c);
	assert(key);

	item *it = item_alloc(key, nkey, 0, 0, vlen);

	if (it == 0) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, NULL, vlen);
		c->write_and_go = conn_swallow;
		return;
	}

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = vlen;
	conn_set_state(c, conn_nread);
	c->substate = bin_reading_sasl_auth_data;
}
static void process_bin_complete_sasl_auth(conn *c) 
{
    assert(settings.sasl);
    const char *out = NULL;
    unsigned int outlen = 0;

    assert(c->item);
    init_sasl_conn(c);

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    char mech[nkey+1];
    memcpy(mech, ITEM_key((item*)c->item), nkey);
    mech[nkey] = 0x00;

    if (settings.verbose)
        fprintf(stderr, "mech:  ``%s'' with %d bytes of data\n", mech, vlen);

    const char *challenge = vlen == 0 ? NULL : ITEM_data((item*) c->item);

    int result=-1;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        result = sasl_server_start(c->sasl_conn, mech,
                                   challenge, vlen,
                                   &out, &outlen);
        break;
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        result = sasl_server_step(c->sasl_conn,
                                  challenge, vlen,
                                  &out, &outlen);
        break;
    default:
        assert(false); /* CMD should be one of the above */
        /* This code is pretty much impossible, but makes the compiler
           happier */
        if (settings.verbose)
            fprintf(stderr, "Unhandled command %d with challenge %s\n", c->cmd, challenge);
        break;
    }

    item_unlink(c->item);

    if (settings.verbose) {
        fprintf(stderr, "sasl result code:  %d\n", result);
    }

    switch(result) {
    case SASL_OK:
        c->authenticated = true;
        write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        break;
    case SASL_CONTINUE:
        add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0, outlen);
        if(outlen > 0) {
            add_iov(c, out, outlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    default:
        if (settings.verbose)
            fprintf(stderr, "Unknown sasl response:  %d\n", result);
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        c->thread->stats.auth_errors++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
    }
}

static bool authenticated(conn *c) 
{
    assert(settings.sasl);
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
        rv = true;
        break;
    default:
        rv = c->authenticated;
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "authenticated() in cmd 0x%02x is %s\n",
                c->cmd, rv ? "true" : "false");
    }

    return rv;
}

static void dispatch_bin_command(conn* c)
{
	int protocol_error = 0;

	int exlen = c->binary_header.request.extlen;
	int keylen = c->binary_header.request.keylen;
	uint32_t bodylen = c->binary_header.request.bodylen;

	//检查认证
	if (settings.sasl && !authenticated(c)) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
		c->write_and_go = conn_closing;
		return;
	}

	MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
	c->noreply = true;

	if(keylen > KEY_MAX_LENGTH){
		handle_binary_protocol_error(c);
		return ;
	}

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_SETQ:
		c->cmd = PROTOCOL_BINARY_CMD_SET;
		break;
	case PROTOCOL_BINARY_CMD_ADDQ:
		c->cmd = PROTOCOL_BINARY_CMD_ADD;
		break;
	case PROTOCOL_BINARY_CMD_REPLACEQ:
		c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
		break;
	case PROTOCOL_BINARY_CMD_DELETEQ:
		c->cmd = PROTOCOL_BINARY_CMD_DELETE;
		break;
	case PROTOCOL_BINARY_CMD_INCREMENTQ:
		c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
		break;
	case PROTOCOL_BINARY_CMD_DECREMENTQ:
		c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
		break;
	case PROTOCOL_BINARY_CMD_QUITQ:
		c->cmd = PROTOCOL_BINARY_CMD_QUIT;
		break;
	case PROTOCOL_BINARY_CMD_FLUSHQ:
		c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
		break;
	case PROTOCOL_BINARY_CMD_APPENDQ:
		c->cmd = PROTOCOL_BINARY_CMD_APPEND;
		break;
	case PROTOCOL_BINARY_CMD_PREPENDQ:
		c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
		break;
	case PROTOCOL_BINARY_CMD_GETQ:
		c->cmd = PROTOCOL_BINARY_CMD_GET;
		break;
	case PROTOCOL_BINARY_CMD_GETKQ:
		c->cmd = PROTOCOL_BINARY_CMD_GETK;
		break;
	case PROTOCOL_BINARY_CMD_GATQ:
		c->cmd = PROTOCOL_BINARY_CMD_GAT;
		break;
	case PROTOCOL_BINARY_CMD_GATKQ:
		c->cmd = PROTOCOL_BINARY_CMD_GAT;
		break;
	default:
		c->noreply = false;
	}

	switch (c->cmd){
	case PROTOCOL_BINARY_CMD_VERSION:
		if (extlen == 0 && keylen == 0 && bodylen == 0)
			write_bin_response(c, VERSION, 0, 0, strlen(VERSION));
		else
			protocol_error = 1;
		break;

	case PROTOCOL_BINARY_CMD_FLUSH:
		if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
			bin_read_key(c, bin_read_flush_exptime, extlen);
		} 
		else
			protocol_error = 1;
		break;

	case PROTOCOL_BINARY_CMD_NOOP:
		if (extlen == 0 && keylen == 0 && bodylen == 0) {
			write_bin_response(c, NULL, 0, 0, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_REPLACE:
		if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
			bin_read_key(c, bin_reading_set_header, 8);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GETK:
		if (extlen == 0 && bodylen == keylen && keylen > 0) {
			bin_read_key(c, bin_reading_get_key, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_DELETE:
		if (keylen > 0 && extlen == 0 && bodylen == keylen) {
			bin_read_key(c, bin_reading_del_header, extlen);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_INCREMENT:
	case PROTOCOL_BINARY_CMD_DECREMENT:
		if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
			bin_read_key(c, bin_reading_incr_header, 20);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_APPEND:
	case PROTOCOL_BINARY_CMD_PREPEND:
		if (keylen > 0 && extlen == 0) {
			bin_read_key(c, bin_reading_set_header, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_STAT:
		if (extlen == 0) {
			bin_read_key(c, bin_reading_stat, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_QUIT:
		if (keylen == 0 && extlen == 0 && bodylen == 0) {
			write_bin_response(c, NULL, 0, 0, 0);
			c->write_and_go = conn_closing;
			if (c->noreply) {
				conn_set_state(c, conn_closing);
			}
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
		if (extlen == 0 && keylen == 0 && bodylen == 0) {
			bin_list_sasl_mechs(c);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SASL_AUTH:
	case PROTOCOL_BINARY_CMD_SASL_STEP:
		if (extlen == 0 && keylen != 0) {
			bin_read_key(c, bin_reading_sasl_auth, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_TOUCH:
	case PROTOCOL_BINARY_CMD_GAT:
	case PROTOCOL_BINARY_CMD_GATQ:
	case PROTOCOL_BINARY_CMD_GATK:
	case PROTOCOL_BINARY_CMD_GATKQ:
		if (extlen == 4 && keylen != 0) {
			bin_read_key(c, bin_reading_touch_key, 4);
		} else {
			protocol_error = 1;
		}
		break;
	default:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
			bodylen);
	}
	
	if (protocol_error)
		handle_binary_protocol_error(c);
}


static void process_bin_update(conn *c) 
{
    char *key;
    int nkey;
    int vlen;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    req->message.body.flags = ntohl(req->message.body.flags);
    req->message.body.expiration = ntohl(req->message.body.expiration);

    vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);

    if (settings.verbose > 1) {
        int ii;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            fprintf(stderr, "<%d ADD ", c->sfd);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            fprintf(stderr, "<%d SET ", c->sfd);
        } else {
            fprintf(stderr, "<%d REPLACE ", c->sfd);
        }
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }

        fprintf(stderr, " Value len is %d", vlen);
        fprintf(stderr, "\n");
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, req->message.body.flags,
            realtime(req->message.body.expiration), vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, req->message.body.flags, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, NULL, vlen);
        } else {
            out_of_memory(c, "SERVER_ERROR Out of memory allocating item");
        }

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->cmd = NREAD_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            c->cmd = NREAD_SET;
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            c->cmd = NREAD_REPLACE;
            break;
        default:
            assert(0);
    }

    if (ITEM_get_cas(it) != 0) {
        c->cmd = NREAD_CAS;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_append_prepend(conn *c) 
{
	char *key;
	int nkey;
	int vlen;
	item *it;

	assert(c != NULL);

	key = binary_get_key(c);
	nkey = c->binary_header.request.keylen;
	vlen = c->binary_header.request.bodylen - nkey;

	if (settings.verbose > 1) {
		fprintf(stderr, "Value len is %d\n", vlen);
	}

	if (settings.detail_enabled) {
		stats_prefix_record_set(key, nkey);
	}

	it = item_alloc(key, nkey, 0, 0, vlen+2);

	if (it == 0) {
		if (! item_size_ok(nkey, 0, vlen + 2)) {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, NULL, vlen);
		} else {
			out_of_memory(c, "SERVER_ERROR Out of memory allocating item");
		}
		/* swallow the data line */
		c->write_and_go = conn_swallow;
		return;
	}

	ITEM_set_cas(it, c->binary_header.request.cas);

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_APPEND:
		c->cmd = NREAD_APPEND;
		break;
	case PROTOCOL_BINARY_CMD_PREPEND:
		c->cmd = NREAD_PREPEND;
		break;
	default:
		assert(0);
	}

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = vlen;
	conn_set_state(c, conn_nread);
	c->substate = bin_read_set_value;
}

static void process_bin_flush(conn* c)
{
	time_t exptime = 0;
	protocol_binary_request_flush* req = binary_get_request(c);

	if(!settings.flush_enabled){
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
		return;
	}

	if(c->binary_header.request.extlen == sizeof(req->message.body))
		exptime = ntohl(req->message.body.expiration);

	//设置oldest_live,将过期的item回收
	if(exptime > 0)
		settings.oldest_live = realtime(exptime) - 1;
	else
		settings.oldest_live = current_time - 1;

	item_flush_expired();

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.flush_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	write_bin_response(c, NULL, 0, 0, 0);
}

static void process_bin_delete(conn *c) 
{
	item *it;

	protocol_binary_request_delete* req = binary_get_request(c);

	char* key = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;

	assert(c != NULL);

	if (settings.verbose > 1){
		int ii;
		fprintf(stderr, "Deleting ");
		for (ii = 0; ii < nkey; ++ii){
			fprintf(stderr, "%c", key[ii]);
		}
		fprintf(stderr, "\n");
	}

	if (settings.detail_enabled)
		stats_prefix_record_delete(key, nkey);

	it = item_get(key, nkey);
	if (it){
		uint64_t cas = ntohll(req->message.header.request.cas);
		if (cas == 0 || cas == ITEM_get_cas(it)){
			MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			//对item进行删除
			item_unlink(it);

			write_bin_response(c, NULL, 0, 0, 0);
		} 
		else
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);

		item_remove(it);      /* release our reference */
	} 
	else {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);

		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.delete_misses++;
		pthread_mutex_unlock(&c->thread->stats.mutex);
	}
}

static void complete_nread_binary(conn *c) 
{
	assert(c != NULL);
	assert(c->cmd >= 0);

	switch(c->substate) {
	case bin_reading_set_header:
		if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
			c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
				process_bin_append_prepend(c);
		} 
		else
			process_bin_update(c);
		break;
	case bin_read_set_value:
		complete_update_bin(c);
		break;
	case bin_reading_get_key:
	case bin_reading_touch_key:
		process_bin_get_or_touch(c);
		break;
	case bin_reading_stat:
		process_bin_stat(c);
		break;
	case bin_reading_del_header:
		process_bin_delete(c);
		break;
	case bin_reading_incr_header:
		complete_incr_bin(c);
		break;
	case bin_read_flush_exptime:
		process_bin_flush(c);
		break;
	case bin_reading_sasl_auth:
		process_bin_sasl_auth(c);
		break;
	case bin_reading_sasl_auth_data:
		process_bin_complete_sasl_auth(c);
		break;
	default:
		fprintf(stderr, "Not handling substate %d\n", c->substate);
		assert(0);
	}
}

static void reset_cmd_handler(conn* c)
{
	c->cmd = -1;
	c->substate = bin_no_state;
	if(c->item != NULL){
		item_remove(c->item);
		c->item = NULL;
	}

	//对缓冲区的回收
	conn_shrink(c);

	if(c->rbytes > 0)
		conn_set_state(c, conn_parse_cmd);
	else
		conn_set_state(c, conn_waiting);
}

static void complete_nread(conn* c)
{
	assert(c != NULL);
	assert(c->protocol == ascii_prot || c->protocol == binary_prot);

	if(c->protocol == ascii_prot)
		complete_nread_ascii(c);
	else
		complete_nread_binary(c);
}



















