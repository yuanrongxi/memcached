#ifndef __ITEMS_H_
#define __ITEMS_H_

#include <stdint.h>

enum crawler_result_type
{
	CRAWLER_OK = 0,
	CRAWLER_RUNNING,
	CRAWLER_BADCLASS
};

item* do_item_alloc(char* key, const size_t nkey, const int flags, const rel_time_t exptime, int nbytes, const uint32_t cur_hv);
void item_free(item* it);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

int do_item_link(item* it, const uint32_t hv);
void do_item_unlink(item* it, const uint32_t hv);
void do_item_unlink_nolock(item* it, const uint32_t hv);
void do_item_remove(item* it);
void do_item_update(item* it);
void do_item_replace(item *it, item *new_it, const uint32_t hv);

char* do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int* bytes);
void do_item_stats(ADD_STAT add_stats, void* c);
void do_item_stats_totals(ADD_STAT add_stats, void* c);

void do_item_stats_sizes(ADD_STAT add_stats, void* c);
void do_item_flush_expired();

item* do_item_get(const char* key, const size_t nkey, const uint32_t hv);
item* do_item_touch(const char *key, const size_t nkey, uint32_t exptime, const uint32_t hv);

void item_stats_reset();

extern pthread_mutex_t cache_lock;

void item_stats_evictions(uint64_t* evicted);

int start_item_crawler_thread();
int stop_item_crawler_thread();
int init_lru_crawler();
enum crawler_result_type lru_crawler_crawl(char* slabs);

#endif