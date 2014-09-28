#ifndef __SLABS_H_
#define __SLABS_H_

void slabs_init(const size_t limit, const double factor, const bool prealloc);

unsigned int slabs_clsid(const size_t size);

void* slabs_alloc(const size_t size, unsigned int id);

void slabs_free(void* ptr, size_t size, unsigned int id);

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal);

bool get_stats(const char* stat_type, int nkey, ADD_STAT add_stat, void* c);

void slabs_stats(ADD_STAT add_stats, void* c);

int start_slab_maintenance_thread();
void stop_slab_maintenance_thread();


enum reassign_result_type
{
	REASSIGN_OK=0,
	REASSIGN_RUNNING,
	REASSIGN_BADCLASS,
	REASSIGN_NOSPARE,
	REASSIGN_SRC_DST_SAME
};

enum reassign_result_type slabs_reassign(int src, int dst);

void slabs_rebalancer_pause();
void slabs_rebalancer_resume();

#endif






