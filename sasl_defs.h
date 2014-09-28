#ifndef SASL_DEFS_H_
#define SASL_DEFS_H_

#define MAX_SASL_MECH_LEN 32

#if defined(HAVE_SASL_SASL_H) && defined(ENABLE_SASL)

#include <sasl/sasl.h>
void init_sasl(void);

extern char my_sasl_hostname[1025];

#else
typedef void* sasl_conn_t;

#define init_sasl() {}
#define sasl_dispose(x) {}
#define sasl_server_new(a, b, c, d, e, f, g, h) 1
#define sasl_listmech(a, b, c, d, e, f, g, h) 1
#define sasl_server_start(a, b, c, d, e, f) 1
#define sasl_server_step(a, b, c, d, e) 1
#define sasl_getprop(a, b, c) {}

#define SASL_OK 0
#define SASL_CONTINUE -1
#endif


#endif





