#ifndef __PROTOCOL_BINARY_H
#define __PROTOCOL_BINARY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C"{
#endif

typedef enum
{
	PROTOCOL_BINARY_REQ = 0x80;
	PROTOCOL_BINARY_RES = 0x81;
}protocol_binary_magic;

//协议响应码
typedef enum
{
	PROTOCOL_BINARY_RESPONSE_SUCCESS = 0x00,
	PROTOCOL_BINARY_RESPONSE_KEY_ENOENT = 0x01,
	PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS = 0x02,
	PROTOCOL_BINARY_RESPONSE_E2BIG = 0x03,
	PROTOCOL_BINARY_RESPONSE_EINVAL = 0x04,
	PROTOCOL_BINARY_RESPONSE_NOT_STORED = 0x05,
	PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL = 0x06,
	PROTOCOL_BINARY_RESPONSE_AUTH_ERROR = 0x20,
	PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE = 0x21,
	PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND = 0x81,
	PROTOCOL_BINARY_RESPONSE_ENOMEM = 0x82	
}protocol_binary_response_status;

//请求命令类型
typedef enum
{	
	PROTOCOL_BINARY_CMD_GET = 0x00,
	PROTOCOL_BINARY_CMD_PUT = 0x01,
	PROTOCOL_BINARY_CMD_ADD = 0x02,
	PROTOCOL_BINARY_CMD_REPLACE = 0x03,
	PROTOCOL_BINARY_CMD_DELETE = 0x04,
	PROTOCOL_BINARY_CMD_INCREMENT = 0x05,
	PROTOCOL_BINARY_CMD_DECREMENT = 0x06,
	PROTOCOL_BINARY_CMD_QUIT = 0x07,
	PROTOCOL_BINARY_CMD_FLUSH = 0x08,
	PROTOCOL_BINARY_CMD_GETQ = 0x09,
	PROTOCOL_BINARY_CMD_NOOP = 0x0a,
	PROTOCOL_BINARY_CMD_VERSION = 0x0b,
	PROTOCOL_BINARY_CMD_GETK = 0x0c,
	PROTOCOL_BINARY_CMD_GETKQ = 0x0d,
	PROTOCOL_BINARY_CMD_APPEND = 0x0e,
	PROTOCOL_BINARY_CMD_PREPEND = 0x0f,
	PROTOCOL_BINARY_CMD_STAT = 0x10,
	PROTOCOL_BINARY_CMD_SETQ = 0x11,
	PROTOCOL_BINARY_CMD_ADDQ = 0x12,
	PROTOCOL_BINARY_CMD_REPLACEQ = 0x13,
	PROTOCOL_BINARY_CMD_DELETEQ = 0x14,
	PROTOCOL_BINARY_CMD_INCREMENTQ = 0x15,
	PROTOCOL_BINARY_CMD_DECREMENTQ = 0x16,
	PROTOCOL_BINARY_CMD_QUITQ = 0x17,
	PROTOCOL_BINARY_CMD_FLUSHQ = 0x18,
	PROTOCOL_BINARY_CMD_APPENDQ = 0x19,
	PROTOCOL_BINARY_CMD_PREPENDQ = 0x1a,
	PROTOCOL_BINARY_CMD_TOUCH = 0x1c,
	PROTOCOL_BINARY_CMD_GAT = 0x1d,
	PROTOCOL_BINARY_CMD_GATQ = 0x1e,
	PROTOCOL_BINARY_CMD_GATK = 0x23,
	PROTOCOL_BINARY_CMD_GATKQ = 0x24,

	PROTOCOL_BINARY_CMD_SASL_LIST_MECHS = 0x20,
	PROTOCOL_BINARY_CMD_SASL_AUTH = 0x21,
	PROTOCOL_BINARY_CMD_SASL_STEP = 0x22,

	PROTOCOL_BINARY_CMD_RGET      = 0x30,
	PROTOCOL_BINARY_CMD_RSET      = 0x31,
	PROTOCOL_BINARY_CMD_RSETQ     = 0x32,
	PROTOCOL_BINARY_CMD_RAPPEND   = 0x33,
	PROTOCOL_BINARY_CMD_RAPPENDQ  = 0x34,
	PROTOCOL_BINARY_CMD_RPREPEND  = 0x35,
	PROTOCOL_BINARY_CMD_RPREPENDQ = 0x36,
	PROTOCOL_BINARY_CMD_RDELETE   = 0x37,
	PROTOCOL_BINARY_CMD_RDELETEQ  = 0x38,
	PROTOCOL_BINARY_CMD_RINCR     = 0x39,
	PROTOCOL_BINARY_CMD_RINCRQ    = 0x3a,
	PROTOCOL_BINARY_CMD_RDECR     = 0x3b,
	PROTOCOL_BINARY_CMD_RDECRQ    = 0x3c
}protocol_binary_command;

typedef enum
{
	PROTOCOL_BINARY_RAW_BYTES = 0x00
}protocol_binary_datatypes;

typedef union {
	struct {
		uint8_t magic;
		uint8_t opcode;
		uint16_t keylen;
		uint8_t extlen;
		uint8_t datatype;
		uint16_t reserved;
		uint32_t bodylen;
		uint32_t opaque;
		uint64_t cas;
	} request;
	uint8_t bytes[24];
} protocol_binary_request_header;

typedef union{
	struct 
	{
		uint8_t magic;		//协议类型
		uint8_t opcode;		//命令类型
		uint16_t keylen;	
		uint8_t extlen;	
		uint8_t datatype;	
		uint16_t status;
		uint32_t bodylen;
		uint32_t opaque;
		uint64_t cas;
	}response;
	uint8_t bytes[24];
}protocol_binary_response_header;

typedef union{
	struct{
		protocol_binary_response_header
	}message;
	uint8_t bytes[sizeof(protocol_binary_request_header)];
}protocol_binary_request_no_extras;

typedef union {
	struct {
		protocol_binary_response_header header;
	} message;
	uint8_t bytes[sizeof(protocol_binary_response_header)];
} protocol_binary_response_no_extras;

typedef protocol_binary_request_no_extras protocol_binary_request_get;
typedef protocol_binary_request_no_extras protocol_binary_request_getq;
typedef protocol_binary_request_no_extras protocol_binary_request_getk;
typedef protocol_binary_request_no_extras protocol_binary_request_getkq;

typedef union
{
	struct{
		protocol_binary_response_header header;
		struct{
			uint32_t flags;
		}body;
	}message;
	uint8_t bytes[sizeof(protocol_binary_response_header) + 4]
}protocol_binary_response_get;

typedef protocol_binary_response_get protocol_binary_response_getq;
typedef protocol_binary_response_get protocol_binary_response_getk;
typedef protocol_binary_response_get protocol_binary_response_getkq;

typedef protocol_binary_request_no_extras protocol_binary_request_delete;
typedef protocol_binary_response_no_extras protocol_binary_response_delete;

typedef union{
	struct{
		protocol_binary_request_header header;
		struct{
			uint32_t expiration;
		}body;
	}message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
}protocol_binary_request_flush;

typedef protocol_binary_response_no_extras protocol_binary_response_flush;

typedef union {
	struct {
		protocol_binary_request_header header;
		struct {
			uint32_t flags;
			uint32_t expiration;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_set;

typedef protocol_binary_request_set protocol_binary_request_add;
typedef protocol_binary_request_set protocol_binary_request_replace;

typedef protocol_binary_response_no_extras protocol_binary_response_set;
typedef protocol_binary_response_no_extras protocol_binary_response_add;
typedef protocol_binary_response_no_extras protocol_binary_response_replace;

typedef protocol_binary_request_no_extras protocol_binary_request_noop;
typedef protocol_binary_response_no_extras protocol_binary_response_noop;

typedef union {
	struct {
		protocol_binary_request_header header;
		struct {
			uint64_t delta;
			uint64_t initial;
			uint32_t expiration;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 20];
} protocol_binary_request_incr;

typedef protocol_binary_request_incr protocol_binary_request_decr;

typedef union {
	struct {
		protocol_binary_response_header header;
		struct {
			uint64_t value;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_response_header) + 8];
} protocol_binary_response_incr;

typedef protocol_binary_response_incr protocol_binary_response_decr;
typedef protocol_binary_request_no_extras protocol_binary_request_quit;
typedef protocol_binary_response_no_extras protocol_binary_response_quit;
typedef protocol_binary_request_no_extras protocol_binary_request_append;
typedef protocol_binary_request_no_extras protocol_binary_request_prepend;
typedef protocol_binary_response_no_extras protocol_binary_response_append;
typedef protocol_binary_response_no_extras protocol_binary_response_prepend;
typedef protocol_binary_request_no_extras protocol_binary_request_version;
typedef protocol_binary_response_no_extras protocol_binary_response_version;
typedef protocol_binary_request_no_extras protocol_binary_request_stats;
typedef protocol_binary_response_no_extras protocol_binary_response_stats;

typedef union {
	struct {
		protocol_binary_request_header header;
		struct {
			uint32_t expiration;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_touch;

typedef protocol_binary_response_no_extras protocol_binary_response_touch;

typedef union {
	struct {
		protocol_binary_request_header header;
		struct {
			uint32_t expiration;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_gat;

typedef protocol_binary_request_gat protocol_binary_request_gatq;
typedef protocol_binary_request_gat protocol_binary_request_gatk;
typedef protocol_binary_request_gat protocol_binary_request_gatkq;

typedef protocol_binary_response_get protocol_binary_response_gat;
typedef protocol_binary_response_get protocol_binary_response_gatq;
typedef protocol_binary_response_get protocol_binary_response_gatk;
typedef protocol_binary_response_get protocol_binary_response_gatkq;

typedef union {
	struct {
		protocol_binary_response_header header;
		struct {
			uint16_t size;
			uint8_t  reserved;
			uint8_t  flags;
			uint32_t max_results;
		} body;
	} message;
	uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_rangeop;

typedef protocol_binary_request_rangeop protocol_binary_request_rget;
typedef protocol_binary_request_rangeop protocol_binary_request_rset;
typedef protocol_binary_request_rangeop protocol_binary_request_rsetq;
typedef protocol_binary_request_rangeop protocol_binary_request_rappend;
typedef protocol_binary_request_rangeop protocol_binary_request_rappendq;
typedef protocol_binary_request_rangeop protocol_binary_request_rprepend;
typedef protocol_binary_request_rangeop protocol_binary_request_rprependq;
typedef protocol_binary_request_rangeop protocol_binary_request_rdelete;
typedef protocol_binary_request_rangeop protocol_binary_request_rdeleteq;
typedef protocol_binary_request_rangeop protocol_binary_request_rincr;
typedef protocol_binary_request_rangeop protocol_binary_request_rincrq;
typedef protocol_binary_request_rangeop protocol_binary_request_rdecr;
typedef protocol_binary_request_rangeop protocol_binary_request_rdecrq;

#ifdef __cplusplus
};
#endif

#endif




