/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2016 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author:                                                              |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#ifndef PHP_BEANSTALKD_H
#define PHP_BEANSTALKD_H

extern zend_module_entry beanstalkd_module_entry;
#define phpext_beanstalkd_ptr &beanstalkd_module_entry

#define PHP_BEANSTALKD_VERSION "0.1.0" /* Replace with version number for your extension */


#define BSC_BUF_SIZE 4096
#define BSC_DEFAULT_TIMEOUT 1				/* seconds */
#define BSC_KEY_MAX_SIZE 250				/* stoled from memcached sources =) */
#define BSC_DEFAULT_RETRY 15 				/* retry failed server after x seconds */
#define BSC_DEFAULT_SAVINGS 0.2				/* minimum 20% savings for compression to be used */
#define BSC_DEFAULT_CACHEDUMP_LIMIT	100		/* number of entries */

#define BSC_STATUS_FAILED 0
#define BSC_STATUS_DISCONNECTED 1
#define BSC_STATUS_UNKNOWN 2
#define BSC_STATUS_CONNECTED 3

#define BSC_OK 					0
#define BSC_REQUEST_FAILURE 	-1

#define BSC_STANDARD_HASH 1
#define BSC_CONSISTENT_HASH 2
#define BSC_HASH_CRC32 1					/* CRC32 hash function */
#define BSC_HASH_FNV1A 2					/* FNV-1a hash function */

#define BSC_CONSISTENT_POINTS 160			/* points per server */
#define BSC_CONSISTENT_BUCKETS 1024			/* number of precomputed buckets, should be power of 2 */

#ifdef PHP_WIN32
#	define PHP_BEANSTALKD_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_BEANSTALKD_API __attribute__ ((visibility("default")))
#else
#	define PHP_BEANSTALKD_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

#include "ext/standard/php_smart_str_public.h"

PHP_MINIT_FUNCTION(beanstalkd);
PHP_MSHUTDOWN_FUNCTION(beanstalkd);
PHP_RINIT_FUNCTION(beanstalkd);
PHP_MINFO_FUNCTION(beanstalkd);

PHP_FUNCTION(beanstalkd_connect);
PHP_FUNCTION(beanstalkd_pconnect);
PHP_FUNCTION(beanstalkd_add_server);
PHP_FUNCTION(beanstalkd_set_server_params);

typedef struct bsc {
    php_stream				*stream;                //数据流句柄（指针）
    char					inbuf[BSC_BUF_SIZE];    //用来存放从流中读取的数据的字符空间
    smart_str				outbuf;
    char					*host;
    unsigned short			port;
    long					timeout;
    long					timeoutms; /* takes precedence over timeout */
    long					connect_timeoutms; /* takes precedence over timeout */
    long					failed;
    long					retry_interval;
    int						persistent;
    int						status;
    char					*error;					/* last error message */
    int						errnum;					/* last error code */
    zval					*failure_callback;
    zend_bool				in_free;
} bsc_t;


/* hashing strategy */
typedef unsigned int (*bsc_hash_function)(const char *, int);
typedef void * (*bsc_hash_create_state)(bsc_hash_function);
typedef void (*bsc_hash_free_state)(void *);
typedef bsc_t * (*bsc_hash_find_server)(void *, const char *, int TSRMLS_DC);
typedef void (*bsc_hash_add_server)(void *, bsc_t *, unsigned int);

#define bsc_pool_find(pool, key, key_len) \
	pool->hash->find_server(pool->hash_state, key, key_len)

typedef struct bsc_hash {
    bsc_hash_create_state	create_state;
    bsc_hash_free_state		free_state;
    bsc_hash_find_server	find_server;
    bsc_hash_add_server		add_server;
} bsc_hash_t;

typedef struct bsc_pool {
    bsc_t					**servers;
    int						num_servers;
    bsc_t					**requests;
    int						compress_threshold;
    double					min_compress_savings;
    zend_bool				in_free;
    bsc_hash_t				*hash;
    void					*hash_state;
} bsc_pool_t;

/* 32 bit magic FNV-1a prime and init */
#define FNV_32_PRIME 0x01000193
#define FNV_32_INIT 0x811c9dc5


ZEND_BEGIN_MODULE_GLOBALS(beanstalkd)
	long debug_mode;
	long default_port;
	long num_persistent;
	long compression_level;
	long allow_failover;
	long chunk_size;
	long max_failover_attempts;
	long hash_strategy;
	long hash_function;
	long default_timeout_ms;
ZEND_END_MODULE_GLOBALS(beanstalkd)

#if (PHP_MAJOR_VERSION == 5) && (PHP_MINOR_VERSION >= 3)
#   define BEANSTALKD_IS_CALLABLE(cb_zv, flags, cb_sp) zend_is_callable((cb_zv), (flags), (cb_sp) TSRMLS_CC)
#else
#   define BEANSTALKD_IS_CALLABLE(cb_zv, flags, cb_sp) zend_is_callable((cb_zv), (flags), (cb_sp))
#endif

#if (PHP_MAJOR_VERSION == 5) && (PHP_MINOR_VERSION >= 4)
#    define BEANSTALKD_LIST_INSERT(item, list) zend_list_insert(item, list TSRMLS_CC);
#else
#    define BEANSTALKD_LIST_INSERT(item, list) zend_list_insert(item, list);
#endif

bsc_t *bsc_server_new(char *, int, unsigned short, int, int, int TSRMLS_DC);
bsc_t *bsc_find_persistent(char *, int, int, int, int TSRMLS_DC);
int bsc_server_failure(bsc_t * TSRMLS_DC);
void bsc_server_deactivate(bsc_t * TSRMLS_DC);

int bsc_prepare_key(zval *, char *, unsigned int * TSRMLS_DC);
int bsc_prepare_key_ex(const char *, unsigned int, char *, unsigned int * TSRMLS_DC);

bsc_pool_t *bsc_pool_new(TSRMLS_D);
void bsc_pool_free(bsc_pool_t * TSRMLS_DC);
void bsc_pool_add(bsc_pool_t *, bsc_t *, unsigned int);
int bsc_pool_store(bsc_pool_t *, const char *, int, const char *, int, int, int, const char *, int TSRMLS_DC);
int bsc_open(bsc_t *, int, char **, int * TSRMLS_DC);
int bsc_exec_retrieval_cmd(bsc_pool_t *, const char *, int, zval **, zval * TSRMLS_DC);
int bsc_delete(bsc_t *, const char *, int, int TSRMLS_DC);

/* In every utility function you add that needs to use variables 
   in php_beanstalkd_globals, call TSRMLS_FETCH(); after declaring other 
   variables used by that function, or better yet, pass in TSRMLS_CC
   after the last function argument and declare your utility function
   with TSRMLS_DC after the last declared argument.  Always refer to
   the globals in your function as BEANSTALKD_G(variable).  You are 
   encouraged to rename these macros something shorter, see
   examples in any other php module directory.
*/

#ifdef ZTS
#define BEANSTALKD_G(v) TSRMG(beanstalkd_globals_id, zend_beanstalkd_globals *, v)
#else
#define BEANSTALKD_G(v) (beanstalkd_globals.v)
#endif

#ifndef ZSTR
#define ZSTR
#endif

#endif	/* PHP_BEANSTALKD_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
