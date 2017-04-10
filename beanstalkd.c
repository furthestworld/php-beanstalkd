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
  | Author:  xuanskyer <furthestworld@icloud.com>                        |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include <stdio.h>
#include <fcntl.h>

#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#include <zlib.h>
#include <time.h>
#include "ext/standard/crc32.h"
#include "ext/standard/info.h"
#include "ext/standard/php_string.h"
#include "ext/standard/php_var.h"
#include "ext/standard/php_smart_str.h"
#include "php_network.h"
#include "php_beanstalkd.h"

#ifndef ZEND_ENGINE_2
#define OnUpdateLong OnUpdateInt
#endif

/* True global resources - no need for thread safety here */
static int le_beanstalkd_pool, le_beanstalkd;
static zend_class_entry *beanstalkd_class_entry_ptr;

ZEND_DECLARE_MODULE_GLOBALS(beanstalkd)


static PHP_INI_MH(OnUpdateChunkSize) /* {{{ */
    {
        long int lval;

    lval = strtol(new_value, NULL, 10);
    if (lval <= 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.chunk_size must be a positive integer ('%s' given)", new_value);
        return FAILURE;
    }

    return OnUpdateLong(entry, new_value, new_value_length, mh_arg1, mh_arg2, mh_arg3, stage TSRMLS_CC);
    }
/* }}} */

static PHP_INI_MH(OnUpdateFailoverAttempts) /* {{{ */
    {
        long int lval;

    lval = strtol(new_value, NULL, 10);
    if (lval <= 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.max_failover_attempts must be a positive integer ('%s' given)", new_value);
        return FAILURE;
    }

    return OnUpdateLong(entry, new_value, new_value_length, mh_arg1, mh_arg2, mh_arg3, stage TSRMLS_CC);
    }
/* }}} */

static PHP_INI_MH(OnUpdateHashStrategy) /* {{{ */
    {
        if (!strcasecmp(new_value, "standard")) {
            BEANSTALKD_G(hash_strategy) = BSC_STANDARD_HASH;
        }
        else if (!strcasecmp(new_value, "consistent")) {
            BEANSTALKD_G(hash_strategy) = BSC_CONSISTENT_HASH;
        }
        else {
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "beanstalkd.hash_strategy must be in set {standard, consistent} ('%s' given)", new_value);
            return FAILURE;
        }

        return SUCCESS;
    }
/* }}} */

static PHP_INI_MH(OnUpdateHashFunction) /* {{{ */
    {
        if (!strcasecmp(new_value, "crc32")) {
            BEANSTALKD_G(hash_function) = BSC_HASH_CRC32;
        }
        else if (!strcasecmp(new_value, "fnv")) {
            BEANSTALKD_G(hash_function) = BSC_HASH_FNV1A;
        }
        else {
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "beanstalkd.hash_function must be in set {crc32, fnv} ('%s' given)", new_value);
            return FAILURE;
        }

        return SUCCESS;
    }
/* }}} */

static PHP_INI_MH(OnUpdateDefaultTimeout) /* {{{ */
    {
        long int lval;

    lval = strtol(new_value, NULL, 10);
    if (lval <= 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.default_timeout must be a positive number greater than or equal to 1 ('%s' given)", new_value);
        return FAILURE;
    }
    BEANSTALKD_G(default_timeout_ms) = lval;
    return SUCCESS;
    }
/* }}} */
/* {{{ PHP_INI
 */
PHP_INI_BEGIN()

STD_PHP_INI_ENTRY("beanstalkd.allow_failover",    "1",        PHP_INI_ALL, OnUpdateLong,        allow_failover,    zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.max_failover_attempts",    "20",    PHP_INI_ALL, OnUpdateFailoverAttempts,        max_failover_attempts,    zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.default_port",        "11300",    PHP_INI_ALL, OnUpdateLong,        default_port,    zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.chunk_size",        "8192",        PHP_INI_ALL, OnUpdateChunkSize,    chunk_size,        zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.hash_strategy",        "standard",    PHP_INI_ALL, OnUpdateHashStrategy,    hash_strategy,    zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.hash_function",        "crc32",    PHP_INI_ALL, OnUpdateHashFunction,    hash_function,    zend_beanstalkd_globals,    beanstalkd_globals)
STD_PHP_INI_ENTRY("beanstalkd.default_timeout_ms",    "1000",    PHP_INI_ALL, OnUpdateDefaultTimeout,    default_timeout_ms,    zend_beanstalkd_globals,    beanstalkd_globals)

PHP_INI_END()
/* }}} */


/* {{{ internal function protos */
static void _bsc_pool_list_dtor(zend_rsrc_list_entry *TSRMLS_DC);

static void _bsc_pserver_list_dtor(zend_rsrc_list_entry *TSRMLS_DC);

static void bsc_server_free(bsc_t *TSRMLS_DC);

static void bsc_server_disconnect(bsc_t *TSRMLS_DC);

static int bsc_server_store(bsc_t *, const char *, int TSRMLS_DC);

static int bsc_get_pool(zval *, bsc_pool_t **TSRMLS_DC);

static int bsc_readline(bsc_t *TSRMLS_DC);

static char *bsc_get_version(bsc_t *TSRMLS_DC);

static int bsc_str_left(char *, char *, int, int);

static int bsc_sendcmd(bsc_t *, const char *, int TSRMLS_DC);

static int bsc_parse_response(bsc_t *bsc, char *, int, char **, int *, int *, int *);

static int bsc_exec_retrieval_cmd_multi(bsc_pool_t *, zval *, zval **, zval *TSRMLS_DC);

static int bsc_read_value(bsc_t *, char **, int *, char **, int *, int *TSRMLS_DC);

static int bsc_flush(bsc_t *, int TSRMLS_DC);

static void php_bsc_store(INTERNAL_FUNCTION_PARAMETERS, char *, int);

static int bsc_get_stats(bsc_t *, char *, int, int, zval *TSRMLS_DC);

static int bsc_incr_decr(bsc_t *, int, char *, int, int, long *TSRMLS_DC);

static void php_bsc_incr_decr(INTERNAL_FUNCTION_PARAMETERS, int);

static void php_bsc_connect(INTERNAL_FUNCTION_PARAMETERS, int);

extern bsc_hash_t bsc_standard_hash;
extern bsc_hash_t bsc_consistent_hash;

/* }}} */


/* {{{ php_beanstalkd_init_globals
 */
static void php_beanstalkd_init_globals(zend_beanstalkd_globals *beanstalkd_globals) {
    BEANSTALKD_G(debug_mode) = 0;
    BEANSTALKD_G(num_persistent) = 0;
    BEANSTALKD_G(compression_level) = Z_DEFAULT_COMPRESSION;
    BEANSTALKD_G(hash_strategy) = BSC_STANDARD_HASH;
    BEANSTALKD_G(hash_function) = BSC_HASH_CRC32;
    BEANSTALKD_G(default_timeout_ms) = (BSC_DEFAULT_TIMEOUT) * 1000;
}
/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(beanstalkd)
    {

        zend_class_entry beanstalkd_class_entry;
    INIT_CLASS_ENTRY(beanstalkd_class_entry, "Beanstalkd", php_beanstalkd_class_functions);
    beanstalkd_class_entry_ptr = zend_register_internal_class(&beanstalkd_class_entry TSRMLS_CC);

    le_beanstalkd_pool = zend_register_list_destructors_ex(_bsc_pool_list_dtor, NULL, "beanstalkd connection", module_number);
    le_pbeanstalkd = zend_register_list_destructors_ex(NULL, _bsc_pserver_list_dtor, "persistent beanstalkd connection", module_number);

#ifdef ZTS
    ts_allocate_id(&beanstalkd_globals_id, sizeof(zend_beanstalkd_globals), (ts_allocate_ctor) php_beanstalkd_init_globals, NULL);
#else
    php_beanstalkd_init_globals(&beanstalkd_globals TSRMLS_CC);
#endif

    REGISTER_INI_ENTRIES();

    return SUCCESS;
   
    }
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(beanstalkd)
    {
        UNREGISTER_INI_ENTRIES();
    return SUCCESS;
    }
/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(beanstalkd)
    {
        return SUCCESS;
    }
/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(beanstalkd)
    {
        return SUCCESS;
    }
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(beanstalkd)
    {
        php_info_print_table_start();
    php_info_print_table_header(2, "beanstalkd support", "enabled");
    php_info_print_table_end();

    DISPLAY_INI_ENTRIES();
    }
/* }}} */

/* {{{ beanstalkd_functions[]
 *
 * Every user visible function must have an entry in beanstalkd_functions[].
 */
    const
zend_function_entry beanstalkd_functions[] = {
    PHP_FE(beanstalkd_add_server, NULL)
    PHP_FE(beanstalkd_use, NULL)
    PHP_FE(beanstalkd_put, NULL)
    PHP_FE(beanstalkd_watch, NULL)
    PHP_FE(beanstalkd_reserve, NULL)
    PHP_FE(beanstalkd_bury, NULL)
    PHP_FE(beanstalkd_ignore, NULL)
    PHP_FE(beanstalkd_delete, NULL)
    PHP_FE(beanstalkd_kick, NULL)
    PHP_FE(beanstalkd_list_tubes, NULL)
    PHP_FE_END    /* Must be the last line in beanstalkd_functions[] */
};
/* }}} */

static zend_function_entry php_beanstalkd_class_functions[] = {
    PHP_FALIAS(add_server, beanstalkd_add_server, NULL)
    PHP_FALIAS(use, beanstalkd_use, NULL)
    PHP_FALIAS(put, beanstalkd_put, NULL)
    PHP_FALIAS(watch, beanstalkd_watch, NULL)
    PHP_FALIAS(reserve, beanstalkd_reserve, NULL)
    PHP_FALIAS(bury, beanstalkd_bury, NULL)
    PHP_FALIAS(ignore, beanstalkd_ignore, NULL)
    PHP_FALIAS(delete, beanstalkd_delete, NULL)
    PHP_FALIAS(kick, beanstalkd_kick, NULL)
    PHP_FALIAS(list_tubes, beanstalkd_list_tubes, NULL)
    { NULL, NULL, NULL }
};
/* {{{ beanstalkd_module_entry
 */
zend_module_entry beanstalkd_module_entry = {
    STANDARD_MODULE_HEADER,
    "beanstalkd",
    beanstalkd_functions,
    PHP_MINIT(beanstalkd),
    PHP_MSHUTDOWN(beanstalkd),
    PHP_RINIT(beanstalkd),        /* Replace with NULL if there's nothing to do at request start */
    PHP_RSHUTDOWN(beanstalkd),    /* Replace with NULL if there's nothing to do at request end */
    PHP_MINFO(beanstalkd),
    PHP_BEANSTALKD_VERSION,
    STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_BEANSTALKD
ZEND_GET_MODULE(beanstalkd)
#endif


/*
 * 这里定义了一个类型为timeval结构体的超时时间变量。
 * 这个timeval结构体的定义在time.h里面。自行查找。
 * 好久没看C了。猛一看到这个有点短时性蒙逼， 一般定义结构体不都是个变量么？还可以这么玩啊
 * 定义+初始化一起。毫秒 转 秒+微秒
 */
static struct timeval _convert_timeoutms_to_ts(long msecs) /* {{{ */
{
    struct timeval tv;
    int secs = 0;

    secs = msecs / 1000;
    tv.tv_sec = secs;
    tv.tv_usec = ((msecs - (secs * 1000)) * 1000) % 1000000;
    return tv;
}
/* }}} */

/*
 * beanstalkd连接释放函数，各种嵌套调用啊！
 * 这里又调用了bsc_pool_free。
 * zend_rsrc_list_entry是zend_list.h里面定义的一个结构体变量_zend_rsrc_list_entry，用来描述一个资源。
 * 这个函数的大招就是：调用bsc_pool_free去释放一个资源。
 * 接下来就请大家和记者一起连线bsc_pool_free，看看它是怎么做的————
 */
static void _bsc_pool_list_dtor(zend_rsrc_list_entry *rsrc TSRMLS_DC) /* {{{ */
{
    bsc_pool_free((bsc_pool_t *) rsrc->ptr
    TSRMLS_CC);
}

/* }}} */

static void _bsc_pserver_list_dtor(zend_rsrc_list_entry *rsrc TSRMLS_DC) /* {{{ */
{
    bsc_server_free((bsc_t *) rsrc->ptr
    TSRMLS_CC);
}

/* }}} */

bsc_t *bsc_server_new(char *host, int host_len, unsigned short port, int persistent, int timeout, int retry_interval
                      TSRMLS_DC) /* {{{ */
{
    bsc_t *bsc = pemalloc(sizeof(bsc_t), persistent);
    memset(bsc, 0, sizeof(*bsc));

    bsc->host = pemalloc(host_len + 1, persistent);
    memcpy(bsc->host, host, host_len);
    bsc->host[host_len] = '\0';

    bsc->port = port;
    bsc->status = MMC_STATUS_DISCONNECTED;

    bsc->persistent = persistent;
    if (persistent) {
        MEMCACHE_G(num_persistent)++;
    }

    bsc->timeout = timeout;
    bsc->retry_interval = retry_interval;

    return bsc;
}

/* }}} */

static void bsc_server_callback_dtor(zval **callback TSRMLS_DC) /* {{{ */
{
    zval **this_obj;

    if (!callback || !*callback) return;

    if (Z_TYPE_PP(callback) == IS_ARRAY &&
        zend_hash_index_find(Z_ARRVAL_PP(callback), 0, (void **) &this_obj) == SUCCESS &&
        Z_TYPE_PP(this_obj) == IS_OBJECT) {
        zval_ptr_dtor(this_obj);
    }
    zval_ptr_dtor(callback);
}

/* }}} */

static void bsc_server_callback_ctor(zval **callback TSRMLS_DC) /* {{{ */
{
    zval **this_obj;

    if (!callback || !*callback) return;

    if (Z_TYPE_PP(callback) == IS_ARRAY &&
        zend_hash_index_find(Z_ARRVAL_PP(callback), 0, (void **) &this_obj) == SUCCESS &&
        Z_TYPE_PP(this_obj) == IS_OBJECT) {
        zval_add_ref(this_obj);
    }
    zval_add_ref(callback);
}

/* }}} */

static void bsc_server_sleep(bsc_t *bsc TSRMLS_DC) /* 
	prepare server struct for persistent sleep {{{ */
{
    bsc_server_callback_dtor(&bsc->failure_callback
    TSRMLS_CC);
    bsc->failure_callback = NULL;

    if (bsc->error != NULL) {
        pefree(bsc->error, bsc->persistent);
        bsc->error = NULL;
    }
}
/* }}} */

/*
 *
 */
static void bsc_server_free(bsc_t *bsc TSRMLS_DC) /* {{{ */
{
    if (bsc->in_free) {
        php_error_docref(NULL
        TSRMLS_CC, E_ERROR, "Recursive reference detected, bailing out");
        return;
    }
    bsc->in_free = 1;

    bsc_server_sleep(bsc
    TSRMLS_CC);

    if (bsc->persistent) {
        free(bsc->host);
        free(bsc);
        MEMCACHE_G(num_persistent)--;
    } else {
        if (bsc->stream != NULL) {
            php_stream_close(bsc->stream);
        }
        efree(bsc->host);
        efree(bsc);
    }
}

/* }}} */

static void bsc_server_seterror(bsc_t *bsc, const char *error, int errnum) /* {{{ */
{
    if (error != NULL) {
        if (bsc->error != NULL) {
            pefree(bsc->error, bsc->persistent);
        }

        bsc->error = pestrdup(error, bsc->persistent);
        bsc->errnum = errnum;
    }
}

/* }}} */

static void bsc_server_received_error(bsc_t *bsc, int response_len)  /* {{{ */
{
    if (bsc_str_left(bsc->inbuf, "ERROR", response_len, sizeof("ERROR") - 1) ||
        bsc_str_left(bsc->inbuf, "CLIENT_ERROR", response_len, sizeof("CLIENT_ERROR") - 1) ||
        bsc_str_left(bsc->inbuf, "SERVER_ERROR", response_len, sizeof("SERVER_ERROR") - 1)) {
        bsc->inbuf[response_len < MMC_BUF_SIZE - 1 ? response_len : MMC_BUF_SIZE - 1] = '\0';
        bsc_server_seterror(bsc, bsc->inbuf, 0);
    } else {
        bsc_server_seterror(bsc, "Received malformed response", 0);
    }
}

/* }}} */

int
bsc_server_failure(bsc_t *bsc TSRMLS_DC) /*determines if a request should be retried or is a hard network failure {{{ */
{
    switch (bsc->status) {
        case MMC_STATUS_DISCONNECTED:
            return 0;

            /* attempt reconnect of sockets in unknown state */
        case MMC_STATUS_UNKNOWN:
            bsc->status = MMC_STATUS_DISCONNECTED;
            return 0;
    }

    bsc_server_deactivate(bsc
    TSRMLS_CC);
    return 1;
}
/* }}} */
/*
 * 真正的beanstalkd存储操作。
 * 原理就是使用zend封装好的API，进行数据流操作：命令写入流（php_stream_write），然后读取流（bsc_readline）
 * 这里有个疑惑的地方：写入之后就立刻读取，这里的实时性是如何保证的呢？是不是读取流的时候是个阻塞等待操作？
 */
static int bsc_server_store(bsc_t *bsc, const char *request, int request_len TSRMLS_DC) /* {{{ */
{
    int response_len;
    php_netstream_data_t *sock = (php_netstream_data_t *) bsc->stream->abstract;

    if (bsc->timeoutms > 1) {
        sock->timeout = _convert_timeoutms_to_ts(bsc->timeoutms);
    }

    if (php_stream_write(bsc->stream, request, request_len) != request_len) {
        bsc_server_seterror(bsc, "Failed sending command and value to stream", 0);
        return -1;
    }

    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0) {
        return -1;
    }

    if (bsc_str_left(bsc->inbuf, "STORED", response_len, sizeof("STORED") - 1)) {
        return 1;
    }

    /* return FALSE */
    if (bsc_str_left(bsc->inbuf, "NOT_STORED", response_len, sizeof("NOT_STORED") - 1)) {
        return 0;
    }

    /* return FALSE without failover */
    if (bsc_str_left(bsc->inbuf, "SERVER_ERROR out of memory", response_len,
                     sizeof("SERVER_ERROR out of memory") - 1) ||
        bsc_str_left(bsc->inbuf, "SERVER_ERROR object too large", response_len,
                     sizeof("SERVER_ERROR object too large") - 1)) {
        return 0;
    }

    bsc_server_received_error(bsc, response_len);
    return -1;
}
/* }}} */

/*
 * 1. key不能为空，限制key的最大长度
 * 2. 替换key中的空格为下划线
 */
int
bsc_prepare_key_ex(const char *key, unsigned int key_len, char *result, unsigned int *result_len TSRMLS_DC)  /* {{{ */
{
    unsigned int i;
    if (key_len == 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "Key cannot be empty");
        return MMC_REQUEST_FAILURE;
    }

    *result_len = key_len < MMC_KEY_MAX_SIZE ? key_len : MMC_KEY_MAX_SIZE;
    result[*result_len] = '\0';

    for (i = 0; i < *result_len; i++) {
        result[i] = ((unsigned char) key[i]) > ' ' ? key[i] : '_';
    }

    return MMC_OK;
}

/* }}} */

int bsc_prepare_key(zval *key, char *result, unsigned int *result_len TSRMLS_DC)  /* {{{ */
{
    if (Z_TYPE_P(key) == IS_STRING) {
        return bsc_prepare_key_ex(Z_STRVAL_P(key), Z_STRLEN_P(key), result, result_len
        TSRMLS_CC);
    } else {
        int res;
        zval *keytmp;
        ALLOC_ZVAL(keytmp);

        *keytmp = *key;
        zval_copy_ctor(keytmp);
        convert_to_string(keytmp);

        res = bsc_prepare_key_ex(Z_STRVAL_P(keytmp), Z_STRLEN_P(keytmp), result, result_len
        TSRMLS_CC);

        zval_dtor(keytmp);
        FREE_ZVAL(keytmp);

        return res;
    }
}

/* }}} */

static unsigned int bsc_hash_crc32(const char *key, int key_len) /* CRC32 hash {{{ */
{
    unsigned int crc = ~0;
    int i;

    for (i = 0; i < key_len; i++) {
        CRC32(crc, key[i]);
    }

    return ~crc;
}

/* }}} */

static unsigned int bsc_hash_fnv1a(const char *key, int key_len) /* FNV-1a hash {{{ */
{
    unsigned int hval = FNV_32_INIT;
    int i;

    for (i = 0; i < key_len; i++) {
        hval ^= (unsigned int) key[i];
        hval *= FNV_32_PRIME;
    }

    return hval;
}
/* }}} */

/*
 * 连接池hash策略和hash函数初始化
 * 在这里完成了在php_beanstalkd.h中提到的“抽象类”的继承和抽象方法的具体实现的工作。
 *
 */
static void bsc_pool_init_hash(bsc_pool_t *pool TSRMLS_DC) /* {{{ */
{
    bsc_hash_function hash;

    /*
     * 这里指定了“抽象类”的继承关系。以扩展默认的来说，
     * 这里继续把bsc_standard_hash类比做一个子类，它继承了这个抽象类，并且实现了4个抽象方法。
     */
    switch (MEMCACHE_G(hash_strategy)) {
        case MMC_CONSISTENT_HASH:
            pool->hash = &bsc_consistent_hash;
            break;
        default:
            pool->hash = &bsc_standard_hash;
    }

    /*
     * 这里指定了具体的hash函数
     */
    switch (MEMCACHE_G(hash_function)) {
        case MMC_HASH_FNV1A:
            hash = &bsc_hash_fnv1a;
            break;
        default:
            hash = &bsc_hash_crc32;
    }

    /*
     * 经过上面之后，即完成了抽象类的继承和抽象方法的实现。以默认为例：
     *  pool->hash->create_state(hash) 相当于 执行了
     *  bsc_standard_hash->bsc_standard_create_state函数
     *  后面的pool->hash->add_server等函数同理。
     */
    pool->hash_state = pool->hash->create_state(hash);
}
/* }}} */

/*
 * 新建一个连接池
 */
bsc_pool_t *bsc_pool_new(TSRMLS_D) /* {{{ */
{
    bsc_pool_t *pool = emalloc(sizeof(bsc_pool_t));
    pool->num_servers = 0;
    pool->compress_threshold = 0;
    pool->in_free = 0;
    pool->min_compress_savings = MMC_DEFAULT_SAVINGS;

    bsc_pool_init_hash(pool
    TSRMLS_CC);

    return pool;
}
/* }}} */

/*
 * 3、2、1————
 * 好的，_bsc_pool_list_dtor。大家可以看到这里传递了一个bsc_pool_t类型的指针过来。
 * 我们这里就开始释放这个指针所指向的空间，释放他们。是的。无罪释放。
 * 释放连接池时，也释放这个池中的所有连接和这些连接的 hash_state
 * 这里会有个多层的嵌套调用链(在调用正常的情况下如下)：
 * 短连接池：bsc_pool_free->bsc_server_free->php_stream_close->php_stream_close->efree
 * 长连接池：bsc_pool_free->bsc_server_free->bsc_server_sleep->free
 */
void bsc_pool_free(bsc_pool_t *pool TSRMLS_DC) /* {{{ */
{
    int i;
    /*
     * 这里在释放动作开始的时候先设置一个标识，表示正在释放中。。
     * 所以先判断是否正在释放中，防止递归释放。
     */
    if (pool->in_free) {
        php_error_docref(NULL
        TSRMLS_CC, E_ERROR, "Recursive reference detected, bailing out");
        return;
    }
    pool->in_free = 1;

    for (i = 0; i < pool->num_servers; i++) {
        if (!pool->servers[i]) {
            continue;
        }
        if (pool->servers[i]->persistent == 0 && pool->servers[i]->host != NULL) {
            bsc_server_free(pool->servers[i]
            TSRMLS_CC);
        } else {
            bsc_server_sleep(pool->servers[i]
            TSRMLS_CC);
        }
        pool->servers[i] = NULL;
    }

    if (pool->num_servers) {
        efree(pool->servers);
        efree(pool->requests);
    }

    pool->hash->free_state(pool->hash_state);
    efree(pool);
}
/* }}} */

/*
 * 向连接池中添加一个连接
 */
void bsc_pool_add(bsc_pool_t *pool, bsc_t *bsc, unsigned int weight) /* {{{ */
{
    /* add server and a preallocated request pointer */
    if (pool->num_servers) {
        pool->servers = erealloc(pool->servers, sizeof(bsc_t *) * (pool->num_servers + 1));
        pool->requests = erealloc(pool->requests, sizeof(bsc_t *) * (pool->num_servers + 1));
    } else {
        pool->servers = emalloc(sizeof(bsc_t *));
        pool->requests = emalloc(sizeof(bsc_t *));
    }

    pool->servers[pool->num_servers] = bsc;
    pool->num_servers++;

    pool->hash->add_server(pool->hash_state, bsc, weight);
}

/* }}} */

static int bsc_pool_close(bsc_pool_t *pool TSRMLS_DC) /* disconnects and removes all servers in the pool {{{ */
{
    if (pool->num_servers) {
        int i;

        for (i = 0; i < pool->num_servers; i++) {
            if (pool->servers[i]->persistent == 0 && pool->servers[i]->host != NULL) {
                bsc_server_free(pool->servers[i]
                TSRMLS_CC);
            } else {
                bsc_server_sleep(pool->servers[i]
                TSRMLS_CC);
            }
        }

        efree(pool->servers);
        pool->servers = NULL;
        pool->num_servers = 0;

        efree(pool->requests);
        pool->requests = NULL;

        /* reallocate the hash strategy state */
        pool->hash->free_state(pool->hash_state);
        bsc_pool_init_hash(pool
        TSRMLS_CC);
    }

    return 1;
}

/* }}} */

int bsc_pool_store(bsc_pool_t *pool, const char *command, int command_len, const char *key, int key_len, int flags,
                   int expire, const char *value, int value_len TSRMLS_DC) /* {{{ */
{
    bsc_t *bsc;
    char *request;
    int request_len, result = -1;
    char *key_copy = NULL, *data = NULL;

    if (key_len > MMC_KEY_MAX_SIZE) {
        key = key_copy = estrndup(key, MMC_KEY_MAX_SIZE);
        key_len = MMC_KEY_MAX_SIZE;
    }

    /* autocompress large values */
    if (pool->compress_threshold && value_len >= pool->compress_threshold) {
        flags |= MMC_COMPRESSED;
    }

    if (flags & MMC_COMPRESSED) {
        unsigned long data_len;

        if (!bsc_compress(&data, &data_len, value, value_len TSRMLS_CC)) {
            /* bsc_server_seterror(bsc, "Failed to compress data", 0); */
            return -1;
        }

        /* was enough space saved to motivate uncompress processing on get */
        if (data_len < value_len * (1 - pool->min_compress_savings)) {
            value = data;
            value_len = data_len;
        } else {
            flags &= ~MMC_COMPRESSED;
            efree(data);
            data = NULL;
        }
    }

    request = emalloc(
        command_len
        + 1 /* space */
        + key_len
        + 1 /* space */
        + MAX_LENGTH_OF_LONG
        + 1 /* space */
        + MAX_LENGTH_OF_LONG
        + 1 /* space */
        + MAX_LENGTH_OF_LONG
        + sizeof("\r\n") - 1
        + value_len
        + sizeof("\r\n") - 1
        + 1
    );

    request_len = sprintf(request, "%s %s %d %d %d\r\n", command, key, flags, expire, value_len);

    memcpy(request + request_len, value, value_len);
    request_len += value_len;

    memcpy(request + request_len, "\r\n", sizeof("\r\n") - 1);
    request_len += sizeof("\r\n") - 1;

    request[request_len] = '\0';

    while (result < 0 && (bsc = bsc_pool_find(pool, key, key_len
        TSRMLS_CC)) != NULL) {
        if ((result = bsc_server_store(bsc, request, request_len TSRMLS_CC)) < 0) {
            bsc_server_failure(bsc
            TSRMLS_CC);
        }
    }

    if (key_copy != NULL) {
        efree(key_copy);
    }

    if (data != NULL) {
        efree(data);
    }

    efree(request);

    return result;
}

/* }}} */

static int bsc_compress(char **result, unsigned long *result_len, const char *data, int data_len TSRMLS_DC) /* {{{ */
{
    int status, level = MEMCACHE_G(compression_level);

    *result_len = data_len + (data_len / 1000) + 25 + 1; /* some magic from zlib.c */
    *result = (char *) emalloc(*result_len);

    if (!*result) {
        return 0;
    }

    if (level >= 0) {
        status = compress2((unsigned char *) *result, result_len, (unsigned const char *) data, data_len, level);
    } else {
        status = compress((unsigned char *) *result, result_len, (unsigned const char *) data, data_len);
    }

    if (status == Z_OK) {
        *result = erealloc(*result, *result_len + 1);
        (*result)[*result_len] = '\0';
        return 1;
    }

    switch (status) {
        case Z_MEM_ERROR:
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "Not enough memory to perform compression");
            break;
        case Z_BUF_ERROR:
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "Not enough room in the output buffer to perform compression");
            break;
        case Z_STREAM_ERROR:
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "Invalid compression level");
            break;
        default:
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "Unknown error during compression");
            break;
    }

    efree(*result);
    return 0;
}

/* }}}*/

static int bsc_uncompress(char **result, unsigned long *result_len, const char *data, int data_len) /* {{{ */
{
    int status;
    unsigned int factor = 1, maxfactor = 16;
    char *tmp1 = NULL;

    do {
        *result_len = (unsigned long) data_len * (1 << factor++);
        *result = (char *) erealloc(tmp1, *result_len);
        status = uncompress((unsigned char *) *result, result_len, (unsigned const char *) data, data_len);
        tmp1 = *result;
    } while (status == Z_BUF_ERROR && factor < maxfactor);

    if (status == Z_OK) {
        *result = erealloc(*result, *result_len + 1);
        (*result)[*result_len] = '\0';
        return 1;
    }

    efree(*result);
    return 0;
}
/* }}}*/

/*
 * 这个函数的技能就是获取一个可用的连接池
 */
static int bsc_get_pool(zval *id, bsc_pool_t **pool TSRMLS_DC) /* {{{ */
{
    zval **connection;
    int resource_type;

    if (Z_TYPE_P(id) != IS_OBJECT ||
        zend_hash_find(Z_OBJPROP_P(id), "connection", sizeof("connection"), (void **) &connection) == FAILURE) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "No servers added to beanstalkd connection");
        return 0;
    }

    *pool = (bsc_pool_t *) zend_list_find(Z_LVAL_PP(connection), &resource_type);

    if (!*pool || resource_type != le_beanstalkd_pool) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "Invalid Beanstalkd->connection member variable");
        return 0;
    }

    return Z_LVAL_PP(connection);
}

/* }}} */

static int _bsc_open(bsc_t *bsc, char **error_string, int *errnum TSRMLS_DC) /* {{{ */
{
    struct timeval tv;
    char *hostname = NULL, *hash_key = NULL, *errstr = NULL;
    int hostname_len, err = 0;

    /* close open stream */
    if (bsc->stream != NULL) {
        bsc_server_disconnect(bsc
        TSRMLS_CC);
    }

    if (bsc->connect_timeoutms > 0) {
        tv = _convert_timeoutms_to_ts(bsc->connect_timeoutms);
    } else {
        tv.tv_sec = bsc->timeout;
        tv.tv_usec = 0;
    }

    if (bsc->port) {
        hostname_len = spprintf(&hostname, 0, "%s:%d", bsc->host, bsc->port);
    } else {
        hostname_len = spprintf(&hostname, 0, "%s", bsc->host);
    }

    if (bsc->persistent) {
        spprintf(&hash_key, 0, "beanstalkd:%s", hostname);
    }

#if PHP_API_VERSION > 20020918
    bsc->stream = php_stream_xport_create( hostname, hostname_len,
                                           ENFORCE_SAFE_MODE | REPORT_ERRORS,
                                           STREAM_XPORT_CLIENT | STREAM_XPORT_CONNECT,
                                           hash_key, &tv, NULL, &errstr, &err);
#else
    if (bsc->persistent) {
        switch (php_stream_from_persistent_id(hash_key, &(bsc->stream) TSRMLS_CC)) {
            case PHP_STREAM_PERSISTENT_SUCCESS:
                if (php_stream_eof(bsc->stream)) {
                    php_stream_pclose(bsc->stream);
                    bsc->stream = NULL;
                    break;
                }
            case PHP_STREAM_PERSISTENT_FAILURE:
                break;
        }
    }

    if (!bsc->stream) {
        int socktype = SOCK_STREAM;
        bsc->stream = php_stream_sock_open_host(bsc->host, bsc->port, socktype, &tv, hash_key);
    }

#endif

    efree(hostname);
    if (bsc->persistent) {
        efree(hash_key);
    }

    if (!bsc->stream) {
        MMC_DEBUG(("_bsc_open: can't open socket to host"));
        bsc_server_seterror(bsc, errstr != NULL ? errstr : "Connection failed", err);
        bsc_server_deactivate(bsc
        TSRMLS_CC);

        if (errstr) {
            if (error_string) {
                *error_string = errstr;
            } else {
                efree(errstr);
            }
        }
        if (errnum) {
            *errnum = err;
        }

        return 0;
    }

    php_stream_auto_cleanup(bsc->stream);
    php_stream_set_option(bsc->stream, PHP_STREAM_OPTION_READ_TIMEOUT, 0, &tv);
    php_stream_set_option(bsc->stream, PHP_STREAM_OPTION_WRITE_BUFFER, PHP_STREAM_BUFFER_NONE, NULL);
    php_stream_set_chunk_size(bsc->stream, MEMCACHE_G(chunk_size));

    bsc->status = MMC_STATUS_CONNECTED;

    if (bsc->error != NULL) {
        pefree(bsc->error, bsc->persistent);
        bsc->error = NULL;
    }

    return 1;
}

/* }}} */

int bsc_open(bsc_t *bsc, int force_connect, char **error_string, int *errnum TSRMLS_DC) /* {{{ */
{
    switch (bsc->status) {
        case MMC_STATUS_DISCONNECTED:
            return _bsc_open(bsc, error_string, errnum
            TSRMLS_CC);

        case MMC_STATUS_CONNECTED:
            return 1;

        case MMC_STATUS_UNKNOWN:
            /* check connection if needed */
            if (force_connect) {
                char *version;
                if ((version = bsc_get_version(bsc TSRMLS_CC)) == NULL && !_bsc_open(bsc, error_string, errnum
                TSRMLS_CC)) {
                    break;
                }
                if (version) {
                    efree(version);
                }
                bsc->status = MMC_STATUS_CONNECTED;
            }
            return 1;

        case MMC_STATUS_FAILED:
            if (bsc->retry_interval >= 0 && (long) time(NULL) >= bsc->failed + bsc->retry_interval) {
                if (_bsc_open(bsc, error_string, errnum TSRMLS_CC) /*&& bsc_flush(bsc, 0 TSRMLS_CC) > 0*/) {
                    return 1;
                }
            }
            break;
    }
    return 0;
}

/* }}} */

static void bsc_server_disconnect(bsc_t *bsc TSRMLS_DC) /* {{{ */
{
    if (bsc->stream != NULL) {
        if (bsc->persistent) {
            php_stream_pclose(bsc->stream);
        } else {
            php_stream_close(bsc->stream);
        }
        bsc->stream = NULL;
    }
    bsc->status = MMC_STATUS_DISCONNECTED;
}

/* }}} */

void bsc_server_deactivate(bsc_t *bsc TSRMLS_DC) /* 	disconnect and marks the server as down {{{ */
{
    bsc_server_disconnect(bsc
    TSRMLS_CC);
    bsc->status = MMC_STATUS_FAILED;
    bsc->failed = (long) time(NULL);

    if (bsc->failure_callback != NULL) {
        zval *retval = NULL;
        zval *host, *tcp_port, *udp_port, *error, *errnum;
        zval **params[5];

        params[0] = &host;
        params[1] = &tcp_port;
        params[2] = &udp_port;
        params[3] = &error;
        params[4] = &errnum;

        MAKE_STD_ZVAL(host);
        MAKE_STD_ZVAL(tcp_port);
        MAKE_STD_ZVAL(udp_port);
        MAKE_STD_ZVAL(error);
        MAKE_STD_ZVAL(errnum);

        ZVAL_STRING(host, bsc->host, 1);
        ZVAL_LONG(tcp_port, bsc->port);
        ZVAL_LONG(udp_port, 0);

        if (bsc->error != NULL) {
            ZVAL_STRING(error, bsc->error, 1);
        } else {
            ZVAL_NULL(error);
        }
        ZVAL_LONG(errnum, bsc->errnum);

        call_user_function_ex(EG(function_table), NULL, bsc->failure_callback, &retval, 5, params, 0, NULL
        TSRMLS_CC);

        zval_ptr_dtor(&host);
        zval_ptr_dtor(&tcp_port);
        zval_ptr_dtor(&udp_port);
        zval_ptr_dtor(&error);
        zval_ptr_dtor(&errnum);

        if (retval != NULL) {
            zval_ptr_dtor(&retval);
        }
    } else {
        php_error_docref(NULL
        TSRMLS_CC, E_NOTICE, "Server %s (tcp %d) failed with: %s (%d)",
            bsc->host, bsc->port, bsc->error, bsc->errnum);
    }
}
/* }}} */

/*
 * 这个函数的大招就是：
 * 从指定的beanstalkd连接的数据流中读取一行数据（最长读取内容为 MMC_BUF_SIZE 长度），
 * 读取成功则返回读取的数据的长度，否则返回-1
 */
static int bsc_readline(bsc_t *bsc TSRMLS_DC) /* {{{ */
{
    char *response;
    size_t response_len;

    if (bsc->stream == NULL) {
        bsc_server_seterror(bsc, "Socket is closed", 0);
        return -1;
    }

    response = php_stream_get_line(bsc->stream, ZSTR(bsc->inbuf), MMC_BUF_SIZE, &response_len);
    if (response) {
        MMC_DEBUG(("bsc_readline: read data:"));
        MMC_DEBUG(("bsc_readline:---"));
        MMC_DEBUG(("%s", response));
        MMC_DEBUG(("bsc_readline:---"));
        return response_len;
    }

    bsc_server_seterror(bsc, "Failed reading line from stream", 0);
    return -1;
}

/* }}} */

static char *bsc_get_version(bsc_t *bsc TSRMLS_DC) /* {{{ */
{
    char *version_str;
    int response_len;

    if (bsc_sendcmd(bsc, "version", sizeof("version") - 1 TSRMLS_CC) < 0) {
        return NULL;
    }

    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0) {
        return NULL;
    }

    if (bsc_str_left(bsc->inbuf, "VERSION ", response_len, sizeof("VERSION ") - 1)) {
        version_str = estrndup(bsc->inbuf + sizeof("VERSION ") - 1,
                               response_len - (sizeof("VERSION ") - 1) - (sizeof("\r\n") - 1));
        return version_str;
    }

    bsc_server_seterror(bsc, "Malformed version string", 0);
    return NULL;
}

/* }}} */

static int bsc_str_left(char *haystack, char *needle, int haystack_len, int needle_len) /* {{{ */
{
    char *found;

    found = php_memnstr(haystack, needle, needle_len, haystack + haystack_len);
    if ((found - haystack) == 0) {
        return 1;
    }
    return 0;
}

/* }}} */

static int bsc_sendcmd(bsc_t *bsc, const char *cmd, int cmdlen TSRMLS_DC) /* {{{ */
{
    char *command;
    int command_len;
    php_netstream_data_t *sock = (php_netstream_data_t *) bsc->stream->abstract;

    if (!bsc || !cmd) {
        return -1;
    }

    MMC_DEBUG(("bsc_sendcmd: sending command '%s'", cmd));

    command = emalloc(cmdlen + sizeof("\r\n"));
    memcpy(command, cmd, cmdlen);
    memcpy(command + cmdlen, "\r\n", sizeof("\r\n") - 1);
    command_len = cmdlen + sizeof("\r\n") - 1;
    command[command_len] = '\0';

    if (bsc->timeoutms > 1) {
        sock->timeout = _convert_timeoutms_to_ts(bsc->timeoutms);
    }

    if (php_stream_write(bsc->stream, command, command_len) != command_len) {
        bsc_server_seterror(bsc, "Failed writing command to stream", 0);
        efree(command);
        return -1;
    }
    efree(command);

    return 1;
}

/* }}}*/

static int bsc_parse_response(bsc_t *bsc, char *response, int response_len, char **key, int *key_len, int *flags,
                              int *value_len) /* {{{ */
{
    int i = 0, n = 0;
    int spaces[3];

    if (!response || response_len <= 0) {
        bsc_server_seterror(bsc, "Empty response", 0);
        return -1;
    }

    MMC_DEBUG(("bsc_parse_response: got response '%s'", response));

    for (i = 0, n = 0; i < response_len && n < 3; i++) {
        if (response[i] == ' ') {
            spaces[n++] = i;
        }
    }

    MMC_DEBUG(("bsc_parse_response: found %d spaces", n));

    if (n < 3) {
        bsc_server_seterror(bsc, "Malformed VALUE header", 0);
        return -1;
    }

    if (key != NULL) {
        int len = spaces[1] - spaces[0] - 1;

        *key = emalloc(len + 1);
        *key_len = len;

        memcpy(*key, response + spaces[0] + 1, len);
        (*key)[len] = '\0';
    }

    *flags = atoi(response + spaces[1]);
    *value_len = atoi(response + spaces[2]);

    if (*flags < 0 || *value_len < 0) {
        bsc_server_seterror(bsc, "Malformed VALUE header", 0);
        return -1;
    }

    MMC_DEBUG(("bsc_parse_response: 1st space is at %d position", spaces[1]));
    MMC_DEBUG(("bsc_parse_response: 2nd space is at %d position", spaces[2]));
    MMC_DEBUG(("bsc_parse_response: flags = %d", *flags));
    MMC_DEBUG(("bsc_parse_response: value_len = %d ", *value_len));

    return 1;
}

/* }}} */

static int bsc_postprocess_value(zval **return_value, char *value, int value_len TSRMLS_DC) /* 
	post-process a value into a result zval struct, value will be free()'ed during process {{{ */
{
    const char *value_tmp = value;
    php_unserialize_data_t var_hash;
    PHP_VAR_UNSERIALIZE_INIT(var_hash);

    if (!php_var_unserialize(return_value, (const unsigned char **) &value_tmp,
                             (const unsigned char *) (value_tmp + value_len), &var_hash
        TSRMLS_CC)) {
        ZVAL_FALSE(*return_value);
        PHP_VAR_UNSERIALIZE_DESTROY(var_hash);
        efree(value);
        php_error_docref(NULL
        TSRMLS_CC, E_NOTICE, "unable to unserialize data");
        return 0;
    }

    PHP_VAR_UNSERIALIZE_DESTROY(var_hash);
    efree(value);
    return 1;
}
/* }}} */

/*
 * 关于读取数据这里有个开始比较疑惑的地方：一个获取操作，做了2次bsc read。
 * 这个和beanstalkd服务器返回数据格式有关。在官方的协议文档中这样说明：
 *
 *   After this command, the client expects zero or more items, each of
 *   which is received as a text line followed by a data block. After all
 *   the items have been transmitted, the server sends the string
 *
 *   "END\r\n"
 *
 *   to indicate the end of response.
 *
 *   Each item sent by the server looks like this:
 *
 *   VALUE <key> <flags> <bytes> [<cas unique>]\r\n
 *   <data block>\r\n
 *
 * 意思就是这个获取的返回数据是多行的，类似下面这样：
 *   VALUE <key> <flags> <bytes> [<cas unique>]\r\n
 *   <data block>\r\n
 *   END\r\n
 * 所以扩展的实现是，先获取要获取的值，然后获取最后的END行。
 * 详情猛戳：https://github.com/beanstalkdd/beanstalkdd/blob/master/doc/protocol.txt
 */
int bsc_exec_retrieval_cmd(bsc_pool_t *pool, const char *key, int key_len, zval **return_value, zval *return_flags
                           TSRMLS_DC) /* {{{ */
{
    bsc_t *bsc;
    char *command, *value;
    int result = -1, command_len, response_len, value_len, flags = 0;

    MMC_DEBUG(("bsc_exec_retrieval_cmd: key '%s'", key));

    command_len = spprintf(&command, 0, "get %s", key);

    while (result < 0 && (bsc = bsc_pool_find(pool, key, key_len
        TSRMLS_CC)) != NULL) {
        MMC_DEBUG(("bsc_exec_retrieval_cmd: found server '%s:%d' for key '%s'", bsc->host, bsc->port, key));

        /* send command and read value */
        if ((result = bsc_sendcmd(bsc, command, command_len TSRMLS_CC)) > 0 &&
                                                                          (result = bsc_read_value(bsc, NULL, NULL,
                                                                                                   &value, &value_len,
                                                                                                   &flags
        TSRMLS_CC)) >= 0) {

            /* not found */
            if (result == 0) {
                ZVAL_FALSE(*return_value);
            }
                /* read "END" */
            else if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0 || !bsc_str_left(bsc->inbuf, "END", response_len,
                                                                                       sizeof("END") - 1)) {
                bsc_server_seterror(bsc, "Malformed END line", 0);
                result = -1;
            }
            else if (flags & MMC_SERIALIZED) {
                result = bsc_postprocess_value(return_value, value, value_len
                TSRMLS_CC);
            } else {
                ZVAL_STRINGL(*return_value, value, value_len, 0);
            }
        }

        if (result < 0) {
            bsc_server_failure(bsc
            TSRMLS_CC);
        }
    }

    if (return_flags != NULL) {
        zval_dtor(return_flags);
        ZVAL_LONG(return_flags, flags);
    }

    efree(command);
    return result;
}

/* }}} */

static int
bsc_exec_retrieval_cmd_multi(bsc_pool_t *pool, zval *keys, zval **return_value, zval *return_flags TSRMLS_DC) /* {{{ */
{
    bsc_t *bsc;
    HashPosition pos;
    zval **zkey;
    char *result_key, *value;
    char key[MMC_KEY_MAX_SIZE];
    unsigned int key_len;

    int i = 0, j, num_requests, result, result_status, result_key_len, value_len, flags;
    bsc_queue_t serialized = {0};        /* bsc_queue_t<zval *>, pointers to zvals which need unserializing */

    array_init(*return_value);

    if (return_flags != NULL) {
        zval_dtor(return_flags);
        array_init(return_flags);
    }

    /* until no retrival errors or all servers have failed */
    do {
        result_status = num_requests = 0;
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(keys), &pos);

        /* first pass to build requests for each server */
        while (zend_hash_get_current_data_ex(Z_ARRVAL_P(keys), (void **) &zkey, &pos) == SUCCESS) {
            if (bsc_prepare_key(*zkey, key, &key_len TSRMLS_CC) == MMC_OK) {
                /* schedule key if first round or if missing from result */
                if ((!i || !zend_hash_exists(Z_ARRVAL_PP(return_value), key, key_len)) &&
                    (bsc = bsc_pool_find(pool, key, key_len
                    TSRMLS_CC)) != NULL) {
                    if (!(bsc->outbuf.len)) {
                        smart_str_appendl(&(bsc->outbuf), "get", sizeof("get") - 1);
                        pool->requests[num_requests++] = bsc;
                    }

                    smart_str_appendl(&(bsc->outbuf), " ", 1);
                    smart_str_appendl(&(bsc->outbuf), key, key_len);
                    MMC_DEBUG(
                        ("bsc_exec_retrieval_cmd_multi: scheduled key '%s' for '%s:%d' request length '%d'", key, bsc->host, bsc->port, bsc->outbuf.len));
                }
            }

            zend_hash_move_forward_ex(Z_ARRVAL_P(keys), &pos);
        }

        /* second pass to send requests in parallel */
        for (j = 0; j < num_requests; j++) {
            smart_str_0(&(pool->requests[j]->outbuf));

            if ((result = bsc_sendcmd(pool->requests[j], pool->requests[j]->outbuf.c,
                                      pool->requests[j]->outbuf.len
                TSRMLS_CC)) < 0) {
                bsc_server_failure(pool->requests[j]
                TSRMLS_CC);
                result_status = result;
            }
        }

        /* third pass to read responses */
        for (j = 0; j < num_requests; j++) {
            if (pool->requests[j]->status != MMC_STATUS_FAILED) {
                for (value = NULL; (result = bsc_read_value(pool->requests[j], &result_key, &result_key_len, &value,
                                                            &value_len, &flags TSRMLS_CC)) > 0;
                value = NULL) {
                    if (flags & MMC_SERIALIZED) {
                        zval *result;
                        MAKE_STD_ZVAL(result);
                        ZVAL_STRINGL(result, value, value_len, 0);

                        /* don't store duplicate values */
                        if (zend_hash_add(Z_ARRVAL_PP(return_value), result_key, result_key_len + 1, &result,
                                          sizeof(result), NULL) == SUCCESS) {
                            bsc_queue_push(&serialized, result);
                        } else {
                            zval_ptr_dtor(&result);
                        }
                    } else {
                        add_assoc_stringl_ex(*return_value, result_key, result_key_len + 1, value, value_len, 0);
                    }

                    if (return_flags != NULL) {
                        add_assoc_long_ex(return_flags, result_key, result_key_len + 1, flags);
                    }

                    efree(result_key);
                }

                /* check for server failure */
                if (result < 0) {
                    bsc_server_failure(pool->requests[j]
                    TSRMLS_CC);
                    result_status = result;
                }
            }

            smart_str_free(&(pool->requests[j]->outbuf));
        }
    } while (result_status < 0 && MEMCACHE_G(allow_failover) && i++ < MEMCACHE_G(max_failover_attempts));

    /* post-process serialized values */
    if (serialized.len) {
        zval *value;

        while ((value = (zval *) bsc_queue_pop(&serialized)) != NULL) {
            bsc_postprocess_value(&value, Z_STRVAL_P(value), Z_STRLEN_P(value)
            TSRMLS_CC);
        }

        bsc_queue_free(&serialized);
    }

    return result_status;
}
/* }}} */
/*
 * 1.读取 VALUE <key> <flags> <bytes>\r\n 这一行
 * 2.读取真正的数据块行
 * 各种内存操作啊，高空走钢丝一般~
 */
static int
bsc_read_value(bsc_t *bsc, char **key, int *key_len, char **value, int *value_len, int *flags TSRMLS_DC) /* {{{ */
{
    char *data;
    int response_len, data_len, i, size;

    /* read "VALUE <key> <flags> <bytes>\r\n" header line */
    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0) {
        MMC_DEBUG(("failed to read the server's response"));
        return -1;
    }

    /* reached the end of the data */
    if (bsc_str_left(bsc->inbuf, "END", response_len, sizeof("END") - 1)) {
        return 0;
    }

    if (bsc_parse_response(bsc, bsc->inbuf, response_len, key, key_len, flags, &data_len) < 0) {
        return -1;
    }

    MMC_DEBUG(("bsc_read_value: data len is %d bytes", data_len));

    /* data_len + \r\n + \0 */
    data = emalloc(data_len + 3);

    /*
     * 这个地方的for循环应该是考虑到读取数据流一次未读取完全的情况：
     *  php_stream_read这个函数将返回从数据流实际读到缓冲区中的数据字节数. 为了保证读取数据完整，多次读取？
     */
    for (i = 0; i < data_len + 2; i += size) {
        if ((size = php_stream_read(bsc->stream, data + i, data_len + 2 - i)) == 0) {
            bsc_server_seterror(bsc, "Failed reading value response body", 0);
            if (key) {
                efree(*key);
            }
            efree(data);
            return -1;
        }
    }

    data[data_len] = '\0';

    if ((*flags & MMC_COMPRESSED) && data_len > 0) {
        char *result_data;
        unsigned long result_len = 0;

        if (!bsc_uncompress(&result_data, &result_len, data, data_len)) {
            bsc_server_seterror(bsc, "Failed to uncompress data", 0);
            if (key) {
                efree(*key);
            }
            efree(data);
            php_error_docref(NULL
            TSRMLS_CC, E_NOTICE, "unable to uncompress data");
            return 0;
        }

        efree(data);
        data = result_data;
        data_len = result_len;
    }

    *value = data;
    *value_len = data_len;
    return 1;
}

/* }}} */

int bsc_delete(bsc_t *bsc, const char *key, int key_len, int time TSRMLS_DC) /* {{{ */
{
    char *command;
    int command_len, response_len;

    command_len = spprintf(&command, 0, "delete %s %d", key, time);

    MMC_DEBUG(("bsc_delete: trying to delete '%s'", key));

    if (bsc_sendcmd(bsc, command, command_len TSRMLS_CC) < 0) {
        efree(command);
        return -1;
    }
    efree(command);

    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0){
        MMC_DEBUG(("failed to read the server's response"));
        return -1;
    }

    MMC_DEBUG(("bsc_delete: server's response is '%s'", bsc->inbuf));

    if (bsc_str_left(bsc->inbuf, "DELETED", response_len, sizeof("DELETED") - 1)) {
        return 1;
    }

    if (bsc_str_left(bsc->inbuf, "NOT_FOUND", response_len, sizeof("NOT_FOUND") - 1)) {
        return 0;
    }

    bsc_server_received_error(bsc, response_len);
    return -1;
}

/* }}} */

static int bsc_flush(bsc_t *bsc, int timestamp TSRMLS_DC) /* {{{ */
{
    char *command;
    int command_len, response_len;

    MMC_DEBUG(("bsc_flush: flushing the cache"));

    if (timestamp) {
        command_len = spprintf(&command, 0, "flush_all %d", timestamp);
    } else {
        command_len = spprintf(&command, 0, "flush_all");
    }

    if (bsc_sendcmd(bsc, command, command_len TSRMLS_CC) < 0) {
        efree(command);
        return -1;
    }
    efree(command);

    /* get server's response */
    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0){
        return -1;
    }

    MMC_DEBUG(("bsc_flush: server's response is '%s'", bsc->inbuf));

    if (bsc_str_left(bsc->inbuf, "OK", response_len, sizeof("OK") - 1)) {
        return 1;
    }

    bsc_server_received_error(bsc, response_len);
    return -1;
}
/* }}} */

/*
 * STAT 6:chunk_size 64
 */
static int bsc_stats_parse_stat(char *start, char *end, zval *result TSRMLS_DC)  /* {{{ */
{
    char *space, *colon, *key;
    long index = 0;

    /* find space delimiting key and value */
    if ((space = php_memnstr(start, " ", 1, end)) == NULL) {
        return 0;
    }

    /* find colon delimiting subkeys */
    if ((colon = php_memnstr(start, ":", 1, space - 1)) != NULL) {
        zval *element, **elem;
        key = estrndup(start, colon - start);

        /* find existing or create subkey array in result */
        if ((is_numeric_string(key, colon - start, &index, NULL, 0) &&
             zend_hash_index_find(Z_ARRVAL_P(result), index, (void **) &elem) != FAILURE) ||
            zend_hash_find(Z_ARRVAL_P(result), key, colon - start + 1, (void **) &elem) != FAILURE) {
            element = *elem;
        } else {
            MAKE_STD_ZVAL(element);
            array_init(element);
            add_assoc_zval_ex(result, key, colon - start + 1, element);
        }

        efree(key);
        return bsc_stats_parse_stat(colon + 1, end, element
        TSRMLS_CC);
    }

    /* no more subkeys, add value under last subkey */
    key = estrndup(start, space - start);
    add_assoc_stringl_ex(result, key, space - start + 1, space + 1, end - space, 1);
    efree(key);

    return 1;
}
/* }}} */

/*
 * ITEM test_key [3 b; 1157099416 s]
 */
static int bsc_stats_parse_item(char *start, char *end, zval *result TSRMLS_DC)  /* {{{ */
{
    char *space, *value, *value_end, *key;
    zval *element;

    /* find space delimiting key and value */
    if ((space = php_memnstr(start, " ", 1, end)) == NULL) {
        return 0;
    }

    MAKE_STD_ZVAL(element);
    array_init(element);

    /* parse each contained value */
    for (value = php_memnstr(space, "[", 1, end);
         value != NULL && value <= end; value = php_memnstr(value + 1, ";", 1, end)) {
        do {
            value++;
        } while (*value == ' ' && value <= end);

        if (value <= end && (value_end = php_memnstr(value, " ", 1, end)) != NULL && value_end <= end) {
            add_next_index_stringl(element, value, value_end - value, 1);
        }
    }

    /* add parsed values under key */
    key = estrndup(start, space - start);
    add_assoc_zval_ex(result, key, space - start + 1, element);
    efree(key);

    return 1;
}

/* }}} */

static int bsc_stats_parse_generic(char *start, char *end, zval *result TSRMLS_DC)  /* {{{ */
{
    char *space, *key;

    /* "stats maps" returns "\n" delimited lines, other commands uses "\r\n" */
    if (*end == '\r') {
        end--;
    }

    if (start <= end) {
        if ((space = php_memnstr(start, " ", 1, end)) != NULL) {
            key = estrndup(start, space - start);
            add_assoc_stringl_ex(result, key, space - start + 1, space + 1, end - space, 1);
            efree(key);
        } else {
            add_next_index_stringl(result, start, end - start, 1);
        }
    }

    return 1;
}

/* }}} */

static int bsc_get_stats(bsc_t *bsc, char *type, int slabid, int limit, zval *result TSRMLS_DC) /* {{{ */
{
    char *command;
    int command_len, response_len;

    if (slabid) {
        command_len = spprintf(&command, 0, "stats %s %d %d", type, slabid, limit);
    } else if (type) {
        command_len = spprintf(&command, 0, "stats %s", type);
    } else {
        command_len = spprintf(&command, 0, "stats");
    }

    if (bsc_sendcmd(bsc, command, command_len TSRMLS_CC) < 0) {
        efree(command);
        return -1;
    }

    efree(command);
    array_init(result);

    while ((response_len = bsc_readline(bsc TSRMLS_CC)) >= 0) {
        if (bsc_str_left(bsc->inbuf, "ERROR", response_len, sizeof("ERROR") - 1) ||
            bsc_str_left(bsc->inbuf, "CLIENT_ERROR", response_len, sizeof("CLIENT_ERROR") - 1) ||
            bsc_str_left(bsc->inbuf, "SERVER_ERROR", response_len, sizeof("SERVER_ERROR") - 1)) {

            zend_hash_destroy(Z_ARRVAL_P(result));
            FREE_HASHTABLE(Z_ARRVAL_P(result));

            ZVAL_FALSE(result);
            return 0;
        } else if (bsc_str_left(bsc->inbuf, "RESET", response_len, sizeof("RESET") - 1)) {
            zend_hash_destroy(Z_ARRVAL_P(result));
            FREE_HASHTABLE(Z_ARRVAL_P(result));

            ZVAL_TRUE(result);
            return 1;
        } else if (bsc_str_left(bsc->inbuf, "ITEM ", response_len, sizeof("ITEM ") - 1)) {
            if (!bsc_stats_parse_item(bsc->inbuf + (sizeof("ITEM ") - 1), bsc->inbuf + response_len - sizeof("\r\n"),
                                      result
                TSRMLS_CC)) {
                zend_hash_destroy(Z_ARRVAL_P(result));
                FREE_HASHTABLE(Z_ARRVAL_P(result));
                return -1;
            }
        } else if (bsc_str_left(bsc->inbuf, "STAT ", response_len, sizeof("STAT ") - 1)) {
            if (!bsc_stats_parse_stat(bsc->inbuf + (sizeof("STAT ") - 1), bsc->inbuf + response_len - sizeof("\r\n"),
                                      result
                TSRMLS_CC)) {
                zend_hash_destroy(Z_ARRVAL_P(result));
                FREE_HASHTABLE(Z_ARRVAL_P(result));
                return -1;
            }
        } else if (bsc_str_left(bsc->inbuf, "END", response_len, sizeof("END") - 1)) {
            break;
        } else if (!bsc_stats_parse_generic(bsc->inbuf, bsc->inbuf + response_len - sizeof("\n"), result TSRMLS_CC)) {
            zend_hash_destroy(Z_ARRVAL_P(result));
            FREE_HASHTABLE(Z_ARRVAL_P(result));
            return -1;
        }
    }

    if (response_len < 0) {
        zend_hash_destroy(Z_ARRVAL_P(result));
        FREE_HASHTABLE(Z_ARRVAL_P(result));
        return -1;
    }

    return 1;
}

/* }}} */

static int bsc_incr_decr(bsc_t *bsc, int cmd, char *key, int key_len, int value, long *number TSRMLS_DC) /* {{{ */
{
    char *command;
    int command_len, response_len;

    if (cmd > 0) {
        command_len = spprintf(&command, 0, "incr %s %d", key, value);
    } else {
        command_len = spprintf(&command, 0, "decr %s %d", key, value);
    }

    if (bsc_sendcmd(bsc, command, command_len TSRMLS_CC) < 0) {
        efree(command);
        return -1;
    }
    efree(command);

    if ((response_len = bsc_readline(bsc TSRMLS_CC)) < 0) {
        MMC_DEBUG(("failed to read the server's response"));
        return -1;
    }

    MMC_DEBUG(("bsc_incr_decr: server's answer is: '%s'", bsc->inbuf));
    if (bsc_str_left(bsc->inbuf, "NOT_FOUND", response_len, sizeof("NOT_FOUND") - 1)) {
        MMC_DEBUG(("failed to %sement variable - item with such key not found", cmd > 0 ? "incr" : "decr"));
        return 0;
    } else if (bsc_str_left(bsc->inbuf, "ERROR", response_len, sizeof("ERROR") - 1) ||
               bsc_str_left(bsc->inbuf, "CLIENT_ERROR", response_len, sizeof("CLIENT_ERROR") - 1) ||
               bsc_str_left(bsc->inbuf, "SERVER_ERROR", response_len, sizeof("SERVER_ERROR") - 1)) {
        bsc_server_received_error(bsc, response_len);
        return -1;
    }

    *number = (long) atol(bsc->inbuf);
    return 1;
}

/* }}} */

static void php_bsc_store(INTERNAL_FUNCTION_PARAMETERS, char *command, int command_len) /* {{{ */
{
    bsc_pool_t *pool;
    zval *value, *bsc_object = getThis();

    int result, key_len;
    char *key;
    long flags = 0, expire = 0;
    char key_tmp[MMC_KEY_MAX_SIZE];
    unsigned int key_tmp_len;

    php_serialize_data_t value_hash;
    smart_str buf = {0};

    if (bsc_object == NULL) {
        if (zend_parse_parameters(ZEND_NUM_ARGS()
            TSRMLS_CC, "Osz|ll", &bsc_object, beanstalkd_class_entry_ptr, &key, &key_len, &value, &flags, &expire) == FAILURE) {
            return;
        }
    } else {
        if (zend_parse_parameters(ZEND_NUM_ARGS()
            TSRMLS_CC, "sz|ll", &key, &key_len, &value, &flags, &expire) == FAILURE) {
            return;
        }
    }

    //转换key中的不合法字符
    if (bsc_prepare_key_ex(key, key_len, key_tmp, &key_tmp_len TSRMLS_CC) != MMC_OK) {
        RETURN_FALSE;
    }

    if (!bsc_get_pool(bsc_object, &pool TSRMLS_CC) || !pool->num_servers) {
        RETURN_FALSE;
    }

    switch (Z_TYPE_P(value)) {
        case IS_STRING:
            result = bsc_pool_store(
                pool, command, command_len, key_tmp, key_tmp_len, flags, expire,
                Z_STRVAL_P(value), Z_STRLEN_P(value)
            TSRMLS_CC);
            break;

        case IS_LONG:
        case IS_DOUBLE:
        case IS_BOOL: {
            zval value_copy;

            /* FIXME: we should be using 'Z' instead of this, but unfortunately it's PHP5-only */
            value_copy = *value;
            zval_copy_ctor(&value_copy);
            convert_to_string(&value_copy);

            result = bsc_pool_store(
                pool, command, command_len, key_tmp, key_tmp_len, flags, expire,
                Z_STRVAL(value_copy), Z_STRLEN(value_copy)
            TSRMLS_CC);

            zval_dtor(&value_copy);
            break;
        }

        default: {
            zval value_copy, *value_copy_ptr;

            /* FIXME: we should be using 'Z' instead of this, but unfortunately it's PHP5-only */
            value_copy = *value;
            zval_copy_ctor(&value_copy);
            value_copy_ptr = &value_copy;

            PHP_VAR_SERIALIZE_INIT(value_hash);
            php_var_serialize(&buf, &value_copy_ptr, &value_hash
            TSRMLS_CC);
            PHP_VAR_SERIALIZE_DESTROY(value_hash);

            if (!buf.c) {
                /* something went really wrong */
                zval_dtor(&value_copy);
                php_error_docref(NULL
                TSRMLS_CC, E_WARNING, "Failed to serialize value");
                RETURN_FALSE;
            }

            flags |= MMC_SERIALIZED;
            zval_dtor(&value_copy);

            result = bsc_pool_store(
                pool, command, command_len, key_tmp, key_tmp_len, flags, expire,
                buf.c, buf.len
            TSRMLS_CC);
        }
    }

    if (flags & MMC_SERIALIZED) {
        smart_str_free(&buf);
    }

    if (result > 0) {
        RETURN_TRUE;
    }

    RETURN_FALSE;
}

/* }}} */

static void php_bsc_incr_decr(INTERNAL_FUNCTION_PARAMETERS, int cmd) /* {{{ */
{
    bsc_t *bsc;
    bsc_pool_t *pool;
    int result = -1, key_len;
    long value = 1, number;
    char *key;
    zval *bsc_object = getThis();
    char key_tmp[MMC_KEY_MAX_SIZE];
    unsigned int key_tmp_len;

    if (bsc_object == NULL) {
        if (zend_parse_parameters(ZEND_NUM_ARGS()
            TSRMLS_CC, "Os|l", &bsc_object, beanstalkd_class_entry_ptr, &key, &key_len, &value) == FAILURE) {
            return;
        }
    } else {
        if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|l", &key, &key_len, &value) == FAILURE) {
            return;
        }
    }

    if (!bsc_get_pool(bsc_object, &pool TSRMLS_CC) || !pool->num_servers) {
        RETURN_FALSE;
    }

    if (bsc_prepare_key_ex(key, key_len, key_tmp, &key_tmp_len TSRMLS_CC) != MMC_OK) {
        RETURN_FALSE;
    }

    while (result < 0 && (bsc = bsc_pool_find(pool, key_tmp, key_tmp_len
        TSRMLS_CC)) != NULL) {
        if ((result = bsc_incr_decr(bsc, cmd, key_tmp, key_tmp_len, value, &number TSRMLS_CC)) < 0) {
            bsc_server_failure(bsc
            TSRMLS_CC);
        }
    }

    if (result > 0) {
        RETURN_LONG(number);
    }
    RETURN_FALSE;
}

/* }}} */

static void php_bsc_connect(INTERNAL_FUNCTION_PARAMETERS, int persistent) /* {{{ */
{
    zval **connection, *bsc_object = getThis();
    bsc_t *bsc = NULL;
    bsc_pool_t *pool;
    int resource_type, host_len, errnum = 0, list_id;
    char *host, *error_string = NULL;
    long port = MEMCACHE_G(default_port), timeout = MMC_DEFAULT_TIMEOUT, timeoutms = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS()
        TSRMLS_CC, "s|lll", &host, &host_len, &port, &timeout, &timeoutms) == FAILURE) {
        return;
    }

    if (timeoutms < 1) {
        timeoutms = MEMCACHE_G(default_timeout_ms);
    }

    /* initialize and connect server struct */
    if (persistent) {
        bsc = bsc_find_persistent(host, host_len, port, timeout, MMC_DEFAULT_RETRY
        TSRMLS_CC);
    } else {
        MMC_DEBUG(("php_bsc_connect: creating regular connection"));
        bsc = bsc_server_new(host, host_len, port, 0, timeout, MMC_DEFAULT_RETRY
        TSRMLS_CC);
    }

    bsc->timeout = timeout;
    bsc->connect_timeoutms = timeoutms;

    if (!bsc_open(bsc, 1, &error_string, &errnum TSRMLS_CC)) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "Can't connect to %s:%ld, %s (%d)", host, port, error_string ? error_string
                                                                                           : "Unknown error", errnum);
        if (!persistent) {
            bsc_server_free(bsc
            TSRMLS_CC);
        }
        if (error_string) {
            efree(error_string);
        }
        RETURN_FALSE;
    }

    /* initialize pool and object if need be */
    if (!bsc_object) {
        pool = bsc_pool_new(TSRMLS_C);
        bsc_pool_add(pool, bsc, 1);

        object_init_ex(return_value, beanstalkd_class_entry_ptr);
        list_id = MEMCACHE_LIST_INSERT(pool, le_beanstalkd_pool);
        add_property_resource(return_value, "connection", list_id);
    } else if (zend_hash_find(Z_OBJPROP_P(bsc_object), "connection", sizeof("connection"), (void **) &connection) !=
               FAILURE) {
        pool = (bsc_pool_t *) zend_list_find(Z_LVAL_PP(connection), &resource_type);
        if (!pool || resource_type != le_beanstalkd_pool) {
            php_error_docref(NULL
            TSRMLS_CC, E_WARNING, "Unknown connection identifier");
            RETURN_FALSE;
        }

        bsc_pool_add(pool, bsc, 1);
        RETURN_TRUE;
    } else {
        pool = bsc_pool_new(TSRMLS_C);
        bsc_pool_add(pool, bsc, 1);

        list_id = MEMCACHE_LIST_INSERT(pool, le_beanstalkd_pool);
        add_property_resource(bsc_object, "connection", list_id);
        RETURN_TRUE;
    }
}
/* }}} */
/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
