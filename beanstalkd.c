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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"

#include <stdio.h>
#include <fcntl.h>
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


/* {{{ beanstalkd_functions[]
 *
 * Every user visible function must have an entry in beanstalkd_functions[].
 */

zend_function_entry beanstalkd_functions[] = {
    PHP_FE(beanstalkd_connect, NULL)
    PHP_FE(beanstalkd_pconnect, NULL)
    PHP_FE(beanstalkd_put, NULL)
    PHP_FE(beanstalkd_use, NULL)
    PHP_FE(beanstalkd_reserve, NULL)
    PHP_FE(beanstalkd_delete, NULL)
    PHP_FE(beanstalkd_bury, NULL)
    PHP_FE(beanstalkd_touch, NULL)
    PHP_FE(beanstalkd_watch, NULL)
    PHP_FE(beanstalkd_ignore, NULL)
    PHP_FE(beanstalkd_peek, NULL)
    PHP_FE(beanstalkd_kick, NULL)
    PHP_FE(beanstalkd_kick_job, NULL)
    PHP_FE(beanstalkd_stats_job, NULL)
    PHP_FE(beanstalkd_stats_tube, NULL)
    PHP_FE(beanstalkd_stats, NULL)
    PHP_FE(beanstalkd_list_tubes, NULL)
    PHP_FE(beanstalkd_list_tube_used, NULL)
    PHP_FE(beanstalkd_list_tube_watched, NULL)
    PHP_FE(beanstalkd_quit, NULL)
    PHP_FE(beanstalkd_pause_tube, NULL)
    PHP_FE_END    /* Must be the last line in beanstalkd_functions[] */
};
/* }}} */

static zend_function_entry php_beanstalkd_class_functions[] = {
    PHP_FALIAS(connect, beanstalkd_connect, NULL)
    PHP_FALIAS(pconnect, beanstalkd_pconnect, NULL)
    PHP_FALIAS(put, beanstalkd_put, NULL)
    PHP_FALIAS(use, beanstalkd_use, NULL)
    PHP_FALIAS(reserve, beanstalkd_reserve, NULL)
    PHP_FALIAS(delete, beanstalkd_delete, NULL)
    PHP_FALIAS(bury, beanstalkd_bury, NULL)
    PHP_FALIAS(touch, beanstalkd_touch, NULL)
    PHP_FALIAS(watch, beanstalkd_watch, NULL)
    PHP_FALIAS(ignore, beanstalkd_ignore, NULL)
    PHP_FALIAS(peek, beanstalkd_peek, NULL)
    PHP_FALIAS(kick, beanstalkd_kick, NULL)
    PHP_FALIAS(kick_job, beanstalkd_kick_job, NULL)
    PHP_FALIAS(stats_job, beanstalkd_stats_job, NULL)
    PHP_FALIAS(stats_tube, beanstalkd_stats_tube, NULL)
    PHP_FALIAS(stats, beanstalkd_stats, NULL)
    PHP_FALIAS(list_tubes, beanstalkd_list_tubes, NULL)
    PHP_FALIAS(list_tube_used, beanstalkd_list_tube_used, NULL)
    PHP_FALIAS(list_tube_watched, beanstalkd_list_tube_watched, NULL)
    PHP_FALIAS(quit, beanstalkd_quit, NULL)
    PHP_FALIAS(pause_tube, beanstalkd_pause_tube, NULL)
    PHP_FE_END
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

/* {{{ PHP_INI
 */
static PHP_INI_MH(OnUpdateChunkSize)  {
    long int lval;

    lval = strtol(new_value, NULL, 10);
    if (lval <= 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.chunk_size must be a positive integer ('%s' given)", new_value);
        return FAILURE;
    }

    return OnUpdateLong(entry, new_value, new_value_length, mh_arg1, mh_arg2, mh_arg3, stage TSRMLS_CC);
}

static PHP_INI_MH(OnUpdateFailoverAttempts)  {
    long int lval;

    lval = strtol(new_value, NULL, 10);
    if (lval <= 0) {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.max_failover_attempts must be a positive integer ('%s' given)", new_value);
        return FAILURE;
    }

    return OnUpdateLong(entry, new_value, new_value_length, mh_arg1, mh_arg2, mh_arg3, stage TSRMLS_CC);
}

static PHP_INI_MH(OnUpdateHashStrategy)  {
    if (!strcasecmp(new_value, "standard")) {
        BEANSTALKD_G(hash_strategy) = bsc_STANDARD_HASH;
    }
    else if (!strcasecmp(new_value, "consistent")) {
        BEANSTALKD_G(hash_strategy) = bsc_CONSISTENT_HASH;
    }
    else {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.hash_strategy must be in set {standard, consistent} ('%s' given)", new_value);
        return FAILURE;
    }

    return SUCCESS;
}

static PHP_INI_MH(OnUpdateHashFunction){
    if (!strcasecmp(new_value, "crc32")) {
        BEANSTALKD_G(hash_function) = bsc_HASH_CRC32;
    }
    else if (!strcasecmp(new_value, "fnv")) {
        BEANSTALKD_G(hash_function) = bsc_HASH_FNV1A;
    }
    else {
        php_error_docref(NULL
        TSRMLS_CC, E_WARNING, "beanstalkd.hash_function must be in set {crc32, fnv} ('%s' given)", new_value);
        return FAILURE;
    }

    return SUCCESS;
}

static PHP_INI_MH(OnUpdateDefaultTimeout) {
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

static int bsc_compress(char **, unsigned long *, const char *, int TSRMLS_DC);

static int bsc_uncompress(char **, unsigned long *, const char *, int);

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
/* }}} */

/* {{{ hash strategies */
extern bsc_hash_t bsc_standard_hash;
extern bsc_hash_t bsc_consistent_hash;
/* }}} */


/* {{{ php_beanstalkd_init_globals
 */
static void php_beanstalkd_init_globals(zend_beanstalkd_globals *beanstalkd_globals TSRMLS_DC) {
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
PHP_MINIT_FUNCTION(beanstalkd) {
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
PHP_MSHUTDOWN_FUNCTION(beanstalkd) {
    UNREGISTER_INI_ENTRIES();
    return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(beanstalkd) {

    BEANSTALKD_G(debug_mode) = 0;
    return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(beanstalkd) {
    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(beanstalkd) {
    php_info_print_table_start();
    php_info_print_table_header(2, "beanstalkd support", "enabled");
    php_info_print_table_end();

    DISPLAY_INI_ENTRIES();
}
/* }}} */
#if ZEND_DEBUG
void bsc_debug(const char *format, ...) /* {{{ */
{
    TSRMLS_FETCH();

    if (BEANSTALKD_G(debug_mode)) {
        char buffer[1024];
        va_list args;

        va_start(args, format);
        vsnprintf(buffer, sizeof(buffer)-1, format, args);
        va_end(args);
        buffer[sizeof(buffer)-1] = '\0';
        php_printf("%s<br />\n", buffer);
    }
}
/* }}} */
#endif

PHP_FUNCTION(beanstalkd_connect) {
    php_bsc_connect(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0);
}

PHP_FUNCTION(beanstalkd_pconnect) {
    php_bsc_pconnect(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0);
}

PHP_FUNCTION(beanstalkd_put) {
    php_bsc_store(INTERNAL_FUNCTION_PARAM_PASSTHRU, "put", sizeof("put") - 1);
}

PHP_FUNCTION(beanstalkd_use) {
    php_bsc_store(INTERNAL_FUNCTION_PARAM_PASSTHRU, "use", sizeof("use") - 1);
}


PHP_FUNCTION(beanstalkd_debug) {
#if ZEND_DEBUG
zend_bool onoff;

if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "b", &onoff) == FAILURE) {
    return;
}

BEANSTALKD_G(debug_mode) = onoff ? 1 : 0;

RETURN_TRUE;
#else
    RETURN_FALSE;
#endif

}
/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
