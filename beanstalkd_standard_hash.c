/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2007 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.0 of the PHP license,       |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_0.txt.                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Authors: Antony Dovgal <tony@daylessday.org>                         |
  |          Mikael Johansson <mikael AT synd DOT info>                  |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_beanstalkd.h"

ZEND_EXTERN_MODULE_GLOBALS(beanstalkd)

typedef struct beans_conn_standard_state {
	int						num_servers;
	beans_conn_t					**buckets;
	int						num_buckets;
	beans_conn_hash_function		hash;
} beans_conn_standard_state_t;

void *beans_conn_standard_create_state(beans_conn_hash_function hash) /* {{{ */
{
	beans_conn_standard_state_t *state = emalloc(sizeof(beans_conn_standard_state_t));
	memset(state, 0, sizeof(beans_conn_standard_state_t));
	state->hash = hash;
	return state;
}
/* }}} */

void beans_conn_standard_free_state(void *s) /* {{{ */
{
	beans_conn_standard_state_t *state = s;
	if (state != NULL) {
		if (state->buckets != NULL) {
			efree(state->buckets);
		}
		efree(state);
	}
}
/* }}} */

static unsigned int beans_conn_hash(beans_conn_standard_state_t *state, const char *key, int key_len) /* {{{ */
{
	unsigned int hash = (state->hash(key, key_len) >> 16) & 0x7fff;
  	return hash ? hash : 1;
}
/* }}} */

beans_conn_t *beans_conn_standard_find_server(void *s, const char *key, int key_len TSRMLS_DC) /* {{{ */
{
	beans_conn_standard_state_t *state = s;
	beans_conn_t *beans_conn;

	if (state->num_servers > 1) {
		unsigned int hash = beans_conn_hash(state, key, key_len), i;
		beans_conn = state->buckets[hash % state->num_buckets];

		/* perform failover if needed */
		for (i=0; !beans_conn_open(beans_conn, 0, NULL, NULL TSRMLS_CC) && MEMCACHE_G(allow_failover) && i<MEMCACHE_G(max_failover_attempts); i++) {
			char *next_key = emalloc(key_len + MAX_LENGTH_OF_LONG + 1);
			int next_len = sprintf(next_key, "%d%s", i+1, key);
			MMC_DEBUG(("beans_conn_standard_find_server: failed to connect to server '%s:%d' status %d, trying next", beans_conn->host, beans_conn->port, beans_conn->status));

			hash += beans_conn_hash(state, next_key, next_len);
			beans_conn = state->buckets[hash % state->num_buckets];

			efree(next_key);
		}
	}
	else {
		beans_conn = state->buckets[0];
		beans_conn_open(beans_conn, 0, NULL, NULL TSRMLS_CC);
	}

	return beans_conn->status != MMC_STATUS_FAILED ? beans_conn : NULL;
}
/* }}} */

void beans_conn_standard_add_server(void *s, beans_conn_t *beans_conn, unsigned int weight) /* {{{ */
{
	beans_conn_standard_state_t *state = s;
	int i;

	/* add weight number of buckets for this server */
	if (state->num_buckets) {
		state->buckets = erealloc(state->buckets, sizeof(beans_conn_t *) * (state->num_buckets + weight));
	}
	else {
		state->buckets = emalloc(sizeof(beans_conn_t *) * (weight));
	}

	for (i=0; i<weight; i++) {
		state->buckets[state->num_buckets + i] = beans_conn;
	}

	state->num_buckets += weight;
	state->num_servers++;
}
/* }}} */

beans_conn_hash_t beans_conn_standard_hash = {
	beans_conn_standard_create_state,
	beans_conn_standard_free_state,
	beans_conn_standard_find_server,
	beans_conn_standard_add_server
};

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
