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

typedef struct bsc_standard_state {
	int						num_servers;
	bsc_t					**buckets;
	int						num_buckets;
	bsc_hash_function		hash;
} bsc_standard_state_t;

void *bsc_standard_create_state(bsc_hash_function hash) /* {{{ */
{
	bsc_standard_state_t *state = emalloc(sizeof(bsc_standard_state_t));
	memset(state, 0, sizeof(bsc_standard_state_t));
	state->hash = hash;
	return state;
}
/* }}} */

void bsc_standard_free_state(void *s) /* {{{ */
{
	bsc_standard_state_t *state = s;
	if (state != NULL) {
		if (state->buckets != NULL) {
			efree(state->buckets);
		}
		efree(state);
	}
}
/* }}} */

static unsigned int bsc_hash(bsc_standard_state_t *state, const char *key, int key_len) /* {{{ */
{
	unsigned int hash = (state->hash(key, key_len) >> 16) & 0x7fff;
  	return hash ? hash : 1;
}
/* }}} */

bsc_t *bsc_standard_find_server(void *s, const char *key, int key_len TSRMLS_DC) /* {{{ */
{
	bsc_standard_state_t *state = s;
	bsc_t *bsc;

	if (state->num_servers > 1) {
		unsigned int hash = bsc_hash(state, key, key_len), i;
		bsc = state->buckets[hash % state->num_buckets];

		/* perform failover if needed */
		for (i=0; !bsc_open(bsc, 0, NULL, NULL TSRMLS_CC) && BEANSTALKD_G(allow_failover) && i<BEANSTALKD_G(max_failover_attempts); i++) {
			char *next_key = emalloc(key_len + MAX_LENGTH_OF_LONG + 1);
			int next_len = sprintf(next_key, "%d%s", i+1, key);
			BSC_DEBUG(("bsc_standard_find_server: failed to connect to server '%s:%d' status %d, trying next", bsc->host, bsc->port, bsc->status));

			hash += bsc_hash(state, next_key, next_len);
			bsc = state->buckets[hash % state->num_buckets];

			efree(next_key);
		}
	}
	else {
		bsc = state->buckets[0];
		bsc_open(bsc, 0, NULL, NULL TSRMLS_CC);
	}

	return bsc->status != BSC_STATUS_FAILED ? bsc : NULL;
}
/* }}} */

void bsc_standard_add_server(void *s, bsc_t *bsc, unsigned int weight) /* {{{ */
{
	bsc_standard_state_t *state = s;
	int i;

	/* add weight number of buckets for this server */
	if (state->num_buckets) {
		state->buckets = erealloc(state->buckets, sizeof(bsc_t *) * (state->num_buckets + weight));
	}
	else {
		state->buckets = emalloc(sizeof(bsc_t *) * (weight));
	}

	for (i=0; i<weight; i++) {
		state->buckets[state->num_buckets + i] = bsc;
	}

	state->num_buckets += weight;
	state->num_servers++;
}
/* }}} */

bsc_hash_t bsc_standard_hash = {
	bsc_standard_create_state,
	bsc_standard_free_state,
	bsc_standard_find_server,
	bsc_standard_add_server
};

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
