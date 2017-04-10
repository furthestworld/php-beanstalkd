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

#include <stdlib.h>

#include "php.h"
#include "php_beanstalkd.h"

ZEND_EXTERN_MODULE_GLOBALS(beanstalkd)

typedef struct beans_conn_consistent_point {
	beans_conn_t					*server;
	unsigned int			point;
} beans_conn_consistent_point_t;

typedef struct beans_conn_consistent_state {
	int						num_servers;
	beans_conn_consistent_point_t	*points;
	int						num_points;
	beans_conn_t					*buckets[MMC_CONSISTENT_BUCKETS];
	int						buckets_populated;
	beans_conn_hash_function		hash;
} beans_conn_consistent_state_t;

void *beans_conn_consistent_create_state(beans_conn_hash_function hash) /* {{{ */
{
	beans_conn_consistent_state_t *state = emalloc(sizeof(beans_conn_consistent_state_t));
	memset(state, 0, sizeof(beans_conn_consistent_state_t));
	state->hash = hash;
	return state;
}
/* }}} */

void beans_conn_consistent_free_state(void *s) /* {{{ */
{
	beans_conn_consistent_state_t *state = s;
	if (state != NULL) {
		if (state->points != NULL) {
			efree(state->points);
		}
		efree(state);
	}
}
/* }}} */

static int beans_conn_consistent_compare(const void *a, const void *b) /* {{{ */
{
	if (((beans_conn_consistent_point_t *)a)->point < ((beans_conn_consistent_point_t *)b)->point) {
		return -1;
	}
	if (((beans_conn_consistent_point_t *)a)->point > ((beans_conn_consistent_point_t *)b)->point) {
		return 1;
	}
	return 0;
}
/* }}} */

static beans_conn_t *beans_conn_consistent_find(beans_conn_consistent_state_t *state, unsigned int point) /* {{{ */
{
	int lo = 0, hi = state->num_points - 1, mid;

	while (1) {
		/* point is outside interval or lo >= hi, wrap-around */
		if (point <= state->points[lo].point || point > state->points[hi].point) {
			return state->points[lo].server;
		}

		/* test middle point */
		mid = lo + (hi - lo) / 2;
		MMC_DEBUG(("beans_conn_consistent_find: lo %d, hi %d, mid %d, point %u, midpoint %u", lo, hi, mid, point, state->points[mid].point));

		/* perfect match */
		if (point <= state->points[mid].point && point > (mid ? state->points[mid-1].point : 0)) {
			return state->points[mid].server;
		}

		/* too low, go up */
		if (state->points[mid].point < point) {
			lo = mid + 1;
		}
		else {
			hi = mid - 1;
		}
	}
}
/* }}} */

static void beans_conn_consistent_populate_buckets(beans_conn_consistent_state_t *state) /* {{{ */
{
	unsigned int i, step = 0xffffffff / MMC_CONSISTENT_BUCKETS;

	qsort((void *)state->points, state->num_points, sizeof(beans_conn_consistent_point_t), beans_conn_consistent_compare);
	for (i=0; i<MMC_CONSISTENT_BUCKETS; i++) {
		state->buckets[i] = beans_conn_consistent_find(state, step * i);
	}

	state->buckets_populated = 1;
}
/* }}} */

beans_conn_t *beans_conn_consistent_find_server(void *s, const char *key, int key_len TSRMLS_DC) /* {{{ */
{
	beans_conn_consistent_state_t *state = s;
	beans_conn_t *beans_conn;

	if (state->num_servers > 1) {
		unsigned int i, hash = state->hash(key, key_len);

		if (!state->buckets_populated) {
			beans_conn_consistent_populate_buckets(state);
		}

		beans_conn = state->buckets[hash % MMC_CONSISTENT_BUCKETS];

		/* perform failover if needed */
		for (i=0; !beans_conn_open(beans_conn, 0, NULL, NULL TSRMLS_CC) && MEMCACHE_G(allow_failover) && i<MEMCACHE_G(max_failover_attempts); i++) {
			char *next_key = emalloc(key_len + MAX_LENGTH_OF_LONG + 1);
			int next_len = sprintf(next_key, "%s-%d", key, i);
			MMC_DEBUG(("beans_conn_consistent_find_server: failed to connect to server '%s:%d' status %d, trying next", beans_conn->host, beans_conn->port, beans_conn->status));

			hash = state->hash(next_key, next_len);
			beans_conn = state->buckets[hash % MMC_CONSISTENT_BUCKETS];

			efree(next_key);
		}
	}
	else {
		beans_conn = state->points[0].server;
		beans_conn_open(beans_conn, 0, NULL, NULL TSRMLS_CC);
	}

	return beans_conn->status != MMC_STATUS_FAILED ? beans_conn : NULL;
}
/* }}} */

void beans_conn_consistent_add_server(void *s, beans_conn_t *beans_conn, unsigned int weight) /* {{{ */
{
	beans_conn_consistent_state_t *state = s;
	int i, key_len, points = weight * MMC_CONSISTENT_POINTS;

	/* buffer for "host:port-i\0" */
	char *key = emalloc(strlen(beans_conn->host) + MAX_LENGTH_OF_LONG * 2 + 3);

	/* add weight * MMC_CONSISTENT_POINTS number of points for this server */
	state->points = erealloc(state->points, sizeof(beans_conn_consistent_point_t) * (state->num_points + points));

	for (i=0; i<points; i++) {
		key_len = sprintf(key, "%s:%d-%d", beans_conn->host, beans_conn->port, i);
		state->points[state->num_points + i].server = beans_conn;
		state->points[state->num_points + i].point = state->hash(key, key_len);
		MMC_DEBUG(("beans_conn_consistent_add_server: key %s, point %lu", key, state->points[state->num_points + i].point));
	}

	state->num_points += points;
	state->num_servers++;
	state->buckets_populated = 0;

	efree(key);
}
/* }}} */

beans_conn_hash_t beans_conn_consistent_hash = {
	beans_conn_consistent_create_state,
	beans_conn_consistent_free_state,
	beans_conn_consistent_find_server,
	beans_conn_consistent_add_server
};

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
