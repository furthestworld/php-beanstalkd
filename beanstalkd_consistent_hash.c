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

typedef struct bsc_consistent_point {
	bsc_t					*server;
	unsigned int			point;
} bsc_consistent_point_t;

typedef struct bsc_consistent_state {
	int						num_servers;
	bsc_consistent_point_t	*points;
	int						num_points;
	bsc_t					*buckets[BSC_CONSISTENT_BUCKETS];
	int						buckets_populated;
	bsc_hash_function		hash;
} bsc_consistent_state_t;

void *bsc_consistent_create_state(bsc_hash_function hash) /* {{{ */
{
	bsc_consistent_state_t *state = emalloc(sizeof(bsc_consistent_state_t));
	memset(state, 0, sizeof(bsc_consistent_state_t));
	state->hash = hash;
	return state;
}
/* }}} */

void bsc_consistent_free_state(void *s) /* {{{ */
{
	bsc_consistent_state_t *state = s;
	if (state != NULL) {
		if (state->points != NULL) {
			efree(state->points);
		}
		efree(state);
	}
}
/* }}} */

static int bsc_consistent_compare(const void *a, const void *b) /* {{{ */
{
	if (((bsc_consistent_point_t *)a)->point < ((bsc_consistent_point_t *)b)->point) {
		return -1;
	}
	if (((bsc_consistent_point_t *)a)->point > ((bsc_consistent_point_t *)b)->point) {
		return 1;
	}
	return 0;
}
/* }}} */

static bsc_t *bsc_consistent_find(bsc_consistent_state_t *state, unsigned int point) /* {{{ */
{
	int lo = 0, hi = state->num_points - 1, mid;

	while (1) {
		/* point is outside interval or lo >= hi, wrap-around */
		if (point <= state->points[lo].point || point > state->points[hi].point) {
			return state->points[lo].server;
		}

		/* test middle point */
		mid = lo + (hi - lo) / 2;
		BSC_DEBUG(("bsc_consistent_find: lo %d, hi %d, mid %d, point %u, midpoint %u", lo, hi, mid, point, state->points[mid].point));

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

static void bsc_consistent_populate_buckets(bsc_consistent_state_t *state) /* {{{ */
{
	unsigned int i, step = 0xffffffff / BSC_CONSISTENT_BUCKETS;

	qsort((void *)state->points, state->num_points, sizeof(bsc_consistent_point_t), bsc_consistent_compare);
	for (i=0; i<BSC_CONSISTENT_BUCKETS; i++) {
		state->buckets[i] = bsc_consistent_find(state, step * i);
	}

	state->buckets_populated = 1;
}
/* }}} */

bsc_t *bsc_consistent_find_server(void *s, const char *key, int key_len TSRMLS_DC) /* {{{ */
{
	bsc_consistent_state_t *state = s;
	bsc_t *bsc;

	if (state->num_servers > 1) {
		unsigned int i, hash = state->hash(key, key_len);

		if (!state->buckets_populated) {
			bsc_consistent_populate_buckets(state);
		}

		bsc = state->buckets[hash % BSC_CONSISTENT_BUCKETS];

		/* perform failover if needed */
		for (i=0; !bsc_open(bsc, 0, NULL, NULL TSRMLS_CC) && BEANSTALKD_G(allow_failover) && i<BEANSTALKD_G(max_failover_attempts); i++) {
			char *next_key = emalloc(key_len + MAX_LENGTH_OF_LONG + 1);
			int next_len = sprintf(next_key, "%s-%d", key, i);
			BSC_DEBUG(("bsc_consistent_find_server: failed to connect to server '%s:%d' status %d, trying next", bsc->host, bsc->port, bsc->status));

			hash = state->hash(next_key, next_len);
			bsc = state->buckets[hash % BSC_CONSISTENT_BUCKETS];

			efree(next_key);
		}
	}
	else {
		bsc = state->points[0].server;
		bsc_open(bsc, 0, NULL, NULL TSRMLS_CC);
	}

	return bsc->status != BSC_STATUS_FAILED ? bsc : NULL;
}
/* }}} */

void bsc_consistent_add_server(void *s, bsc_t *bsc, unsigned int weight) /* {{{ */
{
	bsc_consistent_state_t *state = s;
	int i, key_len, points = weight * BSC_CONSISTENT_POINTS;

	/* buffer for "host:port-i\0" */
	char *key = emalloc(strlen(bsc->host) + MAX_LENGTH_OF_LONG * 2 + 3);

	/* add weight * BSC_CONSISTENT_POINTS number of points for this server */
	state->points = erealloc(state->points, sizeof(bsc_consistent_point_t) * (state->num_points + points));

	for (i=0; i<points; i++) {
		key_len = sprintf(key, "%s:%d-%d", bsc->host, bsc->port, i);
		state->points[state->num_points + i].server = bsc;
		state->points[state->num_points + i].point = state->hash(key, key_len);
		BSC_DEBUG(("bsc_consistent_add_server: key %s, point %lu", key, state->points[state->num_points + i].point));
	}

	state->num_points += points;
	state->num_servers++;
	state->buckets_populated = 0;

	efree(key);
}
/* }}} */

bsc_hash_t bsc_consistent_hash = {
	bsc_consistent_create_state,
	bsc_consistent_free_state,
	bsc_consistent_find_server,
	bsc_consistent_add_server
};

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
