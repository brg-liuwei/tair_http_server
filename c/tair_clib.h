#ifndef __TAIR_CLIB_H__
#define __TAIR_CLIB_H__

#include <sys/types.h>
#include "tair_cvec.h"
#include "tair_cmap.h"

#if __cplusplus
extern "C"
{
#endif

void *tair_create(const char *master,
        const char *slave, const char *group);

void tair_destroy(void *client);

int tair_put(void *client, int area,
        const char *key, size_t klen, 
        const char *val, size_t vlen,
        int expire, const char **errmsg);

const char *tair_get(void *client, int area,
        const char *key, size_t klen,
        size_t *vlen, const char **errmsg);

int tair_remove(void *client, int area,
        const char *key, size_t klen, const char **errmsg);

int tair_incr(void *client, int area,
        const char *key, size_t klen,
        int count, int expire,
        int *ret, const char **errmsg);

int tair_decr(void *client, int area,
        const char *key, size_t klen,
        int count, int expire,
        int *ret, const char **errmsg);

int tair_mget(void *client, int area, cvec *v, cmap **m, const char **errmsg);

/* ------ range operates ------ */
int tair_prefix_put(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char *val, size_t vlen,
        int expire, const char **errmsg);

const char *tair_prefix_get(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        size_t *vlen, const char **errmsg);

int tair_prefix_remove(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char **errmsg);

/**
 * @brief
 *
 * @param client: tair client pointer
 * @param area: tair storage area
 * @param pkey: primary key
 * @param pklen: length of pkey
 * @param skey: start key to seek, seek from the first skey if equals "" or NULL
 * @param sklen: length of skey
 * @param ekey: end key to seek, seek to the last ekey if equals "" or NULL
 * @param eklen: length of ekey
 * @param offset: skip first ${offset} key
 * @param limit: limit response less than ${limit} key
 * @param vec: a cvec pointer, cleanup should be invoked by caller. for detail, see as follows
 * @param errmsg: if not set NULL and some errors happen, this pointer will set to some error msg, caller CANNOT free the memory
 *
 * @return TAIR_RETURN_SUCCESS -- success, 
 *         TAIR_HAS_MORE_DATA  -- limited by package size (1M default)
 *         <0                  -- fail
 *
 * usage:
 *
 * cvec  *v;
 * const char *errmsg;
 * int rc = tair_get_range(cli, area, pk, pkl, sk, skl, ek, ekl, off, limit, &v, &errmsg);
 * if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_HAS_MORE_DATA) {
 *     for (v->begin(v); v->valid(v); v->next(v)) {
 *         // do_some_stuff(v->cur_data(v), v->cur_size(v));
 *     }
 *     v->cleanup(v);
 * } else {
 *     // error_log(errmsg);
 * }
 *
 */
int tair_get_range(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char *ekey, size_t eklen,
        int offset, int limit,
        cvec **vec, const char **errmsg);

#if __cplusplus
}
#endif

#endif
