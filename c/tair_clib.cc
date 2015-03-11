#include "tair_clib.h"
#include "tair_client_api.hpp"
//#include "tblog.h"
#include <iostream>
#include <sys/time.h>
#include <stdlib.h>
#include <assert.h>

using namespace std;

#if __cplusplus
extern "C"
{
#endif

void *tair_create(const char *master, const char *slave,
        const char *group)
{
    tair::tair_client_api *client = new tair::tair_client_api;
    bool rc = client->startup(master, slave, group);
    return rc ? (void *)client : NULL;
}

void tair_destroy(void *client)
{
    if (client) {
        ((tair::tair_client_api *)client)->close();
        delete (tair::tair_client_api *)client;
    }
}

int tair_put(void *client, int area, const char *key, size_t klen,
        const char *val, size_t vlen, int expire, const char **errmsg)
{
    tair::data_entry k(key, (int)klen, false);
    tair::data_entry v(val, (int)vlen, false);
    int rc = ((tair::tair_client_api *)client)->put(area,
            k, v, expire, 0, true);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

/* invoker should free the char array returned */
const char *tair_get(void *client, int area, const char *key,
        size_t klen, size_t *vlen, const char **errmsg)
{
    int   rv;
    char *rc;
    tair::data_entry  k(key, (int)klen, false);
    tair::data_entry *v;

    rv = ((tair::tair_client_api *)client)->get(area, k, v);
    if (rv != 0) {
        if (vlen != NULL) {
            *vlen = 0;
        }
        if (errmsg != NULL) {
            *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rv);
        }
        return NULL;
    }

    if ((rc = (char *)calloc(sizeof(char), v->get_size() + 1)) == NULL) {
        if (vlen != NULL) {
            *vlen = 0;
        }
        if (errmsg != NULL) {
            *errmsg = "cannot allocate any more memory\n";
        }
        goto end;
    }

    if (vlen != NULL) {
        *vlen = v->get_size();
    }
    memcpy(rc, v->get_data(), v->get_size());
    rc[v->get_size()] = '\0';

end:
    delete v;
    return rc;
}

int tair_remove(void *client, int area, const char *key, size_t klen, const char **errmsg)
{
    tair::data_entry k(key, (int)klen, false);
    int rc = ((tair::tair_client_api *)client)->remove(area, k);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

int tair_incr(void *client, int area, const char *key, size_t klen,
        int count, int expire, int *ret, const char **errmsg)
{
    tair::data_entry k(key, int(klen), false);
    int rc = ((tair::tair_client_api *)client)->incr(area, k, count, ret, 0, expire);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

int tair_decr(void *client, int area, const char *key, size_t klen,
        int count, int expire, int *ret, const char **errmsg)
{
    tair::data_entry k(key, int(klen), false);
    int rc = ((tair::tair_client_api *)client)->decr(area, k, count, ret, 0, expire);
    if (rc != 0 && *errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

static void __cmap_begin(cmap *m)
{
    m->cnt = 0;
    if (m->cnt >= m->total) {
        m->valid_ = 0;
        return;
    }
    m->valid_ = 1;
    m->cur_key = m->data;
    m->cur_key_len = m->keys_len[m->cnt];
    m->cur_val = m->cur_key + m->cur_key_len;
    m->cur_val_len = m->vals_len[m->cnt];
}

static int __cmap_valid(cmap *m)
{
    return m->valid_;
}

static void __cmap_next(cmap *m)
{
    ++m->cnt;
    if (m->cnt >= m->total) {
        m->valid_ = 0;
        return;
    }
    m->valid_ = 1;
    m->cur_key = m->cur_val + m->cur_val_len;
    m->cur_key_len = m->keys_len[m->cnt];
    m->cur_val = m->cur_key + m->cur_key_len;
    m->cur_val_len = m->vals_len[m->cnt];
}

static void __cmap_cleanup(cmap *m)
{
    free(m->data);
    free(m->keys_len);
    free(m->vals_len);
    free(m);
}

void cmap_begin(void *m)
{
    __cmap_begin((cmap *)m);
}

int cmap_valid(void *m)
{
    return __cmap_valid((cmap *)m);
}

void cmap_next(void *m)
{
    return __cmap_next((cmap *)m);
}

void cmap_cleanup(void *m)
{
    __cmap_cleanup((cmap *)m);
}

const char *cmap_key(void *m, int *len)
{
    *len = ((cmap *)m)->cur_key_len;
    return (const char *)((cmap *)m)->cur_key;
}

const char *cmap_val(void *m, int *len)
{
    *len = ((cmap *)m)->cur_val_len;
    return (const char *)((cmap *)m)->cur_val;
}

cmap *cmap_create(void *tair_map)
{
    cmap  *m;

    m = (cmap *)calloc(sizeof(cmap), 1);
    assert(m != NULL);

    /* calc tair_keyvalue_map key/value total size */
    int pairs = 0;
    int size = 0;
    tair::tair_keyvalue_map *tmap = (tair::tair_keyvalue_map *)tair_map;

    for (tair::tair_keyvalue_map::iterator itr = tmap->begin();
            itr != tmap->end(); ++itr)
    {
        pairs++;
        size += itr->first->get_size() + itr->second->get_size();
    }

    /* set cmap */
    m->data = (char *)calloc(sizeof(char), size);
    assert(m->data != NULL);

    m->keys_len = (int *)calloc(sizeof(int), pairs);
    assert(m->keys_len != NULL);

    m->vals_len = (int *)calloc(sizeof(int), pairs);
    assert(m->vals_len != NULL);

    m->cnt = 0;
    m->total = pairs;

#define cpymem(dst, src, n) ((char *)memcpy(dst, src, n) + n)

    char *p = m->data;
    for (tair::tair_keyvalue_map::iterator itr = tmap->begin();
            itr != tmap->end(); ++itr)
    {
        p = cpymem(p, itr->first->get_data(), itr->first->get_size());
        p = cpymem(p, itr->second->get_data(), itr->second->get_size());
    }


    m->begin = __cmap_begin;
    m->valid = __cmap_valid;
    m->next = __cmap_next;
    m->cleanup = __cmap_cleanup;

    return m;
}

int tair_mget(void *client, int area, const char **keys, int klens[], size_t nkeys , void **m)
{
    int  n;

    tair::tair_keyvalue_map     kvmap;
    vector<tair::data_entry *>  key_vec;

    for (n = 0; n != nkeys; ++n) {
        key_vec.push_back(new tair::data_entry((const char *)keys[n], klens[n], false));
    }

    n = ((tair::tair_client_api *)client)->mget(area, key_vec, kvmap);

    *m = (void *)cmap_create(&kvmap);

    /* cleanup vector */
    for (vector<tair::data_entry *>::iterator itr = key_vec.begin();
            itr != key_vec.end(); ++itr)
    {
        delete *itr;
    }
    key_vec.erase(key_vec.begin(), key_vec.end());

    for (tair::tair_keyvalue_map::iterator itr = kvmap.begin();
            itr != kvmap.end(); ++itr)
    {
        delete itr->first;
        delete itr->second;
    }
    kvmap.erase(kvmap.begin(), kvmap.end());

    return n;
}

int tair_prefix_put(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char *val, size_t vlen,
        int expire, const char **errmsg)
{
    tair::data_entry pk(pkey, (int)pklen, false);
    tair::data_entry sk(skey, (int)sklen, false);
    tair::data_entry v(val, (int)vlen, false);

    int rc = ((tair::tair_client_api *)client)->prefix_put(area, pk, sk, v, expire, 0);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

const char *tair_prefix_get(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        size_t *vlen, const char **errmsg)
{
    tair::data_entry pk(pkey, (int)pklen, false);
    tair::data_entry sk(skey, (int)sklen, false);
    tair::data_entry *v;

    int rc = ((tair::tair_client_api *)client)->prefix_get(area, pk, sk, v);
    if (rc != 0) {
        if (errmsg != NULL) {
            *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
        }
        return NULL;
    }

    char *rv = (char *)calloc(v->get_size(), sizeof(char));
    if (rv == NULL) {
        if (errmsg != NULL) {
            *errmsg = "cannot allocate any more memory\n";
        }
        return NULL;
    }

    memcpy(rv, v->get_data(), v->get_size());
    *vlen = (size_t)v->get_size();
    return rv;
}

int tair_prefix_remove(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char **errmsg)
{
    tair::data_entry pk(pkey, (int)pklen, false);
    tair::data_entry sk(skey, (int)sklen, false);

    int rc = ((tair::tair_client_api *)client)->prefix_remove(area, pk, sk);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
    }
    return rc;
}

static void __cvec_begin(cvec *v)
{
    v->idx = 0;
}

static int __cvec_valid(cvec *v)
{
    return v->idx >= v->size ? 0 : 1;
}

static void __cvec_next(cvec *v)
{
    ++v->idx;
}

static void __cvec_cleanup(cvec *v)
{
    vector<tair::data_entry *> *vec = (vector<tair::data_entry *> *)v->values;
    for (vector<tair::data_entry *>::iterator itr = vec->begin();
            itr != vec->end(); ++itr)
    {
        delete *itr;
    }
    vec->erase(vec->begin(), vec->end());
    delete vec;

    free(v);
}

static const char *__cvec_cur_data(cvec *v)
{
    assert(v->valid(v));
    return ((*(vector<tair::data_entry *> *)v->values)[v->idx])->get_data();
}

static size_t __cvec_cur_size(cvec *v)
{
    assert(v->valid(v));
    return (size_t)((*(vector<tair::data_entry *> *)v->values)[v->idx])->get_size();
}

static cvec *create_cvec(void *values)
{
    vector<tair::data_entry *> *vec = (vector<tair::data_entry *> *)values;

    cvec *v = (cvec *)calloc(sizeof(cvec), 1);
    assert(v != NULL);

    v->idx = 0;
    v->size = vec->size();
    v->values = values;

    v->begin = __cvec_begin;
    v->valid = __cvec_valid;
    v->next = __cvec_next;
    v->cleanup = __cvec_cleanup;

    v->cur_data = __cvec_cur_data;
    v->cur_size = __cvec_cur_size;

    return v;
}

int tair_get_range(void *client, int area,
        const char *pkey, size_t pklen,
        const char *skey, size_t sklen,
        const char *ekey, size_t eklen,
        int offset, int limit,
        cvec **vec, const char **errmsg)
{
    tair::data_entry pk(pkey, pklen, false);
    tair::data_entry sk(skey, sklen, false);
    tair::data_entry ek(ekey, eklen, false);

    const short type = CMD_RANGE_ALL; // CMD_RANGE_KEY_ONL / CMD_RANGE_VALUE_ONL
    vector<tair::data_entry *> *values = new vector<tair::data_entry *>;

    int rc = ((tair::tair_client_api *)client)->get_range(area, pk, sk, ek, 
            offset, limit, *values, type);

    switch (rc) {
        case TAIR_RETURN_SUCCESS:
            *vec = create_cvec(values);
            if (errmsg != NULL) {
                *errmsg = "ok";
            }
            break;

        case TAIR_HAS_MORE_DATA:
            *vec = create_cvec(values);
            if (errmsg != NULL) {
                *errmsg = "has more data";
            }
            break;

        default:
            if (errmsg != NULL) {
                *errmsg = ((tair::tair_client_api *)client)->get_error_msg(rc);
            }
            break;
    }
    return rc;
}


#if __cplusplus
}
#endif
