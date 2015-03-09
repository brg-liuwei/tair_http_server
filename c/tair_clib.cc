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

int tair_put(void *client, int area, const char *key, size_t klen, const char *val, size_t vlen, int expire)
{
    tair::data_entry k(key, (int)klen, false);
    tair::data_entry v(val, (int)vlen, false);
    return ((tair::tair_client_api *)client)->put(area,
            k, v, expire, 0, true);
}

/* invoker should free the char array returned */
char *tair_get(void *client, int area, const char *key, size_t klen, size_t *vlen)
{
    char *rc;
    tair::data_entry  k(key, (int)klen);
    tair::data_entry *v;

    if (0 != ((tair::tair_client_api *)client)->get(area, k, v)) {
        if (vlen != NULL) {
            *vlen = 0;
        }
        return NULL;
    }

    if ((rc = (char *)calloc(sizeof(char), v->get_size() + 1)) == NULL) {
        if (vlen != NULL) {
            *vlen = 0;
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

int tair_remove(void *client, int area, const char *key, size_t klen)
{
    tair::data_entry k(key, (int)klen);
    return ((tair::tair_client_api *)client)->remove(area, k);
}

int tair_incr(void *client, int area, const char *key, size_t klen, int count, int expire, int *ret)
{
    tair::data_entry k(key, int(klen));
    return ((tair::tair_client_api *)client)->incr(area, k, count, ret, 0, expire);
}

int tair_decr(void *client, int area, const char *key, size_t klen, int count, int expire, int *ret)
{
    tair::data_entry k(key, int(klen));
    return ((tair::tair_client_api *)client)->decr(area, k, count, ret, 0, expire);
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
        key_vec.push_back(new tair::data_entry((const char *)keys[n], klens[n]));
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


#if __cplusplus
}
#endif
