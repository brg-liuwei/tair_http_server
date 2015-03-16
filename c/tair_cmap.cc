#include "tair_cmap.h"
#include <assert.h>
#include <sys/types.h>
#include <stdlib.h>
#include "tair_client_api.hpp"

#if __cplusplus
extern "C"
{
#endif

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
    int *klen = m->keys_len, *vlen = m->vals_len;
    for (tair::tair_keyvalue_map::iterator itr = tmap->begin();
            itr != tmap->end(); ++itr)
    {
        p = cpymem(p, itr->first->get_data(), itr->first->get_size());
        *klen++ = itr->first->get_size();

        p = cpymem(p, itr->second->get_data(), itr->second->get_size());
        *vlen++ = itr->second->get_size();
    }

    m->begin = __cmap_begin;
    m->valid = __cmap_valid;
    m->next = __cmap_next;
    m->cleanup = __cmap_cleanup;

    return m;
}

/* cmap wrappers */
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
    __cmap_next((cmap *)m);
}

void cmap_cleanup(void *m)
{
    __cmap_cleanup((cmap *)m);
}

const char *cmap_key(void *m, int *len)
{
    if (len != NULL) {
        *len = ((cmap *)m)->cur_key_len;
    }
    return (const char *)((cmap *)m)->cur_key;
}

const char *cmap_val(void *m, int *len)
{
    if (len != NULL) {
        *len = ((cmap *)m)->cur_val_len;
    }
    return (const char *)((cmap *)m)->cur_val;
}

#if __cplusplus
}
#endif

