#ifndef __TAIR_CLIB_H__
#define __TAIR_CLIB_H__

#include <sys/types.h>

#if __cplusplus
extern "C"
{
#endif

void *tair_create(const char *master,
        const char *slave, const char *group);

void tair_destroy(void *client);

int tair_put(void *client, int area,
        const char *key, size_t klen, 
        const char *val, size_t vlen, int expire);

char *tair_get(void *client, int area,
        const char *key, size_t klen, size_t *vlen);

int tair_remove(void *client, int area,
        const char *key, size_t klen);

int tair_incr(void *client, int area,
        const char *key, size_t klen,
        int count, int expire, int *ret);

int tair_decr(void *client, int area,
        const char *key, size_t klen,
        int count, int expire, int *ret);

typedef struct __cmap {
    char        *data;

    int          cnt;
    int          total;
    int         *keys_len;
    int         *vals_len;

    int          valid_;
    const char  *cur_key;
    int          cur_key_len;
    const char  *cur_val;
    int          cur_val_len;

    void        (*begin)(struct __cmap *__this);
    int         (*valid)(struct __cmap *__this);
    void        (*next)(struct __cmap *__this);
    void        (*cleanup)(struct __cmap *__this);
} cmap;

/* cmap iterator usage:

 cmap *m = cmap_create(tair_map);
 assert(m != NULL);
 for (m->begin(m); m->valid(m); m->next(m)) {
    do_some_stuff(m->cur_key, m->cur_key_len);
    do_some_stuff(m->cur_val, m->cur_val_len);
 }
 m->cleanup(m);

*/

cmap *cmap_create(void *tair_map);
int tair_mget(void *client, int area, const char **keys, int klens[], size_t nkeys , void **m);
void cmap_begin(void *m);
int cmap_valid(void *m);
void cmap_cleanup(void *m);
const char *cmap_key(void *m, int *len);
const char *cmap_val(void *m, int *len);

#if __cplusplus
}
#endif

#endif
