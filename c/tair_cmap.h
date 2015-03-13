#ifndef __TAIR_CMAP_H__
#define __TAIR_CMAP_H__

#if __cplusplus
extern "C"
{
#endif

/* cmap iterator usage:

 cmap *m = cmap_create(tair_map);
 assert(m != NULL);
 for (cmap_begin(m); cmap_valid(m); cmap_next(m)) {
    int klen, vlen;
    const char *key, *val;

    key = cmap_key(m, &klen);
    val = cmap_val(m, &vlen);

    // do_some_stuff(key, klen, val, vlen);
 }
 cmap_cleanup(m);
*/

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

cmap *cmap_create(void *tair_map);
void cmap_begin(void *m);
int cmap_valid(void *m);
void cmap_next(void *m);
void cmap_cleanup(void *m);
const char *cmap_key(void *m, int *len);
const char *cmap_val(void *m, int *len);


#if __cplusplus
}
#endif

#endif
