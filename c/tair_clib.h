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

#if __cplusplus
}
#endif

#endif
