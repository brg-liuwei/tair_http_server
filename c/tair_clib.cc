#include "tair_client_api.hpp"
//#include "tblog.h"
#include <iostream>
#include <sys/time.h>

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


#if __cplusplus
}
#endif










