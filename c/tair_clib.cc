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

int tair_mget(void *client, int area, cvec *keys, cmap **m, const char **errmsg)
{
    int  n;
    tair::tair_keyvalue_map  kvmap;

    n = ((tair::tair_client_api *)client)->mget(area, *(vector<tair::data_entry *> *)keys->values, kvmap);
    if (n != 0 && n != -3983) {
        if (errmsg != NULL) {
            *errmsg = ((tair::tair_client_api *)client)->get_error_msg(n);
        }
        return n;
    }

    *m = cmap_create(&kvmap);

    for (tair::tair_keyvalue_map::iterator itr = kvmap.begin();
            itr != kvmap.end(); /* void */)
    {
        tair::tair_keyvalue_map::iterator pre_itr = itr;
        ++itr;
        delete pre_itr->first;
        delete pre_itr->second;
    }
    // kvmap.erase(kvmap.begin(), kvmap.end());

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

    const short type = CMD_RANGE_ALL; // CMD_RANGE_KEY_ONL | CMD_RANGE_VALUE_ONL
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
