#include "tair_cvec.h"
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include "tair_client_api.hpp"

using namespace std;

#if __cplusplus
extern "C"
{
#endif

static void __cvec_add(cvec *v, const char *s, size_t len)
{
    vector<tair::data_entry *> *vec = (vector<tair::data_entry *> *)(v->values);
    vec->push_back(new tair::data_entry(s, (int)len, true));
    v->size = vec->size();
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

#define cvec_assign(v, vec) do { \
    v->idx = 0; \
    v->size = vec->size(); \
    v->values = vec; \
    v->add = __cvec_add; \
    v->begin = __cvec_begin; \
    v->valid = __cvec_valid; \
    v->next = __cvec_next; \
    v->cleanup = __cvec_cleanup; \
    v->cur_data = __cvec_cur_data; \
    v->cur_size = __cvec_cur_size; \
} while (0)

cvec *new_cvec()
{
    vector<tair::data_entry *> *vec = new vector<tair::data_entry *>();
    assert(vec != NULL);
    cvec *v = (cvec *)calloc(sizeof(cvec), 1);
    assert(v != NULL);

    cvec_assign(v, vec);

    return v;
}

cvec *create_cvec(void *values)
{
    vector<tair::data_entry *> *vec = (vector<tair::data_entry *> *)values;

    cvec *v = (cvec *)calloc(sizeof(cvec), 1);
    assert(v != NULL);

    cvec_assign(v, vec);

    return v;
}

/* cvec wrappers */
void cvec_add(void *v, const char *s, size_t len)
{
    cvec *vec = (cvec *)v;
    vec->add(vec, s, len);
}

void cvec_begin(void *v)
{
    cvec *vec = (cvec *)v;
    vec->begin(vec);
}

int cvec_valid(void *v)
{
    cvec *vec = (cvec *)v;
    return vec->valid(vec);
}

void cvec_next(void *v)
{
    cvec *vec = (cvec *)v;
    vec->next(vec);
}

const char *cvec_cur_data(void *v)
{
    cvec *vec = (cvec *)v;
    return vec->cur_data(vec);
}

size_t cvec_cur_size(void *v)
{
    cvec *vec = (cvec *)v;
    return vec->cur_size(vec);
}

void cvec_cleanup(void *v)
{
    cvec *vec = (cvec *)v;
    vec->cleanup(vec);
}

#if __cplusplus
}
#endif

