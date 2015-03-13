#ifndef __TAIR_CVEC_H__
#define __TAIR_CVEC_H__

#include <sys/types.h>

#if __cplusplus
extern "C"
{
#endif

/* cvec iterator usage:
 
   cvec *v = new_cvec();
   cvec_add(v, s1, length(s1));
   cvec_add(v, s2, length(s2));
   cvec_add(v, sN, length(sN));

   for (cvec_begin(v); cvec_valid(v) == 1; cvec_next(v)) {
       // do_some_stuff(cvec_cur_data(v), cvec_cur_size(v));
   }

   cvec_cleanup(v);

 */

typedef struct __cvec {
    size_t    idx;
    size_t    size;
    void     *values;

    void    (*add)(struct __cvec *__this, const char *s, size_t len);
    void    (*begin)(struct __cvec *__this);
    int     (*valid)(struct __cvec *__this);
    void    (*next)(struct __cvec *__this);
    void    (*cleanup)(struct __cvec *__this);

    const char   *(*cur_data)(struct __cvec *__this);
    size_t        (*cur_size)(struct __cvec *__this);
} cvec;

/* 
   brief of create_cvec(void *)

   values MUST be a pointer of type:
      vector<tair::data_entry *>

   !!! NOTICE !!!
   if you do not actually know what are you doing,
   please DONOT call this interface!!!
   !!! NOTICE !!!
*/
cvec *create_cvec(void *values);

cvec *new_cvec();
void cvec_add(void *v, const char *s, size_t len);

void cvec_begin(void *v);
int cvec_valid(void *v);
void cvec_next(void *v);
const char *cvec_cur_data(void *v);
size_t cvec_cur_size(void *v);
void cvec_cleanup(void *v);

#if __cplusplus
}
#endif

#endif
