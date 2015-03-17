package tair

/*
#include <stdlib.h>

#cgo CFLAGS: -I/usr/local/tair_http_server/c -I/opt/tair_pkg/tblib_root/include/tbnet -I/opt/tair_pkg/tblib_root/include/tbsys
#cgo LDFLAGS: -L/opt/tair_pkg/tair_bin/lib -L/opt/tair_pkg/tblib_root/lib -L/usr/local/tair_http_server/c -ltbnet -ltbsys -ltair_clib

#include "tair_clib.h"
*/
import "C"

import (
	"errors"
	"strconv"
	"sync"
	"unsafe"
)

var handlerDefaultSize int = 64
var handlerChan chan unsafe.Pointer

var once sync.Once

func Init() error {
	once.Do(tairLoadConf)

	cmaster := C.CString(Configs["master"])
	defer C.free(unsafe.Pointer(cmaster))
	cslave := C.CString(Configs["slave"])
	defer C.free(unsafe.Pointer(cslave))
	cgroup := C.CString(Configs["group"])
	defer C.free(unsafe.Pointer(cgroup))

	handlerSize, _ := strconv.Atoi(Configs["handlerSize"])
	if handlerSize < handlerDefaultSize {
		handlerSize = handlerDefaultSize
	}
	handlerChan = make(chan unsafe.Pointer, handlerSize)

	for i := 0; i != handlerSize; i++ {
		handler := unsafe.Pointer(C.tair_create(cmaster, cslave, cgroup))
		if handler == unsafe.Pointer(nil) {
			return errors.New("create tair handler err\n")
		}
		handlerChan <- handler
	}
	return nil
}

func put(area int, key string, val string, expire int) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	cval := C.CString(val)
	defer C.free(unsafe.Pointer(cval))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char

	if 0 == C.tair_put(handler, C.int(area), ckey, C.size_t(len(key)),
		cval, C.size_t(len(val)), C.int(expire), &errmsg) {
		return nil
	} else {
		return errors.New("put tair key " + key + " err: " + C.GoString(errmsg) + "\n")
	}
}

func get(area int, key string) (string, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var vlen C.size_t
	var errmsg *C.char

	cval := C.tair_get(handler, C.int(area), ckey, C.size_t(len(key)), &vlen, &errmsg)
	defer C.free(unsafe.Pointer(cval))

	if unsafe.Pointer(cval) == nil || vlen == C.size_t(0) {
		return "", errors.New("get tair key " + key + " err: " + C.GoString(errmsg) + "\n")
	}

	return C.GoStringN(cval, C.int(vlen)), nil
}

func del(area int, key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char

	if 0 == C.tair_remove(handler, C.int(area), ckey, C.size_t(len(key)), &errmsg) {
		return nil
	} else {
		return errors.New("del tair key " + key + " err: " + C.GoString(errmsg) + "\n")
	}
}

func incr(area int, key string, count int, expire int) (newCount int, err error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	var cNewCount C.int

	if 0 == C.tair_incr(handler, C.int(area), ckey, C.size_t(len(key)),
		C.int(count), C.int(expire), &cNewCount, &errmsg) {
		newCount = int(cNewCount)
	} else {
		err = errors.New("incr tair key " + key + " err: " + C.GoString(errmsg) + "\n")
	}
	return
}

func decr(area int, key string, count int, expire int) (newCount int, err error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	var cNewCount C.int

	if 0 == C.tair_decr(handler, C.int(area), ckey, C.size_t(len(key)),
		C.int(count), C.int(expire), &cNewCount, &errmsg) {
		newCount = int(cNewCount)
	} else {
		err = errors.New("decr tair key " + key + " err: " + C.GoString(errmsg) + "\n")
	}
	return
}

func mget(area int, keys []string) (m map[string]string, err error) {
	cvec := unsafe.Pointer((C.new_cvec()))
	defer C.cvec_cleanup(cvec)

	for i := 0; i != len(keys); i++ {
		ck := C.CString(keys[i])
		defer C.free(unsafe.Pointer(ck))
		C.cvec_add(cvec, ck, C.size_t(len(keys[i])))
	}

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	var cm unsafe.Pointer

	rc := C.tair_mget(handler, C.int(area), (*C.struct___cvec)(unsafe.Pointer(cvec)),
		(**C.struct___cmap)(unsafe.Pointer(&cm)), &errmsg)
	defer C.cmap_cleanup(cm)

	if rc != 0 && rc != -3983 {
		/* TAIR_RETURN_PARTIAL_SUCCESS: -3983 */
		err = errors.New("mget err: " + C.GoString(errmsg))
		return
	}

	m = make(map[string]string)
	for C.cmap_begin(cm); C.cmap_valid(cm) == C.int(1); C.cmap_next(cm) {
		var klen, vlen C.int
		ckey := C.cmap_key(cm, &klen)
		cval := C.cmap_val(cm, &vlen)
		m[C.GoStringN((*C.char)(ckey), klen)] = C.GoStringN((*C.char)(cval), vlen)
	}
	return
}

func prefix_put(area int, pkey string, skey string, val string, expire int) error {
	cpkey := C.CString(pkey)
	defer C.free(unsafe.Pointer(cpkey))
	cskey := C.CString(skey)
	defer C.free(unsafe.Pointer(cskey))
	cval := C.CString(val)
	defer C.free(unsafe.Pointer(cval))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char

	if 0 == C.tair_prefix_put(handler, C.int(area), cpkey, C.size_t(len(pkey)),
		cskey, C.size_t(len(skey)), cval, C.size_t(len(val)), C.int(expire), &errmsg) {
		return nil
	} else {
		return errors.New("prefix put tair pkey " + pkey +
			", skey: " + skey + ", err: " + C.GoString(errmsg) + "\n")
	}
}

func prefix_get(area int, pkey string, skey string) (string, error) {
	cpkey := C.CString(pkey)
	defer C.free(unsafe.Pointer(cpkey))
	cskey := C.CString(skey)
	defer C.free(unsafe.Pointer(cskey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	var vlen C.size_t

	cval := C.tair_prefix_get(handler, C.int(area), cpkey, C.size_t(len(pkey)),
		cskey, C.size_t(len(skey)), &vlen, &errmsg)
	defer C.free(unsafe.Pointer(cval))

	if unsafe.Pointer(cval) == nil || vlen == C.size_t(0) {
		return "", errors.New("prefix_get tair pkey:" + pkey + ", skey:" +
			skey + ", err:" + C.GoString(errmsg) + "\n")
	}
	return C.GoStringN(cval, C.int(vlen)), nil
}

func prefix_remove(area int, pkey string, skey string) error {
	cpkey := C.CString(pkey)
	defer C.free(unsafe.Pointer(cpkey))
	cskey := C.CString(skey)
	defer C.free(unsafe.Pointer(cskey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	if 0 == C.tair_prefix_remove(handler, C.int(area), cpkey, C.size_t(len(pkey)),
		cskey, C.size_t(len(skey)), &errmsg) {
		return nil
	} else {
		return errors.New("prefix_remove pkey: " + pkey + ", skey: " +
			skey + ", err: " + C.GoString(errmsg) + "\n")
	}
}

func get_range(area int, pkey, skey, ekey string, offset, limit int) ([]string, error) {
	cpkey, cskey, cekey := C.CString(pkey), C.CString(skey), C.CString(ekey)
	defer C.free(unsafe.Pointer(cpkey))
	defer C.free(unsafe.Pointer(cskey))
	defer C.free(unsafe.Pointer(cekey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var v *C.cvec
	var errmsg *C.char

	rc := C.tair_get_range(handler, C.int(area), cpkey, C.size_t(len(pkey)),
		cskey, C.size_t(len(skey)), cekey, C.size_t(len(ekey)),
		C.int(offset), C.int(limit), &v, &errmsg)
	if rc == C.int(0) || rc == 150 {
		// 150: TAIR_HAS_MORE_DATA
		slice := make([]string, 0)
		vec := unsafe.Pointer(v)
		defer C.cvec_cleanup(vec)
		for C.cvec_begin(vec); C.cvec_valid(vec) == C.int(1); C.cvec_next(vec) {
			slice = append(slice, C.GoStringN(C.cvec_cur_data(vec), C.int(C.cvec_cur_size(vec))))
		}
		return slice, nil
	} else {
		return nil, errors.New("get_range err: " + C.GoString(errmsg))
	}
}
