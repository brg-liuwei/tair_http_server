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
	"fmt"
	"net/http"
	"strconv"
	"unsafe"
)

var handlerDefaultSize int = 64
var handlerChan chan unsafe.Pointer

var Configs map[string]string
var Urls map[string]func(http.ResponseWriter, *http.Request)

func tairLoadConf() {
	if Configs == nil {
		Configs = make(map[string]string)
	}
	Configs["master"] = "zk1:5198"
	Configs["slave"] = "zk2:5198"
	Configs["group"] = "ldb_group"
	Configs["port"] = "9999"
	Configs["handlerSize"] = "32"

	Urls["/tair_set"] = Set
	Urls["/tair_get"] = Get
	Urls["/tair_del"] = Del
	Urls["/tair_incr"] = Incr
	Urls["/tair_decr"] = Decr
	Urls["/tair_mget"] = Mget

	// Urls["/tair_prefix_set"] = PrefixSet
	// Urls["/tair_prefix_get"] = PrefixGet
	// Urls["/tair_prefix_remove"] = PrefixRemove
	// Urls["/tair_get_range"] = GetRange
}

func Init() error {
	tairLoadConf()

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
        var cptr *C.char
	ckeys := C.calloc(C.size_t(unsafe.Sizeof(cptr)), C.size_t(len(keys)))
	defer C.free(ckeys)
        var csize_t C.size_t
	cklens := C.calloc(C.size_t(unsafe.Sizeof(csize_t)), C.size_t(len(keys)))
	defer C.free(cklens)

	for i := 0; i != len(keys); i++ {
		// TODO: create a c wrapper to handle **ptr

		entry := ckeys + unsafe.Pointer(i*int(unsafe.Sizeof(cptr)))
		*entry = C.CString(keys[i])
		defer C.free(unsafe.Pointer(*entry))

		lentry := *C.size_t(cklens + i*unsafe.Sizeof(csize_t))
		*lentry = len(keys[i])
	}

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var errmsg *C.char
	var cm *C.void

	rc := C.tair_mget(handler, area, ckeys, cklens, C.size_t(len(keys)), &cm, &errmsg)
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
		m[C.GoStringN(C.char(ckey), klen)] = C.GoStringN(C.char(cval), vlen)
	}
	return
}

/*
 * http wrapper
 */
type RestfulWrapper struct {
	w      http.ResponseWriter
	r      *http.Request
	parsed bool
}

func (this *RestfulWrapper) Init(w http.ResponseWriter, r *http.Request) {
	this.w = w
	this.r = r
	this.parsed = false
}

/*
 * @brief: this func CAN and ANY CAN appear in defer invoke
 */
func (this *RestfulWrapper) Recover() {
	if err := recover(); err != nil {
		http.Error(this.w, fmt.Sprint(err), http.StatusOK)
	}
}

func (this *RestfulWrapper) parse() {
	if err := this.r.ParseForm(); err != nil {
		panic("parse form err: " + err.Error() + "\n")
	}
}

func (this *RestfulWrapper) MethodCheck(method ...string) {
	var ok bool = false
	var support string
	for _, m := range method {
		if m == this.r.Method {
			ok = true
			break
		}
		support += m + ","
	}
	if !ok {
		panic("only support: " + support)
	}
}

func (this *RestfulWrapper) Get(key string) string {
	if !this.parsed {
		this.parse()
	}
	return this.r.FormValue(key)
}

func (this *RestfulWrapper) GetAndPanic(key string) (val string) {
	if !this.parsed {
		this.parse()
	}
	if val = this.r.FormValue(key); len(val) == 0 {
		panic("need key: " + key + "\n")
	}
	return
}

func (this *RestfulWrapper) Post(key string) string {
	if !this.parsed {
		this.parse()
	}
	return this.r.PostFormValue(key)
}

func (this *RestfulWrapper) PostAndPanic(key string) (val string) {
	if !this.parsed {
		this.parse()
	}
	if val = this.r.PostFormValue(key); len(val) == 0 {
		panic("need key: " + key + "\n")
	}
	return
}

/*
 * RESTful interface below
 */
func Set(w http.ResponseWriter, r *http.Request) {
	var rw RestfulWrapper
	rw.Init(w, r)
	defer rw.Recover()

	var key, val string
	var area, expire int
	var err error

	if r.Method == "POST" || r.Method == "PUT" {
		key = rw.PostAndPanic("key")
		val = rw.PostAndPanic("val")
		expire, _ = strconv.Atoi(rw.Post("expire"))
		if area, err = strconv.Atoi(rw.Post("area")); err != nil {
			panic("param area should be numeric string\n")
		}
	} else if r.Method == "GET" {
		key = rw.GetAndPanic("key")
		val = rw.GetAndPanic("val")
		expire, _ = strconv.Atoi(rw.Get("expire"))
		if area, err = strconv.Atoi(rw.Get("area")); err != nil {
			panic("param area should be numeric string\n")
		}
	} else {
		panic("only support POST,PUT,GET\n")
	}

	if err = put(area, key, val, expire); err != nil {
		http.Error(w, err.Error()+"\n", http.StatusOK)
	}

	http.Error(w, "set ok\n", http.StatusOK)
}

func Get(w http.ResponseWriter, r *http.Request) {
	var rw RestfulWrapper
	rw.Init(w, r)
	defer rw.Recover()
	rw.MethodCheck("GET")

	var err error
	var key, val string
	var area int

	key = rw.GetAndPanic("key")
	if area, err = strconv.Atoi(rw.Get("area")); err != nil {
		panic("param area should be numeric string\n")
	}

	if val, err = get(area, key); err != nil {
		http.Error(w, err.Error()+"\n", http.StatusOK)
	} else {
		http.Error(w, val, http.StatusOK)
	}
}

func Del(w http.ResponseWriter, r *http.Request) {
	var rw RestfulWrapper
	rw.Init(w, r)
	defer rw.Recover()
	rw.MethodCheck("GET")

	var err error
	var key string
	var area int

	key = rw.GetAndPanic("key")
	if area, err = strconv.Atoi(rw.Get("area")); err != nil {
		panic("param area should be numeric string\n")
	}

	if err = del(area, key); err != nil {
		http.Error(w, err.Error()+"\n", http.StatusOK)
	} else {
		http.Error(w, "delete ok\n", http.StatusOK)
	}
}

type counterFunc func(area int, key string, count int, expire int) (newCount int, err error)

func counter(w http.ResponseWriter, r *http.Request, f counterFunc) {
	var rw RestfulWrapper
	rw.Init(w, r)
	defer rw.Recover()
	rw.MethodCheck("GET")

	var err error
	var key, val string
	var area, expire, count, newCount int

	key = rw.GetAndPanic("key")
	val = rw.GetAndPanic("count")
	expire, _ = strconv.Atoi(rw.Get("expire"))

	if area, err = strconv.Atoi(rw.Get("area")); err != nil {
		panic("param area should be numeric string\n")
	}
	if count, err = strconv.Atoi(val); err != nil {
		panic("count must be number\n")
	}

	if newCount, err = f(area, key, count, expire); err != nil {
		http.Error(w, err.Error()+"\n", http.StatusOK)
	} else {
		http.Error(w, strconv.Itoa(newCount), http.StatusOK)
	}
}

func Incr(w http.ResponseWriter, r *http.Request) {
	counter(w, r, incr)
}

func Decr(w http.ResponseWriter, r *http.Request) {
	counter(w, r, decr)
}

/* A simple restful interface of mget */
func Mget(w http.ResponseWriter, r *http.Request) {
	var rw RestfulWrapper
	rw.Init(w, r)
	defer rw.Recover()
	rw.MethodCheck("GET")

	var err error
	var keys []string
	var vals string
	var area int

	keys = rw.GetAndPanic("keys").Split(",")
	if area, err = rw.Get("area"); err != nil {
		panic("param area should be numeric string\n")
	}

	if m, err := mget(area, keys); err != nil {
		http.Error(w, err.Error()+"\n", http.StatusOK)
	} else {
		var s string
		for k, v := range m {
			s += fmt.Sprintf("(%s,%s)\n", k, v)
		}
		http.Error(w, s, http.StatusOK)
	}
}
