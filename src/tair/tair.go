package tair

/*
#include <stdlib.h>

#cgo CFLAGS: -I/usr/local/tair_http_server/c -I/usr/local/include -I/opt/tair_pkg/tblib_root/include/tbnet -I/opt/tair_pkg/tblib_root/include/tbsys
//#cgo LDFLAGS: -L/root/tair_bin/lib -L/tblib_root/lib -L/home/wliu/tair_http_server/c -ltairclientapi_c -ltbnet -ltbsys
#cgo LDFLAGS: -L/opt/tair_pkg/tair_bin/lib -L/opt/tair_pkg/tblib_root/lib -L/usr/local/tair_http_server/c -ltair_clib -ltbnet -ltbsys

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

var handlerDefaultSize int = 8
var handlerChan chan unsafe.Pointer

var Configs map[string]string

func tairLoadConf() {
	if Configs == nil {
		Configs = make(map[string]string)
	}
	Configs["master"] = "zk1:5198"
	Configs["slave"] = "zk2:5198"
	Configs["group"] = "ldb_group"
	Configs["port"] = "9999"
	Configs["set"] = "/tair_set"
	Configs["get"] = "/tair_get"
	Configs["del"] = "/tair_del"
	Configs["incr"] = "/tair_incr"
	Configs["decr"] = "/tair_decr"
	Configs["handlerSize"] = "32"
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
			return errors.New("create tair handler err")
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

	if 0 == C.tair_put(handler, C.int(area), ckey, C.size_t(len(key)),
		cval, C.size_t(len(val)), C.int(expire)) {
		return nil
	} else {
		return errors.New("put tair key " + key + " err")
	}
}

func get(area int, key string) (string, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var vlen C.size_t
	cval := C.tair_get(handler, C.int(area), ckey, C.size_t(len(key)), &vlen)
	defer C.free(unsafe.Pointer(cval))

	if unsafe.Pointer(cval) == nil || vlen == C.size_t(0) {
		return "", errors.New("get tair key " + key + " err")
	}

	return C.GoStringN(cval, C.int(vlen)), nil
}

func del(area int, key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	if 0 == C.tair_remove(handler, C.int(area), ckey, C.size_t(len(key))) {
		return nil
	} else {
		return errors.New("del tair key " + key + " err")
	}
}

func incr(area int, key string, count int, expire int) (newCount int, err error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var cNewCount C.int
	if 0 == C.tair_incr(handler, C.int(area), ckey, C.size_t(len(key)),
		C.int(count), C.int(expire), &cNewCount) {
		newCount = int(cNewCount)
	} else {
		err = errors.New("incr tair key " + key + " err")
	}
	return
}

func decr(area int, key string, count int, expire int) (newCount int, err error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	handler := <-handlerChan
	defer func(h unsafe.Pointer) { handlerChan <- h }(handler)

	var cNewCount C.int
	if 0 == C.tair_decr(handler, C.int(area), ckey, C.size_t(len(key)),
		C.int(count), C.int(expire), &cNewCount) {
		newCount = int(cNewCount)
	} else {
		err = errors.New("decr tair key " + key + " err")
	}
	return
}

func rep(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(msg)))
	w.Write([]byte(msg))
}

func Set(w http.ResponseWriter, r *http.Request) {
	var err error
	var key, val, areaStr, expStr string
	var area, expire int

	if err = r.ParseForm(); err != nil {
		rep(w, "parse form err: "+err.Error())
		return
	}

	if r.Method == "POST" || r.Method == "PUT" {
		key = r.PostFormValue("key")
		val = r.PostFormValue("val")
		expStr = r.PostFormValue("expire")
		areaStr = r.PostFormValue("area")
	} else if r.Method == "GET" {
		key = r.FormValue("key")
		val = r.FormValue("val")
		expStr = r.FormValue("expire")
		areaStr = r.FormValue("area")
	} else {
		rep(w, "only support POST,PUT,GET")
		return
	}

	if len(areaStr) == 0 {
		rep(w, "need param area")
		return
	} else if area, err = strconv.Atoi(areaStr); err != nil {
		rep(w, "need number param of area")
		return
	}

	if len(expStr) == 0 {
		expire = 0
	} else if expire, err = strconv.Atoi(expStr); err != nil {
		rep(w, "need number param of expire")
		return
	}

	if err = put(area, key, val, expire); err != nil {
		rep(w, err.Error())
		return
	}

	rep(w, "set ok")
}

func Get(w http.ResponseWriter, r *http.Request) {
	var err error
	var key, val, areaStr string
	var area int

	if err = r.ParseForm(); err != nil {
		rep(w, "parse form err: "+err.Error())
		return
	}

	if r.Method != "GET" {
		rep(w, "only get support")
		return
	} else {
		key = r.FormValue("key")
		areaStr = r.FormValue("area")
	}

	if len(areaStr) == 0 {
		rep(w, "need param area")
		return
	} else if area, err = strconv.Atoi(areaStr); err != nil {
		rep(w, "need number param of area")
		return
	}

	if val, err = get(area, key); err != nil {
		rep(w, err.Error())
	} else {
		rep(w, val)
	}
}

func Del(w http.ResponseWriter, r *http.Request) {
	var err error
	var key, areaStr string
	var area int

	if err = r.ParseForm(); err != nil {
		rep(w, "parse form err: "+err.Error())
		return
	}

	if r.Method != "GET" {
		rep(w, "only get support")
		return
	} else {
		key = r.FormValue("key")
		areaStr = r.FormValue("area")
	}

	if len(areaStr) == 0 {
		rep(w, "need param area")
		return
	} else if area, err = strconv.Atoi(areaStr); err != nil {
		rep(w, "need number param of area")
		return
	}

	if err = del(area, key); err != nil {
		rep(w, err.Error())
	} else {
		rep(w, "delete ok")
	}
}

func Incr(w http.ResponseWriter, r *http.Request) {
	var err error
	var key, val, areaStr, expStr string
	var area, expire, count, newCount int

	if err = r.ParseForm(); err != nil {
		rep(w, "parse form err: "+err.Error())
		return
	}

	if r.Method == "GET" {
		key = r.FormValue("key")
		val = r.FormValue("count")
		expStr = r.FormValue("expire")
		areaStr = r.FormValue("area")
	} else {
		rep(w, "only support GET")
		return
	}

	if len(areaStr) == 0 {
		rep(w, "need param area")
		return
	} else if area, err = strconv.Atoi(areaStr); err != nil {
		rep(w, "need number param of area")
		return
	}

	if count, err = strconv.Atoi(val); err != nil {
		rep(w, "count must be number")
		return
	}

	if len(expStr) == 0 {
		expire = 0
	} else if expire, err = strconv.Atoi(expStr); err != nil {
		rep(w, "need number param of expire")
		return
	}

	if newCount, err = incr(area, key, count, expire); err != nil {
		rep(w, err.Error())
		return
	}

	rep(w, strconv.Itoa(newCount))
}

func Decr(w http.ResponseWriter, r *http.Request) {
	var err error
	var key, val, areaStr, expStr string
	var area, expire, count, newCount int

	if err = r.ParseForm(); err != nil {
		rep(w, "parse form err: "+err.Error())
		return
	}

	if r.Method == "GET" {
		key = r.FormValue("key")
		val = r.FormValue("count")
		expStr = r.FormValue("expire")
		areaStr = r.FormValue("area")
	} else {
		rep(w, "only support GET")
		return
	}

	if len(areaStr) == 0 {
		rep(w, "need param area")
		return
	} else if area, err = strconv.Atoi(areaStr); err != nil {
		rep(w, "need number param of area")
		return
	}

	if count, err = strconv.Atoi(val); err != nil {
		rep(w, "count must be number")
		return
	}

	if len(expStr) == 0 {
		expire = 0
	} else if expire, err = strconv.Atoi(expStr); err != nil {
		rep(w, "need number param of expire")
		return
	}

	if newCount, err = decr(area, key, count, expire); err != nil {
		rep(w, err.Error())
		return
	}

	rep(w, strconv.Itoa(newCount))
}
