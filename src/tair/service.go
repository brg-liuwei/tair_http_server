package tair

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var Configs map[string]string
var Urls map[string]func(http.ResponseWriter, *http.Request)

func tairLoadConf() {
	if Configs == nil {
		Configs = make(map[string]string)
	}
	/* TODO: read conf from file */
	Configs["master"] = "zk1:5198"
	Configs["slave"] = "zk2:5198"
	Configs["group"] = "ldb_group"
	Configs["port"] = "9999"
	Configs["handlerSize"] = "32"

	if Urls == nil {
		Urls = make(map[string]func(http.ResponseWriter, *http.Request))
	}
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
	var area int

	keys = strings.Split(rw.GetAndPanic("keys"), ",")
	if len(keys) == 0 {
		panic("need param keys separated by ','")
	}
	if area, err = strconv.Atoi(rw.Get("area")); err != nil {
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
