package main

import (
	"tair"

	"net/http"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err := tair.Init(); err != nil {
		panic(err)
	}
	http.HandleFunc(tair.Configs["set"], tair.Set)
	http.HandleFunc(tair.Configs["get"], tair.Get)
	http.HandleFunc(tair.Configs["del"], tair.Del)
	http.HandleFunc(tair.Configs["incr"], tair.Incr)
	http.HandleFunc(tair.Configs["decr"], tair.Decr)
	panic(http.ListenAndServe(":"+tair.Configs["port"], nil))
}
