package main

import (
	"tair"

	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
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
