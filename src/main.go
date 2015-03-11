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

	for url, f := range tair.Urls {
		http.HandleFunc(url, f)
	}
	panic(http.ListenAndServe(":"+tair.Configs["port"], nil))
}
