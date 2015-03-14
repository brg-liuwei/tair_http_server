package tair

import (
	"fmt"
	"net/http"
)

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
