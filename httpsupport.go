package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
)

func newClientHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			log.Fatalf("client run panic %s:%v", buf, e)
		}
		notifier.Done()
	}()
	notifier.Add(1)
	w.Write([]byte("Test Passed"))
}

func startHttpListener() {
	defer func() {
		fmt.Println("stopped RESP server...")
		notifier.Done()
	}()
	notifier.Add(1)
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		newClientHTTP(w, r)
	})

	svr := http.Server{Handler: mux}
	svr.Serve(tcplistener)
}
