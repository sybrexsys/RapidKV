package main

import (
	"fmt"
	"net/http"
)

func startHttpListener(cfg *config) {
	fmt.Println("http started")
	server := &http.Server{}
	if err := server.ListenAndServe(); err != nil {
		fmt.Println(err.Error())
		return
	}
}
