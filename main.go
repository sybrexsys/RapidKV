package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/sybrexsys/RapidKV/server"
)

var (
	notifier    sync.WaitGroup
	srv         *server.ServerKV
	tcplistener *net.TCPListener
	quit        chan struct{}
)

func main() {
	sigs := make(chan os.Signal, 1)
	quit = make(chan struct{}, 1)

	cfg, err := loadConfig()
	if err != nil {
		cfg = &defConfig
	}
	srv = server.CreateServer()

	listener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(cfg.Port))
	if err != nil {
		log.Fatal("Listener was not created")
		return
	}
	var ok bool
	tcplistener, ok = listener.(*net.TCPListener)
	if !ok {
		log.Fatal("Invalid listener was created for server")
	}
	if cfg.StartAsREST {
		fmt.Println("Started sa REST/HTTP server on port " + strconv.Itoa(cfg.Port))
		go startHttpListener()
	} else {
		fmt.Println("Started sa RESP server on port " + strconv.Itoa(cfg.Port))
		go startRESPListener()
	}

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	notifier.Add(1)
	go func() {
		sig := <-sigs
		fmt.Println("Stop signal was received")
		fmt.Println(sig)
		notifier.Done()
		listener.Close()
		close(quit)
	}()
	notifier.Wait()
	fmt.Println("exiting")
	srv.Close()
}
