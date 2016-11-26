package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sybrexsys/RapidKV/server"
)

var notifier sync.WaitGroup
var srv *server.ServerKV

var con net.Conn

func main() {
	sigs := make(chan os.Signal, 1)
	telnetstop = make(chan struct{}, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	notifier.Add(1)
	cfg, err := loadConfig()
	if err != nil {
		cfg = &defConfig
	}
	srv = server.CreateServer()
	if cfg.StartAsREST {
		go startHttpListener(cfg)
	} else {
		go startTelnetListener(cfg)
	}

	go func() {
		sig := <-sigs
		fmt.Println("Stop signal was received")
		fmt.Println(sig)
		notifier.Done()
		if cfg.StartAsREST {

		} else {
			fmt.Println("Stop signal was sent to RESP server")
			telnetstop <- struct{}{}
		}
	}()
	fmt.Println("awaiting signal")
	notifier.Wait()
	fmt.Println("exiting")
	srv.Close()
}
