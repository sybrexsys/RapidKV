package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sybrexsys/RapidKV/server"
)

var notifier sync.WaitGroup
var srv *server.ServerKV

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
		fmt.Println()
		fmt.Println(sig)
		notifier.Done()
		if cfg.StartAsREST {
			telnetstop <- struct{}{}
		}
	}()
	fmt.Println("awaiting signal")
	notifier.Wait()
	fmt.Println("exiting")
	srv.Close()
}
