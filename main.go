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
)

var (
	notifier      sync.WaitGroup
	dbProtect     sync.RWMutex
	databases     map[int]*Database
	firstDatabase *Database
	tcplistener   *net.TCPListener
	quit          chan struct{}
	needAuth      bool
	cfg           *config
)

func getDataBase(index int) *Database {
	dbProtect.RLock()
	db, ok := databases[index]
	if ok {
		dbProtect.RUnlock()
		return db
	}
	dbProtect.RUnlock()
	dbProtect.Lock()
	defer dbProtect.Unlock()
	db, ok = databases[index]
	if ok {
		dbProtect.RUnlock()
		return db
	}
	tmp := CreateDatabase(cfg.ShardCount)
	databases[index] = tmp
	return tmp
}

func main() {
	sigs := make(chan os.Signal, 1)
	quit = make(chan struct{}, 1)
	var err error
	cfg, err = loadConfig()
	if err != nil {
		cfg = &defConfig
	}
	databases = make(map[int]*Database)
	firstDatabase = CreateDatabase(cfg.ShardCount)
	databases[0] = firstDatabase
	needAuth = cfg.AuthPass != ""

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
	for _, db := range databases {
		db.Close()
	}
}
