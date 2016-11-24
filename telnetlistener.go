package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

var telnetstop chan struct{}

func handleConn(c net.Conn) {
	defer c.Close()
	for {
		_, err := io.WriteString(c, time.Now().Format("15:04:05\n"))
		if err != nil {
			return // Например, отключение клиента
		}
		time.Sleep(1 * time.Second)
	}
}

func startTelnetListener(cfg *config) {
	fmt.Println("telnet started")
	defer func() {
		fmt.Println("stopped telnet server...")
		notifier.Done()
	}()
	notifier.Add(1)
	listener, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatal("Telnet listener was not created")
		return
	}
	ls, ok := listener.(*net.TCPListener)
	if !ok {
		log.Fatal("invalid listener was created for telnet connection")
	}
mainloop:
	for {
		ls.SetDeadline(time.Now().Add(time.Millisecond * 200))
		conn, err := ls.Accept()
		select {
		case <-telnetstop:
			fmt.Println("stop signal was received")
			break mainloop
		default:
		}
		if err != nil {
			netErr, ok := err.(net.Error)
			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			} else {
				fmt.Println(err.Error())
				return
			}
		}
		go handleConn(conn)
	}
}
