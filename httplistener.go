package main

import (
    "net/http"
)

var httpstop chan struct{}

/*
http.Handle("/foo", fooHandler)

http.HandleFunc("/bar", func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
})

*/
ServeHTTP(ResponseWriter, *Request)


func startHttpListener(cfg *config) {
	fmt.Println("http started")
	defer func() {
		fmt.Println("stopped http server...")
		notifier.Done()
	}()
	notifier.Add(1)
	listener, err := net.Listen("tcp", "localhost:8800")
	if err != nil {
		log.Fatal("HTTP listener was not created")
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
