package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
)

type RapidVKError string

type RapidVKClientOptions struct {
	Address        string
	Password       string
	Port           uint16
	MaxConnections uint16
}

var DefaultOptions = RapidVKClientOptions{
	Address:        "127.0.0.1",
	Port:           45332,
	MaxConnections: 32,
	Password:       "",
}

func (err RapidVKError) Error() string { return "RapidVKError Error: " + string(err) }

type Client struct {
	options  *RapidVKClientOptions
	db       int
	password string
	//the connection pool
	sync.Mutex
	pool chan net.Conn
}

func CreateClient(DBIdx int, Options *RapidVKClientOptions) (*Client, error) {
	if Options == nil {
		Options = &DefaultOptions
	}
	tmp := &Client{
		options: Options,
		db:      DBIdx,
		pool:    make(chan net.Conn, Options.MaxConnections),
	}
	for i := uint16(0); i < Options.MaxConnections; i++ {
		tmp.pool <- nil
	}
	return tmp, nil
}

func (client *Client) Close() error {
	close(client.pool)
	for conn := range client.pool {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}

func (client *Client) openConnection() (net.Conn, error) {

	var addr = fmt.Sprintf("%s:%d", client.options.Address, client.options.Port)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return c, err
	}

	//handle authentication here authored by @shxsun
	if client.options.Password != "" {
		cmd := fmt.Sprintf("AUTH %s\r\n", client.options.Password)
		_, err = client.rawSend(c, []byte(cmd))
		if err != nil {
			return nil, err
		}
	}

	if client.db != 0 {
		cmd := fmt.Sprintf("SELECT %d\r\n", client.db)
		_, err = client.rawSend(c, []byte(cmd))
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (client *Client) popCon() (net.Conn, error) {
	// grab a connection from the pool
	for {
		select {
		case con := <-client.pool:
			return con, nil
		default:
			client.Lock()
			if len(client.pool) < int(client.options.MaxConnections) {
				return client.openConnection()
			}
			client.Unlock()
			runtime.Gosched()
		}
	}
}

func (client *Client) pushCon(c net.Conn) {
	client.pool <- c
}

func readResponse(reader *bufio.Reader) (interface{}, error) {

	var line string
	var err error

	//read until the first non-whitespace line
	for {
		line, err = reader.ReadString('\n')
		if len(line) == 0 || err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			break
		}
	}

	if line[0] == '+' {
		return strings.TrimSpace(line[1:]), nil
	}

	if strings.HasPrefix(line, "-ERR ") {
		errmesg := strings.TrimSpace(line[5:])
		return nil, RapidVKError(errmesg)
	}

	return "", nil

}

func (client *Client) rawSend(c net.Conn, cmd []byte) (interface{}, error) {
	_, err := c.Write(cmd)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(c)

	data, err := readResponse(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (client *Client) sendCommand(cmd string, args ...string) (data interface{}, err error) {
	// grab a connection from the pool
	var b []byte
	c, err := client.popCon()
	if err != nil {
		return nil, err
	}
	defer client.pushCon(c)

	b = commandBytes(cmd, args...)
	data, err = client.rawSend(c, b)
	if err == io.EOF {
		c, err = client.openConnection()
		if err != nil {
			return nil, err
		}
		data, err = client.rawSend(c, b)
	}
	return data, err
}

func commandBytes(cmd string, args ...string) []byte {
	var cmdbuf bytes.Buffer
	fmt.Fprintf(&cmdbuf, "*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(cmd), cmd)
	for _, s := range args {
		fmt.Fprintf(&cmdbuf, "$%d\r\n%s\r\n", len(s), s)
	}
	return cmdbuf.Bytes()
}
