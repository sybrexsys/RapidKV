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
	Port:           18018,
	MaxConnections: 32,
	Password:       "test",
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

func CreateClient(Options *RapidVKClientOptions) (*Client, error) {
	if Options == nil {
		Options = &DefaultOptions
	}
	tmp := &Client{
		options: Options,
		pool:    make(chan net.Conn, Options.MaxConnections),
	}
	return tmp, nil
}

func (client *Client) Close() error {
	close(client.pool)
	for conn := range client.pool {
		conn.Close()
	}
	return nil
}

func (client *Client) openConnection() (net.Conn, error) {

	var addr = fmt.Sprintf("%s:%d", client.options.Address, client.options.Port)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return c, err
	}
	if client.options.Password != "" {
		cmd := fmt.Sprintf("AUTH %s\r\n", client.options.Password)
		_, err = client.rawSend(c, []byte(cmd))
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (client *Client) popCon() (net.Conn, error) {
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
