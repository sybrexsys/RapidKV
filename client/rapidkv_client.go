package client

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/sybrexsys/RapidKV/datamodel"
)

type RapidKVError string

func (err RapidKVError) Error() string { return string(err) }

type RapidVKError string

type RapidVKClientOptions struct {
	Address  string
	Password string
	Port     uint16
}

var DefaultOptions = RapidVKClientOptions{
	Address:  "127.0.0.1",
	Port:     18018,
	Password: "test",
}

func (err RapidVKError) Error() string { return "RapidVKError Error: " + string(err) }

type Client struct {
	options    *RapidVKClientOptions
	password   string
	connection net.Conn
	commands   bytes.Buffer
	count      int
	pipelining bool
}

func CreateClient(Options *RapidVKClientOptions) (*Client, error) {
	if Options == nil {
		Options = &DefaultOptions
	}
	tmp := &Client{
		options: Options,
		count:   0,
	}
	tconn, err := tmp.openConnection()
	if err != nil {
		return nil, err
	}
	tmp.connection = tconn
	return tmp, nil
}

func (client *Client) Close() error {
	client.connection.Close()
	return nil
}

func (client *Client) Flush() ([]datamodel.CustomDataType, error) {
	if !client.pipelining {
		return nil, RapidKVError("not in pipelining state")
	}
	arr := make([]datamodel.CustomDataType, client.count)
	_, err := client.commands.WriteTo(client.connection)
	if err != nil {
		return nil, err
	}
	bufreader := bufio.NewReader(client.connection)
	for i := 0; i < client.count; i++ {
		answer, err := datamodel.LoadRespFromIO(bufreader, true)
		if err != nil {
			return nil, err
		}
		arr[i] = answer
	}
	client.count = 0
	client.commands.Reset()
	return arr, nil
}

func (client *Client) Pipelining(state bool) ([]datamodel.CustomDataType, error) {
	if state == client.pipelining {
		return nil, nil
	}
	if client.pipelining {
		return client.Flush()
	}
	client.pipelining = true
	return nil, nil
}

func (client *Client) openConnection() (net.Conn, error) {
	var addr = fmt.Sprintf("%s:%d", client.options.Address, client.options.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return c, err
	}
	if client.options.Password != "" {
		res, err := client.sendCommand(c, "AUTH", client.options.Password)
		if err != nil {
			c.Close()
			return nil, err
		}
		answer, ok := res.(datamodel.DataString)
		if ok && answer.IsError() {
			c.Close()
			return nil, RapidKVError(answer.Get())
		}
	}
	return c, nil
}

func (client *Client) sendCommand(connection net.Conn, cmd string, args ...string) (datamodel.CustomDataType, error) {
	if client.pipelining {
		client.commands.Write(client.prepareCommand(cmd, args...))
		client.count++
		return nil, nil
	}
	buf := client.prepareCommand(cmd, args...)
	_, err := connection.Write(buf)
	if err != nil {
		return nil, err
	}
	bufreader := bufio.NewReader(connection)
	return datamodel.LoadRespFromIO(bufreader, true)
}

func (client *Client) SendCommand(cmd string, args ...string) (datamodel.CustomDataType, error) {
	return client.sendCommand(client.connection, cmd, args...)
}

func (client *Client) prepareCommand(cmd string, args ...string) []byte {
	header := "*" + strconv.Itoa(len(args)+1) + "\r\n$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n"
	for _, s := range args {
		header = header + "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
	}
	return []byte(header)
}
