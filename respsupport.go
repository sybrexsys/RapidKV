package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"

	"time"

	"strconv"

	"github.com/sybrexsys/RapidKV/datamodel"
)

var telnetstop chan struct{}

type clientConnection struct {
	answers         []datamodel.CustomDataType
	answersSize     int
	needQuit        bool
	currentDatabase *Database
	authorized      bool
	inMulti         bool
	queue           datamodel.DataArray
	writer          *bufio.Writer
	reader          *bufio.Reader
}

func (cc *clientConnection) setCapacity(newCapacity int) {
	tmp := make([]datamodel.CustomDataType, newCapacity)
	copy(tmp, cc.answers)
	cc.answers = tmp
}

func (cc *clientConnection) grow() {
	var Delta int
	Cap := len(cc.answers)
	if Cap > 64 {
		Delta = Cap / 4
	} else {
		if Cap > 8 {
			Delta = 16
		} else {
			Delta = 4
		}
	}
	cc.setCapacity(Cap + Delta)
}

func (cc *clientConnection) pushAnswer(answer datamodel.CustomDataType) error {
	_, err := cc.writer.Write(datamodel.ConvertToRASP(answer))
	if cc.reader.Buffered() == 0 {
		cc.writer.Flush()
	}
	return err
}

func (cc *clientConnection) processOneRESPCommandWithoutLock(command datamodel.DataArray) datamodel.CustomDataType {
	comName := command.Get(0).(datamodel.DataString).Get()
	f, ok := commandList[strings.ToLower(comName)]
	if !ok {
		return datamodel.CreateError("ERR Unknown command")
	}
	if needAuth && !cc.authorized {
		return datamodel.CreateError("ERR Not authorized")
	}

	if f == nil {
		return datamodel.CreateError("ERR Command not implemented")
	}
	if command.Count() < 2 {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	command.Remove(0)
	command.Remove(0)
	return f(cc.currentDatabase, key, command)
}

func (cc *clientConnection) processTransaction() datamodel.CustomDataType {
	cnt := cc.queue.Count()
	answers := datamodel.CreateArray(cnt)
	cc.currentDatabase.Lock()
	defer func() {
		cc.inMulti = false
		cc.queue = datamodel.CreateArray(10)
		cc.currentDatabase.Unlock()
	}()
	for i := 0; i < cnt; i++ {
		answers.Add(cc.processOneRESPCommandWithoutLock(cc.queue.Get(i).(datamodel.DataArray)))
	}
	return answers
}

func (cc *clientConnection) processOneRESPCommand(command datamodel.CustomDataType) datamodel.CustomDataType {
	arr, ok := command.(datamodel.DataArray)
	if !ok {
		return datamodel.CreateError("ERR Invalid command")
	}
	if arr.Count() < 1 {
		return datamodel.CreateError("ERR Invalid command")
	}
	comdat := arr.Get(0)
	str, okstr := comdat.(datamodel.DataString)
	if !okstr {
		return datamodel.CreateError("ERR Invalid command")
	}
	commandName := strings.ToLower(str.Get())

	concom, isConnectionCommand := connectionCommands[commandName]
	if isConnectionCommand {
		if concom.needAuth && !cc.authorized {
			return datamodel.CreateError("ERR Not authorized")
		}
		return concom.function(cc, arr)
	}
	if commandName == "multi" {
		cc.queue = datamodel.CreateArray(10)
		cc.inMulti = true
		return datamodel.CreateSimpleString("OK")
	}
	if commandName == "discard" {
		cc.inMulti = false
		cc.queue = datamodel.CreateArray(10)
		return datamodel.CreateSimpleString("OK")
	}
	if commandName == "exec" {
		return cc.processTransaction()
	}
	if cc.inMulti {
		cc.queue.Add(command)
		return datamodel.CreateSimpleString("QUEUED")
	}
	cc.currentDatabase.RLock()
	defer cc.currentDatabase.RUnlock()
	return cc.processOneRESPCommandWithoutLock(arr)
}

func checkEOF(reader *bufio.Reader) (bool, error) {
	_, err := reader.ReadByte()
	if err == nil {
		reader.UnreadByte()
		return false, nil
	}
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func saveOneDataToOut(answer datamodel.CustomDataType, writer *bufio.Writer) error {
	_, err := writer.Write(datamodel.ConvertToRASP(answer))
	return err
}

func processRESPConnection(c net.Conn) {
	var request datamodel.CustomDataType
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			fmt.Printf("client run panic %s:%v\r\nLast command to server was:%s\r\n", buf, e, datamodel.DataObjectToString(request))
		}
		c.Close()
		notifier.Done()
	}()
	fmt.Printf("New client connection detected. Remote address: %s\r\n", c.RemoteAddr().String())
	notifier.Add(1)

	cc := &clientConnection{
		answers:         make([]datamodel.CustomDataType, 100),
		answersSize:     0,
		currentDatabase: firstDatabase,
		authorized:      !needAuth,
		writer:          bufio.NewWriter(c),
		reader:          bufio.NewReader(c),
	}
	go func() {
		<-quit
		c.SetReadDeadline(time.Now().Add(0))
	}()
	for {
		request, err := datamodel.LoadRespFromIO(cc.reader, true)
		if err != nil {
			parseError, ok := err.(datamodel.ParseError)
			if !ok {
				fmt.Printf("Client connection lost.\r\n")
				return
			}
			answer := datamodel.CreateError(parseError.Error())
			if cc.pushAnswer(answer) != nil {
				return
			}
			continue
		}
		answer := cc.processOneRESPCommand(request)
		if cc.pushAnswer(answer) != nil {
			return
		}
		if cc.needQuit {
			cc.writer.Flush()
			break
		}
	}
}

func startRESPListener() {
	defer func() {
		fmt.Println("stopped RESP server...")
		notifier.Done()
	}()
	notifier.Add(1)

mainloop:
	for {
		conn, err := tcplistener.Accept()
		select {
		case <-quit:
			fmt.Println("listener stop signal was received")
			break mainloop
		default:
		}
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		go processRESPConnection(conn)
	}
}

func authCommand(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType {
	if needAuth {
		item := command.Get(1)
		str, ok := item.(datamodel.DataString)
		if !ok {
			datamodel.CreateError("ERR Invalid parameter")
		}
		cc.authorized = str.Get() == cfg.AuthPass
		if !cc.authorized {
			return datamodel.CreateError("ERR Invalid password")
		}
	}
	return datamodel.CreateSimpleString("OK")
}

func quitCommand(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType {
	cc.needQuit = true
	return datamodel.CreateSimpleString("OK")
}

func selectCommand(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType {
	idx, err := getInt(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Invalid parameter")
	}
	cc.currentDatabase = getDataBase(idx)
	return datamodel.CreateSimpleString("OK")
}

func echoCommand(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Invalid parameter")
	}
	return datamodel.CreateString(key)

}

func pingCommand(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType {
	item := command.Get(1)
	switch value := item.(type) {
	case datamodel.DataNull:
		return datamodel.CreateSimpleString("PONG")
	case datamodel.DataString:
		return datamodel.CreateString(value.Get())
	case datamodel.DataInt:
		return datamodel.CreateString(strconv.Itoa(value.Get()))
	default:
		return datamodel.CreateError("ERR Invalid parameter")
	}
}

type connectionCommand struct {
	needAuth bool
	function func(cc *clientConnection, command datamodel.DataArray) datamodel.CustomDataType
}

var connectionCommands = map[string]connectionCommand{
	"auth": {
		needAuth: false,
		function: authCommand,
	},
	"select": {
		needAuth: true,
		function: selectCommand,
	},
	"echo": {
		needAuth: false,
		function: echoCommand,
	},
	"ping": {
		needAuth: false,
		function: pingCommand,
	},
	"quit": {
		needAuth: false,
		function: quitCommand,
	},
}
